#!/bin/bash
# Deployment script for the Loggr system

set -e

# Load environment variables
if [ -f .env ]; then
    echo "Loading environment variables from .env"
    export $(grep -v '^#' .env | xargs)
else
    echo "No .env file found, using default values"
fi

# Set default values if not provided
AWS_REGION=${AWS_REGION:-"us-east-1"}
S3_BUCKET_NAME=${S3_BUCKET_NAME:-"loggr-logs"}
DYNAMODB_TABLE_NAME=${DYNAMODB_TABLE_NAME:-"loggr-logs"}
SNS_TOPIC_NAME=${SNS_TOPIC_NAME:-"loggr-alerts"}
LAMBDA_FUNCTION_NAME=${LAMBDA_FUNCTION_NAME:-"loggr-alert-handler"}

echo "=== Loggr Deployment ==="
echo "AWS Region: $AWS_REGION"
echo "S3 Bucket: $S3_BUCKET_NAME"
echo "DynamoDB Table: $DYNAMODB_TABLE_NAME"
echo "SNS Topic: $SNS_TOPIC_NAME"
echo "Lambda Function: $LAMBDA_FUNCTION_NAME"
echo

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install it first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install it first."
    exit 1
fi

# Create a temporary directory for deployment
TEMP_DIR=$(mktemp -d)
echo "Created temporary directory: $TEMP_DIR"

# Function to clean up temporary files
cleanup() {
    echo "Cleaning up temporary files"
    rm -rf "$TEMP_DIR"
}

# Register the cleanup function to be called on exit
trap cleanup EXIT

# Step 1: Start Kafka
echo "=== Step 1: Starting Kafka ==="
cd kafka
docker-compose down || true
docker-compose up -d
cd ..
echo "Kafka started successfully"
echo

# Step 2: Create S3 bucket
echo "=== Step 2: Creating S3 bucket ==="
if aws s3api head-bucket --bucket "$S3_BUCKET_NAME" 2>/dev/null; then
    echo "S3 bucket $S3_BUCKET_NAME already exists"
else
    echo "Creating S3 bucket $S3_BUCKET_NAME"
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$S3_BUCKET_NAME" --region "$AWS_REGION"
    else
        aws s3api create-bucket --bucket "$S3_BUCKET_NAME" --region "$AWS_REGION" --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
    
    # Configure lifecycle policy
    echo "Configuring lifecycle policy for S3 bucket"
    cat > "$TEMP_DIR/lifecycle-policy.json" << EOF
{
    "Rules": [
        {
            "ID": "ExpireLogs",
            "Status": "Enabled",
            "Prefix": "logs",
            "Expiration": {
                "Days": 90
            }
        }
    ]
}
EOF
    aws s3api put-bucket-lifecycle-configuration --bucket "$S3_BUCKET_NAME" --lifecycle-configuration file://"$TEMP_DIR/lifecycle-policy.json"
fi
echo "S3 bucket setup completed"
echo

# Step 3: Create DynamoDB table
echo "=== Step 3: Creating DynamoDB table ==="
if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE_NAME" --region "$AWS_REGION" 2>/dev/null; then
    echo "DynamoDB table $DYNAMODB_TABLE_NAME already exists"
else
    echo "Creating DynamoDB table $DYNAMODB_TABLE_NAME"
    aws dynamodb create-table \
        --table-name "$DYNAMODB_TABLE_NAME" \
        --attribute-definitions \
            AttributeName=service,AttributeType=S \
            AttributeName=timestamp,AttributeType=S \
            AttributeName=log_level,AttributeType=S \
        --key-schema \
            AttributeName=service,KeyType=HASH \
            AttributeName=timestamp,KeyType=RANGE \
        --global-secondary-indexes \
            "IndexName=LogLevelIndex,KeySchema=[{AttributeName=log_level,KeyType=HASH},{AttributeName=timestamp,KeyType=RANGE}],Projection={ProjectionType=ALL},ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}" \
        --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
        --region "$AWS_REGION"
        
    echo "Waiting for DynamoDB table to become active"
    aws dynamodb wait table-exists --table-name "$DYNAMODB_TABLE_NAME" --region "$AWS_REGION"
fi
echo "DynamoDB table setup completed"
echo

# Step 4: Create SNS topic
echo "=== Step 4: Creating SNS topic ==="
SNS_TOPIC_ARN=$(aws sns create-topic --name "$SNS_TOPIC_NAME" --region "$AWS_REGION" --output text --query 'TopicArn')
echo "SNS topic ARN: $SNS_TOPIC_ARN"
echo "SNS topic setup completed"
echo

# Step 5: Create Lambda function
echo "=== Step 5: Creating Lambda function ==="
# Create a deployment package
echo "Creating Lambda deployment package"
mkdir -p "$TEMP_DIR/lambda"
cp alerts/lambda_handler.py "$TEMP_DIR/lambda/"
cd "$TEMP_DIR/lambda"
zip -r ../lambda_function.zip .
cd -

# Create IAM role for Lambda
echo "Creating IAM role for Lambda"
ROLE_NAME="loggr-lambda-role"
POLICY_NAME="loggr-lambda-policy"

# Check if the role already exists
if aws iam get-role --role-name "$ROLE_NAME" 2>/dev/null; then
    echo "IAM role $ROLE_NAME already exists"
    ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)
else
    # Create the role
    cat > "$TEMP_DIR/trust-policy.json" << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

    ROLE_ARN=$(aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document file://"$TEMP_DIR/trust-policy.json" --query 'Role.Arn' --output text)
    
    # Attach policies
    aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    
    # Create custom policy
    cat > "$TEMP_DIR/lambda-policy.json" << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:Query",
                "dynamodb:Scan"
            ],
            "Resource": "arn:aws:dynamodb:$AWS_REGION:*:table/$DYNAMODB_TABLE_NAME"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sns:Publish",
                "sns:Subscribe",
                "sns:ListSubscriptionsByTopic"
            ],
            "Resource": "$SNS_TOPIC_ARN"
        }
    ]
}
EOF

    aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" --policy-document file://"$TEMP_DIR/lambda-policy.json"
    
    # Wait for the role to be available
    echo "Waiting for IAM role to become available"
    sleep 10
fi

# Create or update the Lambda function
if aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" 2>/dev/null; then
    echo "Updating Lambda function $LAMBDA_FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --zip-file fileb://"$TEMP_DIR/lambda_function.zip" \
        --region "$AWS_REGION"
else
    echo "Creating Lambda function $LAMBDA_FUNCTION_NAME"
    aws lambda create-function \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --runtime python3.9 \
        --handler lambda_handler.lambda_handler \
        --role "$ROLE_ARN" \
        --zip-file fileb://"$TEMP_DIR/lambda_function.zip" \
        --environment "Variables={ALERTS_TABLE_NAME=$DYNAMODB_TABLE_NAME,NOTIFICATION_TOPIC_ARN=$SNS_TOPIC_ARN}" \
        --region "$AWS_REGION"
fi

# Subscribe the Lambda function to the SNS topic
echo "Subscribing Lambda function to SNS topic"
aws sns subscribe \
    --topic-arn "$SNS_TOPIC_ARN" \
    --protocol lambda \
    --notification-endpoint $(aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" --query 'Configuration.FunctionArn' --output text) \
    --region "$AWS_REGION"

# Add permission for SNS to invoke Lambda
echo "Adding permission for SNS to invoke Lambda"
aws lambda add-permission \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --statement-id sns-invoke \
    --action lambda:InvokeFunction \
    --principal sns.amazonaws.com \
    --source-arn "$SNS_TOPIC_ARN" \
    --region "$AWS_REGION" || true  # Ignore error if permission already exists

echo "Lambda function setup completed"
echo

# Step 6: Update configuration
echo "=== Step 6: Updating configuration ==="
# Update .env file with the created resources
if [ -f .env ]; then
    # Backup the existing .env file
    cp .env .env.bak
    echo "Backed up existing .env file to .env.bak"
fi

cat > .env << EOF
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_PREFIX=loggr
KAFKA_CONSUMER_GROUP=loggr-consumer-group

# AWS Configuration
AWS_REGION=$AWS_REGION
# AWS_ACCESS_KEY_ID=your_access_key_id
# AWS_SECRET_ACCESS_KEY=your_secret_access_key

# S3 Configuration
S3_BUCKET_NAME=$S3_BUCKET_NAME
S3_PREFIX=logs
S3_RETENTION_DAYS=90

# DynamoDB Configuration
DYNAMODB_TABLE_NAME=$DYNAMODB_TABLE_NAME
DYNAMODB_READ_CAPACITY_UNITS=5
DYNAMODB_WRITE_CAPACITY_UNITS=5

# SNS Configuration
SNS_TOPIC_ARN=$SNS_TOPIC_ARN
SNS_TOPIC_NAME=$SNS_TOPIC_NAME

# Flink Configuration
FLINK_PARALLELISM=2
FLINK_CHECKPOINT_INTERVAL=60000
FLINK_STATE_BACKEND=filesystem
FLINK_CHECKPOINT_DIR=/tmp/flink-checkpoints

# Anomaly Detection Configuration
ANOMALY_ERROR_RATE_THRESHOLD=0.1
ANOMALY_WINDOW_SIZE_MINUTES=5
ANOMALY_MIN_LOG_COUNT=100
ANOMALY_SENSITIVITY=3.0
EOF

echo "Updated .env file with deployed resources"
echo

# Step 7: Final instructions
echo "=== Deployment Completed Successfully ==="
echo
echo "Loggr system has been deployed with the following components:"
echo "- Kafka: Running locally with Docker Compose"
echo "- S3 Bucket: $S3_BUCKET_NAME"
echo "- DynamoDB Table: $DYNAMODB_TABLE_NAME"
echo "- SNS Topic: $SNS_TOPIC_ARN"
echo "- Lambda Function: $LAMBDA_FUNCTION_NAME"
echo
echo "Next steps:"
echo "1. Subscribe your email to the SNS topic for alerts:"
echo "   python -m alerts.sns_publisher --subscribe-email your.email@example.com"
echo
echo "2. Start the log producer to simulate logs:"
echo "   python -m kafka.producer --service auth-service --rate 10"
echo
echo "3. Start the log consumer to view logs:"
echo "   python -m kafka.consumer"
echo
echo "4. Run the Flink job for real-time processing:"
echo "   python -m flink.flink_job"
echo
echo "5. Query logs from DynamoDB:"
echo "   python -m storage.dynamodb --service auth-service --log-level ERROR"
echo
echo "6. List logs in S3:"
echo "   python -m storage.s3_uploader --list --service auth-service"
echo
echo "Enjoy using Loggr!" 