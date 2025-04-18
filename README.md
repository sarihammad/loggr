# Loggr

A real-time log processing system that can ingest, process, store, and analyze logs from distributed microservices while detecting anomalies. The system is fault-tolerant, highly available, and scalable using Kafka, Apache Flink, AWS S3, and DynamoDB.

## System Architecture

### Main Components

1. Log Producers (Microservices emitting logs)
2. Kafka Cluster (Streaming log ingestion)
3. Apache Flink (Real-time log processing & anomaly detection)
4. AWS S3 / DynamoDB (Long-term storage & historical analysis)
5. Alert System (AWS SNS & Lambda)

### Data Flow

1. Logs generated by microservices (REST API, applications, etc.)
2. Logs are published to Kafka topics
3. Apache Flink processes logs in real-time:
   - Detects anomalies (e.g., unusual spikes in errors)
   - Aggregates logs based on service, severity, and timestamps
4. Processed logs are stored in DynamoDB & S3 for querying
5. If an anomaly is detected, AWS SNS triggers notifications

## Setup Instructions

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- AWS CLI configured with appropriate permissions
- Apache Flink (local or cluster setup)

### Installation

1. Clone the repository:

```
git clone https://github.com/sarihammad/loggr.git
cd loggr
```

2. Install dependencies:

```
pip install -r requirements.txt
```

3. Start Kafka using Docker Compose:

```
cd kafka
docker-compose up -d
```

4. Configure AWS credentials:

```
aws configure
```

5. Deploy the system:

```
./deploy.sh
```

## Usage

### Simulating Log Production

```
python kafka/producer.py --service auth-service --rate 100
```

### Viewing Processed Logs

```
python storage/query_logs.py --service auth-service --start-time "2023-03-06T12:00:00Z"
```

## Project Structure

- `kafka/`: Kafka producers, consumers, and configuration
- `flink/`: Apache Flink jobs for real-time processing
- `storage/`: DynamoDB and S3 integration
- `alerts/`: SNS and Lambda functions for alerting
- `config/`: Configuration settings
