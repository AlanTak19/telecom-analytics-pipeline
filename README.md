# 🚀 Telecom Analytics Real-time Pipeline

> **End-to-End Real-time Analytics Pipeline** for processing telecom events using Kafka, Spark, Airflow, and PostgreSQL.

## 📋 Project Overview

This project implements a complete real-time analytics pipeline that:
- ✅ Generates realistic telecom events (calls, SMS, data sessions, balance recharges)
- ✅ Streams events through Apache Kafka
- ✅ Processes data in real-time using Spark Structured Streaming
- ✅ Performs daily batch aggregations
- ✅ Orchestrates workflows with Apache Airflow
- ✅ Stores results in PostgreSQL with proper indexing and partitioning

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌────────────────┐     ┌──────────────┐
│   Event     │────▶│  Kafka   │────▶│ Spark Streaming│────▶│ PostgreSQL   │
│  Generator  │     │  Broker  │     │   (Real-time)  │     │   Database   │
└─────────────┘     └──────────┘     └────────────────┘     └──────────────┘
                                             │
                                             ▼
                                      ┌────────────────┐
                                      │  Spark Batch   │
                                      │  (Daily Jobs)  │
                                      └────────────────┘
                                             │
                                             ▼
                                      ┌────────────────┐
                                      │    Airflow     │
                                      │ (Orchestration)│
                                      └────────────────┘
```

## 🛠️ Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka | 7.5.0 |
| Stream Processing | Apache Spark | 3.5.0 |
| Batch Processing | Apache Spark | 3.5.0 |
| Orchestration | Apache Airflow | 2.7.3 |
| Database | PostgreSQL | 15 |
| Containerization | Docker | 20.10+ |
| Language | Python | 3.11 |

## 📁 Project Structure

```
telecom-analytics-pipeline/
├── docker-compose.yml          # Infrastructure setup
├── .env                        # Configuration (DO NOT COMMIT)
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Python dependencies
├── README.md                   # This file
│
├── src/                        # Source code
│   ├── producer/               # Event generator
│   │   └── event_generator.py
│   ├── streaming/              # Spark streaming jobs
│   │   └── spark_streaming_job.py
│   ├── batch/                  # Spark batch jobs
│   │   └── spark_batch_job.py
│   └── utils/                  # Utilities
│       ├── __init__.py
│       └── config.py
│
├── airflow/                    # Airflow configuration
│   ├── dags/                   # DAG definitions
│   │   └── daily_batch_dag.py
│   ├── plugins/                # Custom plugins
│   └── logs/                   # Airflow logs
│
├── sql/                        # SQL scripts
│   └── init_db.sql             # Database initialization
│
├── data/                       # Data directories (volumes)
│   ├── kafka/                  # Kafka data
│   ├── postgres/               # PostgreSQL data
│   └── airflow/                # Airflow data
│
└── logs/                       # Application logs
```

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** installed and running
- **Python 3.9+** installed
- **16GB RAM** (recommended)
- **20GB free disk space**

### Step 1: Clone and Setup

```powershell
# Clone the repository (or create project manually)
cd C:\Users\YourName\Documents
mkdir telecom-analytics-pipeline
cd telecom-analytics-pipeline

# Copy all files from this project
```

### Step 2: Start Infrastructure

```powershell
# Make sure Docker Desktop is running

# Start all services
docker-compose up -d

# Check services status
docker-compose ps

# View logs
docker-compose logs -f
```

### Step 3: Verify Services

```powershell
# Kafka (should return empty topic list)
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# PostgreSQL
docker exec -it postgres psql -U telecom_user -d telecom_analytics -c "\dt"

# Airflow Web UI
# Open browser: http://localhost:8080
# Login: admin / admin123

# Spark Master UI
# Open browser: http://localhost:8081
```

### Step 4: Install Python Dependencies

```powershell
# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

## 📊 Database Schema

### Tables

1. **raw_events** - All raw events from Kafka
2. **real_time_metrics** - Real-time aggregated metrics (5 & 10 min windows)
3. **anomalies** - Detected suspicious activities
4. **daily_stats** - Daily batch processing results (partitioned by date)
5. **pipeline_monitoring** - Pipeline health and execution tracking

### Views

- **v_latest_metrics** - Last 24 hours of real-time metrics
- **v_daily_summary** - Daily summary by region
- **v_active_anomalies** - Active anomalies (last 7 days)

## 🎯 Pipeline Components

### 1. Event Generator (`src/producer/event_generator.py`)
- Generates realistic telecom events
- Sends to Kafka topic `telecom_events`
- Configurable event rate

### 2. Spark Streaming (`src/streaming/spark_streaming_job.py`)
- Reads from Kafka in real-time
- Performs windowed aggregations (5 and 10 minutes)
- Detects anomalies
- Writes to PostgreSQL

### 3. Spark Batch (`src/batch/spark_batch_job.py`)
- Daily processing of historical data
- Calculates daily statistics by region
- Idempotent (can be re-run safely)

### 4. Airflow DAG (`airflow/dags/daily_batch_dag.py`)
- Orchestrates daily batch job
- Runs at 2 AM daily
- Includes data validation and retry logic

## 🔧 Configuration

All configuration is in `.env` file:

```bash
# Key configurations
KAFKA_BROKER=kafka:9092
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
AIRFLOW_PORT=8080
```

## 📝 Development Workflow

### Running Components Locally

```powershell
# 1. Start event generator
python src/producer/event_generator.py

# 2. Submit Spark streaming job
docker exec -it spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/streaming/spark_streaming_job.py

# 3. Run batch job manually
python src/batch/spark_batch_job.py --date 2024-11-06
```

## 🐛 Troubleshooting

### Docker Services Not Starting

```powershell
# Check Docker is running
docker ps

# Restart services
docker-compose restart

# View logs for specific service
docker-compose logs kafka
docker-compose logs postgres
```

### Port Already in Use

```powershell
# Check which process is using the port
Get-NetTCPConnection -LocalPort 9092

# Stop the service or change port in .env
```

### Database Connection Issues

```powershell
# Test PostgreSQL connection
docker exec -it postgres psql -U telecom_user -d telecom_analytics

# Verify tables exist
\dt
```

## 📈 Monitoring

- **Airflow UI**: http://localhost:8080
- **Spark Master UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432

## 🧪 Testing

```powershell
# Run tests (once implemented)
pytest tests/

# Run with coverage
pytest --cov=src tests/
```

## 📚 Documentation

Detailed documentation for each component:

1. [Event Generator](docs/event_generator.md) - TBD
2. [Streaming Job](docs/streaming.md) - TBD
3. [Batch Job](docs/batch.md) - TBD
4. [Airflow DAG](docs/airflow.md) - TBD

## 🤝 Contributing

This is a student project for KBTU. Not accepting external contributions.

## 📄 License

Educational project - All rights reserved.

## 👨‍💻 Author

**Arslan** - DevOps Engineer & 4th Year Student at KBTU

## 🎯 Project Goals

- ✅ Infrastructure Setup (10 points)
- ⏳ Stream Processing (25 points)
- ⏳ Batch Processing (20 points)
- ⏳ Orchestration (30 points)
- ⏳ Documentation (15 points)

**Target Score: 100/100** 🚀

## 📅 Timeline

- **Deadline**: November 9, 2025, 23:59:59
- **Status**: Infrastructure Setup Complete ✅

---

**Need help?** Check the troubleshooting section or review the logs.