# Flight Data Processing Project

## Overview

This project provides a containerized environment for processing and analyzing flight data.
It uses a combination of PostgreSQL for data storage, dbt for data transformation, and Apache Spark for distributed data processing.

## Project Structure

```
├── data/                   # Source flight data
├── scripts/
│   ├── dbt/                # dbt transformation scripts
│   └── spark/              # Spark processing scripts
├── test/                   # Test files
├── utils/                  # Utility functions
├── docker-compose.yml      # Docker environment configuration
├── README.md               # This documentation
└── profiles.yml            # dbt profile configuration
```

## Prerequisites

- Docker and Docker Compose
- Python 3.6 or higher

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/haibui16022003/flight_analytics.git
cd flight_analytics
```

### 2. Start the Docker environment

```bash
docker-compose up -d
```

This command will start the PostgreSQL database, dbt, and Apache Spark services.

- PostgreSQL: 'postgresql'
- dbt: 'dbt'
- Spark master node: 'spark-master'
- Spark worker node: 'spark-worker'

### 3. Load the flight data

All flight data files should be placed in the `data/` directory.
(updating)

### 4. Run Spark jobs

```bash
docker exec -it spark-master bash
cd /opt/spark/work
spark-submit --jars /opt/spark/jars/postgresql-42.2.23.jar "loader.py"
```

#### Accessing Services

- PostgreSQL: http://localhost:5432 (User: user, Password: password, Database: flights_dwh)
- Spark Master UI: http://localhost:8080

### 5. Run dbt transformations
Option 1: 
```bash
docker exec -it postgresql bash
cd /opt/dbt/flight_analysis
dbt build # If you want to run all models
```
Option 2:
```bash
docker exec -it postgresql bash
cd /opt/dbt/flight_analysis
dbt run model_name # If you want to run a specific model
```

# Troubleshooting

- If you encounter connection issues between services, ensure all containers are running with `docker-compose ps`
- For database connection issues, try waiting a few seconds for PostgreSQL to initialize
- Check container logs with `docker-compose logs <service_name>`

# Stop the Docker environment

```bash
docker-compose down
```

To remove all volumes as well:

```bash
docker-compose down -v
```
