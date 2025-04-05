FROM bitnami/spark:latest

USER root

WORKDIR /opt/spark

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    curl \
    unzip \
    nano \
    python3-pip \
    python3-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P /opt/spark/jars

# Install Python packages
RUN pip3 install --no-cache-dir \
    pandas \
    matplotlib \
    seaborn

# Set permissions
RUN chmod -R 777 /opt/spark

# Set Python 3 as the default Python
RUN ln -sf /usr/bin/python3 /usr/bin/python