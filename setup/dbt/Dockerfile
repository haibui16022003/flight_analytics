FROM bitnami/postgresql:latest

USER root

# Install Python and required dependencies
RUN install_packages \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    git \
    libpq-dev

# Create a virtual environment
RUN python3 -m venv /opt/dbt_venv

# Install dbt and PostgreSQL adapter in the virtual environment
RUN /opt/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core \
    dbt-postgres

# Add the virtual environment bin to PATH
ENV PATH="/opt/dbt_venv/bin:$PATH"

# Switch back to non-root bitnami user
USER 1001
