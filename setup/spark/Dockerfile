FROM bitnami/spark:latest

USER root

WORKDIR /opt/spark

RUN apt-get update && apt-get install -y wget curl unzip nano

RUN mkdir -p /opt/spark/jars && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P /opt/spark/jars

#COPY requirements.txt /opt/spark/requirements.txt
#RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

RUN chmod -R 777 /opt/spark

CMD ["/opt/bitnami/scripts/spark/run.sh"]