FROM apache/spark:3.5.7-scala2.12-java11-python3-r-ubuntu

USER root

WORKDIR /opt/spark-app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"]

