
FROM apache/spark:3.5.9

USER root

# Install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Configure Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy Spark application JAR
RUN mkdir -p /opt/spark-apps

COPY target/scala-2.12/spark-kafka-iceberg.jar /opt/spark-apps/spark-kafka-iceberg.jar

RUN chown -R 185:0 /opt/spark-apps

USER 185
