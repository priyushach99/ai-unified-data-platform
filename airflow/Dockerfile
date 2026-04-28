FROM apache/airflow:2.9.0

USER root

RUN apt-get update \
    && apt-get install -y default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY requirements.txt /tmp/requirements.txt
RUN chown airflow: /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
