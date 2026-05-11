FROM apache/airflow:2.9.1-python3.11

USER root

# Install Temurin 17 (portable JDK, works with glibc 2.17+)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    apt-transport-https \
    gnupg \
    procps \
    && wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" \
       > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update && apt-get install -y temurin-17-jdk \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/temurin-17-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary \
    python-dotenv \
    openai \
    pyspark

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
