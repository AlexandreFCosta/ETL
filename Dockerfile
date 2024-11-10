FROM apache/airflow:2.7.1-python3.11

USER root

# Criação de diretórios e ajuste de permissões
RUN mkdir -p /opt/airflow/silver_layer \
    && mkdir -p /opt/airflow/gold_layer \
    && mkdir -p /opt/airflow/bronze_layer

# Instalação de pacotes necessários, incluindo curl
RUN apt-get update && \
    apt-get install -y jq gcc python3-dev openjdk-11-jdk curl && \
    apt-get clean

# Configuração da variável de ambiente JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Instalação do Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz -o spark.tgz \
    && tar -xzvf spark.tgz -C /usr/local/ \
    && mv /usr/local/spark-3.5.3-bin-hadoop3 /usr/local/spark \
    && rm spark.tgz

ENV SPARK_HOME=/usr/local/spark
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop

RUN groupadd -r airflow && useradd -r -g airflow myuser

# Mudança para o usuário airflow
USER airflow

# Copiar o requirements.txt para o contêiner
COPY requirements.txt /opt/airflow/requirements.txt

# Instalar os pacotes Python a partir do requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt