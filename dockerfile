# Base image: change to whatever you need (e.g., airflow, python, etc.)
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    tar \
    gcc \
    g++ \
    libkrb5-dev \
    libsnappy-dev \
    libzstd-dev \
    libbz2-dev \
    && apt-get clean

# Set Hadoop version
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
ENV PATH=$HADOOP_HOME/bin:$PATH

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Register native libraries
RUN echo "$HADOOP_HOME/lib/native" > /etc/ld.so.conf.d/hadoop.conf && ldconfig

# Optional: install PyArrow or HDFS client libraries
RUN pip install pyarrow pandas "pyicerberg[hive]"

# Create working directory
WORKDIR /app

# Copy your code (optional)
COPY . /app

RUN ["python", "main.py"]
