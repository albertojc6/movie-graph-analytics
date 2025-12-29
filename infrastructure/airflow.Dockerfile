FROM apache/airflow:2.5.1-python3.8

USER airflow

COPY requirements.txt .
RUN python -m pip install --upgrade pip --no-cache-dir && \
    pip install --no-cache-dir -r requirements.txt
RUN pip --no-cache-dir install --upgrade awscli

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

