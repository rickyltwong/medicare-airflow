FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    # apt-get install -y net-tools nano && \
    apt-get clean

# Set JAVA_HOME and add to PATH with absolute path
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH=$PATH:$JAVA_HOME/bin
RUN export JAVA_HOME
    
# Install Spark
ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
RUN curl -sSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Add Spark binaries to PATH
ENV SPARK_HOME="/opt/spark"
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$SPARK_HOME/bin/spark-submit

RUN echo 'export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"' | tee -a  ${SPARK_HOME}/conf/spark-env.sh 

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt