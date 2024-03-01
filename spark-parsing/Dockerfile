FROM debian:11

USER root

RUN apt-get update && apt-get install -y openjdk-17-jre-headless python3 python3-pip python3-click wget procps && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
ARG SPARK_FILE=spark-3.5.0-bin-without-hadoop
ARG HADOOP_VERSION=hadoop-3.3.6
RUN wget --quiet https://archive.apache.org/dist/spark/spark-3.5.0/${SPARK_FILE}.tgz && tar -xaf ${SPARK_FILE}.tgz && \
    rm ${SPARK_FILE}.tgz  && mv ${SPARK_FILE} spark && \
    wget --quiet https://archive.apache.org/dist/hadoop/common/${HADOOP_VERSION}/${HADOOP_VERSION}.tar.gz && tar -xaf ${HADOOP_VERSION}.tar.gz && \
    rm ${HADOOP_VERSION}.tar.gz && mv ${HADOOP_VERSION} hadoop

RUN python3 -m pip --quiet install --upgrade pip

COPY code/requirements.txt /opt
RUN pip3 --quiet install --no-cache-dir -r requirements.txt

RUN wget --quiet https://go.dev/dl/go1.21.7.linux-amd64.tar.gz && tar -C /usr/local -xzf go1.21.7.linux-amd64.tar.gz && rm go1.21.7.linux-amd64.tar.gz

ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.3-src.zip
ENV PYSPARK_PYTHON=python3
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HADOOP_HOME}/bin::/usr/local/go/bin:${PATH}
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY code/spark-* code/log4j.properties code/entrypoint.py code/template.py code/requirements.txt /opt/
COPY code/parse-certs/* /opt/parse-certs/
RUN mv spark-defaults.conf /opt/spark/conf/ && mv spark-env.sh /opt/spark/conf/ && mv log4j.properties /opt/spark/conf/  \
    && mkdir /app && ./entrypoint.py /opt/template.py && cd /opt/parse-certs && make

COPY code/src/* /opt/src/
COPY code/*.py /opt/

WORKDIR /app

ENTRYPOINT ["/opt/entrypoint.py"]
CMD /opt/template.py
