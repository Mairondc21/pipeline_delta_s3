FROM astrocrpublic.azurecr.io/runtime:3.1-13

USER root

RUN python -m venv dt_venv && source dt_venv/bin/activate && \
    pip install 'acryl-datahub[s3,dbt]'==1.4.0.7 && deactivate

RUN apt update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get install -y ant && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

RUN mkdir -p /usr/local/airflow/jars

RUN wget -q -P /usr/local/airflow/jars \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.1.0/delta-spark_2.13-4.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.1.0/delta-storage-4.1.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar

RUN apt-get install -y git && apt-get clean
  
USER astro