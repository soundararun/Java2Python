FROM flink:1.12.1-scala_2.11
# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
WORKDIR /opt/flink/usrlib
COPY --chown=flink:flink event-processor-state-1.0.jar /opt/flink/usrlib/event-processor-state-1.0.jar
RUN chmod -R 777 /opt/flink/usrlib/event-processor-state-1.0.jar