FROM openjdk:latest
COPY target/Kafka-Streams-Customer-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/src/Kafka-Streams-Customer-1.0-SNAPSHOT-jar-with-dependencies.jar
CMD java -cp /usr/src/Kafka-Streams-Customer-1.0-SNAPSHOT-jar-with-dependencies.jar kstream.demo.CustomerStreamPipelineHDI
