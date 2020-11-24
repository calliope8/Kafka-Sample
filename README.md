# Kafka-Sample
C:\apache-zookeeper-3.6.2-bin\bin>zkServer.cmd

C:\kafka>bin\windows\kafka-server-start.bat config\server.properties

C:\kafka>bin\windows\kafka-topics.bat --create --topic MyTestTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1

C:\kafka>bin\windows\kafka-topics.bat --create --topic MyStreamsOutput --zookeeper localhost:2181 --partitions 1 --replication-factor 1

C:\kafka>bin\windows\kafka-topics.bat --describe --topic MyTestTopic --zookeeper localhost:2181

C:\kafka>bin\windows\kafka-console-producer.bat --topic MyTestTopic --bootstrap-server localhost:9092

C:\kafka>bin\windows\kafka-console-consumer.bat --topic MyStreamsOutput --from-beginning --bootstrap-server localhost:9092
