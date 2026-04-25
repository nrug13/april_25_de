docker ps -a | grep kafka
docker exec -it kafka-1 bash
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --create \
 --topic orders-1000 \
 --partitions 3 \
 --replication-factor 2 \
 --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic orders-1000
##lideri 3 dur
##isrde iki broker var

/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --alter --topic orders-1000 \
 --partitions 6



/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --alter --topic orders-1000 \
 --partitions 3
 
 Error while executing topic command : The topic orders-1000 currently has 6 partition(s); 3 would not be an increase.
[2026-04-25 18:55:38,740] ERROR org.apache.kafka.common.errors.InvalidPartitionsException: The topic orders-1000 currently has 6 partition(s); 3 would not be an increase.
 (org.apache.kafka.tools.TopicCommand)




