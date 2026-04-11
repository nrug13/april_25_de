##1. scale adında topic yarat — 2 partition, replication-factor 1.
docker ps -a | grep kafka

docker exec -it kafka-1 bash

kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --create \
 --topic scale \
 --partitions 2 \
 --replication-factor 1
Created topic scale.
kafka-1:/$ 
# b Bütün topic-ləri listələ.
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --list
orders
scale
kafka-1:/$ 
# scale topic-ini describe et. Çıxışda Leader, Replicas, Isr sütunlarını izah et.
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic scale
Topic: scale    TopicId: 0rEI0DlkRzKwrojlNvdaEA PartitionCount: 2       ReplicationFactor: 1    Configs: min.insync.replicas=2
        Topic: scale    Partition: 0    Leader: 3       Replicas: 3     Isr: 3
        Topic: scale    Partition: 1    Leader: 1       Replicas: 1     Isr: 1
kafka-1:/$ 
# scale topic-inin partition sayını 4-ə artır.
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --alter --topic scale \
 --partitions 4
#############################
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe --topic scale
Topic: scale    TopicId: 0rEI0DlkRzKwrojlNvdaEA PartitionCount: 4       ReplicationFactor: 1    Configs: min.insync.replicas=2
        Topic: scale    Partition: 0    Leader: 3       Replicas: 3     Isr: 3
        Topic: scale    Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: scale    Partition: 2    Leader: 2       Replicas: 2     Isr: 2
        Topic: scale    Partition: 3    Leader: 3       Replicas: 3     Isr: 3
#  scale topic-ini sil.
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --delete --topic scale
##mende orders var idi onu da sildim
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic orders
## orders adında topic yarat — 3 partition, replication-factor 1.
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
Created topic orders.
# Terminal-1-də producer başlat, orders topic-inə key ilə mesaj göndər:
##bu terminal 1 ucun
/opt/kafka/bin/kafka-console-producer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic orders \
 --property parse.key=true \
 --property key.separator=,
##bu terminal 2 ucun:
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true

##9. 3 ayrı terminaldə eyni homework-group altında consumer başlat — hamısı orders topic-ini oxusun.
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders --from-beginning \
  --group homework-group \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true

  ##group neye gore lazimdir?
  ##group consumer-lari qruplaşdırır, eyni group-a aid consumer-lar arasında mesajlar paylanır. Bu, yükü bölüşdürmək və mesajların paralel işlənməsini təmin etmək üçün istifadə olunur. Eyni group-a aid consumer-lar, eyni topic-in fərqli partition-larını oxuyaraq mesajları paylaşır, beləliklə hər mesaj yalnız bir consumer tərəfindən işlənir. Bu, mesajların təkrarlanmasının qarşısını alır və sistemin effektivliyini artırır. Eyni zamanda, group-lar consumer-ların koordinasiyasını təmin edir, məsələn, bir consumer aradan qaldırıldıqda, qalan consumer-lar avtomatik olaraq onun partition-larını öz üzərinə götürür, beləliklə mesajların davamlı işlənməsini təmin edir.
  