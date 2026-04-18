docker ps -a | grep kafka
docker exec -it kafka-1 bash
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --create \
 --topic payments \
 --partitions 3 \
 --replication-factor 2

 /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic payments

Topic: payments TopicId: WIfOz3eaTQuvyOpTlMXSUw PartitionCount: 3       ReplicationFactor: 2    Configs: min.insync.replicas=2
        Topic: payments Partition: 0    Leader: 2       Replicas: 2,3   Isr: 2,3
        Topic: payments Partition: 1    Leader: 3       Replicas: 3,1   Isr: 3,1
        Topic: payments Partition: 2    Leader: 1       Replicas: 1,2   Isr: 1,2



/opt/kafka/bin/kafka-console-producer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic payments \
 --property parse.key=true \
 --property key.separator=,



payment-001,100
payment-002,200
payment-001,300
payment-003,400
payment-001,500




/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic payments \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true



  payment-001 olan bütün mesajlar eyni partition-dadırmı? (tersi) he eynidir






  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic payments \
  --property print.key=true \
  --property print.partition=true \
  --property  auto.offset.reset=latest

  ##bele olanda evveline baxmir



  ##partition artir
   --alter --topic <TOPIC_NAME> --partitions <NEW_TOTAL_COUNT>


/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --alter --topic payments \
 --partitions 6
 ##artirdim







 3 terminal aç — hamısı pay-group altında consumer başlat. Producer-dan 6 mesaj göndər.
Hər mesaj hansı consumer-a getdi? Bir consumer-ı bağla, 3 mesaj daha göndər — nə dəyişdi?





/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic payments --from-beginning \
  --group payment-group \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true




5. 3 terminal aç — hamısı pay-group altında consumer başlat. Producer-dan 6 mesaj göndər. 
##idye gore muxtelif qruplra

. topic-rf3 yarat — replication-factor=3, min.insync.replicas=2.
acks=all ilə producer başlat.
kafka-2-ni stop et — mesaj göndər,
işləyirmi? kafka-3-ü də stop et — nə baş verir?





/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --create \
 --topic rf-3 \
 --partitions 3 \
 --replication-factor 3 \
 --config min.insync.replicas=2






 acks=all ilə producer başlat. 



 /opt/kafka/bin/kafka-console-producer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf-3 \
 --producer-property acks=all


 docker stop kafka-2


  /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic rf-3


 burda isrler 1,3 olduu


 docker stop kafka-3

 isrler 1 qaldi

yeni inputlar islemeyecek




replication-factor=1 ilə acks=all eyni nəticəni verir replication-factor=3,
acks=all ilə müqayisədə? Niyə?
bir denesine getse bes edir deye




/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic rf-3 \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true





  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic rf-3 \
  --from-beginning \






  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --entity-type topics \
  --entity-name rf-3 \
  --alter \
  --add-config min.insync.replicas=1 