docker ps -a | grep kafka

docker exec -it kafka-1 bash
# list topics
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --list
# topic-rf3 topic-i yarat — 3 partition, replication-factor 3. min.insync.replicas=2 təyin et.
kafka-1:/$ /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --create \
 --topic rf4 \
 --partitions 3 \
 --replication-factor 3 \
 --config min.insync.replicas=2

##describe
 /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic rf4

Topic: rf4      TopicId: Qst1bilARYyrl2ZhdaEaTw PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: rf4      Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
        Topic: rf4      Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        Topic: rf4      Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1


##############################################
##start producer acks=all ilə
/opt/kafka/bin/kafka-console-producer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf4 \
 --producer-property acks=all
 ##start consumer
/opt/kafka/bin/kafka-console-consumer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf4 \
 --from-beginning

 ##5 mesaj göndər
ms1
ms2
ms3
ms4
ms5
## kafka-2-ni stop et. ISR-i yoxla — nə dəyişdi? Producer-dan mesaj göndər — işləyirmi?
docker stop kafka-2
#isr yoxla
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic rf4
 
 Topic: rf4      TopicId: Qst1bilARYyrl2ZhdaEaTw PartitionCount: 3      ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: rf4      Partition: 0    Leader: 3     Replicas: 3,1,2  Isr: 3,1
        Topic: rf4      Partition: 1    Leader: 1     Replicas: 1,2,3  Isr: 1,3
        Topic: rf4      Partition: 2    Leader: 3     Replicas: 2,3,1  Isr: 3,1
##artiq leader icinde 2 yoxdur.
#mesaj gonderirem, error yoxdur
##kafka-3-u da stop et. ISR-i yoxla — nə dəyişdi? Producer-dan mesaj göndər — işləyirmi?
docker stop kafka-3
#indi error verir, cunku 2 replica offline oldu
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic rf4
Topic: rf4      TopicId: Qst1bilARYyrl2ZhdaEaTw PartitionCount: 3      ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: rf4      Partition: 0    Leader: 1     Replicas: 3,1,2  Isr: 1
        Topic: rf4      Partition: 1    Leader: 1     Replicas: 1,2,3  Isr: 1
        Topic: rf4      Partition: 2    Leader: 1     Replicas: 2,3,1  Isr: 1
kafka-1:/$ 
# Broker-ləri bərpa et. ISR-in geri gəldiyini göstər.
docker start kafka-2
docker start kafka-3
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic rf4
 Topic: rf4      TopicId: Qst1bilARYyrl2ZhdaEaTw PartitionCount: 3       ReplicationFactor: 3  Configs: min.insync.replicas=2
        Topic: rf4      Partition: 0    Leader: 1       Replicas: 3,1,2 Isr: 1,2,3
        Topic: rf4      Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        Topic: rf4      Partition: 2    Leader: 1       Replicas: 2,3,1 Isr: 1,2,3
#isrler geri geldi




##SSENARI 2. ACKS=1
##produceri dayandır, acks=1 ilə başlat
/opt/kafka/bin/kafka-console-producer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf4 \
 --producer-property acks=1
 #5 mesaj göndər
 ack11
    ack12
    ack13
    ack14
    ack15
##kafka-2-ni stop et. ISR-i yoxla — nə dəyişdi? Producer-dan mesaj göndər — işləyirmi?
docker stop kafka-2
#isr yoxla
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
    --describe \
    --topic rf4
Topic: rf4      TopicId: Qst1bilARYyrl2ZhdaEaTw  PartitionCount: 3        ReplicationFactor: 3     Configs: min.insync.replicas=2
        Topic: rf4      Partition: 0     Leader: 1Replicas: 3,1,2 Isr: 1,3
        Topic: rf4      Partition: 1     Leader: 1Replicas: 1,2,3 Isr: 1,3
        Topic: rf4      Partition: 2     Leader: 1Replicas: 2,3,1 Isr: 1,3
#isrler 1,3 oldu. mesaj gedir
##kafka-3-u da stop et. ISR-i yoxla — nə dəyişdi? Producer-dan mesaj göndər — işləyirmi?
docker stop kafka-3
#isr yoxla
/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
        --describe \
        --topic rf4
Topic: rf4      TopicId: Qst1bilARYyrl2ZhdaEaTw PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: rf4      Partition: 0    Leader: 1      Replicas: 3,1,2  Isr: 1
        Topic: rf4      Partition: 1    Leader: 1      Replicas: 1,2,3  Isr: 1
        Topic: rf4      Partition: 2    Leader: 1      Replicas: 2,3,1  Isr: 1
#isler 1 qaldi
#mesaj gonderirem, error yoxdur
ack17_afterkf23
##producerde error yoxdur, amma consumerde mesaj gormur
##broker-ləri bərpa et. ISR-in geri gəldiyini göstər.
docker start kafka-2
docker start kafka-3
/opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka-1:9092 \
 --describe \
 --topic rf4


 ##isrler ve mesajlar geldi



 #####################################
  ## new-group-test adlı yeni group ilə earliest — neçə mesaj gəldi?
/opt/kafka/bin/kafka-console-consumer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf4 \
    --group new-group-test \
    --from-beginning


    ##butun yazdigim mesajlar geldi
      
# Eyni group ilə yenidən başlat — niyə mesaj gəlmir?
/opt/kafka/bin/kafka-console-consumer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf4 \
    --group new-group-test \
    --from-beginning
#baxmayaraq ki from beginning yazmisam, mesaj gelmir, cunku consumer groupun offseti topicdeki son offsete qarsiliq gelir. yeni mesaj gelmedikce offset artmayacaq, ona gore de mesaj gelmeyecek

## teze gonderdiyim mesaj dusdu

# group1-in offset-ini --shift-by -5 ilə geri qaytar. Consumer başlatdıqda hansı mesajlar gəldi?
/opt/kafka/bin/kafka-consumer-groups.sh \
 --bootstrap-server kafka-1:9092 \
 --group new-group-test \
 --topic rf4 \
 --reset-offsets \
 --shift-by -5 \
 --execute
/opt/kafka/bin/kafka-console-consumer.sh \
 --bootstrap-server kafka-1:9092 \
 --topic rf4 \
    --group new-group-test \
    --from-beginning




## 30 mesaj göndər, consumer-ı dayandır. LAG neçədir? Consumer-ı yenidən başlatdıqda LAG nə olur?

##30 mesaj gonder
for i in {1..30}
do
   echo "lag-message-$i" | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-1:9092 \
    --topic rf4 \
    --producer-property acks=all
done
  #lag necedir?
  /opt/kafka/bin/kafka-consumer-groups.sh \
--bootstrap-server kafka-1:9092 \
--describe \
--group new-group-test

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
new-group-test  rf4             0          30              30              0               -               -               -
new-group-test  rf4             1          11              11              0               -               -               -
new-group-test  rf4             2          15              15              0               -               -               -
kafka-1:/$ 
##burda lag 0 oldu, cunku consumer mesajlari oxuyub offseti artirir, ona gore de lag 0 olur


##men mesajlarin hamisini consumerde oxuyandan sonra consumeri dayandirdim
     
