from confluent_kafka import Consumer
from pyspark.sql import SparkSession
import json
consumer = Consumer({
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "group.id": "final-group",
    "auto.offset.reset": "earliest"
})
consumer.subscribe(["orders-1000"])
messages = []









while len(messages) < 1000:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("xeta var:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))
    messages.append(data)

print("oxunan mesaj sayi:", len(messages))
consumer.close()
spark = SparkSession.builder.appName("final").getOrCreate()

df = spark.createDataFrame(messages)
df.show(5)

df.createOrReplaceTempView("orders")

spark.sql("SELECT COUNT(*) FROM orders").show()

spark.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id").show()

spark.sql("SELECT item, COUNT(*) FROM orders GROUP BY item ORDER BY COUNT(*) DESC").show()

spark.sql("SELECT status, COUNT(*) FROM orders GROUP BY status").show()
