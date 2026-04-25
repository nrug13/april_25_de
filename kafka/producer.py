import json
import random
from datetime import datetime
from confluent_kafka import Producer

p = Producer({
    'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',
})

CUSTOMERS = [f'cust-{str(i).zfill(3)}' for i in range(1, 11)]
ITEMS     = ['Laptop', 'Monitor', 'Keyboard', 'Mouse', 'Tablet', 'Phone', 'Headphones']


for i in range(1, 1001):
    order = {
        'order_id':    i,
        'customer_id': random.choice(CUSTOMERS),
        'item':        random.choice(ITEMS),
        'amount':      round(random.uniform(10, 2000), 2),
        'status':      random.choice(['pending', 'completed']),
        'ts':          datetime.utcnow().isoformat(),
    }
    p.produce(
        topic='orders-1000',
        key=order['customer_id'],
        value=json.dumps(order),
    )
    p.poll(0)

    if i % 100 == 0:
        print(f'  {i}/1000' )

p.flush()
print('1000 orders done')