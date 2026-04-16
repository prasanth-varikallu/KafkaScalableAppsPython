from kafka import KafkaProducer
import random
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                         key_serializer=lambda s: s.encode('utf-8') if s is not None else None,
                         value_serializer=lambda s: s.encode('utf-8') if s is None else None
                         )

startKey = random.randint(1, 1000)

for i in range(startKey, startKey + 21):
    producer.send("kafka.learning.orders", key=str(i), value=f'This is order {i}')
    time.sleep(1)