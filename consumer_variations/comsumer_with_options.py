from kafka import KafkaConsumer
import time
consumer = KafkaConsumer("kafka.learning.orders", 
                         enable_auto_commit=False,
                         fetch_min_bytes=10,
                         fetch_max_bytes=2097152,
                         bootstrap_servers="localhost:9092",
                         key_deserializer = lambda b: b.decode("utf-8") if b else None,
                        value_deserializer=lambda b: b.decode("utf-8") if b else None,
                        auto_offset_reset='earliest',
                        group_id='kafka-options-consumer'
                         )

recCount = 0

while True:
    messages = consumer.poll(100)

    for message_list in messages.values():
        for msg in message_list:
            print(f'{msg.key}: {msg.value}')
        recCount += 1
    
    time.sleep(1)

    if recCount % 10 == 0:
        consumer.commit_async()