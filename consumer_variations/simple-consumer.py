from kafka import KafkaConsumer

consumer = KafkaConsumer('kafka.learning.orders', 
                         group_id='kafka-python-consumer', 
                         bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                         auto_offset_reset='earliest',
                        key_deserializer = lambda b: b.decode("utf-8") if b else None,
                        value_deserializer=lambda b: b.decode("utf-8") if b else None
                         )

while(True):
    messages = consumer.poll(100)

    for message in messages:
        print(f"{message}")