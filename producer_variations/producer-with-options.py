from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata, RecordMetadata
import random
from typing import Optional

producer = KafkaProducer(bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"], 
                         acks='all', 
                         compression_type='gzip', 
                         key_serializer=lambda s: s.encode('utf-8') if s else None,
                        value_serializer=lambda s: s.encode('utf-8') if s else None)
print("started")
try:
    print("Started async")
    producer.send("kafka.learning.orders", key=str(random.randint(1001, 2000)), value="Order published with no checks")
    print("Async with no checks")
except Exception as e:
    print("Error")
    print(e)

try:
    print("Started sync")
    sync_response: FutureRecordMetadata = producer.send("kafka.learning.orders", key=str(random.randint(1001, 2000)), value="Sent syncronously")
    val: Optional[RecordMetadata] = sync_response.get()
    if val is None:
        raise Exception()
    print(f"Sync val, partition: {val.partition}, offset: {val.offset}")
except Exception as e:
    print("Error")
    print(e)


def onSuccess(record_metadata):
        print(record_metadata)

def onFailure(ex):
     print("send failed", ex)

try:
    print("Started async")
    future: FutureRecordMetadata = producer.send("kafka.learning.orders", key=str(random.randint(1001, 2000)), value="Order published with callbacks")
    future.add_callback(onSuccess)
    future.add_errback(onFailure)
    print("Async with callback")
except Exception as e:
    print("Error")
    print(e)


producer.flush()