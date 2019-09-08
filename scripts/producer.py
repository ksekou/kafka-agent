from aiokafka import AIOKafkaConsumer
from aiokafka import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition
from aiokafka.errors import IllegalStateError
from typing import List
import asyncio

producer = aiokafka.AIOKafkaProducer(
    loop=loop, bootstrap_servers='localhost:9092')
await producer.start()
try:
    await producer.send_and_wait("my_topic", b"Super message")
finally:
    await producer.stop()