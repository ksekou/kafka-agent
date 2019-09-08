from aiokafka import AIOKafkaConsumer
from aiokafka import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition
from aiokafka.errors import IllegalStateError
from typing import List
import asyncio


class ConsumerGroupeRebalancer(ConsumerRebalanceListener):
    def __init__(self, consumer: AIOKafkaConsumer):
        self.consumer = consumer

    async def on_partitions_assigned(self, assigned: List[TopicPartition]) -> None:
        """This method will be called after partition 
           re-assignment completes and before the consumer
           starts fetching data again.

        Arguments:
            assigned {TopicPartition} -- Topic and partition assigned
            to a given consumer.
        """
        for tp in assigned:
            try:
                position = await self.consumer.position(tp)
                offset = position - 1
            except IllegalStateError:
                offset = -1

            if offset > 0:
                self.consumer.seek(tp, offset)
            else:
                self.consumer.seek_to_beginning(tp)


async def main():
    consumer = AIOKafkaConsumer(
        group_id="dev-group",
        loop=asyncio.get_event_loop(),
        enable_auto_commit=False,
        bootstrap_servers="localhost:9092",
    )
    # listener = ConsumerGroupeRebalancer(consumer=consumer)
    # consumer.subscribe(topics=["dev-topic"], listener=listener)
    consumer.subscribe(topics=["dev-topic"])
    await consumer.start()
    try:
        print("Ready !!!")
        # while True:
        #     msgs = consumer.getmany(timeout_ms=200, max_records=5)
        #     print(f"Got {len(msgs)} messages ")
        async for msg in consumer:
            print(msg)
            tp = TopicPartition(msg.topic, msg.partition)
            await consumer.commit({tp: msg.offset + 1})
    finally:
        await consumer.stop()


asyncio.run(main())
