import asyncio
import logging
from kafka_agent.core import ConsumerGroupeComponent
from kafka_agent.core import Processor
from models import User, Message

log = logging.getLogger(__name__)


async def dev_consumer1(stream):
    async for msg in stream:
        print(f"From 1:stream_id:{id(stream)}:partition:{msg.partition}: {msg}")


async def dev_consumer2(stream):
    async for msg in stream:
        print(f"From 2:stream_id:{id(stream)}:partition:{msg.partition}: {msg}")


async def dev_consumer3(stream):
    async for msg in stream:
        print(f"From 3:stream_id:{id(stream)}:partition:{msg.partition}: {msg}")


async def main():
    try:
        consumer = ConsumerGroupeComponent(
            topics=["dev-topic"],
            key_type=User,
            value_type=Message,
            config={},
            processors=[
                Processor(_coro=dev_consumer1, concurrency=1),
                Processor(_coro=dev_consumer2, concurrency=2),
                Processor(_coro=dev_consumer3, concurrency=3),
            ],
        )
        await consumer.run()
    finally:
        log.error("Shuting down consumers")
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
