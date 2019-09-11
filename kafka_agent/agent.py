from core import kafka_agent
from core import User, Message

config = dict(key_type=User, value_type=Message, concurrency=1)


@kafka_agent
async def consume_stuff(stream):
    async for msg in stream:
        print(msg)
