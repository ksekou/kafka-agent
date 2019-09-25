from kafka_agent.agent import kafka_agent
from models import User, Message

config = dict(key_type=User, value_type=Message, concurrency=6)


@kafka_agent
async def consume_stuff(stream):
    async for msg in stream:
        print(msg)
