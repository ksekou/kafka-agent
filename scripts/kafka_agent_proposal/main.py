from dataclasses import dataclass
from kafka_agent_proposal import models
from kafka_agent_proposal import utils
from kafka_agent_proposal.kafka_agent import KafkaAgent
from kafka_agent_proposal.kafka_agent import BaseApp



app = KafkaAgent(
    "nakamtory",
    partition=7,
    key_value=models.User,
    value_type=models.Message,
    broker=["localhost:9092"],
    config={"producer": {}, "admin_client": {}},
    application=BaseApp,
)

consumer_config = {
    # aiokafka defaults
    # "metadata_max_age_ms": 300000,
    # "max_poll_interval_ms": 300000,
    # "session_timeout_ms": 10000,
    # "heartbeat_interval_ms": 3000,
    # "consumer_timeout_ms": 200,
    # "connections_max_idle_ms": 540000,
    # "request_timeout_ms": 40000,
    # "auto_commit_interval_ms": 5000,
    "max_partition_fetch_bytes": 1024 * 10,
    "metadata_max_age_ms": 10 * 1000,
}


@app.agent(concurrency=2, config=consumer_config)
async def agent000(stream, *args, **kwargs):
    async for msg in stream:
        await utils.process_msesage(msg)

@app.agent(concurrency=5, config=consumer_config)
async def agent003(stream, *args, **kwargs):
    async for msg in stream.filter(on=models.User._id, value=1):
        await utils.process_msesage(msg)

@app.agent(concurrency=3, config=consumer_config)
async def agent005(stream, *args, **kwargs):
    async for user, msg in stream.items():
        if user._type == 'admin':
            await utils.do_admin_task(msg)
        await utils.do_something(user, msg, for_="agent003")


@app.agent(concurrency=5, config=consumer_config)
async def agent007(stream, *args, **kwargs):
    async for msg in stream.take(100, every=5 * 1000):
        await utils.do_something(msg, for_="agent007")


@agent007.on_before_message()
async def before_call(key, msg, *args, **kwargs):
    await utils.do_something(msg, stream)


@agent007.on_after_message()
async def after_call(msg, *args, **kwargs):
    await utils.do_something(msg, stream)


await app.send(topic='name', key=models.User(), value=models.Message())
await app.run()

