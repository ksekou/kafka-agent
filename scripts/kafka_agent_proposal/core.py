import json
import asyncio
import logging
from kafka import KafkaAdminClient
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRebalanceListener
import dataclasses as models
import typing


log = logging.getLogger(__name__)


class BaseAgent:
    pass


class ConsumerAgent(AIOKafkaConsumer):
    @property
    def running(self):
        if hasattr(self, "_running"):
            return self._running
        return False

    @running.setter
    def running(self, value):
        raise AttributeError("Agent state should not be changed.")

    async def start(self):
        await super().start()
        self._running = True

    async def stop(self):
        await super().stop()
        self._running = False


class ProducerAgent(AIOKafkaProducer):
    pass


class AdminAgent(KafkaAdminClient):
    pass


class kafka_agent:
    def __init__(self, coro: typing.Coroutine):
        self.consumers: typing.Dict[int, ConsumerComponent] = {}
        self.config: typing.Dict[int, typing.Any] = {}
        self.producer: AIOKafkaProducer = None
        self.admin_client: KafkaAdminClient = None
        self.coro = coro

    @property
    def name(self):
        return self.coro.__name__

    @property
    def kafka_topic_name(self):
        return f"agent-topic-{self.name}"

    async def __call__(self, *args, **kwargs):
        return await self.coro(*args, **kwargs)

    def on_before_message(self, coro):
        pass

    def on_after_message(self, coro):
        pass

    @property
    def topic(self):
        return f"agent-topic-{self.name}"

    @property
    def topics(self):
        return [self.topic, *self.config.get("topics", [])]

    @property
    def consumer_config(self):
        return {
            "key_type": self.key_type,
            "value_type": self.value_type,
            **self.config.get("consumer", {}),
        }

    @property
    def producer_config(self):
        return self.config.get("producer", {})

    @property
    def admin_client_config(self):
        return self.config.get("admin_client", {})

    @property
    def key_type(self):
        return self.config.get("key_type")

    @property
    def value_type(self):
        return self.config.get("value_type")

    @property
    def concurrency(self):
        return self.config.get("concurrency", 1)

    @property
    def serializer(self):
        return self.config.get("serializer", json)

    @property
    def loop(self):
        return self.config.get("loop", asyncio.get_event_loop())

    @property
    def broker(self):
        return self.config.get("broker", "localhost:9092")

    def configure(self, **config):
        self.config = config
        self.producer = AIOKafkaProducer(
            **{
                "loop": self.loop,
                "bootstrap_servers": self.broker,
                "client_id": f"{self.name}-producer",
                **self.producer_config,
            }
        )
        self.admin_client = KafkaAdminClient(
            **{
                "bootstrap_servers": self.broker,
                "client_id": f"{self.name}-admin-client",
                **self.admin_client_config,
            }
        )
        for idx in range(self.concurrency):
            self.consumers[idx] = ConsumerComponent(
                id=idx, coro=self.coro, config=self.consumer_config
            )

    async def start(self):
        assert hasattr(self, "config") is True, "Agent must be configured."
        await self.producer.start()
        # await asyncio.gather(
        #     *[self.consumers[i].start() for i in range(self.concurrency)]
        # )
        for i in range(self.concurrency):
            await self.consumers[i].start()

    async def reboot_consumer(self, idx):
        await self.consumers[idx].stop()
        await self.consumers[idx].start()

    async def watchdog(self):
        while self.consumers:
            for i in range(self.concurrency):
                try:
                    self.consumers[i].task.exception()
                except asyncio.InvalidStateError:
                    continue
                except asyncio.CancelledError:
                    log.warning(f"Restarting consumer :[{self.name}.{i}] ...")
                    await self.reboot_consumer(i)
                    log.warning(f"Consumer :[{self.name}.{i}] now running.")
                except:
                    log.warning(f"Consumer :[{self.name}.{i}] died.")
                    log.warning(f"Restarting consumer :[{self.name}.{i}] ...")
                    await self.reboot_consumer(i)
                    log.warning(f"Consumer :[{self.name}.{i}] now running.")
            await asyncio.sleep(5 * 1000)

    async def run(self, **config):
        try:
            self.configure(**config)
            await self.start()
            await self.watchdog()
        except:
            log.error(f"Stoping Agnet:[{self.name}].", exc_info=True)
        finally:
            await self.stop()

    async def stop(self):
        await self.producer.stop()
        self.producer = None
        for i in self.consumers.keys():
            self.consumers[i].task.cancel()
            await self.consumers[i].stream.stop()
            del self.consumers[i]

    async def send(self, *topics, **kwargs):
        assert self.producer is not None, "Invalid producer state."
        kwargs.update(
            key=serializer(
                self.key_type, kwargs.get("key", None), _serializer=self._serializer
            ),
            value=serializer(
                self.value_type, kwargs.get("value", None), _serializer=self._serializer
            ),
        )
        topics = topics or [self.kafka_topic_name]
        for topic in topics:
            await self.producer.send(topic, **kwargs)


class Stream(AIOKafkaConsumer):
    def __init__(
        self,
        name,
        *topics,
        loop=None,
        broker=None,
        rebalance_listener=None,
        key_type=None,
        value_type=None,
        serializer=json,
        **config,
    ):
        self.name = name
        self.topics = topics
        self.key_type = key_type
        self.value_type = value_type
        self.serializer = serializer
        self.loop = loop or asyncio.get_event_loop()
        self.config = {
            **{"loop": self.loop, "bootstrap_servers": broker or "localhost:9092"},
            **config,
        }
        self.rebalance_listener = rebalance_listener or ConsumerRebalanceListener()
        super().__init__(**self.config)

    async def start(self):
        self.subscribe(topics=self.topics, listener=self.rebalance_listener)
        await super().start()

    async def read(self):
        async for msg in self:
            yield msg._replace(
                key=deserializer(self.key_type, msg.key, _serializer=self.serializer),
                value=deserializer(
                    self.value_type, msg.value, _serializer=self.serializer
                ),
            )

    async def filter(self, on="key", value=None, func=lambda msg, value: True):
        async for msg in self.read():
            target = vars(msg)[on]
            if func(target, value) is True:
                yield msg

    async def items(self):
        async for msg in self.read():
            yield msg.key, msg.value

    async def take(self, max_, every=5 * 1000):
        pass


@models.dataclass
class ConsumerComponent:
    id: int
    coro: typing.Coroutine
    rebalance_listener: typing.Any = ConsumerRebalanceListener()
    metrics: typing.Dict[str, typing.Any] = models.field(default_factory=dict)
    config: typing.Dict[str, typing.Any] = models.field(default_factory=dict)
    stream: Stream = models.field(init=False, default=None)
    task: asyncio.Task = models.field(init=False, default=None)

    async def start(self):
        self.config.setdefault("serializer", json)
        if hasattr(self, "running"):
            log.warning(f"Consumer: [{self.name}] is currently running.")
            return
        self.stream = Stream(
            self.name,
            *self.topics,
            **{
                "client_id": self.name,
                "group_id": self.group,
                "key_type": self.key_type,
                "value_type": self.value_type,
                "loop": self.loop or asyncio.get_event_loop(),
                **self.config,
            },
        )
        await self.stream.start()
        self.task = asyncio.create_task(self.coro(self.stream))

    async def stop(self):
        self.task.cancel()
        await self.stream.stop()
        self.task, self.stream = None, None

    @property
    def name(self):
        return f"consumer.{self.coro.__name__}.{self.id}"

    @property
    def group(self):
        return f"group.{self.coro.__name__}"

    @property
    def topic(self):
        return f"agent-topic-{self.coro.__name__}"

    @property
    def topics(self):
        return [self.topic, *self.config.get("topics", [])]

    @property
    def key_type(self):
        return self.config.get("key_type")

    @property
    def value_type(self):
        return self.config.get("value_type")

    @property
    def loop(self):
        return self.config.get("loop")

    async def __call__(self):
        await self.task


def deserializer(_type, value, _serializer=json):
    assert _serializer is not None, f"Invalid serializer for {value}."
    if value is None:
        return
    if models.is_dataclass(_type):
        return _type(**value)
    try:
        return _serializer.loads(value)
    except:
        return value


def serializer(_type, value, _serializer=json):
    assert _serializer is not None, f"Invalid serializer for {value}."
    if value is None:
        return
    if models.is_dataclass(_type):
        value = _serializer.dumps(models.asdict(value))
    else:
        value = _serializer.dumps(value)
    return value.encode("utf-8") if isinstance(value, str) else value


@kafka_agent
async def dev_consumer(stream):
    async for msg in stream.items():
        print(msg)


@models.dataclass
class User:
    id: int
    name: str


@models.dataclass
class Message:
    _from: str
    to: typing.List[User]
    body: typing.Any


async def strat_consumer(consumer, config):
    await consumer.run(**config)


if __name__ == "__main__":
    config = {"key_type": User, "value_type": Message}
    # loop = asyncio.get_event_loop()
    # asyncio.run_coroutine_threadsafe(strat_consumer(dev_consumer, config), loop)
    asyncio.run(strat_consumer(dev_consumer, config))

