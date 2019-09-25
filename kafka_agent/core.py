import asyncio
import dataclasses
import functools
import json
import logging
import typing
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from kafka import KafkaAdminClient, KafkaClient
from kafka.admin import NewTopic

log = logging.getLogger(__name__)

DEFAULT_CONFIG = {"bootstrap_servers": "localhost:9092"}
PRODUCER_DEFAULT_CONFIG = {}
CONSUMER_DEFAULT_CONFIG = {}
ADMIN_DEFAULT_CONFIG = {}
DEFAULT_TOPIC_CONFIG = {"partitions": 1, "replicas": 1, "retention_ms": None}


class ConsumerComponent(AIOKafkaConsumer):
    """[summary]
    
    Arguments:
        AIOKafkaConsumer {[type]} -- [description]
    
    Raises:
        AttributeError: [description]
    
    Returns:
        [type] -- [description]
    """

    def __init__(self, *topics, **config):
        self._on_before: typing.Set[typing.Coroutine] = set()
        self._on_after: typing.Set[typing.Coroutine] = set()
        self.key_type = config.pop("key_type", None)
        self.value_type = config.pop("value_type", None)
        self.serializer = config.pop("serializer", json)
        self.skip_invalid = config.pop("skip_invalid", False)
        self.key_deserializer = config.pop(
            "key_deserializer", None
        ) or functools.partial(
            deserializer, _type=self.key_type, _serializer=self.serializer
        )
        self.value_deserializer = config.pop(
            "value_deserializer", None
        ) or functools.partial(
            deserializer, _type=self.value_type, _serializer=self.serializer
        )
        listener = config.pop("listener", ConsumerRebalanceListener())
        config.setdefault("loop", asyncio.get_event_loop())
        config.update(
            key_deserializer=self.key_deserializer,
            value_deserializer=self.value_deserializer,
        )
        super().__init__(*topics, **config)
        self.subscribe(topics=topics, listener=listener)

    @property
    async def running(self):
        try:
            await self.start()
        except AssertionError:
            return False
        return True

    @running.setter
    def running(self, value):
        raise AttributeError("Component state should not be changed.")

    async def start(self):
        await super().start()
        self._running = True

    async def stop(self):
        await super().stop()
        self._running = False

    async def filter(self, on="key", value=None, func=lambda msg, value: True):
        async for msg in self:
            target = vars(msg)[on]
            if func(target, value) is True:
                yield msg

    async def items(self):
        async for msg in self:
            yield msg.key, msg.value

    async def take(self, max_, every=5 * 1000):
        pass


class ProducerComponent(AIOKafkaProducer):
    """[summary]
    
    Arguments:
        AIOKafkaProducer {[type]} -- [description]
    
    Raises:
        AttributeError: [description]
    
    Returns:
        [type] -- [description]
    """

    def __init__(self, *args, **config):
        self.key_type = config.pop("key_type", None)
        self.value_type = config.pop("value_type", None)
        self.serializer = config.pop("serializer", json)
        config.setdefault("loop", asyncio.get_event_loop())
        config.update(
            key_serializer=functools.partial(
                serializer, _type=self.key_type, _serializer=self.serializer
            ),
            value_serializer=functools.partial(
                serializer, _type=self.value_type, _serializer=self.serializer
            ),
        )
        super().__init__(*args, **config)

    async def start(self):
        await super().start()
        self._running = True

    async def stop(self):
        await super().stop()
        self._running = False

    @property
    async def running(self):
        if hasattr(self, "_running"):
            return self._running
        return False

    @running.setter
    def running(self, value):
        raise AttributeError("Component state should not be changed.")


@dataclasses.dataclass
class Processor:
    _coro: typing.Coroutine = dataclasses.field(init=True)
    concurrency: int = dataclasses.field(default=1)
    id: str = dataclasses.field(init=False, default_factory=lambda: str(uuid.uuid4()))
    _on_before: typing.Set[typing.Coroutine] = dataclasses.field(
        init=False, default_factory=set
    )
    _on_after: typing.Set[typing.Coroutine] = dataclasses.field(
        init=False, default_factory=set
    )

    @property
    def on_before(self):
        return self._on_before

    @on_before.setter
    def on_before(self, value):
        if not asyncio.iscoroutinefunction(value):
            raise Exception("Invalid value type")
        self._on_before.add(value)

    @property
    def on_after(self):
        return self._on_after

    @on_after.setter
    def on_after(self, value):
        if not asyncio.iscoroutinefunction(value):
            raise Exception("Invalid value type")
        self._on_after.add(value)

    async def coro(self, stream):
        await self._coro(stream)
        # breakpoint()
        # async for msg in stream:
        #     for before_coro in self.on_before:
        #         msg = await before_coro(msg)
        #     msg = await self._coro(msg)
        #     for after_coro in self.on_before:
        #         msg = await after_coro(msg)


@dataclasses.dataclass
class Worker:
    coro: typing.Coroutine
    task: asyncio.Task = dataclasses.field(default=None)
    processor: Processor = dataclasses.field(init=True, default=None)
    stream: ConsumerComponent = dataclasses.field(init=True, default=None)
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    async def start(self):
        await self.stream.start()
        self.task = asyncio.create_task(self.coro(self.stream))

    async def stop(self):
        self.task.cancel()
        await self.stream.stop()
        self.task = None
        self.stream = None

    @property
    async def running(self):
        if not await self.stream.running:
            return False
        try:
            self.task.result()
        except asyncio.InvalidStateError:
            result = True
        except:
            result = False
        return result


@dataclasses.dataclass
class ConsumerGroupeComponent:
    name: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    key_type: typing.Any = dataclasses.field(default=None, init=True)
    value_type: typing.Any = dataclasses.field(default=None, init=True)
    topics: typing.List[str] = dataclasses.field(init=True, default_factory=list)
    config: typing.Dict[str, typing.Any] = dataclasses.field(
        default_factory=dict, init=True
    )
    processors: typing.List[Processor] = dataclasses.field(default_factory=list)
    workers: typing.List[Worker] = dataclasses.field(default_factory=list)

    @property
    def client_id(self):
        return f"cid-{self.name}"

    @property
    def group_id(self):
        return f"cg-{self.name}"

    async def run(self):
        await self.start()
        await self.watchdog()

    async def watchdog(self):
        while self.workers:
            for idx, worker in enumerate(self.workers):
                try:
                    worker.task.exception()
                except asyncio.InvalidStateError:
                    continue
                except asyncio.CancelledError:
                    log.warning(
                        f"Restarting consumer :[{self.name}.{worker.processor.id}] ..."
                    )
                    del self.workers[idx]
                    self.worker_factory(worker.processor)
                    log.warning(
                        f"Consumer :[{self.name}:{worker.processor.id}] now running."
                    )
                except:
                    log.warning(f"Consumer :[{self.name}.{worker.processor.id}] died.")
                    log.warning(
                        f"Restarting consumer :[{self.name}:{worker.processor.id}] ..."
                    )
                    del self.workers[idx]
                    self.worker_factory(worker.processor)
                    log.warning(
                        f"Consumer :[{self.name}:{worker.processor.id}] now running."
                    )
            await asyncio.sleep(5 * 1000)

    async def start(self):
        for processor in self.processors:
            print(f"Starting : {processor.coro.__name__}")
            await self.worker_factory(processor)

    async def stop(self):
        for worker in self.workers:
            await worker.stop()

    async def worker_factory(self, processor: Processor):
        for idx in range(processor.concurrency):
            stream = ConsumerComponent(
                *self.topics,
                **{
                    **self.config,
                    "key_type": self.key_type,
                    "value_type": self.value_type,
                    "client_id": f"{self.client_id}-{processor.id}-{idx}",
                    "group_id": f"{self.group_id}-{processor.id}",
                },
            )
            worker = Worker(coro=processor.coro, processor=processor, stream=stream)
            await worker.start()
            self.workers.append(worker)


def deserializer(value, *, _type=None, _serializer=json):
    assert _serializer is not None, f"Invalid serializer for {value}."
    value = _serializer.loads(value)
    if dataclasses.is_dataclass(_type):
        return _type(**value)
    return value


def serializer(value, *, _type=None, _serializer=json):
    assert _serializer is not None, f"Invalid serializer for {value}."
    if dataclasses.is_dataclass(_type):
        value = _serializer.dumps(dataclasses.asdict(value))
    elif isinstance(value, (dict, list)):
        value = _serializer.dumps(value)
    return value.encode("utf-8") if isinstance(value, str) else value


if __name__ == "__main__":
    pass
