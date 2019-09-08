import asyncio
import json
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka import KafkaAdminClient
from dataclasses import asdict


class BaseApp:

    components = {
        "producer": AIOKafkaProducer,
        "consumer": AIOKafkaConsumer,
        "admin_client": KafkaAdminClient,
    }

    def __init__(
        self,
        name,
        topic=None,
        key_type=None,
        value_type=None,
        serializer=json,
        *,
        loop=None,
        broker=None,
    ):
        self.name = name
        self.topic = topic or self.name
        self.broker = broker or "localhost:9092"
        self.loop = loop or asyncio.get_event_loop()
        self._key_type = key_type
        self._value_type = value_type
        self._serializer = serializer
        for conponent in self.components:
            vars(self)[f"_{conponent}"] = None

    def init_component(self, name: str, config: Dict[str, Any] = {}, overwrite=False):
        if name not in self.components:
            raise Exception("Invalid BaseApp component")

        if (overwrite is False) and (vars(self)[f"_{name}"] is not None):
            raise Exception(f"Component {name} has already been initialized")

        config = {"bootstrap_servers": self.broker, "loop": self.loop, **config}
        if name == "admin_client":
            del config["loop"]
        vars(self)[f"_{name}"] = self.components[name](**config)

    @property
    def producer(self):
        if self._producer is None:
            raise Exception("producer component must be initialized")
        return self._producer

    @producer.setter
    def producer(self, _):
        raise AttributeError(
            "Producer can't be set, it must be initialized, "
            f"please use {self.__class__.__name__}.init_component('producer', config)"
        )

    async def _init_producer(self, config: Dict[str, Any] = {}, overwrite=False):
        key_serializer = config.pop("key_serializer", None)
        value_serializer = config.pop("value_serializer", None)
        if self._key_type:
            key_serializer = self.serializer
        if self._value_type:
            value_serializer = self.serializer
        config.update(
            client_id=f"{self.name}-producer",
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        self.init_component("producer", config, overwrite=overwrite)
        await self._producer.start()

    @property
    def consumer(self):
        if self._consumer is None:
            raise Exception("consumer component must be initialized")
        return self._consumer

    @consumer.setter
    def consumer(self, _):
        raise AttributeError(
            "Consumer can't be set, it must be initialized, "
            f"please use {self.__class__.__name__}.init_component('consumer', config)"
        )

    async def _init_consumer(self, config: Dict[str, Any] = {}, overwrite=False):
        key_deserializer = config.pop("key_deserializer", None)
        value_deserializer = config.pop("value_deserializer", None)
        if self._key_type:
            key_deserializer = self.deserializer
        if self._value_type:
            value_deserializer = self.deserializer
        config.update(
            client_id=f"{self.name}-consumer",
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )
        self.init_component("consumer", config, overwrite=overwrite)
        await self._consumer.start()

    @property
    def admin_client(self):
        if self._admin_client is None:
            raise Exception("admin_client component must be initialized")
        return self._admin_client

    @admin_client.setter
    def admin_client(self, _):
        raise AttributeError(
            "admin_client can't be set, it must be initialized, "
            f"please use {self.__class__.__name__}.init_component('admin_client', config)"
        )

    def _init_admin_client(self, config: Dict[str, Any] = {}, overwrite=False):
        config.update(client_id=f"{self.name}-admin-client")
        self.init_component("admin_client", config, overwrite=overwrite)

    def serializer(self, value):
        result = self._serializer.dumps(asdict(value))
        if isinstance(result, str):
            result = result.encode("utf-8")
        return result

    def deserializer(self, value):
        return self._serializer.loads(value)

    async def start(self, *, producer={}, consumer={}, admin_client={}, overwrite=True):
        if overwrite:
            try:
                await self.stop()
            except Exception:
                pass
        self._init_admin_client(config=admin_client, overwrite=overwrite)
        await self._init_producer(config=producer, overwrite=overwrite)
        await self._init_consumer(config=consumer, overwrite=overwrite)

    async def stop(self):
        self.admin_client.close()
        await self.producer.stop()
        await self.consumer.stop()


class KafkaAgent:
    def __init__(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        pass

    def agent(self, *args, **kwargs):
        pass


class kafka_agent:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args):
        return self.func(*args)

    def on_before_message(self, func):
        pass

    def on_after_message(self, func):
        pass

    def config(self, *args, **kwargs):
        pass

    async def run(self):
        pass

    async def stop(self):
        pass

    async def filter(self, msg, *, func=None):
        pass

    async def items(self):
        pass

    async def take(self, max_, every=5 * 1000):
        pass

    async def send(self, *args, **kwargs):
        pass

    async def __aiter__(self):
        pass

    async def __anext__(self):
        pass





@kafka_agent
def async def consumer(stream):
    async for key, value in stream.items():
        print(key, value)

consumer.run()

consumer.stop()