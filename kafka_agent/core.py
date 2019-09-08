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


class ConsumerComponent(AIOKafkaConsumer):
    def __init__(self, *args, **kwargs):
        self.key_type = kwargs.pop("key_type", None)
        self.value_type = kwargs.pop("value_type", None)
        super().__init__(self, *args, **kwargs)

    @property
    def running(self):
        if hasattr(self, "_running"):
            return self._running
        return False

    @running.setter
    def running(self, value):
        raise AttributeError("Component state should not be changed.")

    async def start(self):
        await super().start()
        self._running = True

    async def stop(self):
        await super().stop()
        self._running = False


class ProducerComponent(AIOKafkaProducer):
    pass


class AdminComponent(KafkaAdminClient):
    pass
