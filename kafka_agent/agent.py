import dataclasses
import json
import logging
import typing
import uuid
import functools

from aiokafka import ConsumerRebalanceListener
from kafka import KafkaAdminClient, KafkaClient
from kafka.admin import ConfigResource, NewTopic

from kafka_agent.core import (
    ADMIN_DEFAULT_CONFIG,
    CONSUMER_DEFAULT_CONFIG,
    DEFAULT_CONFIG,
    DEFAULT_TOPIC_CONFIG,
    PRODUCER_DEFAULT_CONFIG,
    ConsumerComponent,
    ConsumerGroupeComponent,
    Processor,
    ProducerComponent,
)

log = logging.getLogger(__name__)


class kafka_agent:
    def __init__(self, coro: typing.Coroutine):
        self.coro = coro
        self.producer_running = False
        self.kafka_client: KafkaClient = None
        self.producer: ProducerComponent = None
        self.admin_client: KafkaAdminClient = None
        self.consumer: ConsumerGroupeComponent = None

    @property
    def name(self):
        return f"agent-{self.coro.__name__}"

    def create_topic(self, topic, partitions=1, replicas=3, retention_ms=None):
        assert self.admin_client is not None, "Agent must be configured."
        topic_configs = {}
        if retention_ms is not None:
            topic_configs["retention.ms"] = retention_ms
        new_topic = NewTopic(topic, partitions, replicas, topic_configs=topic_configs)
        return self.admin_client.create_topics([new_topic])

    def get_topic_partition_count(self, topic):
        assert self.kafka_client is not None, "Agent must be configured."
        topic_partition_ids = self.kafka_client.get_partition_ids_for_topic(topic)
        return len(topic_partition_ids)

    # def update_partitions(self, topics, count):
    #     assert self.admin_client is not None, "Agent must be configured."
    #     topics = {
    #         topic: NewTopic(topic, count, DEFAULT_TOPIC_CONFIG["replicas"])
    #         for topic in topics
    #     }
    #     return self.admin_client.create_partitions(topics)

    async def consume(self):
        assert self.consumer is not None, "Agent must be configured."
        try:
            await self.consumer.run()
        except Exception:
            log.error("Shuting down consumers", exc_info=True)
            await self.consumer.stop()

    async def start_producer(self):
        await self.producer.start()
        self.producer_running = True

    async def send(self, *topics, **kwargs):
        assert self.producer is not None, "Agent must be configured."
        if not self.producer_running:
            await self.start_producer()

        results = []
        for topic in topics or self.topics:
            results.append((topic, await self.producer.send(topic, **kwargs)))
        return results

    async def send_and_wait(self, *topics, **kwargs):
        assert self.producer is not None, "Agent must be configured."
        if not self.producer_running:
            await self.start_producer()

        results = []
        for topic in topics or self.topics:
            results.append((topic, await self.producer.send_and_wait(topic, **kwargs)))
        return results

    def configure(
        self,
        topics=None,
        key_type=None,
        value_type=None,
        broker=None,
        consumer=None,
        producer=None,
        admin=None,
        client=None,
        concurrency=1,
    ):
        self.broker = broker or "localhost:9092"
        self.topics = topics or [f"agent-toipc-{self.name}"]
        self.key_type = key_type
        self.value_type = value_type
        self.concurrency = concurrency
        self.consumer_config = consumer or {}
        self.producer_config = producer or {}
        self.admin_config = admin or {}
        self.client_config = client or {}
        self.consumer = ConsumerGroupeComponent(
            name=self.name,
            topics=self.topics,
            key_type=self.key_type,
            value_type=self.value_type,
            processors=[Processor(_coro=self.coro, concurrency=self.concurrency)],
            config={
                **DEFAULT_CONFIG,
                **CONSUMER_DEFAULT_CONFIG,
                **self.consumer_config,
            },
        )
        self.producer = ProducerComponent(
            key_type=self.key_type,
            value_type=self.value_type,
            **{
                **DEFAULT_CONFIG,
                **PRODUCER_DEFAULT_CONFIG,
                **self.consumer_config,
                "bootstrap_servers": self.broker,
                "client_id": f"{self.name}:producer:{id(self)}",
            },
        )

        self.admin_client = KafkaAdminClient(
            **{
                **DEFAULT_CONFIG,
                **ADMIN_DEFAULT_CONFIG,
                **self.admin_config,
                "client_id": f"{self.name}:admin:{id(self)}",
            }
        )

        self.kafka_client = KafkaClient(DEFAULT_CONFIG["bootstrap_servers"])

        for topic in self.topics:
            count = self.get_topic_partition_count(topic)
            if count == 0:
                self.create_topic(
                    topic, {**DEFAULT_TOPIC_CONFIG, "partitions": self.concurrency}
                )
            if self.concurrency > count:
                self.update_partitions([topic], self.concurrency)

    async def __call__(self, *args, **kwargs):
        return await self.coro(*args, **kwargs)
