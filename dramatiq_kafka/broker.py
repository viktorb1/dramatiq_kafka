from base64 import b64decode
from typing import Any, Dict, Iterable, List, Optional, TypeVar

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from dramatiq import Consumer, Message, Broker, Middleware, MessageProxy
from collections import deque
import json
from time import sleep


class KafkaBroker(Broker):
    _instance = None

    # singleton pattern because dramatiq_kafka AppConfig runs multiple times
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        *,
        bootstrap_servers: str = None,
        topic: str = "default",
        dead_letter_topic: str = None,
        group_id: str = "default",
        middleware: Optional[List[Middleware]] = None,
        partitions: Optional[List[int]] = None,
    ) -> None:
        super().__init__(middleware=middleware)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.dead_letter_topic = dead_letter_topic
        self.queues = []

        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
        )
        if partitions:
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            self.consumer.assign(topic_partitions)
        else:
            self.consumer.subscribe(topic)

    def declare_queue(self, queue_name: str) -> None:
        self.queues.append(queue_name)

    def enqueue(
        self,
        message: Message,
        *,
        delay: Optional[int] = None,
    ) -> Message:
        if delay:
            sleep(delay)

        self.producer.send(
            message.queue_name, json.dumps(message.asdict()).encode("utf-8")
        )
        self.producer.flush()
        return message

    def consume(
        self, queue_name: str, prefetch: int = 1, timeout: int = 30000
    ) -> Consumer:
        return _KafkaConsumer(
            self.consumer,
            self.producer,
            topic=queue_name,
            dead_letter_topic=self.dead_letter_topic,
        )

    def get_declared_queues(self) -> Iterable[str]:
        return self.queues

    def get_declared_delay_queues(self) -> Iterable[str]:
        # Doesn't support delay queues
        return []


class _KafkaConsumer(Consumer):
    def __init__(
        self,
        consumer,
        producer,
        topic=None,
        dead_letter_topic=None,
        requeue_topic=None,
    ):
        self.consumer = consumer
        self.producer = producer
        self.dead_letter_topic = dead_letter_topic
        self.requeue_topic = requeue_topic
        self.topic = topic
        self.messages = deque()

    def ack(self, message: Message) -> None:
        self.consumer.commit()

    def nack(self, message: Message) -> None:
        if self.dead_letter_topic:
            rejected_message = message.asdict()
            self.producer.send(
                self.dead_letter_topic,
                json.dumps(rejected_message).encode("utf-8"),
            )
        self.consumer.commit()

    def requeue(self, messages: Iterable[Message]) -> None:
        if self.requeue_topic:
            for message in messages:
                requeued_message = message.asdict()
                self.producer.send(
                    self.requeue_topic,
                    json.dumps(requeued_message).encode("utf-8"),
                )
            self.producer.flush()

    def __next__(self) -> Optional[Message]:
        try:
            return self.messages.popleft()
        except IndexError:
            batch = self.consumer.poll(timeout_ms=100, max_records=20)

            if not batch:
                return None

            for tp, records in batch.items():
                for record in records:
                    message = Message.decode(record.value)
                    self.messages.append(_KafkaMessage(message))

            try:
                return self.messages.popleft()
            except IndexError:
                return None


class _KafkaMessage(MessageProxy):
    def __init__(self, message: Message) -> None:
        super().__init__(message)
