"""
Message Broker Adapter with dual-mode support:
1. Real Kafka / Redpanda broker (if active on localhost:9092)
2. High-performance asynchronous pub/sub event bus fallback (if Kafka/Docker is not running)
Provides identical Producer/Consumer APIs.
"""

import asyncio
import json
import logging
from typing import Callable, Dict, List

logger = logging.getLogger("BrokerAdapter")

class InMemoryTopic:
    def __init__(self, name: str):
        self.name = name
        self.queue = asyncio.Queue(maxsize=5000)
        self.subscribers: List[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=1000)
        self.subscribers.append(q)
        return q

    async def publish(self, message: dict):
        # Push to internal queue
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            pass

        # Broadcast to active subscriber queues
        dead = []
        for sub in self.subscribers:
            try:
                sub.put_nowait(message)
            except asyncio.QueueFull:
                try:
                    sub.get_nowait()
                    sub.put_nowait(message)
                except Exception:
                    pass
            except Exception:
                dead.append(sub)
        for d in dead:
            if d in self.subscribers:
                self.subscribers.remove(d)


class BrokerAdapter:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.is_kafka_connected = False
        self.kafka_producer = None
        self.in_memory_topics: Dict[str, InMemoryTopic] = {}

    def get_or_create_topic(self, topic_name: str) -> InMemoryTopic:
        if topic_name not in self.in_memory_topics:
            self.in_memory_topics[topic_name] = InMemoryTopic(topic_name)
        return self.in_memory_topics[topic_name]

    async def connect(self):
        """Attempt to connect to Kafka/Redpanda; fall back gracefully to async event bus."""
        try:
            from aiokafka import AIOKafkaProducer
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await asyncio.wait_for(producer.start(), timeout=1.5)
            self.kafka_producer = producer
            self.is_kafka_connected = True
            logger.info(f"Connected to Kafka/Redpanda broker at {self.bootstrap_servers}")
        except Exception as e:
            self.is_kafka_connected = False
            self.kafka_producer = None
            logger.info("Kafka/Redpanda broker not detected at localhost:9092. Running high-throughput async event bus mode.")

    async def send(self, topic: str, value: dict):
        """Send event to Kafka topic and/or internal event bus."""
        if self.is_kafka_connected and self.kafka_producer:
            try:
                await self.kafka_producer.send_and_wait(topic, value)
            except Exception as e:
                logger.warning(f"Kafka send failed: {e}. Falling back to internal bus.")
        
        # Always publish to memory bus for sub-millisecond local consumers
        mem_topic = self.get_or_create_topic(topic)
        await mem_topic.publish(value)

    async def subscribe(self, topic: str):
        """Returns an async generator yielding messages from the topic."""
        mem_topic = self.get_or_create_topic(topic)
        sub_queue = mem_topic.subscribe()
        while True:
            msg = await sub_queue.get()
            yield msg

    async def close(self):
        if self.kafka_producer:
            try:
                await self.kafka_producer.stop()
            except Exception:
                pass

# Global Singleton
broker = BrokerAdapter()
