import asyncio
import sys

from aiokafka import AIOKafkaProducer
from faqstorer import settings
import structlog

slog = structlog.get_logger(__name__)

def get_kafka_producer():
    loop = asyncio.get_event_loop()
    slog.info("Producer is connecting to Kafka", bootstrap_servers=settings.queues_urls, topic=settings.topic)
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=settings.queues_urls,
                                request_timeout_ms=settings.kafka_producer_timeout, retry_backoff_ms=1000)
    slog.info("Connected.")
    loop.run_until_complete(producer.start())
    return producer

async def destroy_all():
    loop = asyncio.get_event_loop()
    await asyncio.sleep(1)
    slog.debug("Killing tasks...")
    for task in asyncio.Task.all_tasks():
        task.cancel()
        await asyncio.sleep(1)
    slog.debug("Stopping loop")
    await loop.stop()
    await asyncio.sleep(1)
    slog.debug("Closing loop")
    await loop.close()
    await asyncio.sleep(1)
    slog.debug("Exiting.")
    sys.exit()
