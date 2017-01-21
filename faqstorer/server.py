import json

from aiohttp import web
import arrow
from faqstorer.utils import destroy_all, get_kafka_producer
import structlog

from faqstorer import settings

slog = structlog.get_logger(__name__)

async def kafka_send(kafka_producer, data, topic):
    message = {
        'data': data,
        'received': str(arrow.utcnow())
    }
    message_json_bytes = bytes(json.dumps(message), 'utf-8')
    await kafka_producer.send_and_wait(topic, message_json_bytes)


async def handle(request):
    post_data = await request.json()

    try:
        await kafka_send(request.app['kafka_producer'], post_data, topic=settings.topic)
    except:
        slog.exception("Kafka Error")
        slog.error("Destroying all to boot the worker again.")
        await destroy_all()

    slog.info("Responded")
    return web.Response(status=200)

app = web.Application()
app.router.add_route('POST', '/store', handle)

app['kafka_producer'] = get_kafka_producer()



