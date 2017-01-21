import asyncio
import json
import time

from aiokafka import AIOKafkaConsumer
import aiopg
import arrow
import structlog

from . import settings

slog = structlog.get_logger(__name__)

loop = asyncio.get_event_loop()

async def store_bulk(logg_cur, to_store):
    args = []
    for received, data_record in to_store:
        slog.debug("Storing record.", record=data_record)
        record = json.dumps(data_record)
        tpl = (str(received), str(record))
        row = await logg_cur.mogrify("(%s,%s)", tpl)
        args.append(row.decode('utf8'))

    if args:
        query = 'INSERT INTO data (received, record) VALUES ' + ','.join(args)
        slog.info('executing', query=query, args=args)
        await logg_cur.execute(query)

    slog.debug("Records stored", stored_records=len(args))


async def consume(queue, dbs_connected):
    try:
        slog.info('Waiting for dbs connections.')
        await asyncio.wait_for(dbs_connected, timeout=settings.wait_for_databases)

        slog.info('Connecting to Kafka.', topic=settings.topic)
        consumer = AIOKafkaConsumer(
            settings.topic, loop=loop, bootstrap_servers=settings.queues_urls,
            group_id='rancher_consumers'
        )
        slog.debug('Starting consumer')
        await consumer.start()

        slog.debug('Starting to consume from Kafka queue')
        async for msg in consumer:
            try:
                message = json.loads(msg.value.decode("utf-8"))
                data = message.get('data')
                received = message.get('received')

                slog.debug("Added one record to local queue", qsize=queue.qsize())
                await queue.put((received, data))
            except:
                slog.exception("Unhandled error")
                await asyncio.sleep(0.01)

        await consumer.stop()
    except:
        slog.exception("Unhandled error during kafka (de)connection")
        raise


async def start_flushing(queue, dbs_connected):
    db_logg = await aiopg.create_pool(settings.logs_db_url)

    while True:
        slog.info('Connecting to logs DB')
        async with db_logg.acquire() as logg_conn, logg_conn.cursor() as logg_cur:
            slog.info('Logs db connected')
            await keep_flushing(dbs_connected, logg_cur, queue)
        slog.info("DB connections should be closed now. Waiting and reconnecting.")
        await asyncio.sleep(2)


async def keep_flushing(dbs_connected, logg_cur, queue):
    import traceback
    try:
        dbs_connected.set_result(True)
    except asyncio.futures.InvalidStateError:
        slog.info("Unable to set up DB connection flag.")
    last_stored_time = time.time()
    while True:
        if not queue.empty() and (
                    queue.qsize() > settings.batch_flush_size or
                        time.time() - last_stored_time > settings.batch_max_time):
            to_store = []
            while not queue.empty():
                to_store.append(await queue.get())
            try:
                await store_bulk(logg_cur, to_store)
            except:
                traceback.print_exc()
                slog.exception("Consumer: DB down, I'll try to reconnect")
                break
            last_stored_time = time.time()
        await asyncio.sleep(settings.batch_sleep)
    slog.debug("I'm out of infinite loop.")


def main():
    slog.info("Consumer started")

    dbs_connected = asyncio.Future()
    batch = asyncio.Queue(maxsize=settings.batch_max_size)
    asyncio.ensure_future(consume(batch, dbs_connected))
    asyncio.ensure_future(start_flushing(batch, dbs_connected))
    loop.run_forever()
    loop.close()
    slog.info("Consumer finished.")


if __name__ == '__main__':
    main()
