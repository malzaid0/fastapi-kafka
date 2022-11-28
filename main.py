import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import Body, FastAPI
from loguru import logger

app = FastAPI()

KAFKA_URL = "localhost:9092"
KAFKA_TOPIC = "test-topic"

loop = asyncio.get_event_loop()
producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_URL)
consumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop, bootstrap_servers=KAFKA_URL)


async def consume():
    await consumer.start()
    logger.info("Initializing Kafka")
    try:
        async for msg in consumer:
            logger.info(msg.value.decode("ascii"))
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    logger.info("Booting up")
    await producer.start()
    loop.create_task(consume())


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down")
    await producer.stop()
    await consumer.stop()


@app.post("/produce")
async def kafka_produce(msg: dict = Body()):
    await producer.send(KAFKA_TOPIC, json.dumps(msg).encode("ascii"))
    return "Streamed successfully"


def is_perfect_cycle(num_list):
    registered = []
    if type(num_list) != list or len(num_list) == 0:
        return False
    temp = 0
    for i, num in enumerate(num_list):
        if num >= len(num_list):
            return False
        if i + 1 == len(num_list) and num_list[temp] != num_list[0]:
            return False
        registered.append(num_list[i])
        temp = num_list[num]
    if registered == num_list:
        return True
    return False


@app.post("/perfect-cycle")
def root(body: dict = Body()):
    result = dict()
    for key, value in body.items():
        result[key] = is_perfect_cycle(value)
    return result
