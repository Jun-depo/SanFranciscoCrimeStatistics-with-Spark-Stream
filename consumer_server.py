import asyncio

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:

        # consume 5 messages at a time and has a timeout 1 second.

        # Print the key and value of message(s) you consumed

        messages = c.consume(5, 1.0)
        for message in messages:
            if message is None:
                print(" No message consumed by consumer")
            elif message.error() is not None:
                print(f"Message error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")
                
        await asyncio.sleep(0.01)

def main():
    
    # """Checks for topic and creates the topic if it does not exist"""
    # client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(consume_topic("sf.crimes.counts.1hour_window"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def consume_topic(topic_name):
    """Consumer tasks"""
    t = asyncio.create_task(consume(topic_name))    
    await t


if __name__ == "__main__":
    main()