from confluent_kafka import Consumer
from config import KAFKA_CONFIG, TOPIC_DATOMS, TOPIC_ZEPTO, GROUP_ID

def test_consumer():
    print("Starting Kafka Consumer...")

    consumer_conf = KAFKA_CONFIG.copy()
    consumer_conf.update({
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest"
    })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_ZEPTO, TOPIC_DATOMS])

    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                print("No messages received. Retrying...")
                continue
            if msg.error():
                print("Error:", msg.error())
                continue

            topic = msg.topic()
            message = msg.value().decode('utf-8')
            print(f"[{topic}] Received message: {message}")

    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    test_consumer()
