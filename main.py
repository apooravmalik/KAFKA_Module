import os
import json
import socket
import logging
from datetime import datetime
from confluent_kafka import Consumer
from config import KAFKA_CONFIG, TOPIC_DATOMS, TOPIC_ZEPTO, GROUP_ID, TCP_IP, TCP_PORT

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
# Track loggers and their last used date
topic_loggers = {}
logger_dates = {}

def get_topic_logger(topic_name):
    """Create or refresh a logger for the given topic and current date."""
    current_date = datetime.now().strftime("%d%m%Y")

    # Check if we need to recreate the logger due to date change
    if topic_name not in topic_loggers or logger_dates.get(topic_name) != current_date:
        logger = logging.getLogger(topic_name)

        # Remove old handlers if logger exists
        if topic_name in topic_loggers:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

        log_filename = os.path.join(LOG_DIR, f"{topic_name}-{current_date}.log")
        file_handler = logging.FileHandler(log_filename, encoding="utf-8")
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.setLevel(logging.INFO)
        logger.addHandler(file_handler)
        logger.propagate = False

        topic_loggers[topic_name] = logger
        logger_dates[topic_name] = current_date

    return topic_loggers[topic_name]

def send_tcp_message(message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((TCP_IP, TCP_PORT))
            sock.sendall(message.encode())
            print(f"[TCP] Sent: {message}")
    except Exception as e:
        print(f"[TCP ERROR] {e}")

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
            logger = get_topic_logger(topic)
            logger.info(message)

            print(f"[{topic}] Received message: {message}")

            # TCP logic for specific topic
            if topic == TOPIC_ZEPTO:
                try:
                    data = json.loads(message)
                    alert_name = data.get("alert", {}).get("name")
                    if alert_name:
                        tcp_msg = f"axe,{alert_name}@"
                        send_tcp_message(tcp_msg)
                except json.JSONDecodeError:
                    print("Failed to parse message as JSON.")
                except Exception as e:
                    print(f"Error processing alert: {e}")

    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    test_consumer()
