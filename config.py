from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Kafka client configuration
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
    "security.protocol": os.getenv("SECURITY_PROTOCOL"),
    "sasl.mechanisms": os.getenv("SASL_MECHANISM"),
    "sasl.username": os.getenv("SASL_USERNAME"),
    "sasl.password": os.getenv("SASL_PASSWORD"),
}

TOPIC_ZEPTO = os.getenv("TOPIC_ZEPTO")
TOPIC_DATOMS = os.getenv("TOPIC_DATOMS")

GROUP_ID = os.getenv("GROUP_ID")

TCP_IP = os.getenv("TCP_IP")
TCP_PORT = int(os.getenv("TCP_PORT"))