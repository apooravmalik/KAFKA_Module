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

DB_CONFIG = {
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server'),
    'server': os.getenv('DB_SERVER', 'APOORAV_MALIK'),
    'database': os.getenv('DB_DATABASE', 'sop-manage'),
    'username': os.getenv('DB_USERNAME', 'sa'),
    'password': os.getenv('DB_PASSWORD', ''),
    'trust_cert': os.getenv('DB_TRUST_CERT', 'yes'),
}