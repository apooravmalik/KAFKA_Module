import os
import json
import socket
import logging
from datetime import datetime
from confluent_kafka import Consumer
from config import KAFKA_CONFIG, TOPIC_DATOMS, TOPIC_ZEPTO, GROUP_ID, TCP_IP, TCP_PORT
from database import get_db
from models import Datoms, ThingsUp
import json

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

import json

def parse_datoms_message(message):
    """Parse JSON message for TOPIC_DATOMS according to database schema."""
    try:
        data = json.loads(message)

        # Handle nested fields inside "details"
        details = data.get('details', {})

        parsed_data = {
            'id': data.get('id', ''),
            'category': data.get('category', ''),
            'event_time': data.get('time', 0),  # Assuming 'time' is event_time
            'generated_at': details.get('generated_at', 0),
            'rule_template_name': details.get('rule_template_name', ''),
            'message': details.get('message', ''),
            'rule_param': details.get('rule_param', ''),
            'param_threshold': details.get('param_threshold'),  # Can be None
            'param_value': details.get('param_value', 0),
            'entity_type': details.get('entity_type', ''),
            'entity_id': details.get('entity_id', 0)
        }

        print(f"[DATOMS PARSED] ID: {parsed_data['id']}")
        print(f"[DATOMS PARSED] Category: {parsed_data['category']}")
        print(f"[DATOMS PARSED] Event Time: {parsed_data['event_time']}")
        print(f"[DATOMS PARSED] Generated At: {parsed_data['generated_at']}")
        print(f"[DATOMS PARSED] Rule Template: {parsed_data['rule_template_name']}")
        print(f"[DATOMS PARSED] Message: {parsed_data['message']}")
        print(f"[DATOMS PARSED] Rule Param: {parsed_data['rule_param']}")
        print(f"[DATOMS PARSED] Param Threshold: {parsed_data['param_threshold']}")
        print(f"[DATOMS PARSED] Param Value: {parsed_data['param_value']}")
        print(f"[DATOMS PARSED] Entity Type: {parsed_data['entity_type']}")
        print(f"[DATOMS PARSED] Entity ID: {parsed_data['entity_id']}")
        print("-" * 50)

        return parsed_data

    except json.JSONDecodeError as e:
        print(f"[DATOMS PARSE ERROR] Invalid JSON: {e}")
        return None
    except Exception as e:
        print(f"[DATOMS PARSE ERROR] {e}")
        return None




def parse_zepto_message(message):
    """Parse JSON message for TOPIC_ZEPTO according to database schema."""
    try:
        data = json.loads(message)

        # Extract alert and device details
        alert = data.get('alert', {})
        device = data.get('device', {})
        device_location = device.get('location', {})

        # Extract event (there is only one event in 'events', with dynamic key)
        events = data.get('events', {})
        if not events:
            raise ValueError("No events found in message.")

        # Assume first event (since events is a dict with 1 item)
        event_name, event_data = next(iter(events.items()))

        # Build custom ID
        alert_id = alert.get('alertid', 0)
        event_id = event_data.get('id', 0)
        custom_id = f"alert_{alert_id}_event_{event_id}"

        parsed_data = {
            'id': custom_id,
            'alert_id': alert_id,
            'device_id': device.get('id', 0),
            'device_uniqueid': device.get('uniqueid', ''),
            'device_name': device.get('name', ''),
            'device_time': device.get('devicetime', ''),
            'location_lat': device_location.get('latitude', 0.0),
            'location_long': device_location.get('longitude', 0.0),
            'alert_name': alert.get('name', ''),
            'alert_raised_at': alert.get('raised_at', ''),
            'alert_cleared_at': alert.get('cleared_at'),
            'alert_checked_at': alert.get('checked_at', ''),
            'event_name': event_name,
            'event_id': event_id,
            'event_value': event_data.get('value', 0.0),
            'event_raised': event_data.get('raised', False),
            'event_raised_at': event_data.get('raised_at', ''),
            'event_cleared_at': event_data.get('cleared_at', ''),
            'event_checked_at': event_data.get('checked_at', ''),
            'event_type': event_data.get('type', '')
        }

        # Debug print (optional)
        print(f"[ZEPTO PARSED] ID: {parsed_data['id']}")
        print(f"[ZEPTO PARSED] Alert ID: {parsed_data['alert_id']}")
        print(f"[ZEPTO PARSED] Device ID: {parsed_data['device_id']}")
        print(f"[ZEPTO PARSED] Device Name: {parsed_data['device_name']}")
        print(f"[ZEPTO PARSED] Device Unique ID: {parsed_data['device_uniqueid']}")
        print(f"[ZEPTO PARSED] Device Time: {parsed_data['device_time']}")
        print(f"[ZEPTO PARSED] Location: ({parsed_data['location_lat']}, {parsed_data['location_long']})")
        print(f"[ZEPTO PARSED] Alert Name: {parsed_data['alert_name']}")
        print(f"[ZEPTO PARSED] Alert Raised At: {parsed_data['alert_raised_at']}")
        print(f"[ZEPTO PARSED] Alert Cleared At: {parsed_data['alert_cleared_at']}")
        print(f"[ZEPTO PARSED] Alert Checked At: {parsed_data['alert_checked_at']}")
        print(f"[ZEPTO PARSED] Event Name: {parsed_data['event_name']}")
        print(f"[ZEPTO PARSED] Event ID: {parsed_data['event_id']}")
        print(f"[ZEPTO PARSED] Event Value: {parsed_data['event_value']}")
        print(f"[ZEPTO PARSED] Event Raised: {parsed_data['event_raised']}")
        print(f"[ZEPTO PARSED] Event Raised At: {parsed_data['event_raised_at']}")
        print(f"[ZEPTO PARSED] Event Cleared At: {parsed_data['event_cleared_at']}")
        print(f"[ZEPTO PARSED] Event Checked At: {parsed_data['event_checked_at']}")
        print(f"[ZEPTO PARSED] Event Type: {parsed_data['event_type']}")
        print("-" * 50)

        return parsed_data

    except json.JSONDecodeError as e:
        print(f"[ZEPTO PARSE ERROR] Invalid JSON: {e}")
        return None
    except Exception as e:
        print(f"[ZEPTO PARSE ERROR] {e}")
        return None


def save_to_database(topic, message_dict):
    """Insert the Kafka message into the appropriate database table."""
    db_gen = get_db()
    db = next(db_gen)

    try:
        if topic == TOPIC_DATOMS:
            record = Datoms(
                id=message_dict.get("id"),
                category=message_dict.get("category"),
                event_time=message_dict.get("event_time"),
                generated_at=message_dict.get("generated_at"),
                rule_template_name=message_dict.get("rule_template_name"),
                message=message_dict.get("message"),
                rule_param=message_dict.get("rule_param"),
                param_threshold=message_dict.get("param_threshold"),
                param_value=message_dict.get("param_value"),
                entity_type=message_dict.get("entity_type"),
                entity_id=message_dict.get("entity_id")
            )

        elif topic == TOPIC_ZEPTO:
            alert_id = message_dict.get("alert_id")
            event_id = message_dict.get("event_id")

            record = ThingsUp(
                alert_id=alert_id,
                device_id=message_dict.get("device_id"),
                device_uniqueid=message_dict.get("device_uniqueid"),
                device_name=message_dict.get("device_name"),
                device_time=message_dict.get("device_time"),
                location_lat=message_dict.get("location_lat"),
                location_long=message_dict.get("location_long"),
                alert_name=message_dict.get("alert_name"),
                alert_raised_at=message_dict.get("alert_raised_at"),
                alert_cleared_at=message_dict.get("alert_cleared_at"),
                alert_checked_at=message_dict.get("alert_checked_at"),
                event_name=message_dict.get("event_name"),
                event_id=event_id,
                event_value=message_dict.get("event_value"),
                event_raised=message_dict.get("event_raised"),
                event_raised_at=message_dict.get("event_raised_at"),
                event_cleared_at=message_dict.get("event_cleared_at"),
                event_checked_at=message_dict.get("event_checked_at"),
                event_type=message_dict.get("event_type")
            )
        else:
            print(f"[DB WARNING] Unsupported topic: {topic}")
            return

        db.add(record)
        db.commit()
        print(f"[DB] Successfully inserted record into {topic}")
    except Exception as e:
        db.rollback()
        print(f"[DB ERROR] Failed to insert into {topic}: {e}")
    finally:
        db.close()

def send_tcp_message(message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((TCP_IP, TCP_PORT))
            sock.sendall(message.encode())
            print(f"[TCP] Sent: {message}")
    except Exception as e:
        print(f"[TCP ERROR] {e}")

def send_tcp_for_topic(topic, parsed_data):
    """Send TCP message based on topic with ID and alert name."""
    try:
        if topic == TOPIC_DATOMS:
            # For DATOMS, use the ID and category as alert name
            tcp_msg = f"axe,{parsed_data['id']},{parsed_data['category']}@"
            send_tcp_message(tcp_msg)
        elif topic == TOPIC_ZEPTO:
            # For ZEPTO, use the custom ID and alert name
            tcp_msg = f"axe,{parsed_data['id']},{parsed_data['alert_name']}@"
            send_tcp_message(tcp_msg)
    except Exception as e:
        print(f"[TCP ERROR] Failed to send TCP message for {topic}: {e}")

def test_consumer():
    print("Starting Kafka Consumer...")

    consumer_conf = KAFKA_CONFIG.copy()
    consumer_conf.update({
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest"
    })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_ZEPTO, TOPIC_DATOMS])

    #for testing
    import time
    start_time = time.time()

    try:
        while time.time() - start_time < 15: #add "while True:" when tested
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

            # Parse JSON based on topic
            if topic == TOPIC_DATOMS:
                parsed_data = parse_datoms_message(message)
                if parsed_data:
                    # Save to database
                    save_to_database(topic, parsed_data)
                    # Send TCP message for DATOMS
                    send_tcp_for_topic(topic, parsed_data)
                    
            elif topic == TOPIC_ZEPTO:
                parsed_data = parse_zepto_message(message)
                if parsed_data:
                    # Save to database
                    save_to_database(topic, parsed_data)
                    # Send TCP message for ZEPTO
                    send_tcp_for_topic(topic, parsed_data)
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    test_consumer()