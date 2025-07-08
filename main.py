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

def parse_datoms_message(message):
    """Parse JSON message for TOPIC_DATOMS according to database schema."""
    try:
        data = json.loads(message)
        
        parsed_data = {
            'id': data.get('id', ''),
            'category': data.get('category', ''),
            'event_time': data.get('event_time', 0),
            'generated_at': data.get('generated_at', 0),
            'rule_template_name': data.get('rule_template_name', ''),
            'message': data.get('message', ''),
            'rule_param': data.get('rule_param', ''),
            'param_threshold': data.get('param_threshold'),  # Can be NULL
            'param_value': data.get('param_value', 0),
            'entity_type': data.get('entity_type', ''),
            'entity_id': data.get('entity_id', 0)
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
        
        # Create custom ID as specified: 'alert_10_event_26'
        alert_id = data.get('alert', {}).get('id', 0)
        event_id = data.get('event', {}).get('id', 0)
        custom_id = f"alert_{alert_id}_event_{event_id}"
        
        # Parse device time and alert/event timestamps
        device_time = data.get('device', {}).get('time', '')
        alert_raised_at = data.get('alert', {}).get('raised_at', '')
        alert_cleared_at = data.get('alert', {}).get('cleared_at')
        alert_checked_at = data.get('alert', {}).get('checked_at', '')
        event_raised_at = data.get('event', {}).get('raised_at', '')
        event_cleared_at = data.get('event', {}).get('cleared_at', '')
        event_checked_at = data.get('event', {}).get('checked_at', '')
        
        parsed_data = {
            'id': custom_id,
            'alert_id': alert_id,
            'device_id': data.get('device', {}).get('id', 0),
            'device_uniqueid': data.get('device', {}).get('uniqueid', ''),
            'device_name': data.get('device', {}).get('name', ''),
            'device_time': device_time,
            'location_lat': data.get('location', {}).get('lat', 0.0),
            'location_long': data.get('location', {}).get('long', 0.0),
            'alert_name': data.get('alert', {}).get('name', ''),
            'alert_raised_at': alert_raised_at,
            'alert_cleared_at': alert_cleared_at,
            'alert_checked_at': alert_checked_at,
            'event_name': data.get('event', {}).get('name', ''),
            'event_id': event_id,
            'event_value': data.get('event', {}).get('value', 0.0),
            'event_raised': data.get('event', {}).get('raised', False),
            'event_raised_at': event_raised_at,
            'event_cleared_at': event_cleared_at,
            'event_checked_at': event_checked_at,
            'event_type': data.get('event', {}).get('type', '')
        }
        
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

            # Parse JSON based on topic
            if topic == TOPIC_DATOMS:
                parsed_data = parse_datoms_message(message)
                if parsed_data:
                    # Here you can add database insertion logic for DATOMS
                    pass
                    
            elif topic == TOPIC_ZEPTO:
                parsed_data = parse_zepto_message(message)
                if parsed_data:
                    # Here you can add database insertion logic for ZEPTO
                    pass
                
                # Keep existing TCP logic for ZEPTO alerts
                try:
                    data = json.loads(message)
                    alert_name = data.get("alert", {}).get("name")
                    if alert_name:
                        tcp_msg = f"axe,{alert_name}@"
                        send_tcp_message(tcp_msg)
                except json.JSONDecodeError:
                    print("Failed to parse message as JSON for TCP.")
                except Exception as e:
                    print(f"Error processing alert for TCP: {e}")

    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    test_consumer()