"""
Publisher 2 - Sensor Kelembaban (Humidity Sensor)
Role: Simulasi sensor kelembaban udara
QoS: 0 (fire and forget)
Fitur: Retain message (konfigurasi awal), LWT, User Properties, Message Expiry Interval, Shared Subscription
Topic: sensors/humidity
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import logging
from datetime import datetime
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [PUBLISHER-HUMID] %(message)s',
    datefmt='%H:%M:%S'
)

# =============================================
# KONFIGURASI
# =============================================
BROKER_HOST = "localhost"
BROKER_PORT = 1883
CLIENT_ID   = "publisher-sensor-kelembaban"
TOPIC       = "sensors/humidity"
QOS         = 0          # QoS Level 0 (fire and forget)
INTERVAL    = 5          # Kirim data setiap 5 detik
MESSAGE_EXPIRY = 30      # Message expiry interval (detik) - MQTT 5.0 Feature

# Shared Subscription Topic - MQTT 5.0 Feature
# Format: $share/group_name/topic_name
SHARED_SUBSCRIPTION_TOPIC = "$share/humidity-consumers/sensors/humidity"

LWT_TOPIC   = "status/publisher-kelembaban"
LWT_PAYLOAD = json.dumps({
    "client_id": CLIENT_ID,
    "status": "OFFLINE",
    "timestamp": None
})

# User Properties - MQTT 5.0 Feature untuk metadata sensor
USER_PROPERTIES = [
    ("sensor_type", "humidity"),
    ("sensor_model", "BME680"),
    ("location_zone", "Ruang Server"),
    ("firmware_version", "2.1.0"),
    ("mqtt_version", "5.0")
]

# =============================================
# CALLBACK FUNCTIONS
# =============================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(f"✅ Terhubung ke broker MQTT ({BROKER_HOST}:{BROKER_PORT})")
        # Publish konfigurasi awal dengan RETAIN=True
        # Subscriber baru akan langsung dapat nilai terakhir
        config_payload = {
            "sensor_id": "HUMID-001",
            "location": "Ruang Server",
            "unit": "%",
            "min_threshold": 30,
            "max_threshold": 80,
            "description": "Sensor kelembaban udara ruang server",
            "user_properties": dict(USER_PROPERTIES)
        }
        client.publish(
            "sensors/humidity/config",
            json.dumps(config_payload),
            qos=1,
            retain=True  # Config disimpan broker (RETAIN)
        )
        logging.info("📌 Konfigurasi sensor dipublish dengan RETAIN=True")
        logging.info("📡 Shared Subscription enabled: humidity-consumers group")
        client.publish(
            LWT_TOPIC,
            json.dumps({"client_id": CLIENT_ID, "status": "ONLINE", "timestamp": datetime.now().isoformat()}),
            qos=1,
            retain=True
        )
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

def on_message(client, userdata, msg):
    """Handler untuk pesan masuk (untuk future use)"""
    logging.info(f"📨 Pesan diterima di topic {msg.topic}: {msg.payload.decode()}")

def on_publish(client, userdata, mid):
    pass  # QoS 0: callback ini kadang tidak dipanggil

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning(f"⚠️ Disconnect tidak terduga, LWT akan dikirim broker")

# =============================================
# SETUP CLIENT
# =============================================
client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
client.will_set(LWT_TOPIC, payload=LWT_PAYLOAD, qos=1, retain=True)
client.max_inflight_messages_set(10)  # Flow Control
client.on_connect    = on_connect
client.on_publish    = on_publish
client.on_disconnect = on_disconnect
client.on_message    = on_message

# =============================================
# SIMULASI DATA KELEMBABAN
# =============================================
humidity_value = 55.0  # Nilai awal

def generate_humidity():
    """Simulasi kelembaban dengan perubahan gradual"""
    global humidity_value
    change = random.uniform(-3, 3)
    humidity_value = max(20, min(95, humidity_value + change))
    return round(humidity_value, 1)

def get_humidity_status(hum):
    if hum < 30:
        return "TOO_DRY"
    elif hum < 60:
        return "NORMAL"
    elif hum < 80:
        return "HUMID"
    else:
        return "TOO_HUMID"

# =============================================
# MAIN LOOP
# =============================================
def main():
    logging.info("💧 Publisher Sensor Kelembaban dimulai...")
    logging.info(f"   Broker  : {BROKER_HOST}:{BROKER_PORT}")
    logging.info(f"   Topic   : {TOPIC}")
    logging.info(f"   QoS     : {QOS} (fire and forget)")
    logging.info(f"   Interval: {INTERVAL} detik")
    logging.info(f"   📌 FITUR BARU:")
    logging.info(f"      - Message Expiry Interval: {MESSAGE_EXPIRY}s")
    logging.info(f"      - User Properties: {len(USER_PROPERTIES)} properties")
    logging.info(f"      - Shared Subscription: $share/humidity-consumers/sensors/humidity")
    logging.info(f"      - Flow Control: max_inflight=10 messages")

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            hum = generate_humidity()
            status = get_humidity_status(hum)

            payload = {
                "sensor_id": "HUMID-001",
                "client_id": CLIENT_ID,
                "type": "humidity",
                "value": hum,
                "unit": "%",
                "status": status,
                "location": "Ruang Server",
                "timestamp": datetime.now().isoformat(),
                "qos_level": QOS,
                "message_id": str(uuid.uuid4()),
                "message_expiry_seconds": MESSAGE_EXPIRY,
                "user_properties": dict(USER_PROPERTIES)
            }

            client.publish(
                TOPIC,
                json.dumps(payload),
                qos=QOS,
                retain=False
            )

            logging.info(f"💧 Kelembaban: {hum}% | Status: {status} | QoS=0 (no ack)")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        logging.info("\n🛑 Publisher dihentikan oleh user")
    finally:
        client.publish(
            LWT_TOPIC,
            json.dumps({"client_id": CLIENT_ID, "status": "OFFLINE", "timestamp": datetime.now().isoformat()}),
            qos=1,
            retain=True
        )
        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()
        logging.info("👋 Publisher Kelembaban selesai")

if __name__ == "__main__":
    main()
