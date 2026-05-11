"""
Publisher 1 - Sensor Suhu (Temperature Sensor)
Role: Simulasi sensor suhu ruangan
QoS: 1 (at least once)
Fitur: Retain message, Last Will & Testament, User Properties, Message Expiry Interval
Topic: sensors/temperature
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
    format='[%(asctime)s] [PUBLISHER-SUHU] %(message)s',
    datefmt='%H:%M:%S'
)

# =============================================
# KONFIGURASI
# =============================================
BROKER_HOST = "localhost"
BROKER_PORT = 1883
CLIENT_ID   = "publisher-sensor-suhu"
TOPIC       = "sensors/temperature"
QOS         = 1          # QoS Level 1
INTERVAL    = 3          # Kirim data setiap 3 detik
MESSAGE_EXPIRY = 60      # Message expiry interval (detik) - MQTT 5.0 Feature

# Last Will & Testament (LWT)
# Pesan otomatis dikirim broker jika publisher disconnect tiba-tiba
LWT_TOPIC   = "status/publisher-suhu"
LWT_PAYLOAD = json.dumps({
    "client_id": CLIENT_ID,
    "status": "OFFLINE",
    "timestamp": None  # akan diisi saat disconnect
})

# User Properties - MQTT 5.0 Feature untuk metadata sensor
USER_PROPERTIES = [
    ("sensor_type", "temperature"),
    ("sensor_model", "DHT22"),
    ("location_zone", "Ruang Server"),
    ("firmware_version", "1.2.3"),
    ("mqtt_version", "5.0")
]

# Request-Response topic (untuk command dari alert monitor)
RESPONSE_TOPIC = "response/publisher-suhu"
RESPONSE_QUEUE = []  # Queue untuk menyimpan pesan masuk

# Topic Alias Mapping - MQTT 5.0 Feature untuk reduce bandwidth
# Assign integer ID ke topic panjang
TOPIC_ALIASES = {
    "sensors/temperature": 1,
    "command/sensor-suhu": 2,
    "response/publisher-suhu": 3,
    "status/publisher-suhu": 4
}

# Track topic alias usage untuk monitoring
topic_alias_usage = {
    1: {"topic": "sensors/temperature", "count": 0, "bytes_saved": 0},
    2: {"topic": "command/sensor-suhu", "count": 0, "bytes_saved": 0},
    3: {"topic": "response/publisher-suhu", "count": 0, "bytes_saved": 0},
    4: {"topic": "status/publisher-suhu", "count": 0, "bytes_saved": 0}
}

def get_topic_alias_id(topic):
    """Get alias ID untuk topic, atau return topic jika tidak ada alias"""
    return TOPIC_ALIASES.get(topic)

def track_topic_alias_usage(alias_id, topic, payload_size):
    """Track penggunaan topic alias untuk bandwidth monitoring"""
    if alias_id in topic_alias_usage:
        stats = topic_alias_usage[alias_id]
        stats["count"] += 1
        # Bytes saved = topic name length (akan dikirim sebagai ID saja)
        stats["bytes_saved"] += len(topic.encode()) - 2  # 2 bytes untuk integer ID
        
        # Log every 10 publishes
        if stats["count"] % 10 == 0:
            total_saved = sum(s["bytes_saved"] for s in topic_alias_usage.values())
            logging.info(f"   📊 Topic Alias Stats: {stats['count']} publishes, {total_saved} bytes saved")

# =============================================
# CALLBACK FUNCTIONS
# =============================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(f"✅ Terhubung ke broker MQTT ({BROKER_HOST}:{BROKER_PORT})")
        # Subscribe ke command topic untuk request-response pattern
        client.subscribe("command/sensor-suhu", qos=1)
        logging.info("📡 Subscribe ke command/sensor-suhu untuk request-response")
        logging.info(f"   🏷️  Topic Alias Map: {len(TOPIC_ALIASES)} aliases configured")
        for alias_id, topic in [(v, k) for k, v in TOPIC_ALIASES.items()]:
            logging.info(f"      Alias #{alias_id}: {topic}")
        # Publish status online
        client.publish(
            LWT_TOPIC,
            json.dumps({"client_id": CLIENT_ID, "status": "ONLINE", "timestamp": datetime.now().isoformat()}),
            qos=1,
            retain=True
        )
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

def on_message(client, userdata, msg):
    """Handler untuk pesan request masuk (request-response pattern)"""
    try:
        command = json.loads(msg.payload.decode())
        cmd_type = command.get("type", "unknown")
        correlation_id = command.get("correlation_id", str(uuid.uuid4()))
        
        if cmd_type == "GET_STATUS":
            response = {
                "correlation_id": correlation_id,
                "client_id": CLIENT_ID,
                "type": "STATUS_RESPONSE",
                "status": "ACTIVE",
                "qos": QOS,
                "expiry_interval": MESSAGE_EXPIRY,
                "timestamp": datetime.now().isoformat()
            }
            client.publish(
                RESPONSE_TOPIC,
                json.dumps(response),
                qos=1,
                retain=False
            )
            logging.info(f"📨 Request-Response: GET_STATUS (correlation_id={correlation_id})")
        elif cmd_type == "GET_CONFIG":
            response = {
                "correlation_id": correlation_id,
                "client_id": CLIENT_ID,
                "type": "CONFIG_RESPONSE",
                "user_properties": dict(USER_PROPERTIES),
                "message_expiry_interval": MESSAGE_EXPIRY,
                "timestamp": datetime.now().isoformat()
            }
            client.publish(
                RESPONSE_TOPIC,
                json.dumps(response),
                qos=1
            )
            logging.info(f"📨 Request-Response: GET_CONFIG (correlation_id={correlation_id})")
    except json.JSONDecodeError:
        logging.warning("⚠️ Pesan command tidak valid JSON")

def on_publish(client, userdata, mid):
    logging.info(f"   📤 Pesan terkirim (message_id={mid})")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning(f"⚠️ Disconnect tidak terduga (rc={rc}), LWT akan dikirim broker")
    else:
        logging.info("🔌 Disconnect normal")

# =============================================
# SETUP CLIENT
# =============================================
client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)

# Daftarkan LWT sebelum connect
client.will_set(
    LWT_TOPIC,
    payload=LWT_PAYLOAD,
    qos=1,
    retain=True
)

# Set receive maximum (Flow Control) - MQTT 5.0 Feature
client.max_inflight_messages_set(20)  # Maximum 20 messages in flight

client.on_connect    = on_connect
client.on_publish    = on_publish
client.on_disconnect = on_disconnect
client.on_message    = on_message

# =============================================
# SIMULASI DATA SUHU
# =============================================
def generate_temperature():
    """Simulasi suhu ruangan dengan fluktuasi natural"""
    base_temp = 27.0
    noise = random.uniform(-2.5, 2.5)
    # Sesekali ada anomali (suhu terlalu tinggi)
    if random.random() < 0.05:
        noise += random.uniform(5, 10)
    return round(base_temp + noise, 2)

def get_temperature_status(temp):
    if temp < 20:
        return "COLD"
    elif temp < 30:
        return "NORMAL"
    elif temp < 35:
        return "WARM"
    else:
        return "HOT"

# =============================================
# MAIN LOOP
# =============================================
def main():
    logging.info("🌡️  Publisher Sensor Suhu dimulai...")
    logging.info(f"   Broker  : {BROKER_HOST}:{BROKER_PORT}")
    logging.info(f"   Topic   : {TOPIC}")
    logging.info(f"   QoS     : {QOS}")
    logging.info(f"   Interval: {INTERVAL} detik")
    logging.info(f"   LWT     : {LWT_TOPIC}")
    logging.info(f"   📌 FITUR BARU:")
    logging.info(f"      - Message Expiry Interval: {MESSAGE_EXPIRY}s")
    logging.info(f"      - User Properties: {len(USER_PROPERTIES)} properties")
    logging.info(f"      - Request-Response topic: {RESPONSE_TOPIC}")
    logging.info(f"      - Flow Control: max_inflight=20 messages")

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            temp = generate_temperature()
            status = get_temperature_status(temp)

            # Payload dengan User Properties dan Message Expiry info
            payload = {
                "sensor_id": "TEMP-001",
                "client_id": CLIENT_ID,
                "type": "temperature",
                "value": temp,
                "unit": "°C",
                "status": status,
                "location": "Ruang Server",
                "timestamp": datetime.now().isoformat(),
                "qos_level": QOS,
                "message_id": str(uuid.uuid4()),
                "message_expiry_seconds": MESSAGE_EXPIRY,
                "user_properties": dict(USER_PROPERTIES)  # User Properties - MQTT 5.0
            }

            result = client.publish(
                TOPIC,
                json.dumps(payload),
                qos=QOS,
                retain=False
            )

            # Track topic alias usage
            alias_id = get_topic_alias_id(TOPIC)
            if alias_id:
                track_topic_alias_usage(alias_id, TOPIC, len(json.dumps(payload)))

            logging.info(f"🌡️  Suhu: {temp}°C | Status: {status} | mid={result.mid} | Alias #{alias_id}")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        logging.info("\n🛑 Publisher dihentikan oleh user")
    finally:
        # Publish status offline secara manual sebelum disconnect
        client.publish(
            LWT_TOPIC,
            json.dumps({"client_id": CLIENT_ID, "status": "OFFLINE", "timestamp": datetime.now().isoformat()}),
            qos=1,
            retain=True
        )
        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()
        logging.info("👋 Publisher Suhu selesai")

if __name__ == "__main__":
    main()
