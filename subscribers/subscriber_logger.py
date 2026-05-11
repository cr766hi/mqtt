"""
Subscriber 1 - Data Logger
Role: Menerima dan mencatat SEMUA data sensor ke file log
Fitur: Wildcard subscription (+, #), multi-topic, QoS negotiation
Topics: sensors/# (semua sensor), status/# (semua status)
"""

import paho.mqtt.client as mqtt
import json
import logging
import os
from datetime import datetime

# Setup dual logging: ke console dan ke file
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"mqtt_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)

# =============================================
# KONFIGURASI
# =============================================
BROKER_HOST = "localhost"
BROKER_PORT = 1883
CLIENT_ID   = "subscriber-data-logger"

# Daftar topic yang di-subscribe beserta QoS
SUBSCRIPTIONS = [
    ("sensors/#", 2),       # Semua data sensor, QoS 2
    ("status/#", 1),        # Semua status publisher, QoS 1
]

# Topic Alias mappings untuk bandwidth optimization
TOPIC_ALIASES = {
    "sensors/temperature": 1,
    "sensors/humidity": 2,
    "sensors/humidity/config": 3,
    "sensors/motion": 4,
    "sensors/motion/alert": 5,
    "status/publisher-suhu": 6,
    "status/publisher-kelembaban": 7,
    "status/publisher-gerak": 8,
}

# Topic Alias usage statistics
topic_alias_usage = {}
for alias_id in TOPIC_ALIASES.values():
    topic_alias_usage[alias_id] = {
        "topic": [k for k, v in TOPIC_ALIASES.items() if v == alias_id][0],
        "count": 0,
        "bytes_saved": 0
    }

def get_topic_alias_id(topic):
    """Get alias ID for given topic"""
    return TOPIC_ALIASES.get(topic)

def track_topic_alias_usage(alias_id, topic):
    """Track topic alias usage statistics"""
    if alias_id in topic_alias_usage:
        stats = topic_alias_usage[alias_id]
        stats["count"] += 1
        # Bytes saved = topic name length - 2 bytes untuk integer ID
        stats["bytes_saved"] += len(topic.encode()) - 2
        
        # Log setiap 20 messages
        if stats["count"] % 20 == 0:
            logging.info(f"📊 Topic Alias #{alias_id} stats: {stats['count']} messages, {stats['bytes_saved']} bytes saved")

# =============================================
# STATISTIK
# =============================================
stats = {
    "total_messages": 0,
    "temperature_count": 0,
    "humidity_count": 0,
    "motion_count": 0,
    "alert_count": 0,
    "status_count": 0,
    "start_time": datetime.now().isoformat()
}

# =============================================
# CALLBACK FUNCTIONS
# =============================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("=" * 55)
        logging.info("  SUBSCRIBER DATA LOGGER - TERHUBUNG")
        logging.info("=" * 55)
        logging.info(f"  Broker  : {BROKER_HOST}:{BROKER_PORT}")
        logging.info(f"  Log file: {log_file}")

        # Subscribe ke semua topic sekaligus
        client.subscribe(SUBSCRIPTIONS)
        for topic, qos in SUBSCRIPTIONS:
            logging.info(f"  📡 Subscribe: {topic} (QoS {qos})")
        
        # Log topic aliases
        logging.info(f"  🏷️ Topic Alias Map: {len(TOPIC_ALIASES)} aliases")
        for alias_id, topic in sorted([(v, k) for k, v in TOPIC_ALIASES.items()]):
            logging.info(f"     Alias #{alias_id}: {topic}")
        logging.info("=" * 55)
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

def on_message(client, userdata, msg):
    """Handler utama untuk semua pesan masuk"""
    global stats
    stats["total_messages"] += 1

    topic   = msg.topic
    qos     = msg.qos
    retain  = msg.retain

    # Track topic alias usage
    alias_id = get_topic_alias_id(topic)
    if alias_id:
        track_topic_alias_usage(alias_id, topic)

    try:
        payload = json.loads(msg.payload.decode())
    except Exception:
        payload = msg.payload.decode()

    # ---- Routing berdasarkan topic ----
    if topic == "sensors/temperature":
        stats["temperature_count"] += 1
        handle_temperature(payload, qos, retain)

    elif topic == "sensors/humidity":
        stats["humidity_count"] += 1
        handle_humidity(payload, qos, retain)

    elif topic == "sensors/humidity/config":
        logging.info(f"⚙️  [CONFIG] Konfigurasi humidity diterima (RETAIN={retain}): {json.dumps(payload)}")

    elif topic == "sensors/motion":
        stats["motion_count"] += 1
        handle_motion(payload, qos, retain)

    elif topic == "sensors/motion/alert":
        stats["alert_count"] += 1
        handle_alert(payload, qos, retain)

    elif topic.startswith("status/"):
        stats["status_count"] += 1
        handle_status(topic, payload, qos, retain)

    else:
        logging.info(f"📨 [UNKNOWN] Topic: {topic} | Payload: {payload}")

    # Cetak statistik setiap 10 pesan
    if stats["total_messages"] % 10 == 0:
        print_stats()

def handle_temperature(data, qos, retain):
    value  = data.get('value', 'N/A')
    status = data.get('status', 'N/A')
    ts     = data.get('timestamp', 'N/A')
    logging.info(f"🌡️  [SUHU]    {value}°C | Status: {status} | QoS={qos} | {ts}")

def handle_humidity(data, qos, retain):
    value  = data.get('value', 'N/A')
    status = data.get('status', 'N/A')
    ts     = data.get('timestamp', 'N/A')
    logging.info(f"💧 [HUMID]   {value}% | Status: {status} | QoS={qos} | {ts}")

def handle_motion(data, qos, retain):
    state  = data.get('motion_state', 'N/A')
    conf   = data.get('confidence', 0)
    ts     = data.get('timestamp', 'N/A')
    emoji  = "🚨" if state == "SUSPICIOUS" else ("👤" if state == "DETECTED" else "✅")
    logging.info(f"{emoji} [MOTION]  {state} | Conf={conf} | QoS={qos} | {ts}")

def handle_alert(data, qos, retain):
    alert_id = data.get('alert_id', 'N/A')
    severity = data.get('severity', 'N/A')
    msg      = data.get('message', 'N/A')
    logging.info(f"🔴 [ALERT]   {alert_id} | {severity} | {msg} | RETAIN={retain}")

def handle_status(topic, data, qos, retain):
    client_id = data.get('client_id', topic.split('/')[-1])
    status    = data.get('status', 'N/A')
    ts        = data.get('timestamp', 'N/A')
    emoji     = "🟢" if status == "ONLINE" else "🔴"
    logging.info(f"{emoji} [STATUS]  {client_id} -> {status} | RETAIN={retain} | {ts}")

def on_subscribe(client, userdata, mid, granted_qos):
    logging.info(f"✅ Subscribe dikonfirmasi (mid={mid}) | QoS diberikan broker: {granted_qos}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning(f"⚠️ Disconnect tidak terduga (rc={rc})")
    else:
        logging.info("🔌 Disconnect normal")

def print_stats():
    logging.info("-" * 55)
    logging.info(f"  📊 STATISTIK | Total: {stats['total_messages']} pesan")
    logging.info(f"     Suhu    : {stats['temperature_count']}")
    logging.info(f"     Humid   : {stats['humidity_count']}")
    logging.info(f"     Motion  : {stats['motion_count']}")
    logging.info(f"     Alert   : {stats['alert_count']}")
    logging.info(f"     Status  : {stats['status_count']}")
    logging.info("-" * 55)

# =============================================
# SETUP CLIENT
# =============================================
client = mqtt.Client(client_id=CLIENT_ID)
client.max_inflight_messages_set(20)  # Flow Control
client.on_connect    = on_connect
client.on_message    = on_message
client.on_subscribe  = on_subscribe
client.on_disconnect = on_disconnect

# =============================================
# MAIN
# =============================================
def main():
    logging.info("📝 Data Logger Subscriber dimulai...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)

    try:
        client.loop_forever()  # Blocking loop
    except KeyboardInterrupt:
        logging.info("\n🛑 Logger dihentikan")
        print_stats()
    finally:
        client.disconnect()
        logging.info(f"📁 Log tersimpan di: {log_file}")

if __name__ == "__main__":
    main()
