"""
Subscriber 2 - Alert Monitor & Notifikasi
Role: Memantau alert dan status sistem secara khusus
Fitur: QoS 2, filter topic spesifik, threshold monitoring, Request-Response Pattern
Topics: sensors/motion/alert, sensors/+/alert, status/#
"""

import paho.mqtt.client as mqtt
import json
import logging
import uuid
from datetime import datetime
from collections import deque

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [ALERT-MONITOR] %(message)s',
    datefmt='%H:%M:%S'
)

# =============================================
# KONFIGURASI
# =============================================
BROKER_HOST = "localhost"
BROKER_PORT = 1883
CLIENT_ID   = "subscriber-alert-monitor"

SUBSCRIPTIONS = [
    ("sensors/motion/alert", 2),  # Alert gerak, QoS 2 (exactly once)
    ("sensors/+/alert", 2),       # Semua alert sensor (wildcard single level)
    ("status/#", 1),              # Status semua publisher (wildcard multi level)
    ("sensors/temperature", 1),   # Monitor suhu untuk threshold alert
    ("sensors/humidity", 1),      # Monitor kelembaban untuk threshold alert
    ("response/publisher-suhu", 1),    # Response topic dari publisher suhu
    ("response/publisher-gerak", 2),   # Response topic dari publisher gerak
]

# Threshold untuk generate alert otomatis
TEMP_THRESHOLD_HIGH = 35.0   # °C
HUMIDITY_HIGH       = 80.0   # %
HUMIDITY_LOW        = 30.0   # %

# Topic Alias Mapping - MQTT 5.0 Feature untuk reduce bandwidth
TOPIC_ALIASES = {
    "sensors/motion/alert": 1,
    "sensors/temperature": 2,
    "sensors/humidity": 3,
    "status/publisher-suhu": 4,
    "status/publisher-kelembaban": 5,
    "status/publisher-gerak": 6,
    "response/publisher-suhu": 7,
    "response/publisher-gerak": 8,
    "command/sensor-suhu": 9,
    "command/sensor-gerak": 10
}

# Track topic alias usage
topic_alias_usage = {
    1: {"topic": "sensors/motion/alert", "count": 0},
    2: {"topic": "sensors/temperature", "count": 0},
    3: {"topic": "sensors/humidity", "count": 0},
    4: {"topic": "status/publisher-suhu", "count": 0},
    5: {"topic": "status/publisher-kelembaban", "count": 0},
    6: {"topic": "status/publisher-gerak", "count": 0},
    7: {"topic": "response/publisher-suhu", "count": 0},
    8: {"topic": "response/publisher-gerak", "count": 0},
    9: {"topic": "command/sensor-suhu", "count": 0},
    10: {"topic": "command/sensor-gerak", "count": 0}
}

def get_topic_alias_id(topic):
    """Get alias ID untuk topic"""
    return TOPIC_ALIASES.get(topic)

def track_topic_alias_usage(alias_id, topic):
    """Track penggunaan topic alias"""
    if alias_id in topic_alias_usage:
        topic_alias_usage[alias_id]["count"] += 1
        if topic_alias_usage[alias_id]["count"] % 20 == 0:
            total_count = sum(s["count"] for s in topic_alias_usage.values())
            logging.info(f"   📊 Topic Alias Stats: {total_count} messages received using aliases")

# =============================================
# STATE MONITORING
# =============================================
alert_history = deque(maxlen=20)  # Simpan 20 alert terakhir
publisher_status = {}              # Status setiap publisher
consecutive_temp_alerts = 0
pending_requests = {}              # Track request-response dalam progress

# =============================================
# REQUEST-RESPONSE FUNCTIONS
# =============================================
def send_command_request(client, target_publisher, command_type, **kwargs):
    """Mengirim command request ke publisher dan menunggu response"""
    correlation_id = str(uuid.uuid4())
    
    command = {
        "type": command_type,
        "correlation_id": correlation_id,
        "timestamp": datetime.now().isoformat(),
        **kwargs
    }
    
    # Tentukan command topic berdasarkan target publisher
    command_topic = f"command/{target_publisher}"
    
    pending_requests[correlation_id] = {
        "target": target_publisher,
        "command": command_type,
        "timestamp": datetime.now().isoformat()
    }
    
    client.publish(command_topic, json.dumps(command), qos=1)
    logging.info(f"📨 Request-Response: Mengirim {command_type} ke {target_publisher} (correlation_id={correlation_id})")
    
    return correlation_id

def process_response(topic, data, alias_id=None):
    """Proses response dari publisher"""
    correlation_id = data.get("correlation_id", "N/A")
    response_type = data.get("type", "UNKNOWN")
    
    if correlation_id in pending_requests:
        req = pending_requests[correlation_id]
        logging.info(f"📬 Response diterima: {response_type} dari {req['target']} (correlation_id={correlation_id}, Alias #{alias_id})" if alias_id else f"📬 Response diterima: {response_type} dari {req['target']} (correlation_id={correlation_id})")
        logging.info(f"   Data: {json.dumps(data, indent=2)}")
        del pending_requests[correlation_id]
    else:
        logging.info(f"📬 Response (unsolicited): {response_type} - {data.get('client_id', 'Unknown')} (Alias #{alias_id})" if alias_id else f"📬 Response (unsolicited): {response_type} - {data.get('client_id', 'Unknown')}")

# =============================================
# CALLBACK FUNCTIONS
# =============================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("=" * 55)
        logging.info("  SUBSCRIBER ALERT MONITOR - TERHUBUNG")
        logging.info("=" * 55)

        client.subscribe(SUBSCRIPTIONS)
        for topic, qos in SUBSCRIPTIONS:
            logging.info(f"  📡 Subscribe: {topic} (QoS {qos})")
        logging.info("=" * 55)
        logging.info(f"  Threshold suhu : > {TEMP_THRESHOLD_HIGH}°C")
        logging.info(f"  Threshold humid: < {HUMIDITY_LOW}% atau > {HUMIDITY_HIGH}%")
        logging.info("=" * 55)
        logging.info(f"  🏷️  Topic Alias Map: {len(TOPIC_ALIASES)} aliases configured")
        for alias_id, topic in [(v, k) for k, v in TOPIC_ALIASES.items()]:
            logging.info(f"      Alias #{alias_id}: {topic}")
        logging.info("=" * 55)
        logging.info(f"  📌 FITUR BARU:")
        logging.info(f"     - Request-Response Pattern aktif")
        logging.info(f"     - Shared Subscription support")
        logging.info(f"     - Topic Alias: bandwidth optimization")
        
        # Demo: kirim command GET_STATUS ke publisher suhu
        import threading
        def send_demo_command():
            import time
            time.sleep(2)  # Tunggu sebentar agar koneksi stabil
            logging.info("🔄 Mengirim demo command GET_STATUS ke sensor-suhu...")
            send_command_request(client, "sensor-suhu", "GET_STATUS")
        
        thread = threading.Thread(target=send_demo_command, daemon=True)
        thread.start()
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

def on_message(client, userdata, msg):
    global consecutive_temp_alerts

    topic  = msg.topic
    qos    = msg.qos
    retain = msg.retain

    # Track topic alias usage
    alias_id = get_topic_alias_id(topic)
    if alias_id:
        track_topic_alias_usage(alias_id, topic)

    try:
        payload = json.loads(msg.payload.decode())
    except Exception:
        payload = {"raw": msg.payload.decode()}

    # ---- Routing ----
    if "alert" in topic:
        process_alert(topic, payload, qos, retain, alias_id)

    elif topic.startswith("status/"):
        process_status(topic, payload, retain, alias_id)

    elif topic.startswith("response/"):
        process_response(topic, payload, alias_id)

    elif topic == "sensors/temperature":
        check_temp_threshold(payload, alias_id)

    elif topic == "sensors/humidity":
        check_humidity_threshold(payload, alias_id)

def process_alert(topic, data, qos, retain, alias_id=None):
    """Proses alert masuk"""
    alert_id  = data.get('alert_id', 'N/A')
    severity  = data.get('severity', 'N/A')
    message   = data.get('message', 'N/A')
    ts        = data.get('timestamp', datetime.now().isoformat())
    confidence = data.get('confidence', 0)

    alert_record = {
        "id": alert_id,
        "topic": topic,
        "severity": severity,
        "message": message,
        "confidence": confidence,
        "timestamp": ts,
        "retain": retain,
        "qos": qos,
        "alias_id": alias_id
    }
    alert_history.append(alert_record)

    logging.info("🔴 " + "=" * 50)
    logging.info(f"🔴 ALERT DITERIMA!")
    logging.info(f"🔴 ID       : {alert_id}")
    logging.info(f"🔴 Severity : {severity}")
    logging.info(f"🔴 Pesan    : {message}")
    logging.info(f"🔴 Conf     : {confidence}")
    logging.info(f"🔴 Topic    : {topic} (Alias #{alias_id})" if alias_id else f"🔴 Topic    : {topic}")
    logging.info(f"🔴 QoS      : {qos} | Retain: {retain}")
    logging.info(f"🔴 Waktu    : {ts}")
    logging.info("🔴 " + "=" * 50)
    logging.info(f"   Total alert ditangani: {len(alert_history)}")

def process_status(topic, data, retain, alias_id=None):
    """Monitor status publisher (dari LWT)"""
    client_id = data.get('client_id', 'Unknown')
    status    = data.get('status', 'N/A')
    ts        = data.get('timestamp', 'N/A')

    prev_status = publisher_status.get(client_id)
    publisher_status[client_id] = status

    emoji = "🟢" if status == "ONLINE" else "🔴"

    if prev_status and prev_status != status:
        # Status berubah!
        if status == "OFFLINE":
            logging.info(f"⚠️  PUBLISHER DOWN: {client_id} -> OFFLINE | LWT diterima! (Alias #{alias_id})" if alias_id else f"⚠️  PUBLISHER DOWN: {client_id} -> OFFLINE | LWT diterima!")
        else:
            logging.info(f"✅ PUBLISHER BACK: {client_id} -> ONLINE (Alias #{alias_id})" if alias_id else f"✅ PUBLISHER BACK: {client_id} -> ONLINE")
    else:
        logging.info(f"{emoji} Status: {client_id} -> {status} | {ts} (Alias #{alias_id})" if alias_id else f"{emoji} Status: {client_id} -> {status} | {ts}")

    # Tampilkan ringkasan status semua publisher
    if publisher_status:
        online = sum(1 for s in publisher_status.values() if s == "ONLINE")
        total  = len(publisher_status)
        logging.info(f"   Sistem: {online}/{total} publisher ONLINE")

def check_temp_threshold(data, alias_id=None):
    """Generate alert jika suhu melebihi threshold"""
    global consecutive_temp_alerts
    temp   = data.get('value', 0)
    status = data.get('status', '')
    ts     = data.get('timestamp', '')

    if temp > TEMP_THRESHOLD_HIGH:
        consecutive_temp_alerts += 1
        msg = f"⚠️  THRESHOLD ALERT: Suhu {temp}°C > {TEMP_THRESHOLD_HIGH}°C (ke-{consecutive_temp_alerts}x berturut)"
        if alias_id:
            msg += f" (Alias #{alias_id})"
        logging.info(msg)
        if consecutive_temp_alerts >= 3:
            logging.info(f"🔴 CRITICAL: Suhu tinggi {consecutive_temp_alerts}x berturut! Cek sistem pendingin!")
    else:
        if consecutive_temp_alerts > 0:
            logging.info(f"✅ Suhu kembali normal: {temp}°C (setelah {consecutive_temp_alerts}x alert)")
        consecutive_temp_alerts = 0

def check_humidity_threshold(data, alias_id=None):
    """Generate alert jika kelembaban di luar range"""
    hum    = data.get('value', 0)
    ts     = data.get('timestamp', '')

    if hum > HUMIDITY_HIGH:
        msg = f"⚠️  THRESHOLD ALERT: Kelembaban {hum}% > {HUMIDITY_HIGH}% (terlalu lembab!)"
        if alias_id:
            msg += f" (Alias #{alias_id})"
        logging.info(msg)
    elif hum < HUMIDITY_LOW:
        msg = f"⚠️  THRESHOLD ALERT: Kelembaban {hum}% < {HUMIDITY_LOW}% (terlalu kering!)"
        if alias_id:
            msg += f" (Alias #{alias_id})"
        logging.info(msg)

def on_subscribe(client, userdata, mid, granted_qos):
    logging.info(f"✅ Subscribe dikonfirmasi | QoS granted: {granted_qos}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning(f"⚠️ Disconnect tidak terduga (rc={rc})")

# =============================================
# SETUP CLIENT
# =============================================
client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
client.max_inflight_messages_set(20)  # Flow Control
client.on_connect    = on_connect
client.on_message    = on_message
client.on_subscribe  = on_subscribe
client.on_disconnect = on_disconnect

# =============================================
# MAIN
# =============================================
def main():
    logging.info("🚨 Alert Monitor Subscriber dimulai...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("\n🛑 Alert Monitor dihentikan")
        logging.info(f"   Total alert ditangani sesi ini: {len(alert_history)}")
        for i, alert in enumerate(alert_history, 1):
            logging.info(f"   {i}. [{alert['timestamp']}] {alert['id']} - {alert['severity']}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
