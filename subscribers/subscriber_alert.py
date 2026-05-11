"""
Subscriber 2 - Alert Monitor & Notifikasi
Role: Memantau alert dan status sistem secara khusus
Fitur: QoS 2, filter topic spesifik, threshold monitoring
Topics: sensors/motion/alert, sensors/+/alert, status/#
"""

import paho.mqtt.client as mqtt
import json
import logging
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
]

# Threshold untuk generate alert otomatis
TEMP_THRESHOLD_HIGH = 35.0   # °C
HUMIDITY_HIGH       = 80.0   # %
HUMIDITY_LOW        = 30.0   # %

# =============================================
# STATE MONITORING
# =============================================
alert_history = deque(maxlen=20)  # Simpan 20 alert terakhir
publisher_status = {}              # Status setiap publisher
consecutive_temp_alerts = 0

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
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

def on_message(client, userdata, msg):
    global consecutive_temp_alerts

    topic  = msg.topic
    qos    = msg.qos
    retain = msg.retain

    try:
        payload = json.loads(msg.payload.decode())
    except Exception:
        payload = {"raw": msg.payload.decode()}

    # ---- Routing ----
    if "alert" in topic:
        process_alert(topic, payload, qos, retain)

    elif topic.startswith("status/"):
        process_status(topic, payload, retain)

    elif topic == "sensors/temperature":
        check_temp_threshold(payload)

    elif topic == "sensors/humidity":
        check_humidity_threshold(payload)

def process_alert(topic, data, qos, retain):
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
        "qos": qos
    }
    alert_history.append(alert_record)

    logging.info("🔴 " + "=" * 50)
    logging.info(f"🔴 ALERT DITERIMA!")
    logging.info(f"🔴 ID       : {alert_id}")
    logging.info(f"🔴 Severity : {severity}")
    logging.info(f"🔴 Pesan    : {message}")
    logging.info(f"🔴 Conf     : {confidence}")
    logging.info(f"🔴 Topic    : {topic}")
    logging.info(f"🔴 QoS      : {qos} | Retain: {retain}")
    logging.info(f"🔴 Waktu    : {ts}")
    logging.info("🔴 " + "=" * 50)
    logging.info(f"   Total alert ditangani: {len(alert_history)}")

def process_status(topic, data, retain):
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
            logging.info(f"⚠️  PUBLISHER DOWN: {client_id} -> OFFLINE | LWT diterima! | Retain={retain}")
        else:
            logging.info(f"✅ PUBLISHER BACK: {client_id} -> ONLINE | Retain={retain}")
    else:
        logging.info(f"{emoji} Status: {client_id} -> {status} | {ts}")

    # Tampilkan ringkasan status semua publisher
    if publisher_status:
        online = sum(1 for s in publisher_status.values() if s == "ONLINE")
        total  = len(publisher_status)
        logging.info(f"   Sistem: {online}/{total} publisher ONLINE")

def check_temp_threshold(data):
    """Generate alert jika suhu melebihi threshold"""
    global consecutive_temp_alerts
    temp   = data.get('value', 0)
    status = data.get('status', '')
    ts     = data.get('timestamp', '')

    if temp > TEMP_THRESHOLD_HIGH:
        consecutive_temp_alerts += 1
        logging.info(f"⚠️  THRESHOLD ALERT: Suhu {temp}°C > {TEMP_THRESHOLD_HIGH}°C (ke-{consecutive_temp_alerts}x berturut)")
        if consecutive_temp_alerts >= 3:
            logging.info(f"🔴 CRITICAL: Suhu tinggi {consecutive_temp_alerts}x berturut! Cek sistem pendingin!")
    else:
        if consecutive_temp_alerts > 0:
            logging.info(f"✅ Suhu kembali normal: {temp}°C (setelah {consecutive_temp_alerts}x alert)")
        consecutive_temp_alerts = 0

def check_humidity_threshold(data):
    """Generate alert jika kelembaban di luar range"""
    hum    = data.get('value', 0)
    ts     = data.get('timestamp', '')

    if hum > HUMIDITY_HIGH:
        logging.info(f"⚠️  THRESHOLD ALERT: Kelembaban {hum}% > {HUMIDITY_HIGH}% (terlalu lembab!)")
    elif hum < HUMIDITY_LOW:
        logging.info(f"⚠️  THRESHOLD ALERT: Kelembaban {hum}% < {HUMIDITY_LOW}% (terlalu kering!)")

def on_subscribe(client, userdata, mid, granted_qos):
    logging.info(f"✅ Subscribe dikonfirmasi | QoS granted: {granted_qos}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning(f"⚠️ Disconnect tidak terduga (rc={rc})")

# =============================================
# SETUP CLIENT
# =============================================
client = mqtt.Client(client_id=CLIENT_ID)
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
