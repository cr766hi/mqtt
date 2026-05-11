"""
Publisher 3 - Sensor Gerak (Motion Sensor)
Role: Simulasi sensor PIR untuk deteksi gerakan
QoS: 2 (exactly once) - karena event keamanan harus terkirim tepat sekali
Fitur: Last Will & Testament, Alert topic, QoS 2
Topic: sensors/motion, sensors/motion/alert
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [PUBLISHER-MOTION] %(message)s',
    datefmt='%H:%M:%S'
)

# =============================================
# KONFIGURASI
# =============================================
BROKER_HOST    = "localhost"
BROKER_PORT    = 1883
CLIENT_ID      = "publisher-sensor-gerak"
TOPIC          = "sensors/motion"
TOPIC_ALERT    = "sensors/motion/alert"
QOS            = 2          # QoS Level 2 (exactly once) - paling aman untuk alert
INTERVAL       = 4          # Kirim status setiap 4 detik

LWT_TOPIC      = "status/publisher-gerak"
LWT_PAYLOAD    = json.dumps({
    "client_id": CLIENT_ID,
    "status": "OFFLINE",
    "timestamp": None
})

# =============================================
# CALLBACK FUNCTIONS
# =============================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(f"✅ Terhubung ke broker MQTT ({BROKER_HOST}:{BROKER_PORT})")
        client.publish(
            LWT_TOPIC,
            json.dumps({"client_id": CLIENT_ID, "status": "ONLINE", "timestamp": datetime.now().isoformat()}),
            qos=1,
            retain=True
        )
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

def on_publish(client, userdata, mid):
    logging.info(f"   ✅ QoS 2 handshake selesai (message_id={mid}) - exactly once terjamin")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning(f"⚠️ Disconnect tidak terduga, LWT akan dikirim broker")

# =============================================
# SETUP CLIENT
# =============================================
client = mqtt.Client(client_id=CLIENT_ID)
client.will_set(LWT_TOPIC, payload=LWT_PAYLOAD, qos=1, retain=True)
client.on_connect    = on_connect
client.on_publish    = on_publish
client.on_disconnect = on_disconnect

# =============================================
# SIMULASI SENSOR PIR
# =============================================
motion_count   = 0  # Jumlah deteksi gerak
alert_count    = 0  # Jumlah alert yang dikirim

def simulate_motion():
    """
    Simulasi sensor PIR:
    - 20% chance ada gerakan normal
    - 5% chance ada gerakan mencurigakan (trigger alert)
    - 75% tidak ada gerakan
    """
    r = random.random()
    if r < 0.05:
        return "SUSPICIOUS"
    elif r < 0.25:
        return "DETECTED"
    else:
        return "CLEAR"

def get_confidence(motion_state):
    if motion_state == "SUSPICIOUS":
        return round(random.uniform(0.85, 0.99), 2)
    elif motion_state == "DETECTED":
        return round(random.uniform(0.60, 0.85), 2)
    else:
        return 0.0

# =============================================
# MAIN LOOP
# =============================================
def main():
    global motion_count, alert_count

    logging.info("🚨 Publisher Sensor Gerak dimulai...")
    logging.info(f"   Broker  : {BROKER_HOST}:{BROKER_PORT}")
    logging.info(f"   Topic   : {TOPIC}")
    logging.info(f"   Alert   : {TOPIC_ALERT}")
    logging.info(f"   QoS     : {QOS} (exactly once)")
    logging.info(f"   Interval: {INTERVAL} detik")
    logging.info("   ⚠️  QoS 2 = 4-way handshake, cocok untuk event kritis")

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            motion = simulate_motion()
            confidence = get_confidence(motion)

            if motion in ["DETECTED", "SUSPICIOUS"]:
                motion_count += 1

            payload = {
                "sensor_id": "PIR-001",
                "client_id": CLIENT_ID,
                "type": "motion",
                "motion_detected": motion != "CLEAR",
                "motion_state": motion,
                "confidence": confidence,
                "motion_count_session": motion_count,
                "location": "Pintu Masuk",
                "timestamp": datetime.now().isoformat(),
                "qos_level": QOS
            }

            result = client.publish(
                TOPIC,
                json.dumps(payload),
                qos=QOS
            )

            emoji = "🚨" if motion == "SUSPICIOUS" else ("👤" if motion == "DETECTED" else "✅")
            logging.info(f"{emoji} Motion: {motion} | Confidence: {confidence} | mid={result.mid}")

            # Jika mencurigakan, kirim juga ke topic alert
            if motion == "SUSPICIOUS":
                alert_count += 1
                alert_payload = {
                    "alert_id": f"ALERT-{alert_count:04d}",
                    "sensor_id": "PIR-001",
                    "severity": "HIGH",
                    "message": "Gerakan mencurigakan terdeteksi di Pintu Masuk!",
                    "confidence": confidence,
                    "timestamp": datetime.now().isoformat(),
                    "action_required": True
                }
                client.publish(
                    TOPIC_ALERT,
                    json.dumps(alert_payload),
                    qos=2,      # QoS 2 untuk alert kritis
                    retain=True  # Retain agar subscriber baru tahu last alert
                )
                logging.info(f"   🔴 ALERT dikirim ke {TOPIC_ALERT} (retain=True)")

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
        logging.info("👋 Publisher Gerak selesai")

if __name__ == "__main__":
    main()
