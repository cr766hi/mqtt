"""
Publisher 1 - Sensor Suhu (Temperature Sensor)
Role: Simulasi sensor suhu ruangan
QoS: 1 (at least once)
Fitur: Retain message, Last Will & Testament
Topic: sensors/temperature
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import logging
from datetime import datetime

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

# Last Will & Testament (LWT)
# Pesan otomatis dikirim broker jika publisher disconnect tiba-tiba
LWT_TOPIC   = "status/publisher-suhu"
LWT_PAYLOAD = json.dumps({
    "client_id": CLIENT_ID,
    "status": "OFFLINE",
    "timestamp": None  # akan diisi saat disconnect
})

# =============================================
# CALLBACK FUNCTIONS
# =============================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(f"✅ Terhubung ke broker MQTT ({BROKER_HOST}:{BROKER_PORT})")
        # Publish status online
        client.publish(
            LWT_TOPIC,
            json.dumps({"client_id": CLIENT_ID, "status": "ONLINE", "timestamp": datetime.now().isoformat()}),
            qos=1,
            retain=True
        )
    else:
        logging.error(f"❌ Gagal connect, kode: {rc}")

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
client = mqtt.Client(client_id=CLIENT_ID)

# Daftarkan LWT sebelum connect
client.will_set(
    LWT_TOPIC,
    payload=LWT_PAYLOAD,
    qos=1,
    retain=True
)

client.on_connect    = on_connect
client.on_publish    = on_publish
client.on_disconnect = on_disconnect

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

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            temp = generate_temperature()
            status = get_temperature_status(temp)

            payload = {
                "sensor_id": "TEMP-001",
                "client_id": CLIENT_ID,
                "type": "temperature",
                "value": temp,
                "unit": "°C",
                "status": status,
                "location": "Ruang Server",
                "timestamp": datetime.now().isoformat(),
                "qos_level": QOS
            }

            result = client.publish(
                TOPIC,
                json.dumps(payload),
                qos=QOS,
                retain=False  # Data real-time, tidak perlu retain
            )

            logging.info(f"🌡️  Suhu: {temp}°C | Status: {status} | mid={result.mid}")
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
