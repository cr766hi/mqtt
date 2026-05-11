# 🔌 Tugas Project Implementasi MQTT
## Integrasi Sistem — MQTT IoT Monitoring System

---

## 📁 Struktur Project

```
mqtt_project/
├── config/
│   └── mosquitto.conf          # Konfigurasi broker Mosquitto
├── publishers/
│   ├── publisher_suhu.py       # Publisher 1: Sensor Suhu (QoS 1)
│   ├── publisher_kelembaban.py # Publisher 2: Sensor Kelembaban (QoS 0)
│   └── publisher_gerak.py      # Publisher 3: Sensor Gerak (QoS 2)
├── subscribers/
│   ├── subscriber_logger.py    # Subscriber 1: Data Logger
│   └── subscriber_alert.py     # Subscriber 2: Alert Monitor
├── dashboard/
│   └── index.html              # Dashboard Web (MQTT over WebSocket)
├── logs/                       # Folder log otomatis dibuat
├── requirements.txt
└── README.md
```

---

## 🧩 Arsitektur Sistem

```
  [Publisher Suhu QoS-1]  ──┐
  [Publisher Humid QoS-0]  ──┼──► [Broker Mosquitto] ──┬──► [Subscriber Logger]
  [Publisher Gerak QoS-2]  ──┘     port 1883 (MQTT)    ├──► [Subscriber Alert]
                                    port 9001 (WS)      └──► [Dashboard Web]
```

---

## ✅ Fitur MQTT yang Diimplementasi

| Fitur | Implementasi | Status |
|---|---|---|
| **QoS 0** | Publisher Kelembaban → fire & forget | ✅ |
| **QoS 1** | Publisher Suhu → at least once delivery | ✅ |
| **QoS 2** | Publisher Gerak & Alert → exactly once | ✅ |
| **Retain Message** | Config sensor, Status publisher, Last alert | ✅ |
| **Last Will & Testament** | Semua publisher punya LWT di topic `status/#` | ✅ |
| **Wildcard `+`** | `sensors/+/alert` (single level) | ✅ |
| **Wildcard `#`** | `sensors/#`, `status/#` (multi level) | ✅ |
| **User Properties** | Metadata sensor di setiap pesan (MQTT 5.0) | ✅ |
| **Message Expiry** | TTL untuk sensor data (MQTT 5.0) | ✅ |
| **Request-Response** | Pattern command-response (MQTT 5.0) | ✅ |
| **Shared Subscription** | Distribusi pesan antar consumers (MQTT 5.0) | ✅ |
| **Flow Control** | Max inflight messages per client (MQTT 5.0) | ✅ |
| **WebSocket** | Dashboard web terhubung via WS port 9001 | ✅ |

---

## 🆕 MQTT 5.0 Features Baru

### 1. **User Properties**
Metadata custom untuk setiap sensor di dalam pesan:
```json
{
  "sensor_type": "temperature",
  "sensor_model": "DHT22",
  "location_zone": "Ruang Server",
  "firmware_version": "1.2.3",
  "mqtt_version": "5.0"
}
```

### 2. **Message Expiry Interval**
TTL untuk pesan sensor (time-sensitive data):
- Publisher Suhu: 60 detik
- Publisher Kelembaban: 30 detik (QoS 0)
- Publisher Gerak: 120 detik (alert kritis)

### 3. **Request-Response Pattern**
Command-response untuk control publisher:
```
Command Topics:  command/sensor-suhu, command/sensor-gerak
Response Topics: response/publisher-suhu, response/publisher-gerak
```
Perintah yang didukung: `GET_STATUS`, `GET_CONFIG`, `RESET_COUNTER`, `GET_STATS`

### 4. **Shared Subscription**
Load-balancing untuk multiple consumers:
```
$share/humidity-consumers/sensors/humidity
```

### 5. **Flow Control**
Kontrol jumlah pesan in-flight (max_inflight_messages):
- Publisher Suhu: 20 messages
- Publisher Kelembaban: 10 messages
- Publisher Gerak: 30 messages
- Alert Subscriber: 20 messages

Lihat file `FEATURES_SUMMARY.md` untuk detail lengkap.

---

## 🚀 Cara Menjalankan

### 1. Install Dependencies

```bash
# Install Mosquitto broker
sudo apt-get install mosquitto mosquitto-clients   # Ubuntu/Debian
brew install mosquitto                              # macOS

# Install Python library
pip install -r requirements.txt
```

### 2. Jalankan Broker Mosquitto

```bash
# Dengan konfigurasi custom (WebSocket enabled)
mosquitto -c config/mosquitto.conf

# Atau tanpa file config (default, tapi WebSocket tidak aktif)
mosquitto -v
```

### 3. Jalankan Subscribers (buka terminal baru untuk masing-masing)

```bash
# Terminal 1: Data Logger
python subscribers/subscriber_logger.py

# Terminal 2: Alert Monitor
python subscribers/subscriber_alert.py
```

### 4. Jalankan Publishers (buka terminal baru untuk masing-masing)

```bash
# Terminal 3: Publisher Suhu
python publishers/publisher_suhu.py

# Terminal 4: Publisher Kelembaban
python publishers/publisher_kelembaban.py

# Terminal 5: Publisher Gerak
python publishers/publisher_gerak.py
```

### 5. Buka Dashboard Web

```bash
# Buka file dashboard langsung di browser
open dashboard/index.html       # macOS
xdg-open dashboard/index.html   # Linux
# Atau double-click file dashboard/index.html
```

> ⚠️ **Pastikan Mosquitto berjalan dengan `mosquitto.conf`** agar WebSocket (port 9001) aktif untuk dashboard!

---

## 📊 Penjelasan Publishers

### Publisher 1 — Sensor Suhu (`publisher_suhu.py`)
- **Topic:** `sensors/temperature`
- **QoS:** 1 (At Least Once — ada acknowledgment)
- **Interval:** 3 detik
- **Fitur:** Last Will & Testament di `status/publisher-suhu`
- **Simulasi:** Suhu 27°C ± 2.5°C, sesekali anomali naik 5–10°C

### Publisher 2 — Sensor Kelembaban (`publisher_kelembaban.py`)
- **Topic:** `sensors/humidity`
- **QoS:** 0 (Fire & Forget — tidak ada ack)
- **Interval:** 5 detik
- **Fitur:** Retain message untuk konfigurasi sensor di `sensors/humidity/config`
- **Simulasi:** Kelembaban bergerak gradual 20–95%

### Publisher 3 — Sensor Gerak (`publisher_gerak.py`)
- **Topic:** `sensors/motion`, `sensors/motion/alert`
- **QoS:** 2 (Exactly Once — 4-way handshake, tidak duplikat)
- **Interval:** 4 detik
- **Fitur:** Alert dengan retain=True, LWT
- **Simulasi:** 75% CLEAR, 20% DETECTED, 5% SUSPICIOUS

---

## 📡 Penjelasan Subscribers

### Subscriber 1 — Data Logger (`subscriber_logger.py`)
- **Subscribe:** `sensors/#` (QoS 2) + `status/#` (QoS 1)
- **Fungsi:** Catat semua data ke console dan file log
- **Wildcard:** `#` untuk multi-level (sensor, config, alert)

### Subscriber 2 — Alert Monitor (`subscriber_alert.py`)
- **Subscribe:** `sensors/motion/alert` + `sensors/+/alert` + `status/#` + temp + humid
- **Fungsi:** Monitor alert, LWT status publisher, threshold check
- **Wildcard:** `+` untuk single-level alert di semua sensor

---

## 🌐 Dashboard Web

- Terhubung via **WebSocket** ke broker (port 9001)
- Menampilkan:
  - Nilai real-time suhu, kelembaban, status gerak
  - Gauge bar visual
  - Publisher status (ONLINE/OFFLINE via LWT)
  - Live message log
  - Alert banner merah saat ada motion alert
  - Counter pesan, alert, uptime

---

## 🔍 Penjelasan Fitur MQTT

### QoS (Quality of Service)
| Level | Nama | Cara Kerja | Dipakai di |
|---|---|---|---|
| 0 | Fire & Forget | Kirim tanpa konfirmasi | Sensor kelembaban |
| 1 | At Least Once | Kirim ulang sampai ada ACK | Sensor suhu |
| 2 | Exactly Once | 4-way handshake | Sensor gerak, Alert |

### Retain Message
Broker menyimpan pesan terakhir. Subscriber baru langsung dapat data terkini tanpa menunggu publisher kirim lagi.
- Dipakai: konfigurasi sensor, status publisher, last alert

### Last Will & Testament (LWT)
Pesan yang disiapkan sebelum connect. Broker otomatis mengirim ke topic tertentu jika client disconnect tiba-tiba (crash, putus jaringan).
- Semua publisher daftarkan LWT di `status/publisher-{nama}`

### Wildcard Topic
- `+` → satu level: `sensors/+/alert` cocok dengan `sensors/motion/alert`, `sensors/temp/alert`
- `#` → semua level: `sensors/#` cocok dengan `sensors/temperature`, `sensors/motion/alert`

---

## 👥 Pembagian Role Publisher

| Publisher | Role | QoS | Topic Utama |
|---|---|---|---|
| Sensor Suhu | Monitoring lingkungan — suhu kritis | QoS 1 | `sensors/temperature` |
| Sensor Kelembaban | Monitoring lingkungan — kelembaban | QoS 0 | `sensors/humidity` |
| Sensor Gerak | Keamanan — deteksi intrusi | QoS 2 | `sensors/motion` |
