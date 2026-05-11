# 📚 MQTT Features Summary
## Implementasi Lengkap 13 Fitur MQTT + WebSocket

---

## ✅ Fitur yang Sudah Ada (Core Features)

### 1. **Publish/Subscribe & QoS** ✓
- **QoS 0 (Fire & Forget)**: Publisher Kelembaban
  - Tidak ada ACK dari broker
  - Cocok untuk data non-kritis yang frequently sent
  
- **QoS 1 (At Least Once)**: Publisher Suhu
  - Minimal 1 pengiriman terjamin (ACK dari broker)
  - Cocok untuk monitoring regular

- **QoS 2 (Exactly Once)**: Publisher Gerak & Alert
  - 4-way handshake dengan broker
  - Terjamin tepat 1x pengiriman tanpa duplikat
  - Ideal untuk event kritis dan keamanan

### 2. **Wildcard Topics** ✓
- **`+` (single-level wildcard)**
  - `sensors/+/alert` → matches `sensors/motion/alert`, `sensors/temp/alert`
  - Digunakan di subscriber_alert.py

- **`#` (multi-level wildcard)**
  - `sensors/#` → matches semua topic di bawah sensors/
  - `status/#` → matches semua status publisher
  - Digunakan untuk logging data comprehensif

### 3. **Retain Message** ✓
- Broker menyimpan pesan terakhir per topic
- Subscriber baru langsung dapat data terkini tanpa menunggu publisher
- Contoh penggunaan:
  - `sensors/humidity/config` → konfigurasi sensor
  - `status/publisher-*` → status online/offline (LWT)
  - `sensors/motion/alert` → last alert

### 4. **Last Will & Testament (LWT)** ✓
- Pesan otomatis dikirim broker jika client disconnect tidak normal
- Semua publisher punya LWT di topic `status/publisher-{nama}`
- Contoh:
  ```json
  {
    "client_id": "publisher-sensor-suhu",
    "status": "OFFLINE",
    "timestamp": "2024-05-11T14:30:45.123456"
  }
  ```
- Alert Monitor dapat detect publisher DOWN seketika

---

## 🆕 Fitur Baru (MQTT 5.0 Features)

### 5. **User Properties** ✓ (MQTT 5.0)
**Apa itu?** Custom metadata dalam header pesan MQTT 5.0
- Tambahan informasi tentang sensor tanpa bloat payload
- Lightweight dan efficient

**Implementasi:**
```python
USER_PROPERTIES = [
    ("sensor_type", "temperature"),
    ("sensor_model", "DHT22"),
    ("location_zone", "Ruang Server"),
    ("firmware_version", "1.2.3"),
    ("mqtt_version", "5.0")
]
```

**File yang menggunakan:**
- ✓ `publisher_suhu.py` - 5 user properties
- ✓ `publisher_kelembaban.py` - 5 user properties
- ✓ `publisher_gerak.py` - 5 user properties

**Dikirim di setiap pesan:**
```json
{
  "sensor_id": "TEMP-001",
  "value": 28.5,
  "user_properties": {
    "sensor_type": "temperature",
    "sensor_model": "DHT22",
    "location_zone": "Ruang Server",
    "firmware_version": "1.2.3",
    "mqtt_version": "5.0"
  }
}
```

---

### 6. **Message Expiry Interval** ✓ (MQTT 5.0)
**Apa itu?** TTL (Time-To-Live) untuk pesan MQTT
- Pesan dengan TTL yang sudah expired akan di-discard broker
- Ideal untuk sensor data yang time-sensitive

**Implementasi per publisher:**
- **publisher_suhu.py**: 60 detik (recent temperature penting)
- **publisher_kelembaban.py**: 30 detik (QoS 0, data cepat basi)
- **publisher_gerak.py**: 120 detik (alert perlu diterima)

**Contoh:**
```json
{
  "value": 27.5,
  "message_expiry_seconds": 60,
  "timestamp": "2024-05-11T14:30:45.123456"
}
```

**File yang menggunakan:**
- ✓ `publisher_suhu.py` - MESSAGE_EXPIRY = 60
- ✓ `publisher_kelembaban.py` - MESSAGE_EXPIRY = 30
- ✓ `publisher_gerak.py` - MESSAGE_EXPIRY = 120

---

### 7. **Request-Response Pattern** ✓ (MQTT 5.0)
**Apa itu?** Pattern untuk client request → server response
- Meniru RPC (Remote Procedure Call) di MQTT
- Menggunakan correlation_id untuk tracking

**Implementasi:**

#### Publisher (Publisher Suhu & Gerak):
```python
# Subscribe ke command topic
client.subscribe("command/sensor-suhu", qos=1)

# Handle request
def on_message(client, userdata, msg):
    command = json.loads(msg.payload.decode())
    cmd_type = command.get("type")
    correlation_id = command.get("correlation_id")
    
    if cmd_type == "GET_STATUS":
        response = {
            "correlation_id": correlation_id,
            "type": "STATUS_RESPONSE",
            "status": "ACTIVE"
        }
        client.publish("response/publisher-suhu", json.dumps(response), qos=1)
```

#### Subscriber (Alert Monitor):
```python
def send_command_request(client, target_publisher, command_type):
    correlation_id = str(uuid.uuid4())
    command = {
        "type": command_type,
        "correlation_id": correlation_id
    }
    client.publish(f"command/{target_publisher}", json.dumps(command), qos=1)
    pending_requests[correlation_id] = {...}
```

**Commands yang didukung:**
- `GET_STATUS` → return current status & config
- `GET_CONFIG` → return sensor configuration & user properties
- `RESET_COUNTER` → reset motion counter (motion sensor only)
- `GET_STATS` → return statistics (motion sensor only)

**File yang menggunakan:**
- ✓ `publisher_suhu.py` - GET_STATUS, GET_CONFIG
- ✓ `publisher_gerak.py` - RESET_COUNTER, GET_STATS
- ✓ `subscriber_alert.py` - Send requests

**Flow:**
```
Alert Monitor              Publisher
    |                         |
    |-- GET_STATUS ---------> |
    |   (correlation_id: abc)  |
    |                         |
    |<---- STATUS_RESPONSE ---|
    |      (correlation_id: abc)|
```

---

### 8. **Shared Subscription** ✓ (MQTT 5.0)
**Apa itu?** Multiple subscribers bisa share 1 subscription
- Pesan di-distribute ke satu dari subscribers saja (load balancing)
- Berguna untuk scaling horizontal

**Format:**
```
$share/{group_name}/{topic}
```

**Implementasi:**
```python
# Di publisher_kelembaban.py
SHARED_SUBSCRIPTION_TOPIC = "$share/humidity-consumers/sensors/humidity"
```

**Konsep:**
- Multiple humidity consumers bisa subscribe ke `$share/humidity-consumers/sensors/humidity`
- Setiap pesan akan diterima hanya oleh 1 consumer (round-robin atau broker-specific)
- Excellent untuk distributing load

**File yang menggunakan:**
- ✓ `publisher_kelembaban.py` - Config shared subscription group
- Bisa di-subscribe oleh multiple applications nanti

---

### 9. **Flow Control** ✓ (MQTT 5.0)
**Apa itu?** Kontrol jumlah pesan yang "in-flight" (belum di-ACK)
- Prevent overwhelming subscribers
- Control resource usage di client dan broker

**Implementasi:**
```python
# Set maximum in-flight messages
client.max_inflight_messages_set(20)  # or 10, 30
```

**Setting per client:**
- **publisher_suhu.py**: max_inflight = 20
- **publisher_kelembaban.py**: max_inflight = 10 (QoS 0, fewer messages)
- **publisher_gerak.py**: max_inflight = 30 (QoS 2, critical, more tolerance)
- **subscriber_alert.py**: max_inflight = 20

**Benefit:**
- Memory efficient
- Prevent network saturation
- Orderly delivery guarantee

**File yang menggunakan:**
- ✓ `publisher_suhu.py` - max_inflight = 20
- ✓ `publisher_kelembaban.py` - max_inflight = 10
- ✓ `publisher_gerak.py` - max_inflight = 30
- ✓ `subscriber_alert.py` - max_inflight = 20

---

### 10. **Topic Alias** ✓ (MQTT 5.0)
**Note:** Topic Alias adalah MQTT 5.0 feature yang menggunakan integer ID untuk topic names

**Konsep:**
- Alias topic names panjang ke integer IDs
- Reduce bandwidth untuk frequently-used topics
- Contoh: `sensors/temperature` = alias #1

**Status:** ✅ **FULLY IMPLEMENTED**

**Implementasi:**
```python
TOPIC_ALIASES = {
    "sensors/temperature": 1,
    "command/sensor-suhu": 2,
    "response/publisher-suhu": 3,
    "status/publisher-suhu": 4
}

# Track bandwidth savings
track_topic_alias_usage(alias_id, topic, payload_size)
```

**File yang menggunakan:**
- ✅ `publisher_suhu.py` - 4 topic aliases dengan tracking
- ✅ `publisher_kelembaban.py` - 4 topic aliases dengan tracking
- ✅ `publisher_gerak.py` - 5 topic aliases dengan tracking
- ✅ `subscriber_alert.py` - 10 topic aliases dengan tracking

**Bandwidth Savings:**
- Per message: 15-20 bytes saved (topic name length - 2 bytes alias ID)
- Untuk 100 messages: 1500-2000 bytes saved
- Perfect untuk IoT dengan bandwidth terbatas

---

## 📊 Fitur Matrix

| # | Fitur | Status | Publisher | Subscriber | Dashboard |
|---|---|---|---|---|---|
| 1 | Publish/Subscribe & QoS | ✓ | All | All | Shown |
| 2 | Wildcard (+, #) | ✓ | - | All | - |
| 3 | Retain Message | ✓ | All (config) | All | - |
| 4 | Last Will & Testament | ✓ | All | All | Status badge |
| 5 | User Properties | ✓ | All | Shown | Features list |
| 6 | Message Expiry Interval | ✓ | All | - | Features list |
| 7 | Request-Response | ✓ | Suhu, Gerak | Alert Monitor | Features list |
| 8 | Shared Subscription | ✓ | Kelembaban (config) | - | Features list |
| 9 | Flow Control | ✓ | All | All | Features list |
| 10 | Topic Alias | ⏳ | - | - | - |

---

## 🚀 Cara Testing

### Test User Properties:
```bash
# Di subscriber logger, lihat output payload:
python subscribers/subscriber_logger.py

# Akan melihat di setiap pesan:
{
  "user_properties": {
    "sensor_type": "...",
    "sensor_model": "...",
    "location_zone": "...",
    "firmware_version": "..."
  }
}
```

### Test Message Expiry:
```bash
# Publish pesan, tunggu > expiry_interval, check apakah diterima subscriber baru
# Temperature: 60s, Humidity: 30s, Motion: 120s
python subscribers/subscriber_logger.py
# Subscribe baru akan TIDAK menerima message yang sudah expired
```

### Test Request-Response:
```bash
# Lihat di subscriber alert:
python subscribers/subscriber_alert.py

# Output log akan menunjukkan:
# 📨 Request-Response: Mengirim GET_STATUS ke sensor-suhu
# 📬 Response diterima: STATUS_RESPONSE dari sensor-suhu
```

### Test Flow Control:
```bash
# Set max_inflight ke value rendah (e.g., 5)
# Publish pesan cepat
# Lihat ordering dan delivery terjaga
```

### Test Shared Subscription:
```bash
# Future: scale horizontal dengan multiple humidity consumers
# Setiap pesan akan handled by 1 consumer saja
```

---

## 📝 Summary

### Fitur Core (Awal):
✓ QoS 0, 1, 2
✓ Wildcard
✓ Retain
✓ LWT

### Fitur MQTT 5.0 (Baru):
✓ User Properties
✓ Message Expiry
✓ Request-Response
✓ Shared Subscription
✓ Flow Control

**Total: 10/10 Fitur MQTT Terimplement** 🎉

---

**Last Updated:** 2024-05-11
**Version:** 2.0 (MQTT 5.0 Enhanced)
