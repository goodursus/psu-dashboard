#!/usr/bin/env python3
"""
Dual PSU MQTT Monitor - поддерживает одновременную работу с облачным и локальным MQTT брокерами
"""

import sys
import json
import time
import threading
import os
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Установите paho-mqtt:  pip install paho-mqtt")
    sys.exit(1)

# ── Конфигурация ─────────────────────────────────────────
# Облачный MQTT брокер (HiveMQ)
CLOUD_BROKER   = os.getenv("MQTT_BROKER", "72c1c13ede99499e8d74eae70437db9b.s1.eu.hivemq.cloud")
CLOUD_PORT     = int(os.getenv("MQTT_PORT", 8883))
CLOUD_USERNAME = os.getenv("MQTT_USERNAME", "Ursus")
CLOUD_PASSWORD = os.getenv("MQTT_PASSWORD", "Ursus_123")

# Локальный MQTT брокер
LOCAL_BROKER   = os.getenv("LOCAL_MQTT_BROKER", "192.168.1.103")
LOCAL_PORT     = int(os.getenv("LOCAL_MQTT_PORT", 1883))
LOCAL_USERNAME = os.getenv("LOCAL_MQTT_USERNAME", "")
LOCAL_PASSWORD = os.getenv("LOCAL_MQTT_PASSWORD", "")

# Общие настройки
TOPIC    = "device/psu/#"
TOPIC_STATUS = "device/psu/status"
TOPIC_TELEMETRY = "device/psu/telemetry"

# ── Цвета для терминала ───────────────────────────────
C_RESET  = "\033[0m"
C_GREEN  = "\033[32m"
C_YELLOW = "\033[33m"
C_RED    = "\033[31m"
C_CYAN   = "\033[36m"
C_GREY   = "\033[90m"
C_BLUE   = "\033[34m"

# ── Структуры данных ─────────────────────────────────────
@dataclass
class ChannelState:
    key: str
    label: str = ""
    value: float | None = None
    threshold: float | None = None
    overload: bool = False
    nominal_v: float | None = None
    source: str = ""  # "cloud" или "local"

@dataclass
class TelemetrySnapshot:
    ts: float
    source: str  # "cloud" или "local"
    view: str | None = None
    uptime_s: int | None = None
    channels: dict[str, ChannelState] = field(default_factory=dict)

class DualDataStore:
    def __init__(self, max_points: int = 2000):
        self.lock = threading.Lock()
        self.max_points = max_points
        
        # Статусы подключения
        self.cloud_connected = False
        self.local_connected = False
        
        # Последние данные
        self.last_cloud_status: str | None = None
        self.last_local_status: str | None = None
        self.last_cloud_rx_ts: float | None = None
        self.last_local_rx_ts: float | None = None
        self.last_cloud_telemetry: TelemetrySnapshot | None = None
        self.last_local_telemetry: TelemetrySnapshot | None = None
        
        # Объединенные данные
        self.merged_telemetry: TelemetrySnapshot | None = None
        
        # Исторические данные
        self.series_value: dict[str, deque] = {f"ch{i}": deque(maxlen=max_points) for i in range(4)}
        self.series_threshold: dict[str, deque] = {f"ch{i}": deque(maxlen=max_points) for i in range(4)}

    def update_cloud_status(self, payload: str):
        with self.lock:
            self.last_cloud_status = payload
            self.last_cloud_rx_ts = time.time()

    def update_local_status(self, payload: str):
        with self.lock:
            self.last_local_status = payload
            self.last_local_rx_ts = time.time()

    def update_cloud_telemetry(self, snapshot: TelemetrySnapshot):
        with self.lock:
            self.last_cloud_telemetry = snapshot
            self.last_cloud_rx_ts = snapshot.ts
            self._merge_telemetry()
            self._update_series(snapshot)

    def update_local_telemetry(self, snapshot: TelemetrySnapshot):
        with self.lock:
            self.last_local_telemetry = snapshot
            self.last_local_rx_ts = snapshot.ts
            self._merge_telemetry()
            self._update_series(snapshot)

    def _merge_telemetry(self):
        """Объединяет данные с обоих брокеров, приоритет у локального"""
        if self.last_local_telemetry:
            # Если есть локальные данные - используем их
            self.merged_telemetry = self.last_local_telemetry
        elif self.last_cloud_telemetry:
            # Иначе используем облачные данные
            self.merged_telemetry = self.last_cloud_telemetry

    def _update_series(self, snapshot: TelemetrySnapshot):
        for ch_key, ch in snapshot.channels.items():
            if ch.value is not None:
                self.series_value[ch_key].append((snapshot.ts, float(ch.value)))
            if ch.threshold is not None:
                self.series_threshold[ch_key].append((snapshot.ts, float(ch.threshold)))

    def get_copy(self):
        with self.lock:
            return {
                "cloud_connected": self.cloud_connected,
                "local_connected": self.local_connected,
                "last_cloud_status": self.last_cloud_status,
                "last_local_status": self.last_local_status,
                "last_cloud_rx_ts": self.last_cloud_rx_ts,
                "last_local_rx_ts": self.last_local_rx_ts,
                "merged_telemetry": self.merged_telemetry,
                "series_value": {k: list(v) for k, v in self.series_value.items()},
                "series_threshold": {k: list(v) for k, v in self.series_threshold.items()},
            }

# ── MQTT Backend для двойного подключения ─────────────────────
class DualMqttBackend:
    def __init__(self, store: DualDataStore):
        self.store = store
        
        # Облачный MQTT клиент
        self.cloud_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "psu_dashboard_cloud")
        self.cloud_client.username_pw_set(CLOUD_USERNAME, CLOUD_PASSWORD)
        self.cloud_client.tls_set()
        self.cloud_client.user_data_set({"store": store, "source": "cloud"})
        self.cloud_client.on_connect = self._on_connect
        self.cloud_client.on_message = self._on_message
        self.cloud_client.on_disconnect = self._on_disconnect
        
        # Локальный MQTT клиент
        self.local_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "psu_dashboard_local")
        if LOCAL_USERNAME:
            self.local_client.username_pw_set(LOCAL_USERNAME, LOCAL_PASSWORD)
        self.local_client.user_data_set({"store": store, "source": "local"})
        self.local_client.on_connect = self._on_connect
        self.local_client.on_message = self._on_message
        self.local_client.on_disconnect = self._on_disconnect
        
        # Потоки для каждого клиента
        self.cloud_thread = threading.Thread(target=self._cloud_worker, daemon=True)
        self.local_thread = threading.Thread(target=self._local_worker, daemon=True)
        
        self.cloud_thread.start()
        self.local_thread.start()

    def _cloud_worker(self):
        while True:
            try:
                self.cloud_client.connect(CLOUD_BROKER, CLOUD_PORT, 60)
                self.cloud_client.loop_forever(retry_first_connection=True)
            except Exception as e:
                print(f"{C_RED}[CLOUD MQTT] Connection error: {e}{C_RESET}")
                self.store.cloud_connected = False
                time.sleep(5)

    def _local_worker(self):
        while True:
            try:
                self.local_client.connect(LOCAL_BROKER, LOCAL_PORT, 60)
                self.local_client.loop_forever(retry_first_connection=True)
            except Exception as e:
                print(f"{C_RED}[LOCAL MQTT] Connection error: {e}{C_RESET}")
                self.store.local_connected = False
                time.sleep(5)

    def _on_connect(self, client, userdata, flags, rc):
        store = userdata["store"]
        source = userdata["source"]
        
        if rc == 0:
            if source == "cloud":
                store.cloud_connected = True
                print(f"{C_GREEN}[CLOUD MQTT] Connected{C_RESET}")
            else:
                store.local_connected = True
                print(f"{C_GREEN}[LOCAL MQTT] Connected{C_RESET}")
            
            client.subscribe(TOPIC)
        else:
            if source == "cloud":
                store.cloud_connected = False
                print(f"{C_RED}[CLOUD MQTT] Failed, rc={rc}{C_RESET}")
            else:
                store.local_connected = False
                print(f"{C_RED}[LOCAL MQTT] Failed, rc={rc}{C_RESET}")

    def _on_disconnect(self, client, userdata, rc):
        store = userdata["store"]
        source = userdata["source"]
        
        if source == "cloud":
            store.cloud_connected = False
            print(f"{C_YELLOW}[CLOUD MQTT] Disconnected{C_RESET}")
        else:
            store.local_connected = False
            print(f"{C_YELLOW}[LOCAL MQTT] Disconnected{C_RESET}")

    def _on_message(self, client, userdata, msg):
        store = userdata["store"]
        source = userdata["source"]
        topic = msg.topic
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
        except Exception:
            return

        if topic == TOPIC_STATUS:
            if source == "cloud":
                store.update_cloud_status(payload)
            else:
                store.update_local_status(payload)
            return

        if topic == TOPIC_TELEMETRY:
            snap = self._telemetry_from_payload(payload, source)
            if snap is not None:
                if source == "cloud":
                    store.update_cloud_telemetry(snap)
                else:
                    store.update_local_telemetry(snap)
            return

    def _telemetry_from_payload(self, payload: str, source: str) -> TelemetrySnapshot | None:
        try:
            data = json.loads(payload)
        except Exception:
            return None

        snap = TelemetrySnapshot(ts=time.time(), source=source)
        snap.view = data.get("view")
        try:
            if "uptime" in data:
                snap.uptime_s = int(data.get("uptime"))
        except Exception:
            snap.uptime_s = None

        for ch_key in ("ch0", "ch1", "ch2", "ch3"):
            raw = data.get(ch_key, {})
            if not isinstance(raw, dict):
                raw = {}
            label = raw.get("label", ch_key)
            ch = ChannelState(
                key=ch_key,
                label=str(label),
                overload=bool(raw.get("overload", False)),
                source=source,
            )

            v = raw.get("value", None)
            try:
                ch.value = float(v) if v is not None else None
            except Exception:
                ch.value = None

            thr = raw.get("threshold", None)
            try:
                ch.threshold = float(thr) if thr is not None else None
            except Exception:
                ch.threshold = None

            snap.channels[ch_key] = ch

        return snap

    def publish(self, topic: str, payload: str, retain: bool = False, prefer_local: bool = True) -> bool:
        """Публикация с приоритетом локального брокера"""
        success = False
        
        if prefer_local and self.store.local_connected:
            try:
                self.local_client.publish(topic, payload, retain=retain)
                success = True
                print(f"{C_BLUE}[LOCAL MQTT] Published: {topic}{C_RESET}")
            except Exception:
                pass
        
        if not success and self.store.cloud_connected:
            try:
                self.cloud_client.publish(topic, payload, retain=retain)
                success = True
                print(f"{C_BLUE}[CLOUD MQTT] Published: {topic}{C_RESET}")
            except Exception:
                pass
        
        if not success:
            print(f"{C_RED}[MQTT] Failed to publish: {topic}{C_RESET}")
        
        return success

# ── Streamlit Dashboard ───────────────────────────────────
def main_dashboard():
    try:
        import streamlit as st
        import pandas as pd
        import plotly.graph_objects as go
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install streamlit pandas plotly")
        return

    st.set_page_config(
        page_title="Dual PSU MQTT Dashboard",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("⚡ Dual PSU MQTT Dashboard")
    st.caption("Облачный + Локальный MQTT брокеры")

    # Инициализация session state
    if "store" not in st.session_state:
        st.session_state.store = DualDataStore(max_points=2000)
        st.session_state.mqtt_backend = DualMqttBackend(st.session_state.store)

    store = st.session_state.store
    mqtt_backend = st.session_state.mqtt_backend
    snap = store.get_copy()

    # Статусы подключения
    col1, col2, col3 = st.columns(3)
    with col1:
        cloud_status = "🟢" if snap["cloud_connected"] else "🔴"
        st.metric("Облачный MQTT", f"{cloud_status} {'Connected' if snap['cloud_connected'] else 'Disconnected'}")
    
    with col2:
        local_status = "🟢" if snap["local_connected"] else "🔴"
        st.metric("Локальный MQTT", f"{local_status} {'Connected' if snap['local_connected'] else 'Disconnected'}")
    
    with col3:
        device_status = snap["last_local_status"] or snap["last_cloud_status"] or "—"
        st.metric("Статус устройства", device_status)

    # Проверка данных
    last_tel = snap["merged_telemetry"]
    if last_tel is None:
        st.warning("⏳ Ожидание телеметрии...")
        st.info("Убедитесь что ESP32 подключен к одному из MQTT брокеров")
        return

    # Метрики
    st.divider()
    ch_order = ["ch0", "ch1", "ch2", "ch3"]
    ch_map = last_tel.channels

    row = st.columns(4)
    for i, ch_key in enumerate(ch_order):
        ch = ch_map.get(ch_key, ChannelState(key=ch_key, label=ch_key))
        with row[i]:
            title = ch.label or ch_key
            source_icon = "☁️" if ch.source == "cloud" else "🏠" if ch.source == "local" else ""
            
            if ch_key != "ch3":
                cur_str = f"{ch.value:.2f} A" if isinstance(ch.value, (int, float)) else "—"
                thr_str = f"{ch.threshold:.0f} A" if isinstance(ch.threshold, (int, float)) else "—"
                
                delta = None
                if isinstance(ch.value, (int, float)) and isinstance(ch.threshold, (int, float)):
                    delta = ch.value - ch.threshold
                
                st.metric(f"{title} {source_icon}", cur_str, delta=f"{delta:+.2f} A" if delta is not None else None)
                st.metric("Threshold", thr_str)
                
                if ch.overload:
                    st.error("⚠️ OVERLOAD")
            else:
                temp_str = f"{ch.value:.0f} °C" if isinstance(ch.value, (int, float)) else "—"
                thr_str = f"{ch.threshold:.0f} °C" if isinstance(ch.threshold, (int, float)) else "—"
                
                delta = None
                if isinstance(ch.value, (int, float)) and isinstance(ch.threshold, (int, float)):
                    delta = ch.value - ch.threshold
                
                st.metric("Temperature", temp_str, delta=f"{delta:+.0f} °C" if delta is not None else None)
                st.metric("Threshold", thr_str)
                
                if ch.overload:
                    st.error("🔥 OVER-TEMP")

    # Графики
    st.divider()
    
    def _series_df(ch_keys: list[str], window_s: int = 300):
        rows = []
        now_ts = time.time()
        for ck in ch_keys:
            seq = snap["series_value"].get(ck, [])
            if window_s > 0:
                seq = [(ts, v) for ts, v in seq if (now_ts - ts) <= window_s]
            for ts, v in seq:
                rows.append({"ts": ts, "channel": ck, "value": v})
        if not rows:
            return pd.DataFrame(columns=["ts", "channel", "value"])
        df = pd.DataFrame(rows)
        df["age_s"] = df["ts"].apply(lambda t: float(t) - now_ts)
        return df

    left, right = st.columns([1.6, 1.0])

    with left:
        st.subheader("Токи (A)")
        df = _series_df(["ch0", "ch1", "ch2"])
        if not df.empty:
            fig = go.Figure()
            colors = {"ch0": "#00bcd4", "ch1": "#8bc34a", "ch2": "#e040fb"}
            
            for ck in ("ch0", "ch1", "ch2"):
                d = df[df["channel"] == ck]
                label = ch_map.get(ck, ChannelState(key=ck, label=ck)).label or ck
                fig.add_trace(go.Scatter(
                    x=d["age_s"], y=d["value"], mode="lines", name=label,
                    line=dict(color=colors.get(ck, "#90caf9"), width=2)
                ))
            
            fig.update_layout(
                template="plotly_dark",
                height=400,
                margin=dict(l=10, r=10, t=35, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                xaxis_title="секунд назад",
                yaxis_title="A",
                xaxis=dict(range=[-300, 0])
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных о токе")

    with right:
        st.subheader("Температура (°C)")
        df_t = _series_df(["ch3"])
        if not df_t.empty:
            fig_t = go.Figure()
            fig_t.add_trace(go.Scatter(
                x=df_t["age_s"], y=df_t["value"], mode="lines", name="temp",
                line=dict(color="#ffca28", width=2)
            ))
            fig_t.update_layout(
                template="plotly_dark",
                height=400,
                margin=dict(l=10, r=10, t=35, b=10),
                xaxis_title="секунд назад",
                yaxis_title="°C",
                xaxis=dict(range=[-300, 0])
            )
            st.plotly_chart(fig_t, use_container_width=True)
        else:
            st.info("Нет данных о температуре")

    # Управление
    st.divider()
    with st.expander("🎛️ Управление", expanded=False):
        col_a, col_b = st.columns(2)
        with col_a:
            cmd = st.selectbox("Сервисная команда", ["—", "calibrate", "reboot"])
        with col_b:
            if st.button("Отправить", type="primary"):
                if cmd != "—":
                    ok = mqtt_backend.publish("device/psu/cmd", cmd)
                    st.success("✅ Отправлено" if ok else "❌ Ошибка")
                    st.rerun()

        st.divider()
        ch = st.selectbox("Канал", ["ch0 (+3.3V)", "ch1 (+5V)", "ch2 (+12V)", "ch3 (temp)"])
        ch_idx = int(ch.split()[0].replace("ch", ""))
        
        default_thr = 1 if ch_idx < 3 else 40
        if last_tel and f"ch{ch_idx}" in last_tel.channels:
            ch_state = last_tel.channels[f"ch{ch_idx}"]
            if ch_state.threshold is not None:
                default_thr = int(ch_state.threshold)
        
        if ch_idx < 3:
            val = st.number_input("Порог (A)", min_value=1, max_value=5, value=default_thr, step=1)
        else:
            val = st.number_input("Порог (°C)", min_value=20, max_value=70, value=default_thr, step=5)
        
        if st.button("Установить порог", type="secondary"):
            ok = mqtt_backend.publish(f"device/psu/ch{ch_idx}/set", str(int(val)))
            st.success("✅ Обновлено" if ok else "❌ Ошибка")
            st.rerun()

    # Информация
    st.divider()
    if last_tel:
        source_text = "☁️ Облако" if last_tel.source == "cloud" else "🏠 Локально"
        st.caption(f"📅 Последнее обновление: {datetime.fromtimestamp(last_tel.ts).strftime('%H:%M:%S')} • "
                  f"📍 Источник: {source_text} • "
                  f"👁️ View: {last_tel.view or '—'} • "
                  f"⏱️ Uptime: {last_tel.uptime_s if last_tel.uptime_s is not None else '—'}s")

# ── CLI режим ───────────────────────────────────────────────
def main_cli():
    print("Dual PSU MQTT Monitor - режим CLI")
    print(f"Облачный брокер: {CLOUD_BROKER}:{CLOUD_PORT}")
    print(f"Локальный брокер: {LOCAL_BROKER}:{LOCAL_PORT}")
    print("Нажмите Ctrl+C для выхода\n")
    
    store = DualDataStore()
    backend = DualMqttBackend(store)
    
    try:
        while True:
            time.sleep(1)
            snap = store.get_copy()
            
            # Вывод статусов
            cloud_str = f"{C_GREEN}Connected{C_RESET}" if snap["cloud_connected"] else f"{C_RED}Disconnected{C_RESET}"
            local_str = f"{C_GREEN}Connected{C_RESET}" if snap["local_connected"] else f"{C_RED}Disconnected{C_RESET}"
            
            print(f"\r[{time.strftime('%H:%M:%S')}] Облако: {cloud_str} | Локально: {local_str}", end="", flush=True)
            
            # Вывод последней телеметрии
            if snap["merged_telemetry"]:
                tel = snap["merged_telemetry"]
                source = "☁️" if tel.source == "cloud" else "🏠"
                print(f" | {source} {tel.uptime_s}s", end="", flush=True)
                
    except KeyboardInterrupt:
        print(f"\n{C_YELLOW}[EXIT]{C_RESET} Ctrl+C")

# ── Точка входа ─────────────────────────────────────────────
def _looks_like_streamlit() -> bool:
    try:
        import streamlit as _st  # noqa: F401
        return True
    except Exception:
        return False

if __name__ == "__main__":
    if _looks_like_streamlit():
        main_dashboard()
    else:
        main_cli()
