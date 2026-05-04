#!/usr/bin/env python3
"""
PSU Monitor

Режимы работы:
- CLI (как раньше): `python mqtt_monitor.py [broker_ip]`
- Dashboard (Streamlit): `streamlit run mqtt_monitor.py -- [broker_ip]`

Dashboard показывает метрики и графики по телеметрии `device/psu/telemetry`,
а также позволяет (опционально) отправлять команды установки порогов и сервисные команды.

Использование:
    pip install paho-mqtt
    python mqtt_monitor.py [broker_ip]
"""

import sys
import json
import time
import threading
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Установите paho-mqtt:  pip install paho-mqtt")
    sys.exit(1)

# ── Настройки ─────────────────────────────────────────
def _parse_broker_from_argv(default: str = "72c1c13ede99499e8d74eae70437db9b.s1.eu.hivemq.cloud") -> str:
    # Для Streamlit: аргументы после `--` попадают в sys.argv, и sys.argv[1] часто равен `"--"`.
    # Берём последний "похожий" аргумент (не флаг и не имя .py).
    for token in reversed(sys.argv[1:]):
        if not token or token == "--":
            continue
        if token.startswith("-"):
            continue
        if token.lower().endswith(".py"):
            continue
        return token
    return default


BROKER   = _parse_broker_from_argv()
PORT     = 8883
USERNAME = "Ursus"
PASSWORD = "Ursus_123"
TOPIC    = "device/psu/#"

# ── Цвета для терминала ───────────────────────────────
C_RESET  = "\033[0m"
C_GREEN  = "\033[32m"
C_YELLOW = "\033[33m"
C_RED    = "\033[31m"
C_CYAN   = "\033[36m"
C_GREY   = "\033[90m"

TOPIC_STATUS = "device/psu/status"
TOPIC_TELEMETRY = "device/psu/telemetry"


def _looks_like_streamlit() -> bool:
    # Streamlit запускает скрипт как обычный Python, но добавляет свои аргументы.
    # Самый надёжный признак — наличие модуля streamlit (будет установлен).
    try:
        import streamlit as _st  # noqa: F401
        return True
    except Exception:
        return False


def _parse_nominal_voltage(label: str):
    # "+5V", "+12V", "+3.3V" -> float; иначе None
    if not isinstance(label, str):
        return None
    s = label.strip().upper().replace(" ", "")
    if not s.endswith("V"):
        return None
    s = s[:-1]
    s = s.lstrip("+")
    try:
        return float(s)
    except Exception:
        return None


@dataclass
class ChannelState:
    key: str
    label: str = ""
    value: float | None = None       # current A (для ch0..ch2) или temp C (для ch3)
    threshold: float | None = None
    overload: bool = False
    nominal_v: float | None = None   # если voltage не приходит — берём из label
    voltage: float | None = None     # если ESP начнёт присылать фактическое напряжение
    current: float | None = None     # если ESP начнёт присылать отдельное поле


@dataclass
class TelemetrySnapshot:
    ts: float = field(default_factory=time.time)
    view: str | None = None
    uptime_s: int | None = None
    channels: dict[str, ChannelState] = field(default_factory=dict)


class DataStore:
    def __init__(self, max_points: int = 3000):
        self.lock = threading.Lock()
        self.connected = False
        self.last_status: str | None = None
        self.last_rx_ts: float | None = None
        self.last_telemetry: TelemetrySnapshot | None = None
        self.max_points = max_points
        # timeseries: dict[ch_key] -> deque[(ts, value)]
        self.series_value: dict[str, deque] = {f"ch{i}": deque(maxlen=max_points) for i in range(4)}
        self.series_threshold: dict[str, deque] = {f"ch{i}": deque(maxlen=max_points) for i in range(4)}

    def update_status(self, payload: str):
        with self.lock:
            self.last_status = payload
            self.last_rx_ts = time.time()

    def update_telemetry(self, snapshot: TelemetrySnapshot):
        with self.lock:
            self.last_telemetry = snapshot
            self.last_rx_ts = snapshot.ts
            for ch_key, ch in snapshot.channels.items():
                if ch.value is not None:
                    self.series_value[ch_key].append((snapshot.ts, float(ch.value)))
                if ch.threshold is not None:
                    self.series_threshold[ch_key].append((snapshot.ts, float(ch.threshold)))

    def get_copy(self):
        with self.lock:
            # shallow copies are fine for rendering; we still copy deques to lists
            last_tel = self.last_telemetry
            series_v = {k: list(v) for k, v in self.series_value.items()}
            series_t = {k: list(v) for k, v in self.series_threshold.items()}
            return {
                "connected": self.connected,
                "last_status": self.last_status,
                "last_rx_ts": self.last_rx_ts,
                "last_telemetry": last_tel,
                "series_value": series_v,
                "series_threshold": series_t,
            }


class MqttBackend:
    """
    Persisted MQTT backend for Streamlit.

    Important: Streamlit re-runs the script often; this backend must be created via
    st.cache_resource so that the mqtt client + thread live across re-runs.
    """

    def __init__(self, store: DataStore):
        self.store = store
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "psu_dashboard_py")
        self._client.user_data_set({"store": store})
        self._client.username_pw_set(USERNAME, PASSWORD)
        self._client.tls_set()  # Включаем SSL/TLS
        self._client.on_connect = _mqtt_on_connect
        self._client.on_message = _mqtt_on_message
        self._client.on_disconnect = _mqtt_on_disconnect

        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def _worker(self):
        while True:
            try:
                self._client.connect(BROKER, PORT, 60)
                self._client.loop_forever(retry_first_connection=True)
            except Exception:
                self.store.connected = False
                time.sleep(2.0)

    def publish(self, topic: str, payload: str, retain: bool = False) -> bool:
        try:
            self._client.publish(topic, payload, retain=retain)
            return True
        except Exception:
            return False


def _telemetry_from_payload(payload: str) -> TelemetrySnapshot | None:
    try:
        data = json.loads(payload)
    except Exception:
        return None

    snap = TelemetrySnapshot()
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
        )

        # Поддержка текущего формата ESP32:
        # - ch0..ch2: value = current(A) float
        # - ch3: value = temp(C) int
        # Возможные будущие расширения:
        # - voltage/current отдельными полями
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

        if "voltage" in raw:
            try:
                ch.voltage = float(raw.get("voltage"))
            except Exception:
                ch.voltage = None

        if "current" in raw:
            try:
                ch.current = float(raw.get("current"))
            except Exception:
                ch.current = None

        ch.nominal_v = _parse_nominal_voltage(ch.label)
        snap.channels[ch_key] = ch

    return snap


def _mqtt_on_connect(client, userdata, flags, rc):
    store: DataStore = userdata["store"]
    if rc == 0:
        store.connected = True
        client.subscribe(TOPIC)
    else:
        store.connected = False


def _mqtt_on_disconnect(client, userdata, rc):
    store: DataStore = userdata["store"]
    store.connected = False


def _mqtt_on_message(client, userdata, msg):
    store: DataStore = userdata["store"]
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")

    if topic == TOPIC_STATUS:
        store.update_status(payload)
        return

    if topic == TOPIC_TELEMETRY:
        snap = _telemetry_from_payload(payload)
        if snap is not None:
            store.update_telemetry(snap)
        return


def _start_mqtt_backend(store: DataStore) -> MqttBackend:
    return MqttBackend(store)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"{C_GREEN}[CONNECTED]{C_RESET} Broker: {BROKER}:{PORT}")
        client.subscribe(TOPIC)
        print(f"{C_CYAN}[SUBSCRIBED]{C_RESET} {TOPIC}")
    else:
        print(f"{C_RED}[ERROR]{C_RESET} Connection failed, rc={rc}")


def on_message(client, userdata, msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")

    # Попытка разобрать JSON (для telemetry)
    if topic.endswith("/telemetry"):
        try:
            data = json.loads(payload)
            print(f"\n{C_GREY}{ts}{C_RESET} {C_CYAN}{topic}{C_RESET}")
            for ch_key in ("ch0", "ch1", "ch2", "ch3"):
                ch = data.get(ch_key, {})
                label = ch.get("label", ch_key)
                value = ch.get("value", "?")
                thr   = ch.get("threshold", "?")
                ovl   = ch.get("overload", False)
                color = C_RED if ovl else C_GREEN
                print(f"  {label:>6}: {color}{value:>7}{C_RESET}  "
                      f"threshold={thr}  "
                      f"{'⚠ OVERLOAD' if ovl else ''}")
            view   = data.get("view", "?")
            uptime = data.get("uptime", "?")
            print(f"  view={view}  uptime={uptime}s")
            return
        except json.JSONDecodeError:
            pass

    # Остальные топики — простой вывод
    print(f"{C_GREY}{ts}{C_RESET} {C_YELLOW}{topic}{C_RESET} → {payload}")


def on_disconnect(client, userdata, rc):
    print(f"{C_RED}[DISCONNECTED]{C_RESET} rc={rc}")


def main_cli():
    print(f"PSU MQTT Monitor — connecting to {BROKER}:{PORT} ...")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "psu_monitor_py")
    client.username_pw_set(USERNAME, PASSWORD)
    client.tls_set()  # Включаем SSL/TLS
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n{C_YELLOW}[EXIT]{C_RESET} Ctrl+C")
    except Exception as e:
        print(f"{C_RED}[ERROR]{C_RESET} {e}")
    finally:
        client.disconnect()


def main_dashboard():
    import streamlit as st
    import pandas as pd
    import plotly.graph_objects as go
    # streamlit-autorefresh удобен, но если пакет не установлен/ломается,
    # UI не должен зависать на "RUNNING". Поэтому делаем fallback на HTML таймер.
    try:
        from streamlit_autorefresh import st_autorefresh  # type: ignore
        _HAS_AUTOR = True
    except Exception:
        st_autorefresh = None  # type: ignore
        _HAS_AUTOR = False
    import streamlit.components.v1 as components

    st.set_page_config(
        page_title="PSU MQTT Dashboard",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Чтобы не было "пустого RUNNING", рисуем что-то сразу.
    st.markdown("## PSU MQTT Dashboard")

    @st.cache_resource
    def _get_store() -> DataStore:
        return DataStore(max_points=4000)

    @st.cache_resource
    def _get_mqtt(_store: DataStore) -> MqttBackend:
        return _start_mqtt_backend(_store)

    # Мягкий dark‑styling для метрик/фона (в дополнение к config.toml)
    st.markdown(
        """
        <style>
          .stApp { background: #0b0f17; }
          [data-testid="stMetric"] { background: rgba(255,255,255,0.03); padding: 14px 14px; border-radius: 14px; border: 1px solid rgba(255,255,255,0.06); }
          [data-testid="stMetricLabel"] { opacity: 0.85; }
          /* Top padding increased to avoid overlap with Deploy/Stop header */
          .block-container { padding-top: 4.0rem; padding-bottom: 2rem; }
          .stCaption { opacity: 0.8; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # MQTT backend
    store = _get_store()
    mqtt_backend = _get_mqtt(store)

    st.sidebar.title("PSU MQTT Dashboard")
    st.sidebar.caption(f"Broker: `{BROKER}:{PORT}`  •  Subscribe: `{TOPIC}`")
    st.sidebar.caption(f"SSL/TLS: ✅  •  Auth: {USERNAME}")

    refresh_s = st.sidebar.slider("Auto‑refresh, seconds", min_value=0.0, max_value=5.0, value=1.0, step=0.5)
    window_s = st.sidebar.slider("Chart window, seconds", min_value=10, max_value=600, value=120, step=10)
    max_points = st.sidebar.slider("Chart max points", min_value=200, max_value=4000, value=1200, step=100)

    if refresh_s > 0:
        if _HAS_AUTOR and st_autorefresh is not None:
            st_autorefresh(interval=int(refresh_s * 1000), key="psu_autorefresh")
        else:
            # Не делаем full page reload (он вызывает моргание графиков и панели Stop).
            st.sidebar.warning(
                "Пакет `streamlit-autorefresh` не установлен. "
                "Авто‑обновление отключено, чтобы избежать моргания. "
                "Установите зависимости из `dashboard/requirements.txt`.",
                icon="⚠",
            )

    snap = store.get_copy()

    with st.sidebar.expander("Controls (MQTT)", expanded=False):
        st.caption("Управление опционально; использует топики прошивки.")
        col_a, col_b = st.columns(2)
        with col_a:
            cmd = st.selectbox("Service command", ["—", "calibrate", "reboot"])
        with col_b:
            send_cmd = st.button("Send", width="stretch")
        if send_cmd and cmd != "—":
            ok = mqtt_backend.publish("device/psu/cmd", cmd)
            st.toast("Command sent" if ok else "Publish failed")

        st.divider()
        ch = st.selectbox("Channel set", ["ch0 (+3.3V)", "ch1 (+5V)", "ch2 (+12V)", "ch3 (temp)"])
        ch_idx = int(ch.split()[0].replace("ch", ""))
        last_tel_for_controls: TelemetrySnapshot | None = snap["last_telemetry"]
        default_thr = None
        if last_tel_for_controls is not None:
            ch_key = f"ch{ch_idx}"
            ch_state = last_tel_for_controls.channels.get(ch_key)
            if ch_state is not None and isinstance(ch_state.threshold, (int, float)):
                default_thr = int(ch_state.threshold)
        if ch_idx < 3:
            val = st.number_input("Threshold (A)", min_value=1, max_value=5, value=int(default_thr or 1), step=1)
        else:
            val = st.number_input("Threshold (°C)", min_value=20, max_value=70, value=int(default_thr or 40), step=5)
        if st.button("Publish threshold", width="stretch"):
            ok = mqtt_backend.publish(f"device/psu/ch{ch_idx}/set", str(int(val)))
            st.toast("Threshold published" if ok else "Publish failed")

    # Header / connection
    status = snap["last_status"] or "—"
    connected = bool(snap["connected"])
    last_rx = snap["last_rx_ts"]
    age_s = (time.time() - last_rx) if last_rx else None

    h1, h2, h3, h4 = st.columns([2.1, 1.2, 1.2, 1.5])
    with h1:
        st.subheader("Live telemetry")
        st.caption("Тёмная тема, метрики и интерактивные графики по `device/psu/telemetry`.")
    with h2:
        st.metric("MQTT", "connected" if connected else "disconnected")
    with h3:
        st.metric("Device status", status)
    with h4:
        st.metric("Last RX age", f"{age_s:.1f}s" if age_s is not None else "—")

    last_tel: TelemetrySnapshot | None = snap["last_telemetry"]
    if last_tel is None:
        st.info("Пока нет телеметрии. Проверьте, что ESP32 публикует в `device/psu/telemetry` и брокер доступен.")
        return

    # Metrics grid
    ch_order = ["ch0", "ch1", "ch2", "ch3"]
    ch_map = last_tel.channels

    st.divider()
    row = st.columns(4)
    for i, ch_key in enumerate(ch_order):
        ch = ch_map.get(ch_key, ChannelState(key=ch_key, label=ch_key))
        with row[i]:
            title = ch.label or ch_key
            if ch_key != "ch3":
                # Voltage metric: prefer actual voltage, else nominal from label
                v = ch.voltage if ch.voltage is not None else ch.nominal_v
                v_str = f"{v:.2f} V" if isinstance(v, (int, float)) else "—"

                # Current metric: prefer explicit current, else value
                cur = ch.current if ch.current is not None else ch.value
                cur_str = f"{cur:.2f} A" if isinstance(cur, (int, float)) else "—"

                # Delta to threshold
                delta = None
                if isinstance(cur, (int, float)) and isinstance(ch.threshold, (int, float)):
                    delta = cur - ch.threshold
                delta_str = f"{delta:+.2f} A" if delta is not None else None

                thr_str = f"{ch.threshold:.0f} A" if isinstance(ch.threshold, (int, float)) else "—"
                p_w = None
                if isinstance(cur, (int, float)) and isinstance(v, (int, float)):
                    p_w = float(v) * float(cur)
                p_str = f"{p_w:.2f} W" if p_w is not None else "—"

                st.metric(f"{title} • Voltage", v_str)
                st.metric(
                    f"{title} • Current",
                    cur_str,
                    delta=delta_str,
                    help="Δ = current − threshold",
                )
                st.metric(f"{title} • Threshold", thr_str)
                st.metric(f"{title} • Power", p_str, help="P = U × I (U: voltage if present, else nominal from label)")
                if ch.overload:
                    st.error("OVERLOAD", icon="⚠")
            else:
                temp = ch.value
                temp_str = f"{temp:.0f} °C" if isinstance(temp, (int, float)) else "—"
                delta = None
                if isinstance(temp, (int, float)) and isinstance(ch.threshold, (int, float)):
                    delta = temp - ch.threshold
                delta_str = f"{delta:+.0f} °C" if delta is not None else None
                thr_str = f"{ch.threshold:.0f} °C" if isinstance(ch.threshold, (int, float)) else "—"
                st.metric("Temperature", temp_str, delta=delta_str, help="Δ = temp − threshold")
                st.metric("Temp threshold", thr_str)
                if ch.overload:
                    st.error("OVER‑TEMP", icon="🔥")

    # Charts
    st.divider()
    left, right = st.columns([1.6, 1.0])

    def _series_df(ch_keys: list[str]):
        rows = []
        now_ts = time.time()
        for ck in ch_keys:
            seq = snap["series_value"].get(ck, [])
            # окно по времени
            if window_s is not None and window_s > 0:
                seq = [(ts, v) for ts, v in seq if (now_ts - ts) <= float(window_s)]
            # ограничение по точкам
            if max_points is not None and len(seq) > int(max_points):
                seq = seq[-int(max_points):]
            for ts, v in seq:
                rows.append({"ts": ts, "channel": ck, "value": v})
        if not rows:
            return pd.DataFrame(columns=["ts", "channel", "value"])
        df = pd.DataFrame(rows)
        df["age_s"] = df["ts"].apply(lambda t: float(t) - now_ts)  # newest ~= 0, old negative
        return df

    def _threshold_latest(ch_key: str):
        seq = snap["series_threshold"].get(ch_key, [])
        if not seq:
            return None
        return float(seq[-1][1])

    with left:
        st.subheader("Currents (A)")
        df = _series_df(["ch0", "ch1", "ch2"])
        fig = go.Figure()
        colors = {"ch0": "#00bcd4", "ch1": "#8bc34a", "ch2": "#e040fb"}

        for ck in ("ch0", "ch1", "ch2"):
            d = df[df["channel"] == ck]
            label = ch_map.get(ck, ChannelState(key=ck, label=ck)).label or ck
            fig.add_trace(
                go.Scatter(
                    x=d["age_s"],
                    y=d["value"],
                    mode="lines",
                    name=label,
                    line=dict(color=colors.get(ck, "#90caf9"), width=2),
                )
            )
            thr = _threshold_latest(ck)
            if thr is not None:
                fig.add_hline(y=thr, line_dash="dash", line_color=colors.get(ck, "#90caf9"), opacity=0.45)

        fig.update_layout(
            template="plotly_dark",
            height=420,
            margin=dict(l=10, r=10, t=35, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            xaxis_title="seconds (newest on right)",
            yaxis_title="A",
        )
        fig.update_xaxes(range=[-float(window_s), 0] if window_s else None)
        st.plotly_chart(fig, width="stretch")

    with right:
        st.subheader("Temperature (°C)")
        df_t = _series_df(["ch3"])
        fig_t = go.Figure()
        fig_t.add_trace(
            go.Scatter(
                x=df_t["age_s"],
                y=df_t["value"],
                mode="lines",
                name="temp",
                line=dict(color="#ffca28", width=2),
            )
        )
        thr = _threshold_latest("ch3")
        if thr is not None:
            fig_t.add_hline(y=thr, line_dash="dash", line_color="#ffca28", opacity=0.45)
        fig_t.update_layout(
            template="plotly_dark",
            height=420,
            margin=dict(l=10, r=10, t=35, b=10),
            xaxis_title="seconds (newest on right)",
            yaxis_title="°C",
        )
        fig_t.update_xaxes(range=[-float(window_s), 0] if window_s else None)
        st.plotly_chart(fig_t, width="stretch")

    st.caption(
        f"Last telemetry: {datetime.fromtimestamp(last_tel.ts).strftime('%H:%M:%S')}  •  "
        f"view={last_tel.view or '—'}  •  uptime={last_tel.uptime_s if last_tel.uptime_s is not None else '—'}s"
    )


if __name__ == "__main__":
    # Если установлен Streamlit — предпочитаем dashboard‑режим.
    # CLI оставлен для совместимости с текущим «базовым» монитором.
    if _looks_like_streamlit():
        main_dashboard()
    else:
        main_cli()
