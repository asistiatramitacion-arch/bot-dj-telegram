
import asyncio
import json
import logging
import os
import secrets
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from html import escape as html_escape
from typing import Any, Dict, List, Optional, Tuple

# Importante para Python 3.14 + PyTgCalls
asyncio.set_event_loop(asyncio.new_event_loop())

from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession
from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update, CopyTextButton, ChatPermissions
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from pytgcalls import PyTgCalls


BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
USERBOT_SESSION = os.environ["USERBOT_SESSION"]


def load_userbot_string_session() -> StringSession:
    raw = (USERBOT_SESSION or "").strip()
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1].strip()
    if raw.startswith("'") and raw.endswith("'"):
        raw = raw[1:-1].strip()
    if raw.startswith("USERBOT_SESSION="):
        raw = raw.split("=", 1)[1].strip()
    if not raw:
        raise RuntimeError(
            "USERBOT_SESSION está vacía. Pega en Railway la cadena completa generada por Telethon."
        )
    try:
        return StringSession(raw)
    except Exception as e:
        raise RuntimeError(
            "USERBOT_SESSION inválida. En Railway debes pegar SOLO la cadena generada por client.session.save(), sin comillas, sin saltos de línea y sin USERBOT_SESSION=."
        ) from e

VOICE_CHAT_LINK = os.getenv("VOICE_CHAT_LINK", "").strip()
SEARCH_TRIGGER = os.getenv("SEARCH_TRIGGER", "@sha ").strip()
STATE_PATH = Path(os.getenv("STATE_PATH", "/data/state.json"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/data/downloads"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "1500"))
ALLOWED_CHAT_IDS_RAW = os.getenv("ALLOWED_CHAT_IDS", "").strip()
VOICE_CHAT_LINKS_RAW = os.getenv("VOICE_CHAT_LINKS", "").strip()

STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger("djplan")


AUTO_SIG_OPTIONS = [-1, 0, 5, 10, 15, 20]
AUTO_NEXT_TASKS: Dict[int, asyncio.Task] = {}
SCAN_TASKS: Dict[int, asyncio.Task] = {}
PAGE_SIZE = 10
UI_REFRESH_SECONDS = 10
WATCHDOG_TICK_SECONDS = 2
UNKNOWN_END_FALLBACK_SECONDS = 20
WATCHDOG_TASK: Optional[asyncio.Task] = None
WATCHDOG_RUNTIME: Dict[int, Dict[str, Any]] = {}
TEMP_PIN_TASKS: Dict[int, asyncio.Task] = {}
PANEL_LOCKS: Dict[int, asyncio.Lock] = {}
CONTROL_LOCKS: Dict[int, asyncio.Lock] = {}
BOT_STARTED_AT = int(__import__("time").time())

# =========================
# VALIDACIÓN DE NUEVOS MIEMBROS
# =========================
def env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on", "si", "sí")


def env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


VALIDATION_ENABLED = env_bool("VALIDATION_ENABLED", "true")
VALIDATION_TIMEOUT_MINUTES = max(1, env_int("VALIDATION_TIMEOUT_MINUTES", "10"))
VALIDATION_REMINDER_MINUTES = max(1, env_int("VALIDATION_REMINDER_MINUTES", "3"))
VALIDATION_KICK_IF_TIMEOUT = env_bool("VALIDATION_KICK_IF_TIMEOUT", "true")
VALIDATION_DELETE_WRONG_MESSAGES = env_bool("VALIDATION_DELETE_WRONG_MESSAGES", "false")
VALIDATION_WATCHDOG_SECONDS = max(10, env_int("VALIDATION_WATCHDOG_SECONDS", "30"))

VALIDATION_QUESTIONS_RAW = os.getenv(
    "VALIDATION_QUESTIONS",
    "Nombre:|Edad:|Lugar:|¿Qué buscas en este chat?",
).strip()
VALIDATION_QUESTIONS = [q.strip() for q in VALIDATION_QUESTIONS_RAW.split("|") if q.strip()] or [
    "Nombre:",
    "Edad:",
    "Lugar:",
    "¿Qué buscas en este chat?",
]

VALIDATION_PUBLIC_JOIN_MESSAGE = os.getenv(
    "VALIDATION_PUBLIC_JOIN_MESSAGE",
    "👤 Ha entrado {mention}.\nEstado: pendiente de responder presentación y validación admin para poder hablar.",
)
VALIDATION_INTRO_MESSAGE = os.getenv(
    "VALIDATION_INTRO_MESSAGE",
    "👋 Bienvenido/a {mention}.\n\nAntes de participar debes responder unas preguntas.\nSolo podrás enviar texto hasta completar la presentación.",
)
VALIDATION_REMINDER_MESSAGE = os.getenv(
    "VALIDATION_REMINDER_MESSAGE",
    "⏰ {mention}, recuerda completar la presentación para poder participar.",
)
VALIDATION_TIMEOUT_MESSAGE = os.getenv(
    "VALIDATION_TIMEOUT_MESSAGE",
    "⛔ {mention} no completó la presentación a tiempo.",
)
VALIDATION_APPROVED_MESSAGE = os.getenv(
    "VALIDATION_APPROVED_MESSAGE",
    "✅ Presentación validada. {mention} ya puede participar normalmente.",
)
VALIDATION_REJECTED_MESSAGE = os.getenv(
    "VALIDATION_REJECTED_MESSAGE",
    "❌ Presentación rechazada. Usuario expulsado.",
)

VALIDATION_WATCHDOG_TASK: Optional[asyncio.Task] = None
VALIDATION_JOIN_REQUESTS: Dict[str, Dict[str, Any]] = {}


@dataclass
class Track:
    title: str
    performer: str = ""
    duration: int = 0
    file_id: str = ""
    file_unique_id: str = ""
    mime_type: str = ""
    local_path: str = ""
    original_message_id: int = 0
    added_by_id: int = 0
    added_by_name: str = ""


@dataclass
class ChatState:
    dj_mode: bool = False
    assigned_dj_id: Optional[int] = None
    assigned_dj_name: str = ""
    panel_message_id: Optional[int] = None
    control_message_id: Optional[int] = None
    control_view: str = "home"
    control_page: int = 0
    paused: bool = False
    now_playing: Optional[Dict[str, Any]] = None
    queue: List[Dict[str, Any]] = field(default_factory=list)
    history: List[Dict[str, Any]] = field(default_factory=list)
    library: List[Dict[str, Any]] = field(default_factory=list)
    saved_lists: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    temp_message_ids: List[int] = field(default_factory=list)
    bot_message_ids: List[int] = field(default_factory=list)
    panel_override_text: str = ""
    panel_override_until: Optional[int] = None
    temp_pin_message_id: Optional[int] = None
    live_enabled: bool = False
    auto_track_enabled: bool = False
    auto_sig_seconds: int = -1
    dj_shuffle_enabled: bool = False
    volume: int = 100
    play_started_at: Optional[int] = None
    paused_remaining: Optional[int] = None
    validation_users: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    admin_config: Dict[str, Any] = field(default_factory=dict)
    member_activity: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    muted_users: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    entry_log: List[Dict[str, Any]] = field(default_factory=list)
    expelled_users: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    action_log: List[Dict[str, Any]] = field(default_factory=list)


STATE_CACHE: Dict[int, ChatState] = {}
TRACK_REGISTRY: Dict[int, Dict[int, Dict[str, Any]]] = {}
TRACK_CONTROL_REGISTRY: Dict[int, Dict[int, int]] = {}
PENDING_ACTIONS: Dict[str, Dict[str, Any]] = {}


def parse_admin_ids(raw: str) -> set[int]:
    values: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        part = part.lstrip("=")
        try:
            values.add(int(part))
        except ValueError:
            logger.warning("ADMIN_IDS inválido ignorado: %r", part)
    return values



ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS", ""))


def parse_chat_ids(raw: str) -> set[int]:
    values: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.add(int(part))
        except ValueError:
            logger.warning("ALLOWED_CHAT_IDS inválido ignorado: %r", part)
    return values


def parse_chat_link_map(raw: str) -> Dict[int, str]:
    result: Dict[int, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        chat_raw, url = part.split("=", 1)
        chat_raw = chat_raw.strip()
        url = url.strip()
        try:
            chat_id = int(chat_raw)
        except ValueError:
            logger.warning("VOICE_CHAT_LINKS inválido ignorado: %r", part)
            continue
        if url:
            result[chat_id] = url
    return result


ALLOWED_CHAT_IDS = parse_chat_ids(ALLOWED_CHAT_IDS_RAW)
VOICE_CHAT_LINKS = parse_chat_link_map(VOICE_CHAT_LINKS_RAW)


def chat_is_allowed(chat_id: int) -> bool:
    return not ALLOWED_CHAT_IDS or int(chat_id) in ALLOWED_CHAT_IDS


def get_chat_lock(lock_map: Dict[int, asyncio.Lock], chat_id: int) -> asyncio.Lock:
    lock = lock_map.get(int(chat_id))
    if lock is None:
        lock = asyncio.Lock()
        lock_map[int(chat_id)] = lock
    return lock


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def track_fingerprint_from_dict(data: Dict[str, Any]) -> str:
    unique = (data.get("file_unique_id") or data.get("file_id") or "").strip()
    if unique:
        return f"id:{unique}"
    title = normalize_text(str(data.get("title", "")))
    performer = normalize_text(str(data.get("performer", "")))
    duration = int(data.get("duration") or 0)
    bucket = max(0, duration // 5) if duration > 0 else 0
    if title or performer:
        return f"tp:{title}|{performer}|{bucket}"
    msg_id = int(data.get("original_message_id") or 0)
    return f"msg:{msg_id}"


def track_fingerprint(track: Track) -> str:
    return track_fingerprint_from_dict(asdict(track))


def dedupe_track_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    result: List[Dict[str, Any]] = []
    for item in items:
        fp = track_fingerprint_from_dict(item)
        if fp in seen:
            continue
        seen.add(fp)
        result.append(dict(item))
    return result


def library_item_key_from_dict(data: Dict[str, Any]) -> str:
    title = normalize_text(str(data.get("title", "")))
    if title:
        return f"title:{title}"
    return track_fingerprint_from_dict(data)


def library_item_key(track: Track) -> str:
    return library_item_key_from_dict(asdict(track))


def dedupe_library_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    result: List[Dict[str, Any]] = []
    for item in items:
        key = library_item_key_from_dict(item)
        if key in seen:
            continue
        seen.add(key)
        result.append(dict(item))
    return result


def load_all_states() -> None:
    global STATE_CACHE
    if not STATE_PATH.exists():
        STATE_CACHE = {}
        return
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("No se pudo leer state.json")
        STATE_CACHE = {}
        return

    data: Dict[int, ChatState] = {}
    for chat_id_str, state_data in raw.items():
        try:
            if isinstance(state_data, dict):
                legacy_offset = state_data.pop("auto_next_offset", state_data.pop("autoplay_offset", None))
                if "auto_sig_seconds" not in state_data:
                    if legacy_offset is None:
                        state_data["auto_sig_seconds"] = -1
                    else:
                        try:
                            legacy_offset = int(legacy_offset)
                        except Exception:
                            legacy_offset = -1
                        state_data["auto_sig_seconds"] = abs(legacy_offset) if legacy_offset < 0 else legacy_offset
                state_data.setdefault("live_enabled", False)
                state_data.setdefault("auto_track_enabled", False)
                state_data.setdefault("dj_shuffle_enabled", False)
                state_data.setdefault("control_view", "home")
                state_data.setdefault("control_page", 0)
                state_data.setdefault("panel_override_text", "")
                state_data.setdefault("panel_override_until", None)
                state_data.setdefault("temp_pin_message_id", None)
                state_data.setdefault("validation_users", {})
                state_data.setdefault("admin_config", {})
                state_data.setdefault("member_activity", {})
                state_data.setdefault("muted_users", {})
                state_data.setdefault("entry_log", [])
            state = ChatState(**state_data)
            state.library = dedupe_library_items(state.library)
            state.queue = [dict(item) for item in state.queue]
            state.saved_lists = {name: dedupe_track_items(items) for name, items in state.saved_lists.items()}
            data[int(chat_id_str)] = state
        except Exception:
            logger.exception("Estado inválido para chat %s", chat_id_str)
    STATE_CACHE = data


def save_all_states() -> None:
    payload = {str(chat_id): asdict(state) for chat_id, state in STATE_CACHE.items()}
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_state(chat_id: int) -> ChatState:
    if chat_id not in STATE_CACHE:
        STATE_CACHE[chat_id] = ChatState()
    return STATE_CACHE[chat_id]


def display_name(user) -> str:
    full = " ".join(
        p for p in [getattr(user, "first_name", ""), getattr(user, "last_name", "")] if p
    ).strip()
    if full:
        return full
    if getattr(user, "username", None):
        return user.username
    return str(getattr(user, "id", ""))


def fmt_duration(seconds: int) -> str:
    if not seconds:
        return "--:--"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{sec:02d}"
    return f"{minutes}:{sec:02d}"


def shorten_title(title: str, max_len: int = 20) -> str:
    title = title or "Nada sonando"
    return title if len(title) <= max_len else title[: max_len - 1] + "…"


def probe_duration_seconds(file_path: str) -> int:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        raw = (result.stdout or "").strip()
        if raw:
            return max(0, int(float(raw)))
    except Exception:
        logger.warning("ffprobe no disponible o no pudo leer duración: %s", file_path)

    try:
        from mutagen import File as MutagenFile  # type: ignore
        mf = MutagenFile(file_path)
        if mf is not None and getattr(mf, "info", None) is not None:
            length = getattr(mf.info, "length", 0)
            if length:
                return max(0, int(float(length)))
    except Exception:
        logger.warning("Mutagen no disponible o no pudo leer duración: %s", file_path)

    return 0


def track_key(data: Dict[str, Any]) -> str:
    return data.get("file_unique_id") or data.get("file_id") or data.get("title", "")


def remaining_seconds(state: ChatState) -> Optional[int]:
    if not state.now_playing:
        return None
    track = Track(**state.now_playing)
    duration = int(track.duration or 0)
    if duration <= 0:
        return None
    if state.paused and state.paused_remaining is not None:
        return max(0, int(state.paused_remaining))
    if state.play_started_at is None:
        return duration
    import time as _time
    elapsed = max(0, int(_time.time() - state.play_started_at))
    return max(0, duration - elapsed)


def auto_next_trigger_seconds(state: ChatState) -> int:
    if not state.auto_track_enabled:
        return 0
    return 0 if state.auto_sig_seconds < 0 else int(state.auto_sig_seconds)


def seconds_until_auto_next(state: ChatState) -> Optional[int]:
    remaining = remaining_seconds(state)
    if remaining is None:
        return None
    return max(0, remaining - auto_next_trigger_seconds(state))


def page_total(count: int, page_size: int = PAGE_SIZE) -> int:
    if count <= 0:
        return 1
    return ((count - 1) // page_size) + 1


def clamp_page(page: int, count: int, page_size: int = PAGE_SIZE) -> int:
    total = page_total(count, page_size)
    return max(0, min(page, total - 1))


def page_slice(items: List[Dict[str, Any]], page: int, page_size: int = PAGE_SIZE):
    page = clamp_page(page, len(items), page_size)
    start = page * page_size
    end = start + page_size
    return page, start, end, items[start:end]


def truncated_button_title(title: str, max_len: int = 18) -> str:
    title = title or "Sin título"
    return title if len(title) <= max_len else title[: max_len - 1] + "…"


def format_auto_sig_label(value: int) -> str:
    return "OFF" if int(value) < 0 else f"{int(value)}s"


def h(value: Any) -> str:
    return html_escape(str(value if value is not None else ""), quote=False)


def telegram_html_from_message(message, *, caption: bool = False) -> str:
    """Devuelve texto/caption en HTML preservando entidades de Telegram.

    Esto permite conservar emojis premium/custom emoji cuando Telegram los entrega
    como entidades. Si el bot no está autorizado por Telegram para enviarlos,
    Telegram mostrará el emoji base/fallback o rechazará la entidad.
    """
    try:
        if caption:
            value = getattr(message, "caption_html", None)
            if value:
                return str(value)
            return h(getattr(message, "caption", "") or "")
        value = getattr(message, "text_html", None)
        if value:
            return str(value)
        return h(getattr(message, "text", "") or "")
    except Exception:
        return h((getattr(message, "caption", None) if caption else getattr(message, "text", None)) or "")


def plain_text_from_message(message, *, caption: bool = False) -> str:
    return str((getattr(message, "caption", None) if caption else getattr(message, "text", None)) or "")


def sync_panel_override_expiry(state: ChatState) -> None:
    import time as _time
    if state.panel_override_until and int(_time.time()) >= int(state.panel_override_until):
        state.panel_override_until = None
        state.panel_override_text = ""



def panel_text(state: ChatState) -> str:
    sync_panel_override_expiry(state)
    if state.panel_override_text:
        return f"✨ <b>DJ-PLAN✨ {h(state.panel_override_text)}</b>"

    status = "🛜 ON" if state.live_enabled else "🛑 OFF"
    dj = state.assigned_dj_name or "Sin asignar"

    if state.now_playing:
        track = Track(**state.now_playing)
        song_label = "💽"
        song_value = shorten_title(track.title, 28)
    elif state.queue:
        track = Track(**state.queue[0])
        song_label = "⏭️"
        song_value = shorten_title(track.title, 28)
    else:
        song_label = "❌"
        song_value = "Nada sonando"

    return f"🔊 <b>DIRECTO</b> {status} <b>{song_label}</b> <i>{h(song_value)}</i> | 🎧 DJ: <b>{h(dj)}</b>"


def panel_markup() -> Optional[InlineKeyboardMarkup]:
    return None


def control_header(state: ChatState) -> str:
    current_title = "Nada sonando"
    next_title = "Nada en cola"
    if state.now_playing:
        current = Track(**state.now_playing)
        current_title = shorten_title(current.title, 38)
    if state.queue:
        nxt = Track(**state.queue[0])
        next_title = shorten_title(nxt.title, 38)

    remaining = remaining_seconds(state)
    remaining_label = fmt_duration(remaining) if remaining is not None else "--:--"
    live_label = "ON" if state.live_enabled else "OFF"
    auto_track_label = "ON" if state.auto_track_enabled else "OFF"
    shuffle_label = "ON" if getattr(state, "dj_shuffle_enabled", False) else "OFF"
    auto_sig_label = format_auto_sig_label(state.auto_sig_seconds)
    return (
        "<b>🎛️ CUADRO DE MANDOS DJ 🎛️</b>\n\n"
        f"▶️ Actual: <b>{h(current_title)}</b>\n"
        f"⏭️ Próxima: <b>{h(next_title)}</b>\n"
        f"🕐 Queda: <b>{remaining_label}</b>\n\n"
        f"📋 En cola: <b>{len(state.queue)}</b>\n"
        f"📚 Biblioteca: <b>{len(state.library)}</b>\n"
        f"🎧 DJ actual: <b>{h(state.assigned_dj_name or 'Sin asignar')}</b>\n"
        f"🔴 Live: <b>{live_label}</b>\n"
        f"🏧 Auto: <b>{auto_track_label}</b>\n"
        f"🔀 Aleatorio: <b>{shuffle_label}</b>\n"
        f"⏭️ Temp: <b>{auto_sig_label}</b>\n"
        f"🔊 Vol: <b>{state.volume}</b>\n\n"
    )

def control_panel_text(state: ChatState) -> str:
    return control_header(state) + "Selecciona una acción del panel."
    
def control_panel_markup(state: ChatState) -> InlineKeyboardMarkup:
    voice_button = InlineKeyboardButton("🎧 Ir directo", callback_data="panel_join_live")
    live_label = "🔴LIVE OFF" if state.live_enabled else " 🛜LIVE ON"
    auto_track_label = f"🏧 AUTO {'ON' if state.auto_track_enabled else 'OFF'}"
    auto_sig_label = f"⏭️ Temp. {format_auto_sig_label(state.auto_sig_seconds)}"
    shuffle_label = f"🔀 ALEATORIO {'ON' if getattr(state, 'dj_shuffle_enabled', False) else 'OFF'}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(live_label, callback_data="panel_live_toggle"),
                InlineKeyboardButton("⏭️ PROX.", callback_data="panel_next"),
                InlineKeyboardButton(auto_track_label, callback_data="panel_auto_track"),
            ],
            [
                InlineKeyboardButton(auto_sig_label, callback_data="panel_auto_sig"),
                InlineKeyboardButton(shuffle_label, callback_data="panel_shuffle"),
                InlineKeyboardButton("📋 Ver lista", callback_data="panel_queue"),
            ],
            [
                InlineKeyboardButton("📚 Biblioteca", callback_data="panel_library"),
            ],
            [
                InlineKeyboardButton("💾 Guardar lista", callback_data="panel_save_list"),
                InlineKeyboardButton("📂 Cargar lista", callback_data="panel_load_lists"),
            ],
            [
                InlineKeyboardButton("🔎 Buscar", callback_data="panel_search_help"),
                InlineKeyboardButton("🧭 Rastrear", callback_data="panel_scan"),
            ],
            [
                InlineKeyboardButton("📌 Fijar temporal", callback_data="panel_pin_edit"),
                InlineKeyboardButton("👥 Permisos", callback_data="panel_users"),
                InlineKeyboardButton("🧹 Limpiar", callback_data="panel_clean"),
            ],
            [
                InlineKeyboardButton("🔇 0", callback_data="panel_vol_set|0"),
                InlineKeyboardButton("🔈 25", callback_data="panel_vol_set|25"),
                InlineKeyboardButton("🔉 50", callback_data="panel_vol_set|50"),
                InlineKeyboardButton("🔊 100", callback_data="panel_vol_set|100"),
            ],
            [
                InlineKeyboardButton("🔉 Vol -", callback_data="panel_vol_down"),
                InlineKeyboardButton("🔊 Vol +", callback_data="panel_vol_up"),
                InlineKeyboardButton("🔄 Refresh", callback_data="panel_refresh"),
            ],
            [
                voice_button,
                InlineKeyboardButton("❌ Cerrar sesión", callback_data="panel_close"),
            ],
        ]
    )


def control_back_markup(extra_rows: Optional[List[List[InlineKeyboardButton]]] = None) -> InlineKeyboardMarkup:
    rows = extra_rows[:] if extra_rows else []
    rows.append([InlineKeyboardButton("🔙 Volver al panel", callback_data="panel_home")])
    return InlineKeyboardMarkup(rows)


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🏓 PING", callback_data="bot_ping")],
            [InlineKeyboardButton("🔎 BUSCAR MÚSICA", callback_data="menu_search_help")],
            [InlineKeyboardButton("🎛️ ACTIVAR MODO DJ", callback_data="menu_panel")],
        ]
    )


def queue_text(state: ChatState, page: int = 0) -> str:
    total = len(state.queue)
    total_pages = page_total(total)
    page = clamp_page(page, total)
    current_line = ""
    if state.now_playing:
        current = Track(**state.now_playing)
        current_line = f"🔴 Sonando: <b>{h(current.title)}</b>\n\n"
    if not state.queue:
        return f"<b>Lista actual</b>\n\n{current_line}La cola está vacía."
    return f"<b>Lista actual</b>\n\n{current_line}Página <b>{page+1}/{total_pages}</b> | Total: <b>{total}</b>"


def queue_markup(state: ChatState, page: int = 0) -> InlineKeyboardMarkup:
    total = len(state.queue)
    page = clamp_page(page, total)
    _, start, end, chunk = page_slice(state.queue, page)
    rows: List[List[InlineKeyboardButton]] = []
    for rel_idx, item in enumerate(chunk):
        idx = start + rel_idx
        track = Track(**item)
        rows.append([
            InlineKeyboardButton(
                truncated_button_title(track.title, 56),
                callback_data=f"q|noop|{idx}|{page}",
            )
        ])
        rows.append([
            InlineKeyboardButton("▶️", callback_data=f"q|p|{idx}|{page}"),
            InlineKeyboardButton("⬆️", callback_data=f"q|u|{idx}|{page}") if idx > 0 else InlineKeyboardButton("·", callback_data=f"q|noop|{idx}|{page}"),
            InlineKeyboardButton("⬇️", callback_data=f"q|d|{idx}|{page}") if idx < len(state.queue) - 1 else InlineKeyboardButton("·", callback_data=f"q|noop|{idx}|{page}"),
            InlineKeyboardButton("🗑️", callback_data=f"q|x|{idx}|{page}"),
        ])
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"q|pg|{page-1}|0"))
    if end < len(state.queue):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"q|pg|{page+1}|0"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton("🧹 Vaciar cola", callback_data=f"q|c|0|{page}"),
        InlineKeyboardButton("🔙 Volver", callback_data="q|r|0|0"),
    ])
    return InlineKeyboardMarkup(rows)


def library_text(state: ChatState, page: int = 0) -> str:
    total = len(state.library)
    total_pages = page_total(total)
    page = clamp_page(page, total)
    if not state.library:
        return "<b>Biblioteca</b>\n\nNo hay canciones guardadas todavía."
    return f"<b>Biblioteca</b>\n\nPágina <b>{page+1}/{total_pages}</b> | Total: <b>{total}</b>"


def library_markup(state: ChatState, page: int = 0) -> InlineKeyboardMarkup:
    total = len(state.library)
    page = clamp_page(page, total)
    _, start, end, chunk = page_slice(state.library, page)
    rows: List[List[InlineKeyboardButton]] = []
    for rel_idx, item in enumerate(chunk):
        idx = start + rel_idx
        track = Track(**item)
        rows.append([
            InlineKeyboardButton(
                truncated_button_title(track.title, 58),
                callback_data=f"lib|noop|{idx}|{page}",
            )
        ])
        rows.append([
            InlineKeyboardButton("➕", callback_data=f"lib|q|{idx}|{page}"),
            InlineKeyboardButton("⏭️ Primera", callback_data=f"lib|f|{idx}|{page}"),
            InlineKeyboardButton("🗑️", callback_data=f"lib|x|{idx}|{page}"),
        ])
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"lib|pg|{page-1}|0"))
    if end < len(state.library):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"lib|pg|{page+1}|0"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton("▶️ Reproducir todas", callback_data=f"lib|pa|0|{page}"),
        InlineKeyboardButton("➕ Cola todas", callback_data=f"lib|qa|0|{page}"),
    ])
    rows.append([InlineKeyboardButton("🔙 Volver", callback_data="lib|r|0|0")])
    return InlineKeyboardMarkup(rows)


def saved_lists_text(state: ChatState) -> str:
    lines = ["<b>Listas guardadas</b>"]
    if not state.saved_lists:
        lines.append("\nNo hay listas guardadas.")
        return "\n".join(lines)
    lines.append("")
    for idx, name in enumerate(sorted(state.saved_lists.keys()), start=1):
        lines.append(f"{idx}. {h(name)} ({len(state.saved_lists[name])})")
    return "\n".join(lines)


def saved_lists_markup(state: ChatState) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    names = sorted(state.saved_lists.keys())
    for idx, name in enumerate(names):
        rows.append([
            InlineKeyboardButton(f"▶️ {name[:18]}", callback_data=f"lst|p|{idx}"),
            InlineKeyboardButton("➕ Cola", callback_data=f"lst|a|{idx}"),
            InlineKeyboardButton("🗑️", callback_data=f"lst|x|{idx}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Volver", callback_data="lst|r|0")])
    return InlineKeyboardMarkup(rows)


def current_control_view(state: ChatState) -> tuple[str, InlineKeyboardMarkup]:
    view = state.control_view or "home"
    page = int(state.control_page or 0)

    if view == "queue":
        page = clamp_page(page, len(state.queue))
        state.control_page = page
        return control_header(state) + queue_text(state, page), queue_markup(state, page)

    if view == "library":
        page = clamp_page(page, len(state.library))
        state.control_page = page
        return control_header(state) + library_text(state, page), library_markup(state, page)

    if view == "saved_lists":
        return control_header(state) + saved_lists_text(state), saved_lists_markup(state)

    return control_panel_text(state), control_panel_markup(state)


def set_control_view(state: ChatState, view: str, page: int = 0) -> None:
    state.control_view = view
    state.control_page = max(0, int(page))


async def register_temp_message(chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    state = get_state(chat_id)
    if message_id not in state.temp_message_ids:
        state.temp_message_ids.append(message_id)
        save_all_states()


async def forget_temp_message(chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    state = get_state(chat_id)
    state.temp_message_ids = [mid for mid in state.temp_message_ids if mid != message_id]
    save_all_states()


async def register_bot_message(chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    state = get_state(chat_id)
    if message_id not in state.bot_message_ids:
        state.bot_message_ids.append(message_id)
        save_all_states()


async def forget_bot_message(chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    state = get_state(chat_id)
    state.bot_message_ids = [mid for mid in state.bot_message_ids if mid != message_id]
    save_all_states()


def forget_track_control_message(chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    registry = TRACK_CONTROL_REGISTRY.get(chat_id, {})
    for source_message_id, control_message_id in list(registry.items()):
        if control_message_id == message_id:
            registry.pop(source_message_id, None)


async def safe_delete(bot, chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
    finally:
        forget_track_control_message(chat_id, message_id)
        await forget_temp_message(chat_id, message_id)
        await forget_bot_message(chat_id, message_id)


async def delete_later(bot, chat_id: int, message_id: int, ttl: int) -> None:
    await asyncio.sleep(max(1, ttl))
    await safe_delete(bot, chat_id, message_id)


async def send_temp_message(
    bot,
    chat_id: int,
    text: str,
    *,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    ttl: int = 90,
    parse_mode: str = ParseMode.HTML,
) -> Optional[int]:
    try:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            allow_sending_without_reply=True,
        )
        await register_temp_message(chat_id, msg.message_id)
        await register_bot_message(chat_id, msg.message_id)
        asyncio.create_task(delete_later(bot, chat_id, msg.message_id, ttl))
        return msg.message_id
    except Exception:
        logger.exception("No se pudo enviar mensaje temporal")
        return None


async def cleanup_temp_messages(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    keep = {state.panel_message_id, state.control_message_id}
    for mid in list(state.temp_message_ids):
        if mid not in keep:
            await safe_delete(bot, chat_id, mid)
    state.temp_message_ids = [mid for mid in state.temp_message_ids if mid in keep]
    save_all_states()


async def cleanup_track_controls(bot, chat_id: int) -> None:
    registry = TRACK_CONTROL_REGISTRY.get(chat_id, {})
    for source_message_id, control_message_id in list(registry.items()):
        await safe_delete(bot, chat_id, control_message_id)
        registry.pop(source_message_id, None)


async def cleanup_all_bot_messages(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    known_ids = sorted(set(
        [mid for mid in state.bot_message_ids if mid]
        + [mid for mid in state.temp_message_ids if mid]
        + ([state.panel_message_id] if state.panel_message_id else [])
        + ([state.control_message_id] if state.control_message_id else [])
        + ([state.temp_pin_message_id] if state.temp_pin_message_id else [])
        + list(TRACK_CONTROL_REGISTRY.get(chat_id, {}).values())
    ), reverse=True)
    for mid in known_ids:
        await safe_delete(bot, chat_id, mid)
    state.bot_message_ids = []
    state.temp_message_ids = []
    state.temp_pin_message_id = None
    save_all_states()


async def cleanup_bot_messages_keep_core(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    keep_pinned_id = state.temp_pin_message_id or state.panel_message_id
    keep_ids = {mid for mid in [keep_pinned_id, state.control_message_id] if mid}

    known_ids = sorted(set(
        [mid for mid in state.bot_message_ids if mid]
        + [mid for mid in state.temp_message_ids if mid]
        + ([state.panel_message_id] if state.panel_message_id else [])
        + ([state.control_message_id] if state.control_message_id else [])
        + ([state.temp_pin_message_id] if state.temp_pin_message_id else [])
        + list(TRACK_CONTROL_REGISTRY.get(chat_id, {}).values())
    ), reverse=True)

    for mid in known_ids:
        if mid in keep_ids:
            continue
        await safe_delete(bot, chat_id, mid)

    # Si había un fijado temporal y se limpia, el panel principal puede desaparecer.
    # Lo reflejamos en estado para que, al expirar el temporal, se recree si hace falta.
    if state.temp_pin_message_id and state.panel_message_id and state.panel_message_id not in keep_ids:
        state.panel_message_id = None

    state.bot_message_ids = [mid for mid in state.bot_message_ids if mid in keep_ids]
    state.temp_message_ids = [mid for mid in state.temp_message_ids if mid in keep_ids]

    registry = TRACK_CONTROL_REGISTRY.get(chat_id, {})
    for source_message_id, control_message_id in list(registry.items()):
        if control_message_id not in keep_ids:
            registry.pop(source_message_id, None)

    save_all_states()


async def enforce_single_core_messages(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    keep_ids = {mid for mid in [state.panel_message_id, state.control_message_id, state.temp_pin_message_id] if mid}
    candidates = sorted(set(state.bot_message_ids), reverse=True)
    for mid in candidates:
        if mid in keep_ids:
            continue
        await safe_delete(bot, chat_id, mid)
    state.bot_message_ids = [mid for mid in state.bot_message_ids if mid in keep_ids]
    state.temp_message_ids = [mid for mid in state.temp_message_ids if mid in keep_ids]
    save_all_states()


async def cancel_temporary_pin(chat_id: int) -> None:
    task = TEMP_PIN_TASKS.pop(chat_id, None)
    if task and not task.done():
        task.cancel()


async def _temporary_pin_expirer(bot, chat_id: int, message_id: int, ttl_seconds: int) -> None:
    try:
        await asyncio.sleep(max(1, ttl_seconds))
        state = get_state(chat_id)
        if state.temp_pin_message_id == message_id:
            try:
                await bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
            except Exception:
                pass
            await safe_delete(bot, chat_id, message_id)
            state.temp_pin_message_id = None
            save_all_states()
            if state.panel_message_id:
                try:
                    await bot.pin_chat_message(chat_id=chat_id, message_id=state.panel_message_id, disable_notification=True)
                except Exception:
                    pass
            elif state.dj_mode:
                try:
                    await ensure_panel(bot, chat_id)
                except Exception:
                    logger.exception("No se pudo recrear el panel tras expirar el fijado temporal en chat %s", chat_id)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("No se pudo expirar el fijado temporal en chat %s", chat_id)


async def create_temporary_pin(bot, chat_id: int, text: str, minutes: int) -> None:
    state = get_state(chat_id)
    await cancel_temporary_pin(chat_id)

    if state.temp_pin_message_id:
        try:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=state.temp_pin_message_id)
        except Exception:
            pass
        await safe_delete(bot, chat_id, state.temp_pin_message_id)
        state.temp_pin_message_id = None
        save_all_states()

    msg = await bot.send_message(
        chat_id=chat_id,
        text=f"✨ <b>DJ-PLAN✨ {h(text)}</b>",
        parse_mode=ParseMode.HTML,
    )
    await register_bot_message(chat_id, msg.message_id)
    state.temp_pin_message_id = msg.message_id
    save_all_states()

    try:
        await bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        logger.exception("No se pudo fijar el mensaje temporal")

    await enforce_single_core_messages(bot, chat_id)
    TEMP_PIN_TASKS[chat_id] = asyncio.create_task(_temporary_pin_expirer(bot, chat_id, msg.message_id, minutes * 60))


async def ensure_panel(bot, chat_id: int) -> None:
    async with get_chat_lock(PANEL_LOCKS, chat_id):
        state = get_state(chat_id)
        if not state.dj_mode:
            return
    
        sync_panel_override_expiry(state)
        text = panel_text(state)
        markup = panel_markup()
        old_panel_id = state.panel_message_id
    
        if old_panel_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=old_panel_id,
                    text=text,
                    reply_markup=markup,
                    parse_mode=ParseMode.HTML,
                )
                save_all_states()
                return
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    return
                if "message to edit not found" in str(e).lower():
                    state.panel_message_id = None
                    save_all_states()
            except Exception:
                logger.exception("No se pudo editar el panel fijado; se recreará")
    
        if old_panel_id:
            try:
                await bot.unpin_chat_message(chat_id=chat_id, message_id=old_panel_id)
            except Exception:
                pass
            try:
                await safe_delete(bot, chat_id, old_panel_id)
            except Exception:
                pass
            state.panel_message_id = None
            save_all_states()
    
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
        state.panel_message_id = msg.message_id
        save_all_states()
        await register_bot_message(chat_id, msg.message_id)
    
        try:
            await bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
        except Exception:
            logger.exception("No se pudo fijar el panel")
    
        await enforce_single_core_messages(bot, chat_id)

async def ensure_control_panel(bot, chat_id: int) -> None:
    async with get_chat_lock(CONTROL_LOCKS, chat_id):
        state = get_state(chat_id)
        if not state.dj_mode:
            return
        text, markup = current_control_view(state)
    
        if state.control_message_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=state.control_message_id,
                    text=text,
                    reply_markup=markup,
                    parse_mode=ParseMode.HTML,
                )
                save_all_states()
                return
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    return
                if "message to edit not found" in str(e).lower():
                    state.control_message_id = None
                    save_all_states()
            except Exception:
                logger.exception("No se pudo editar el cuadro de mandos; se recreará")
    
        if state.control_message_id:
            try:
                await safe_delete(bot, chat_id, state.control_message_id)
            except Exception:
                pass
            state.control_message_id = None
            save_all_states()
    
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
        state.control_message_id = msg.message_id
        save_all_states()
        await register_bot_message(chat_id, msg.message_id)
        await enforce_single_core_messages(bot, chat_id)

async def render_control_home(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    set_control_view(state, "home", 0)
    save_all_states()
    await ensure_control_panel(bot, chat_id)


async def render_control_view(bot, chat_id: int, body_text: str, reply_markup: InlineKeyboardMarkup) -> None:
    state = get_state(chat_id)
    set_control_view(state, "home", 0)
    save_all_states()
    text = control_header(state) + body_text
    if state.control_message_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=state.control_message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
            )
            return
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                return
            if "message to edit not found" in str(e).lower():
                state.control_message_id = None
                save_all_states()
        except Exception:
            logger.exception("No se pudo editar la vista del cuadro de mandos")

    if state.control_message_id:
        try:
            await safe_delete(bot, chat_id, state.control_message_id)
        except Exception:
            pass
        state.control_message_id = None
        save_all_states()

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )
    state.control_message_id = msg.message_id
    save_all_states()
    await register_bot_message(chat_id, msg.message_id)
    await enforce_single_core_messages(bot, chat_id)


async def is_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


async def is_controller(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    state = get_state(chat_id)
    if state.assigned_dj_id == user_id:
        return True
    return await is_admin(context, chat_id, user_id)

async def controller_users_text(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> str:
    state = get_state(chat_id)
    lines = ["<b>Usuarios con control del panel</b>", ""]

    async def resolve_name(user_id: Optional[int], fallback: str = "") -> str:
        if not user_id:
            return fallback or "Sin asignar"
        try:
            member = await context.bot.get_chat_member(chat_id, user_id)
            user = getattr(member, "user", None)
            if user:
                return display_name(user)
        except Exception:
            pass
        return fallback or "Usuario no localizado"

    if state.assigned_dj_id:
        dj_name = await resolve_name(state.assigned_dj_id, state.assigned_dj_name or "DJ asignado")
        lines.append(f"🎧 DJ asignado: <b>{h(dj_name)}</b>")
    else:
        lines.append("🎧 DJ asignado: <b>Sin asignar</b>")

    if ADMIN_IDS:
        admin_names: List[str] = []
        for admin_id in sorted(ADMIN_IDS):
            admin_name = await resolve_name(admin_id, "Usuario registrado")
            if admin_name not in admin_names:
                admin_names.append(admin_name)
        if admin_names:
            lines.append("")
            lines.append("<b>Usuarios registrados con control:</b>")
            for admin_name in admin_names:
                lines.append(f"• {h(admin_name)}")
    else:
        lines.append("")
        lines.append("<i>No hay ADMIN_IDS configurados.</i>")

    return "\n".join(lines)


async def build_live_join_url(bot, chat_id: int) -> Optional[str]:
    if int(chat_id) in VOICE_CHAT_LINKS:
        return VOICE_CHAT_LINKS[int(chat_id)]
    if VOICE_CHAT_LINK:
        return VOICE_CHAT_LINK
    try:
        chat = await bot.get_chat(chat_id)
        username = getattr(chat, "username", None)
        if username:
            return f"https://t.me/{username}?videochat"
    except Exception:
        logger.exception("No se pudo resolver el username del chat %s para el acceso al videochat", chat_id)
    return None


def extract_track_from_message(message) -> Optional[Track]:
    if getattr(message, "audio", None):
        audio = message.audio
        return Track(
            title=audio.title or message.caption or audio.file_name or "Sin título",
            performer=audio.performer or "",
            duration=int(audio.duration or 0),
            file_id=audio.file_id,
            file_unique_id=audio.file_unique_id,
            mime_type=audio.mime_type or "audio",
            original_message_id=message.message_id,
        )
    if getattr(message, "voice", None):
        voice = message.voice
        return Track(
            title=message.caption or f"Voice {message.message_id}",
            performer="",
            duration=int(voice.duration or 0),
            file_id=voice.file_id,
            file_unique_id=voice.file_unique_id,
            mime_type="voice",
            original_message_id=message.message_id,
        )
    if getattr(message, "document", None):
        doc = message.document
        file_name = doc.file_name or ""
        lower = file_name.lower()
        audio_like = (doc.mime_type or "").startswith("audio/") or lower.endswith((".mp3", ".m4a", ".ogg", ".wav", ".flac", ".opus"))
        if audio_like:
            return Track(
                title=message.caption or file_name or "Sin título",
                performer="",
                duration=0,
                file_id=doc.file_id,
                file_unique_id=doc.file_unique_id,
                mime_type=doc.mime_type or "document-audio",
                original_message_id=message.message_id,
            )
    return None


def register_detected_track(chat_id: int, message_id: int, track: Track) -> None:
    bucket = TRACK_REGISTRY.setdefault(chat_id, {})
    bucket[message_id] = asdict(track)


def get_detected_track(chat_id: int, message_id: int) -> Optional[Track]:
    data = TRACK_REGISTRY.get(chat_id, {}).get(message_id)
    if not data:
        return None
    return Track(**data)


async def show_track_actions(context: ContextTypes.DEFAULT_TYPE, chat_id: int, source_message_id: int) -> None:
    state = get_state(chat_id)
    if not state.dj_mode:
        return
    existing = TRACK_CONTROL_REGISTRY.setdefault(chat_id, {}).get(source_message_id)
    if existing:
        return
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("▶️ Voice ahora", callback_data=f"det|p|{source_message_id}"),
                InlineKeyboardButton("➕ Cola", callback_data=f"det|q|{source_message_id}"),
            ],
            [
                InlineKeyboardButton("⏭️ Primera cola", callback_data=f"det|f|{source_message_id}"),
                InlineKeyboardButton("📚 Biblioteca", callback_data=f"det|l|{source_message_id}"),
            ],
        ]
    )
    msg_id = await send_temp_message(
        context.bot,
        chat_id,
        "<b>DJ-PLAN</b>\nElige qué hacer con esta canción.",
        reply_to_message_id=source_message_id,
        reply_markup=keyboard,
        ttl=3600,
    )
    if msg_id:
        TRACK_CONTROL_REGISTRY.setdefault(chat_id, {})[source_message_id] = msg_id


async def materialize_track(bot, chat_id: int, track: Track) -> Track:
    if track.local_path and Path(track.local_path).exists():
        if track.duration <= 0:
            track.duration = probe_duration_seconds(track.local_path)
        return track
    if not track.file_id:
        raise RuntimeError("La pista no tiene file_id utilizable por el bot")

    tg_file = await bot.get_file(track.file_id)
    ext = ".bin"
    if track.mime_type == "voice":
        ext = ".ogg"
    elif "ogg" in (track.mime_type or ""):
        ext = ".ogg"
    elif "mpeg" in (track.mime_type or "") or track.title.lower().endswith(".mp3"):
        ext = ".mp3"
    elif "mp4" in (track.mime_type or "") or track.title.lower().endswith(".m4a"):
        ext = ".m4a"
    elif "wav" in (track.mime_type or ""):
        ext = ".wav"

    filename = DOWNLOAD_DIR / f"{chat_id}_{secrets.token_hex(6)}{ext}"
    await tg_file.download_to_drive(custom_path=str(filename))
    track.local_path = str(filename)
    if track.duration <= 0:
        track.duration = probe_duration_seconds(track.local_path)
    return track


def extract_track_from_telethon_message(message, chat_id: int) -> Optional[Track]:
    media = getattr(message, "media", None)
    if not media:
        return None

    is_audio = bool(getattr(message, "audio", None))
    is_voice = bool(getattr(message, "voice", None))
    is_document = bool(getattr(message, "document", None))
    if not (is_audio or is_voice or is_document):
        return None

    file_name = ""
    mime_type = ""
    duration = 0
    performer = ""
    title = ""

    try:
        if getattr(message, "file", None):
            file_name = getattr(message.file, "name", "") or ""
            mime_type = getattr(message.file, "mime_type", "") or ""
    except Exception:
        pass

    doc = getattr(message, "document", None)
    attrs = getattr(doc, "attributes", []) if doc else []
    for attr in attrs:
        if hasattr(attr, "duration") and attr.duration:
            duration = int(attr.duration)
        if hasattr(attr, "performer") and attr.performer:
            performer = attr.performer
        if hasattr(attr, "title") and attr.title:
            title = attr.title
        if hasattr(attr, "voice") and attr.voice:
            mime_type = "voice"

    if not title:
        title = file_name or (getattr(message, "raw_text", "") or "").strip() or f"Track {message.id}"

    audio_like = (
        is_audio
        or is_voice
        or (mime_type.startswith("audio/") if mime_type else False)
        or file_name.lower().endswith((".mp3", ".m4a", ".ogg", ".wav", ".flac", ".opus"))
    )
    if not audio_like:
        return None

    return Track(
        title=title,
        performer=performer,
        duration=duration,
        file_id="",
        file_unique_id=f"telethon:{chat_id}:{message.id}",
        mime_type=mime_type or ("voice" if is_voice else "audio"),
        local_path="",
        original_message_id=message.id,
    )


async def scan_group_history_for_tracks(chat_id: int, limit: int = SCAN_LIMIT) -> tuple[int, int]:
    if not VOICE.client:
        raise RuntimeError("Userbot no iniciado")

    state = get_state(chat_id)
    found = 0
    added = 0
    existing_keys = {library_item_key_from_dict(item) for item in state.library}

    async for message in VOICE.client.iter_messages(chat_id, limit=limit):
        try:
            track = extract_track_from_telethon_message(message, chat_id)
            if not track:
                continue
            found += 1
            unique_key = library_item_key(track)
            if unique_key in existing_keys:
                continue

            ext = ".bin"
            file_name = ""
            try:
                if getattr(message, "file", None):
                    file_name = getattr(message.file, "name", "") or ""
            except Exception:
                pass

            if file_name:
                suffix = Path(file_name).suffix
                if suffix:
                    ext = suffix
            elif track.mime_type == "voice" or "ogg" in track.mime_type:
                ext = ".ogg"
            elif "mpeg" in track.mime_type:
                ext = ".mp3"
            elif "mp4" in track.mime_type:
                ext = ".m4a"
            elif "wav" in track.mime_type:
                ext = ".wav"

            filename = DOWNLOAD_DIR / f"{chat_id}_{message.id}{ext}"
            if not filename.exists():
                await VOICE.client.download_media(message, file=str(filename))

            track.local_path = str(filename)
            if track.duration <= 0:
                track.duration = probe_duration_seconds(track.local_path)
            state.library.append(asdict(track))
            existing_keys.add(unique_key)
            added += 1
        except Exception:
            logger.exception("Error rastreando mensaje %s en chat %s", getattr(message, "id", "?"), chat_id)

    state.library = dedupe_library_items(state.library)
    save_all_states()
    return found, added


async def cleanup_old_files(chat_id: int) -> None:
    state = get_state(chat_id)
    keep_paths = set()
    if state.now_playing and state.now_playing.get("local_path"):
        keep_paths.add(state.now_playing["local_path"])
    for item in state.queue:
        if item.get("local_path"):
            keep_paths.add(item["local_path"])
    for item in state.history[-10:]:
        if item.get("local_path"):
            keep_paths.add(item["local_path"])
    for item in state.library:
        if item.get("local_path"):
            keep_paths.add(item["local_path"])
    for path in DOWNLOAD_DIR.glob(f"{chat_id}_*"):
        try:
            if str(path) not in keep_paths and path.is_file():
                path.unlink(missing_ok=True)
        except Exception:
            logger.exception("No se pudo borrar %s", path)


async def _background_scan(chat_id: int, limit: int) -> None:
    try:
        found, added = await scan_group_history_for_tracks(chat_id, limit=limit)
        if VOICE.application:
            await send_temp_message(
                VOICE.application.bot,
                chat_id,
                (
                    "<b>🧭 Rastreo terminado</b>\n\n"
                    f"🎵 Encontradas: <b>{found}</b>\n"
                    f"📚 Nuevas añadidas: <b>{added}</b>"
                ),
                ttl=120,
            )
            await ensure_panel(VOICE.application.bot, chat_id)
            await ensure_control_panel(VOICE.application.bot, chat_id)
    except Exception:
        logger.exception("Error en rastreo de canciones del chat %s", chat_id)
        if VOICE.application:
            await send_temp_message(VOICE.application.bot, chat_id, "❌ Error durante el rastreo.", ttl=60)
    finally:
        SCAN_TASKS.pop(chat_id, None)


async def start_background_scan(chat_id: int, limit: int = SCAN_LIMIT) -> bool:
    task = SCAN_TASKS.get(chat_id)
    if task and not task.done():
        return False
    SCAN_TASKS[chat_id] = asyncio.create_task(_background_scan(chat_id, limit))
    return True


async def cancel_auto_next(chat_id: int) -> None:
    AUTO_NEXT_TASKS.pop(chat_id, None)
    WATCHDOG_RUNTIME.pop(chat_id, None)


async def schedule_auto_next(chat_id: int, duration: int, offset: int) -> None:
    state = get_state(chat_id)
    if not state.now_playing:
        WATCHDOG_RUNTIME.pop(chat_id, None)
        return
    WATCHDOG_RUNTIME[chat_id] = {
        "token": track_key(state.now_playing),
        "last_remaining": None,
        "none_hits": 0,
        "last_advance_at": 0.0,
        "next_refresh": 0.0,
    }


async def simulate_panel_next(chat_id: int, reason: str = "panel_next") -> bool:
    state = get_state(chat_id)
    if not state.queue:
        return False
    try:
        state.live_enabled = True
        save_all_states()
        logger.info("Simulando panel_next en chat %s (%s)", chat_id, reason)
        await VOICE.play_next_from_queue(chat_id)
        return True
    except Exception:
        logger.exception("Fallo al ejecutar siguiente en chat %s", chat_id)
        return False


async def simulate_panel_prev(bot, chat_id: int, reason: str = "panel_prev") -> bool:
    state = get_state(chat_id)
    if not state.history:
        return False
    try:
        previous = Track(**state.history.pop())
        if state.now_playing:
            state.queue.insert(0, dict(state.now_playing))
        state.live_enabled = True
        save_all_states()
        logger.info("Simulando panel_prev en chat %s (%s)", chat_id, reason)
        await VOICE.play_track(bot, chat_id, previous)
        return True
    except Exception:
        logger.exception("Fallo al ejecutar anterior en chat %s", chat_id)
        return False


async def _watchdog_loop() -> None:
    import time as _time

    while True:
        try:
            await asyncio.sleep(WATCHDOG_TICK_SECONDS)
            bot = VOICE.application.bot if VOICE.application else None
            now_ts = _time.time()

            for chat_id, state in list(STATE_CACHE.items()):
                if not state.dj_mode:
                    WATCHDOG_RUNTIME.pop(chat_id, None)
                    continue

                runtime = WATCHDOG_RUNTIME.setdefault(
                    chat_id,
                    {"token": None, "last_remaining": None, "none_hits": 0, "last_advance_at": 0.0, "next_refresh": 0.0},
                )

                if bot and now_ts >= float(runtime.get("next_refresh", 0.0)):
                    runtime["next_refresh"] = now_ts + UI_REFRESH_SECONDS
                    try:
                        await ensure_panel(bot, chat_id)
                        await ensure_control_panel(bot, chat_id)
                    except Exception:
                        logger.exception("No se pudo refrescar el panel en chat %s", chat_id)

                if not state.live_enabled:
                    runtime["token"] = None
                    runtime["last_remaining"] = None
                    runtime["none_hits"] = 0
                    continue

                if state.auto_track_enabled and not state.now_playing and state.queue:
                    if now_ts - float(runtime.get("last_advance_at", 0.0)) >= 2:
                        runtime["last_advance_at"] = now_ts
                        await simulate_panel_next(chat_id, reason="auto_track_idle")
                    continue

                if not state.now_playing:
                    runtime["token"] = None
                    runtime["last_remaining"] = None
                    runtime["none_hits"] = 0
                    continue

                token = track_key(state.now_playing)
                if runtime.get("token") != token:
                    runtime["token"] = token
                    runtime["last_remaining"] = None
                    runtime["none_hits"] = 0

                if not state.auto_track_enabled or not state.queue:
                    continue

                trigger = 0 if state.auto_sig_seconds < 0 else int(state.auto_sig_seconds)
                remaining = remaining_seconds(state)

                if remaining is not None:
                    runtime["last_remaining"] = remaining
                    runtime["none_hits"] = 0
                    if remaining <= trigger and now_ts - float(runtime.get("last_advance_at", 0.0)) >= 2:
                        runtime["last_advance_at"] = now_ts
                        await simulate_panel_next(chat_id, reason=f"auto_sig_{trigger}")
                    continue

                runtime["none_hits"] = int(runtime.get("none_hits", 0)) + 1
                track = Track(**state.now_playing)
                elapsed = 0
                if state.play_started_at is not None:
                    elapsed = max(0, int(now_ts - state.play_started_at))

                should_advance = False
                if runtime.get("last_remaining") is not None and int(runtime.get("none_hits", 0)) >= 2:
                    should_advance = True
                elif track.duration > 0 and elapsed >= max(0, int(track.duration) - trigger):
                    should_advance = True
                elif track.duration <= 0 and elapsed >= max(UNKNOWN_END_FALLBACK_SECONDS, trigger):
                    should_advance = True

                if should_advance and now_ts - float(runtime.get("last_advance_at", 0.0)) >= 2:
                    runtime["last_advance_at"] = now_ts
                    await simulate_panel_next(chat_id, reason="auto_track_unknown_timer")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Error en watchdog global de auto-continuar")


class VoiceEngine:
    def __init__(self) -> None:
        self.client: Optional[TelegramClient] = None
        self.calls: Optional[PyTgCalls] = None
        self.application: Optional[Application] = None

    async def start(self, application: Application) -> None:
        global WATCHDOG_TASK
        self.application = application
        self.client = TelegramClient(load_userbot_string_session(), API_ID, API_HASH)
        await self.client.start()
        self.calls = PyTgCalls(self.client)
        await self.calls.start()
        if WATCHDOG_TASK is None or WATCHDOG_TASK.done():
            WATCHDOG_TASK = asyncio.create_task(_watchdog_loop())
        logger.info("Userbot + voice engine iniciados")

    async def ensure_videochat_started(self, chat_id: int) -> bool:
        if not self.client:
            return False
        existing = await self._get_input_group_call(chat_id)
        if existing:
            return True
        try:
            entity = await self.client.get_entity(chat_id)
            await self.client(functions.phone.CreateGroupCallRequest(
                peer=entity,
                title='DJ-PLAN'
            ))
            logger.info('Videochat iniciado en chat %s', chat_id)
            return True
        except Exception:
            logger.exception('No se pudo iniciar el videochat en chat %s', chat_id)
            return False

    async def stop(self) -> None:
        global WATCHDOG_TASK
        for chat_id in list(AUTO_NEXT_TASKS.keys()):
            await cancel_auto_next(chat_id)
        if WATCHDOG_TASK and not WATCHDOG_TASK.done():
            WATCHDOG_TASK.cancel()
        WATCHDOG_TASK = None
        WATCHDOG_RUNTIME.clear()
        if self.client:
            await self.client.disconnect()
            self.client = None
        self.calls = None

    async def play_file(self, chat_id: int, file_path: str) -> None:
        if not self.calls:
            raise RuntimeError("Voice engine no iniciado")
        await self.calls.play(chat_id, file_path)

    async def _apply_volume(self, chat_id: int, volume: int) -> None:
        if not self.calls:
            return
        methods_to_try = [
            "change_volume_call",
            "set_call_volume",
            "change_volume",
            "set_volume",
        ]
        for name in methods_to_try:
            method = getattr(self.calls, name, None)
            if callable(method):
                try:
                    await method(chat_id, volume)
                    return
                except TypeError:
                    try:
                        await method(volume, chat_id)
                        return
                    except Exception:
                        continue
                except Exception:
                    continue

    async def play_track(self, bot, chat_id: int, track: Track) -> None:
        import time as _time
        state = get_state(chat_id)
        await self.ensure_videochat_started(chat_id)
        if not track.local_path or not Path(track.local_path).exists():
            track = await materialize_track(bot, chat_id, track)
        if track.duration <= 0 and track.local_path and Path(track.local_path).exists():
            track.duration = probe_duration_seconds(track.local_path)
        await self.play_file(chat_id, track.local_path)
        await self._apply_volume(chat_id, state.volume)
        state.live_enabled = True
        state.now_playing = asdict(track)
        state.paused = False
        state.play_started_at = int(_time.time())
        state.paused_remaining = None
        save_all_states()
        await ensure_panel(bot, chat_id)
        await ensure_control_panel(bot, chat_id)
        await cleanup_old_files(chat_id)
        await schedule_auto_next(chat_id, track.duration, state.auto_sig_seconds)

    async def play_next_from_queue(self, chat_id: int) -> None:
        state = get_state(chat_id)
        bot = self.application.bot if self.application else None
        if not bot:
            return
        await cancel_auto_next(chat_id)
        if not state.queue:
            state.now_playing = None
            state.paused = False
            state.play_started_at = None
            state.paused_remaining = None
            save_all_states()
            await ensure_panel(bot, chat_id)
            await ensure_control_panel(bot, chat_id)
            await cleanup_old_files(chat_id)
            return

        if getattr(state, "dj_shuffle_enabled", False) and len(state.queue) > 1:
            next_data = state.queue.pop(secrets.randbelow(len(state.queue)))
        else:
            next_data = state.queue.pop(0)
        if state.now_playing:
            old_key = track_key(state.now_playing)
            new_key = track_key(next_data)
            if old_key and old_key != new_key:
                state.history.append(state.now_playing)
                state.history = state.history[-25:]
        save_all_states()
        await self.play_track(bot, chat_id, Track(**next_data))

    async def toggle_live(self, chat_id: int) -> bool:
        state = get_state(chat_id)

        if state.live_enabled:
            if state.now_playing:
                current = dict(state.now_playing)
                current_key = track_key(current)
                first_key = track_key(state.queue[0]) if state.queue else ""
                if not state.queue or current_key != first_key:
                    state.queue.insert(0, current)
            state.live_enabled = False
            state.now_playing = None
            state.paused = False
            state.play_started_at = None
            state.paused_remaining = None
            save_all_states()
            await cancel_auto_next(chat_id)
            await self.leave(chat_id, end_videochat=True)
            if self.application:
                await ensure_panel(self.application.bot, chat_id)
                await ensure_control_panel(self.application.bot, chat_id)
            return False

        state.live_enabled = True
        state.paused = False
        save_all_states()
        await self.ensure_videochat_started(chat_id)

        if state.queue:
            await self.play_next_from_queue(chat_id)
        elif self.application:
            await ensure_panel(self.application.bot, chat_id)
            await ensure_control_panel(self.application.bot, chat_id)
        return True


    async def set_volume(self, chat_id: int, volume: int) -> int:
        state = get_state(chat_id)
        state.volume = max(0, min(200, int(volume)))
        save_all_states()
        await self._apply_volume(chat_id, state.volume)
        if self.application:
            await ensure_control_panel(self.application.bot, chat_id)
        return state.volume

    async def change_volume(self, chat_id: int, delta: int) -> int:
        state = get_state(chat_id)
        return await self.set_volume(chat_id, int(state.volume) + int(delta))

    async def _get_input_group_call(self, chat_id: int):
        if not self.client:
            return None
        try:
            entity = await self.client.get_entity(chat_id)
            if getattr(entity, "megagroup", False) or getattr(entity, "broadcast", False):
                full = await self.client(functions.channels.GetFullChannelRequest(channel=entity))
            else:
                full = await self.client(functions.messages.GetFullChatRequest(chat_id=abs(int(chat_id))))
            full_chat = getattr(full, "full_chat", None)
            call = getattr(full_chat, "call", None) if full_chat else None
            if not call:
                return None
            call_id = getattr(call, "id", None)
            access_hash = getattr(call, "access_hash", None)
            if call_id is None or access_hash is None:
                return None
            return types.InputGroupCall(id=call_id, access_hash=access_hash)
        except Exception:
            logger.exception("No se pudo obtener la group call para cerrar el videochat en chat %s", chat_id)
            return None

    async def end_videochat(self, chat_id: int) -> bool:
        if not self.client:
            return False
        input_call = await self._get_input_group_call(chat_id)
        if not input_call:
            return False
        try:
            await self.client(functions.phone.DiscardGroupCallRequest(call=input_call))
            logger.info("Videochat cerrado en chat %s", chat_id)
            return True
        except Exception:
            logger.exception("No se pudo cerrar el videochat en chat %s", chat_id)
            return False

    async def leave(self, chat_id: int, *, end_videochat: bool = False) -> None:
        await cancel_auto_next(chat_id)
        if end_videochat:
            await self.end_videochat(chat_id)
        if self.calls:
            try:
                await self.calls.leave_call(chat_id)
            except Exception:
                pass


VOICE = VoiceEngine()



# =========================
# MÓDULO: ADMIN PLAN / CONFIGURACIÓN FÁCIL
# =========================
DEFAULT_ADMIN_CONFIG: Dict[str, Any] = {
    "validation_enabled": VALIDATION_ENABLED,
    "validation_timeout_minutes": VALIDATION_TIMEOUT_MINUTES,
    "validation_reminder_minutes": VALIDATION_REMINDER_MINUTES,
    "validation_kick_if_timeout": VALIDATION_KICK_IF_TIMEOUT,
    "validation_delete_wrong_messages": VALIDATION_DELETE_WRONG_MESSAGES,
    "validation_questions": VALIDATION_QUESTIONS,
    "validation_public_join_message": VALIDATION_PUBLIC_JOIN_MESSAGE,
    "validation_intro_message": VALIDATION_INTRO_MESSAGE,
    "validation_reminder_message": VALIDATION_REMINDER_MESSAGE,
    "validation_timeout_message": VALIDATION_TIMEOUT_MESSAGE,
    "validation_approved_message": VALIDATION_APPROVED_MESSAGE,
    "validation_rejected_message": VALIDATION_REJECTED_MESSAGE,
    "validation_approver_mode": "telegram_admins",  # telegram_admins | admin_ids | creator
    "validation_auto_approve_join_requests": True,
    "command_cleanup_mode": "off",  # off | instant | ttl
    "command_cleanup_ttl_seconds": 15,
    "pregonero_max_mentions_per_message": 4,
    "pregonero_text": "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}",
    "pregonero_media": None,
    "pregonero_buttons": [],
    "pregonero_media_position": "above",
    "pregonero_manual_users": [],
    "farewell_enabled": True,
    "farewell_message": "👋 {mention} ha salido del grupo.",
    "farewell_media": None,
    "farewell_buttons": [],
    "farewell_media_position": "above",
    "rules_auto_after_approve": False,
    "rules_media": None,
    "rules_buttons": [],
    "rules_media_position": "above",
    "validation_public_join_media_position": "above",
    "validation_intro_media": None,
    "validation_intro_buttons": [],
    "validation_intro_media_position": "above",
    "validation_reminder_media": None,
    "validation_reminder_buttons": [],
    "validation_reminder_media_position": "above",
    "validation_timeout_media": None,
    "validation_timeout_buttons": [],
    "validation_timeout_media_position": "above",
    "validation_approved_media_position": "above",
    "validation_rejected_media_position": "above",
    "validation_public_join_media": None,
    "validation_public_join_buttons": [],
    "validation_approved_media": None,
    "validation_approved_buttons": [],
    "validation_rejected_media": None,
    "validation_rejected_buttons": [],
    "rules_text": "📌 Normas del grupo\n\n1. Respeta al resto.\n2. No spam.\n3. Preséntate al entrar.",
    "hot_mode": "manual",
    "hot_level": 1,
    "hot_random_include_level5": False,
    "hot_auto_enabled": False,
    "hot_auto_interval_seconds": 180,
    "hot_auto_include_hot": False,
    "hot_auto_delete_enabled": True,
    "hot_auto_delete_seconds": 45,
    "hot_custom_questions": {},
    "hot_ranking": {},
    "hot_last_auto_ts": 0,
    "hot_auto_min_messages": 5,
    "hot_auto_min_users": 2,
    "hot_auto_activity_window_seconds": 240,
    "hot_pin_button_text": "🎲 Enviar preguntita / retito",
    "dj_music_pin_text": "🎧 <b>Música en directo</b>\n\nPulsa el botón para unirte al directo musical del grupo.",
    "dj_music_pin_button_text": "🎧 Escuchar música",
    "chat_title": "",
}

def admin_cfg(chat_id: int) -> Dict[str, Any]:
    state = get_state(chat_id)
    if not isinstance(state.admin_config, dict):
        state.admin_config = {}
    changed = False
    for key, value in DEFAULT_ADMIN_CONFIG.items():
        if key not in state.admin_config:
            state.admin_config[key] = value
            changed = True
    if changed:
        save_all_states()
    return state.admin_config

def cfg_value(chat_id: int, key: str, default: Any = None) -> Any:
    return admin_cfg(chat_id).get(key, DEFAULT_ADMIN_CONFIG.get(key, default))

def cfg_set(chat_id: int, key: str, value: Any) -> None:
    admin_cfg(chat_id)[key] = value
    save_all_states()

def cfg_questions(chat_id: int) -> List[str]:
    raw = cfg_value(chat_id, "validation_questions", VALIDATION_QUESTIONS)
    if isinstance(raw, str):
        questions = [x.strip() for x in raw.split("|") if x.strip()]
    else:
        questions = [str(x).strip() for x in list(raw or []) if str(x).strip()]
    return questions or ["Nombre:", "Edad:", "Lugar:", "¿Qué buscas en este chat?"]

def bool_label(value: Any) -> str:
    return "ON ✅" if bool(value) else "OFF ❌"

def parse_minutes_arg(raw: str, default_minutes: int = 10) -> int:
    raw = (raw or "").strip().lower()
    try:
        if raw.endswith("h"):
            return max(1, int(raw[:-1]) * 60)
        if raw.endswith("m"):
            return max(1, int(raw[:-1]))
        return max(1, int(raw or default_minutes))
    except Exception:
        return default_minutes


def command_cleanup_label(chat_id: int) -> str:
    mode = str(cfg_value(chat_id, "command_cleanup_mode", "off"))
    ttl = int(cfg_value(chat_id, "command_cleanup_ttl_seconds", 15) or 15)
    if mode == "instant":
        return "al ejecutar"
    if mode == "ttl":
        return f"tras {ttl}s"
    return "OFF"


def next_command_cleanup_mode(chat_id: int) -> str:
    current = str(cfg_value(chat_id, "command_cleanup_mode", "off"))
    return {"off": "instant", "instant": "ttl", "ttl": "off"}.get(current, "off")


async def cleanup_command_invocation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    chat_id = update.effective_chat.id
    mode = str(cfg_value(chat_id, "command_cleanup_mode", "off"))
    if mode == "off":
        return
    message_id = update.message.message_id
    if mode == "instant":
        await safe_delete(context.bot, chat_id, message_id)
        return
    ttl = int(cfg_value(chat_id, "command_cleanup_ttl_seconds", 15) or 15)
    asyncio.create_task(delete_later(context.bot, chat_id, message_id, max(1, ttl)))


def user_record_from_user(user) -> Dict[str, Any]:
    return {
        "user_id": int(getattr(user, "id", 0) or 0),
        "name": display_name(user),
        "username": f"@{getattr(user, 'username', '')}" if getattr(user, "username", None) else "",
        "is_bot": bool(getattr(user, "is_bot", False)),
    }


def remember_member_activity(chat_id: int, user, *, kind: str = "message", source: str = "") -> None:
    if not user or getattr(user, "is_bot", False):
        return
    state = get_state(chat_id)
    uid = str(int(user.id))
    now = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
    previous = state.member_activity.get(uid, {})
    count_key = "message_count" if kind != "entry" else "entry_count"
    record = {
        **previous,
        **user_record_from_user(user),
        "last_seen_ts": now,
        "last_seen_kind": kind,
        "last_source": source or kind,
        count_key: int(previous.get(count_key, 0) or 0) + 1,
    }
    if "first_seen_ts" not in record:
        record["first_seen_ts"] = now
    state.member_activity[uid] = record
    save_all_states()


def remember_entry(chat_id: int, user, *, source: str = "new_chat_member") -> None:
    if not user or getattr(user, "is_bot", False):
        return
    state = get_state(chat_id)
    now = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
    entry = {
        **user_record_from_user(user),
        "joined_ts": now,
        "source": source,
    }
    state.entry_log.append(entry)
    state.entry_log = state.entry_log[-300:]
    remember_member_activity(chat_id, user, kind="entry", source=source)
    uid = str(int(user.id))
    state.member_activity.setdefault(uid, {}).update({"joined_ts": now, "join_source": source})
    save_all_states()


def mention_from_known_user(user_id: int, record: Optional[Dict[str, Any]] = None) -> str:
    record = record or {}
    name = record.get("name") or record.get("username") or str(user_id)
    return f"<a href=\"tg://user?id={int(user_id)}\">{h(name)}</a>"


def mark_user_muted(chat_id: int, user_id: int, *, user=None, reason: str = "", until_ts: Optional[int] = None) -> None:
    state = get_state(chat_id)
    existing = state.muted_users.get(str(user_id), {})
    base = user_record_from_user(user) if user else {}
    if not base:
        base = {
            "user_id": int(user_id),
            "name": existing.get("name", "") or state.member_activity.get(str(user_id), {}).get("name", ""),
            "username": existing.get("username", "") or state.member_activity.get(str(user_id), {}).get("username", ""),
        }
    base.update({
        "muted_ts": _now_ts() if "_now_ts" in globals() else int(__import__("time").time()),
        "reason": reason or existing.get("reason", "silenciado"),
        "until_ts": until_ts,
    })
    state.muted_users[str(user_id)] = base
    save_all_states()


def unmark_user_muted(chat_id: int, user_id: int) -> None:
    state = get_state(chat_id)
    state.muted_users.pop(str(user_id), None)
    save_all_states()


def fmt_ts(ts: Any) -> str:
    try:
        from datetime import datetime
        return datetime.fromtimestamp(int(ts)).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "—"


def admin_muted_users_text(chat_id: int) -> str:
    state = get_state(chat_id)
    rows = [(uid, r) for uid, r in state.muted_users.items()]
    rows += [(uid, r) for uid, r in state.validation_users.items() if r.get("status") in ("answering", "pending_admin", "timeout")]
    dedup: Dict[str, Dict[str, Any]] = {}
    for uid, record in rows:
        dedup[str(uid)] = {**dedup.get(str(uid), {}), **record}
    if not dedup:
        return "<b>🔇 Usuarios silenciados</b>\n\nNo tengo usuarios silenciados registrados."
    lines = ["<b>🔇 Usuarios silenciados</b>", ""]
    for uid, record in list(dedup.items())[:60]:
        status = record.get("status") or record.get("reason") or "silenciado"
        lines.append(f"• {mention_from_known_user(int(uid), record)} — <b>{h(status)}</b> · {fmt_ts(record.get('muted_ts') or record.get('joined_ts'))}")
    if len(dedup) > 60:
        lines.append(f"… y {len(dedup)-60} más")
    return "\n".join(lines)


def admin_last_entries_text(chat_id: int, limit: int = 20) -> str:
    state = get_state(chat_id)
    entries = list(reversed(state.entry_log[-limit:]))
    if not entries:
        return "<b>🚪 Últimas entradas</b>\n\nAún no tengo entradas registradas desde que activaste este sistema."
    lines = ["<b>🚪 Últimas entradas</b>", ""]
    for entry in entries:
        uid = int(entry.get("user_id") or 0)
        source = entry.get("source") or "entrada"
        lines.append(f"• {mention_from_known_user(uid, entry)} · {fmt_ts(entry.get('joined_ts'))} · <i>{h(source)}</i>")
    return "\n".join(lines)


def admin_inactive_users_text(chat_id: int, days: int = 10, limit: int = 60) -> str:
    state = get_state(chat_id)
    now = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
    cutoff = now - days * 86400
    inactive = [
        (uid, r) for uid, r in state.member_activity.items()
        if int(r.get("last_seen_ts") or 0) < cutoff and not r.get("is_bot")
    ]
    inactive.sort(key=lambda item: int(item[1].get("last_seen_ts") or 0))
    if not inactive:
        return f"<b>🕙 Inactivos {days} días</b>\n\nNo tengo usuarios inactivos registrados en los últimos {days} días."
    lines = [f"<b>🕙 Inactivos {days} días</b>", ""]
    for uid, record in inactive[:limit]:
        lines.append(f"• {mention_from_known_user(int(uid), record)} — último registro: <b>{fmt_ts(record.get('last_seen_ts'))}</b>")
    if len(inactive) > limit:
        lines.append(f"… y {len(inactive)-limit} más")
    lines.append("\n<i>Nota: Telegram no permite al bot listar todos los miembros antiguos. Esto usa usuarios vistos por entradas, mensajes o validaciones.</i>")
    return "\n".join(lines)


def admin_ranking_text(chat_id: int, limit: int = 20) -> str:
    state = get_state(chat_id)
    ranked = [
        (uid, r, int(r.get("message_count") or 0))
        for uid, r in state.member_activity.items()
        if not r.get("is_bot")
    ]
    ranked.sort(key=lambda item: item[2], reverse=True)
    if not ranked:
        return "<b>🏆 Ranking de actividad</b>\n\nAún no hay actividad registrada."
    lines = ["<b>🏆 Ranking de actividad</b>", ""]
    for pos, (uid, record, count) in enumerate(ranked[:limit], start=1):
        lines.append(f"{pos}. {mention_from_known_user(int(uid), record)} — <b>{count}</b> mensajes")
    return "\n".join(lines)


def add_action_log(chat_id: int, action: str, detail: str = "", *, user_id: Optional[int] = None) -> None:
    try:
        state = get_state(chat_id)
        state.action_log.append({
            "ts": _now_ts() if "_now_ts" in globals() else int(__import__("time").time()),
            "action": str(action),
            "detail": str(detail or ""),
            "user_id": int(user_id) if user_id else None,
        })
        state.action_log = state.action_log[-800:]
        save_all_states()
    except Exception:
        logger.exception("No se pudo registrar acción del bot")


def mark_user_expelled(chat_id: int, user_id: int, *, record: Optional[Dict[str, Any]] = None, reason: str = "", by_user_id: Optional[int] = None) -> None:
    try:
        state = get_state(chat_id)
        base = dict(record or {})
        base.setdefault("user_id", int(user_id))
        base["expelled_ts"] = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
        base["reason"] = reason or base.get("reason") or "expulsado"
        if by_user_id:
            base["by_user_id"] = int(by_user_id)
        state.expelled_users[str(int(user_id))] = base
        save_all_states()
        add_action_log(chat_id, "expulsión", f"{base.get('name') or user_id} · {base.get('reason','')}", user_id=by_user_id)
    except Exception:
        logger.exception("No se pudo registrar usuario expulsado")


def admin_expelled_users_text(chat_id: int, limit: int = 80) -> str:
    state = get_state(chat_id)
    rows = sorted(state.expelled_users.items(), key=lambda item: int(item[1].get("expelled_ts") or 0), reverse=True)
    if not rows:
        return "<b>🚫 Usuarios expulsados</b>\n\nNo tengo expulsiones registradas por el bot."
    lines = ["<b>🚫 Usuarios expulsados</b>", ""]
    for uid, record in rows[:limit]:
        reason = record.get("reason") or "expulsado"
        lines.append(f"• {mention_from_known_user(int(uid), record)} — <b>{h(reason)}</b> · {fmt_ts(record.get('expelled_ts'))}")
    if len(rows) > limit:
        lines.append(f"… y {len(rows)-limit} más")
    return "\n".join(lines)


def admin_action_log_text(chat_id: int, days: int = 3, limit: int = 100) -> str:
    state = get_state(chat_id)
    now = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
    cutoff = now - days * 86400
    rows = [r for r in state.action_log if int(r.get("ts") or 0) >= cutoff]
    rows.sort(key=lambda r: int(r.get("ts") or 0), reverse=True)
    if not rows:
        return f"<b>📜 LOG {days}d</b>\n\nNo hay acciones registradas en los últimos {days} días."
    lines = [f"<b>📜 LOG {days}d</b>", ""]
    for r in rows[:limit]:
        lines.append(f"• {fmt_ts(r.get('ts'))} · <b>{h(r.get('action',''))}</b> — {h(r.get('detail',''))}")
    if len(rows) > limit:
        lines.append(f"… y {len(rows)-limit} más")
    return "\n".join(lines)


def cfg_back_markup(chat_id: int, block: str = "") -> InlineKeyboardMarkup:
    target = f"cfg|block|{chat_id}|{block}" if block else f"cfg|open|{chat_id}"
    label = "🔙 Volver al bloque" if block else "🔙 Volver al panel"
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=target)]])


def cfg_status(value: Any) -> str:
    return "✅" if bool(value) else "❌"


def media_status(chat_id: int, profile: str) -> str:
    try:
        return "✅" if cfg_value(chat_id, profile_field(profile, "media"), None) else "❌"
    except Exception:
        return "❌"


def buttons_status(chat_id: int, profile: str) -> str:
    try:
        buttons = cfg_value(chat_id, profile_field(profile, "buttons"), [])
        return "✅" if isinstance(buttons, list) and len(buttons) > 0 else "❌"
    except Exception:
        return "❌"


def media_position_label(chat_id: int, profile: str) -> str:
    try:
        pos = str(cfg_value(chat_id, profile_field(profile, "position"), "above"))
    except Exception:
        pos = "above"
    return "⬆️ Arriba" if pos != "below" else "⬇️ Debajo"


def section_button(title: str, chat_id: int) -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton(title, callback_data=f"cfg|noop|{chat_id}")]


def preview_button(chat_id: int, profile: str, label: str) -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton(label, callback_data=f"cfg|preview|{chat_id}|{profile}")]


def profile_control_rows(chat_id: int, profile: str, text_label: str = "✏️ Texto") -> List[List[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton(text_label, callback_data=f"cfg|edit_text|{chat_id}|{profile_field(profile, 'text')}"),
            InlineKeyboardButton(f"🖼 Media {media_status(chat_id, profile)}", callback_data=f"cfg|media|{chat_id}|{profile}"),
            InlineKeyboardButton(f"⌨️ Botones {buttons_status(chat_id, profile)}", callback_data=f"cfg|buttons|{chat_id}|{profile}"),
        ],
        [InlineKeyboardButton(f"📍 Multimedia {media_position_label(chat_id, profile)}", callback_data=f"cfg|pos|{chat_id}|{profile}")],
    ]


def known_pregonero_mentions(chat_id: int) -> List[str]:
    state = get_state(chat_id)
    users: Dict[str, Dict[str, Any]] = {}
    for uid, record in state.member_activity.items():
        if not record.get("is_bot"):
            users[str(uid)] = record
    for uid, record in state.validation_users.items():
        if record.get("status") in ("validated", "pending_admin", "answering"):
            users.setdefault(str(uid), record)
    for entry in state.entry_log:
        uid = str(entry.get("user_id") or "")
        if uid:
            users.setdefault(uid, entry)
    mentions: List[str] = []
    for uid, record in users.items():
        try:
            mentions.append(mention_from_known_user(int(uid), record))
        except Exception:
            continue

    manual = cfg_value(chat_id, "pregonero_manual_users", [])
    if isinstance(manual, str):
        manual = [manual]
    for raw in list(manual or []):
        value = str(raw).strip()
        if not value:
            continue
        if value.startswith("@"):
            mentions.append(h(value))
        elif value.isdigit():
            mentions.append(f'<a href="tg://user?id={int(value)}">{h(value)}</a>')
        elif value.startswith("tg://user?id="):
            mentions.append(f'<a href="{h(value)}">usuario</a>')
        else:
            mentions.append(h(value))

    deduped: List[str] = []
    seen: set[str] = set()
    for mention in mentions:
        key = mention.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(mention)
    return deduped


async def send_pregonero(context: ContextTypes.DEFAULT_TYPE, chat_id: int, *, title: str = "") -> None:
    mentions = known_pregonero_mentions(chat_id)
    if not mentions:
        await send_temp_message(context.bot, chat_id, "📣 No tengo usuarios registrados todavía para mencionar.", ttl=40)
        return
    max_per_message = int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4) or 4)
    max_per_message = max(1, min(4, max_per_message))
    template = str(cfg_value(chat_id, "pregonero_text", "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}"))
    if title:
        template = title + "\n\n{mentions}"
    for start in range(0, len(mentions), max_per_message):
        chunk = mentions[start:start + max_per_message]
        text = template.replace("{mentions}", " ".join(chunk)).replace("{count}", str(len(chunk))).replace("{total}", str(len(mentions)))
        await send_configured_profile_message(context.bot, chat_id, "pregonero", text)
        await asyncio.sleep(0.6)
    add_action_log(chat_id, "pregonero", f"{len(mentions)} menciones en bloques de {max_per_message}")


def ping_text() -> str:
    uptime = max(0, (_now_ts() if "_now_ts" in globals() else int(__import__("time").time())) - BOT_STARTED_AT)
    minutes, seconds = divmod(uptime, 60)
    hours, minutes = divmod(minutes, 60)
    return f"🏓 <b>PONG</b>\n\n✅ Bot activo\n⏱️ Uptime: <b>{hours:02d}:{minutes:02d}:{seconds:02d}</b>"


def command_handler(names, callback):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat and update.effective_user:
            remember_member_activity(update.effective_chat.id, update.effective_user, kind="command", source=(update.message.text.split()[0] if update.message and update.message.text else "command"))
        await callback(update, context)
        await cleanup_command_invocation(update, context)
    return CommandHandler(names, wrapped)


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not chat_is_allowed(update.effective_chat.id):
        return
    await update.message.reply_html(ping_text())


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE, kind: str) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    if kind == "muted":
        await update.message.reply_html(admin_muted_users_text(chat_id))
    elif kind == "entries":
        await update.message.reply_html(admin_last_entries_text(chat_id))
    elif kind == "inactive":
        await update.message.reply_html(admin_inactive_users_text(chat_id, 10))
    elif kind == "ranking":
        await update.message.reply_html(admin_ranking_text(chat_id))


async def silenciados_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "muted")


async def entradas_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "entries")


async def inactivos_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "inactive")


async def ranking_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "ranking")


async def pregonero_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    raw = " ".join(context.args).strip().lower()
    # Permite /pregonero y también /el plan te llama.
    if (update.message.text or "").lower().startswith("/el") and raw != "plan te llama":
        await update.message.reply_text("Uso: /el plan te llama")
        return
    await send_pregonero(context, chat_id)

def admin_panel_text(chat_id: int) -> str:
    cfg = admin_cfg(chat_id)
    state = get_state(chat_id)
    pending = sum(1 for r in state.validation_users.values() if r.get("status") == "pending_admin")
    answering = sum(1 for r in state.validation_users.values() if r.get("status") == "answering")
    return (
        "<b>🛡️ ADMIN PLAN</b>\n\n"
        f"Validación: <b>{bool_label(cfg.get('validation_enabled'))}</b>\n"
        f"Preguntas: <b>{len(cfg_questions(chat_id))}</b>\n"
        f"Tiempo límite: <b>{cfg.get('validation_timeout_minutes')} min</b>\n"
        f"Recordatorio: <b>{cfg.get('validation_reminder_minutes')} min</b>\n"
        f"Expulsar si no responde: <b>{bool_label(cfg.get('validation_kick_if_timeout'))}</b>\n"
        f"Autoaprobar solicitudes: <b>{bool_label(cfg.get('validation_auto_approve_join_requests'))}</b>\n\n"
        f"Respondiendo: <b>{answering}</b> | Pendientes admin: <b>{pending}</b>\n"
        f"Borrado de comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n\n"
        "Comandos rápidos:\n"
        "<code>/plan</code> · <code>/Djplan</code> · <code>/ajustes</code>\n"
        "<code>/presentate</code> respondiendo a un usuario\n"
        "<code>/el plan te llama</code> · <code>/pregonero</code>\n"
        "<code>/setpreguntas Nombre:|Edad:|Lugar:|¿Qué buscas?</code>\n"
        "<code>/ban</code>, <code>/mute 10m</code>, <code>/kick</code> respondiendo a un usuario."
    )

def admin_panel_markup(chat_id: int) -> InlineKeyboardMarkup:
    cfg = admin_cfg(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏓 Ping", callback_data="adm|ping"), InlineKeyboardButton("📣 Pregonero", callback_data="adm|pregonero")],
        [InlineKeyboardButton(f"Validación {bool_label(cfg.get('validation_enabled'))}", callback_data="adm|toggle_validation")],
        [InlineKeyboardButton(f"🚪 Autoaprobar {bool_label(cfg.get('validation_auto_approve_join_requests'))}", callback_data="adm|toggle_autoapprove")],
        [InlineKeyboardButton("📋 Pendientes", callback_data="adm|pendientes"), InlineKeyboardButton("📌 Normas", callback_data="adm|reglas")],
        [InlineKeyboardButton("🔇 Silenciados", callback_data="adm|muted"), InlineKeyboardButton("🚪 Últimas entradas", callback_data="adm|entries")],
        [InlineKeyboardButton("🕙 Inactivos 10d", callback_data="adm|inactive"), InlineKeyboardButton("🏆 Ranking", callback_data="adm|ranking")],
        [InlineKeyboardButton("⏱️ Tiempo -", callback_data="adm|time_minus"), InlineKeyboardButton("⏱️ Tiempo +", callback_data="adm|time_plus")],
        [InlineKeyboardButton("🔔 Record -", callback_data="adm|rem_minus"), InlineKeyboardButton("🔔 Record +", callback_data="adm|rem_plus")],
        [InlineKeyboardButton(f"Expulsar timeout {bool_label(cfg.get('validation_kick_if_timeout'))}", callback_data="adm|toggle_kick_timeout")],
        [InlineKeyboardButton(f"🧹 Comandos: {command_cleanup_label(chat_id)}", callback_data="adm|cleanup_mode")],
        [InlineKeyboardButton("❌ Cerrar", callback_data="adm|close")],
    ])

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message or not chat_is_allowed(update.effective_chat.id):
        return
    chat_id = update.effective_chat.id
    remember_chat_title(chat_id, update.effective_chat.title or "")
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    await update.message.reply_html(admin_panel_text(chat_id), reply_markup=admin_panel_markup(chat_id))

async def admin_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return False
    data = query.data or ""
    if not data.startswith("adm|"):
        return False
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores.", show_alert=True)
        return True
    action = data.split("|", 1)[1]
    cfg = admin_cfg(chat_id)
    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            pass
        return True
    if action == "ping":
        await query.answer("PONG ✅", show_alert=False)
        await query.edit_message_text(ping_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "pregonero":
        await query.answer("Lanzando pregonero…")
        await send_pregonero(context, chat_id)
        return True
    if action == "muted":
        await query.edit_message_text(admin_muted_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "entries":
        await query.edit_message_text(admin_last_entries_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "inactive":
        await query.edit_message_text(admin_inactive_users_text(chat_id, 10), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "ranking":
        await query.edit_message_text(admin_ranking_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "cleanup_mode":
        cfg_set(chat_id, "command_cleanup_mode", next_command_cleanup_mode(chat_id))
        await query.edit_message_text(admin_panel_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup(chat_id))
        return True
    if action == "toggle_validation":
        cfg_set(chat_id, "validation_enabled", not bool(cfg.get("validation_enabled")))
    elif action == "toggle_kick_timeout":
        cfg_set(chat_id, "validation_kick_if_timeout", not bool(cfg.get("validation_kick_if_timeout")))
    elif action == "toggle_autoapprove":
        cfg_set(chat_id, "validation_auto_approve_join_requests", not bool(cfg.get("validation_auto_approve_join_requests")))
    elif action == "time_plus":
        cfg_set(chat_id, "validation_timeout_minutes", int(cfg.get("validation_timeout_minutes", 10)) + 1)
    elif action == "time_minus":
        cfg_set(chat_id, "validation_timeout_minutes", max(1, int(cfg.get("validation_timeout_minutes", 10)) - 1))
    elif action == "rem_plus":
        cfg_set(chat_id, "validation_reminder_minutes", int(cfg.get("validation_reminder_minutes", 3)) + 1)
    elif action == "rem_minus":
        cfg_set(chat_id, "validation_reminder_minutes", max(1, int(cfg.get("validation_reminder_minutes", 3)) - 1))
    elif action == "reglas":
        await query.edit_message_text(str(cfg_value(chat_id, "rules_text")), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    elif action == "pendientes":
        await query.answer("Usa /pendientes para ver y validar.", show_alert=True)
        return True
    elif action == "back":
        pass
    else:
        await query.answer("Acción no reconocida.", show_alert=True)
        return True
    await query.edit_message_text(admin_panel_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup(chat_id))
    return True

async def set_questions_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    raw = " ".join(context.args).strip()
    if not raw:
        await update.message.reply_text("Uso: /setpreguntas Nombre:|Edad:|Lugar:|¿Qué buscas en este chat?")
        return
    questions = [q.strip() for q in raw.split("|") if q.strip()]
    cfg_set(chat_id, "validation_questions", questions)
    await update.message.reply_text("✅ Preguntas actualizadas:\n" + "\n".join(f"{i+1}. {q}" for i, q in enumerate(questions)))

async def set_time_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    minutes = parse_minutes_arg(" ".join(context.args), 10)
    cfg_set(chat_id, "validation_timeout_minutes", minutes)
    await update.message.reply_text(f"✅ Tiempo límite actualizado: {minutes} minutos")

async def set_reminder_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    minutes = parse_minutes_arg(" ".join(context.args), 3)
    cfg_set(chat_id, "validation_reminder_minutes", minutes)
    await update.message.reply_text(f"✅ Recordatorio actualizado: {minutes} minutos")

async def validation_toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    command = (update.message.text or "").split()[0].lower()
    desired = False if "off" in command else True
    cfg_set(chat_id, "validation_enabled", desired)
    await update.message.reply_text(f"✅ Validación: {bool_label(desired)}")

async def set_rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    text = update.message.text.partition(" ")[2].strip()
    if not text:
        await update.message.reply_text("Uso: /setreglas texto de las normas")
        return
    cfg_set(chat_id, "rules_text", text)
    await update.message.reply_text("✅ Normas actualizadas.")

async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    await update.message.reply_html(str(cfg_value(update.effective_chat.id, "rules_text")))

async def set_join_message_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    text = update.message.text.partition(" ")[2].strip()
    if not text:
        await update.message.reply_text("Uso: /setbienvenida texto. Variables: {mention}, {name}, {first}, {username}, {id}")
        return
    cfg_set(chat_id, "validation_public_join_message", text)
    await update.message.reply_text("✅ Mensaje público de entrada actualizado.")

async def set_intro_message_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    text = update.message.text.partition(" ")[2].strip()
    if not text:
        await update.message.reply_text("Uso: /setintro texto. Variables: {mention}, {name}, {first}, {username}, {id}")
        return
    cfg_set(chat_id, "validation_intro_message", text)
    await update.message.reply_text("✅ Mensaje de preguntas actualizado.")

async def moderation_reply_target(update: Update) -> Optional[int]:
    if update.message and update.message.reply_to_message and update.message.reply_to_message.from_user:
        return update.message.reply_to_message.from_user.id
    return None

async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    target_id = await moderation_reply_target(update)
    if not target_id:
        await update.message.reply_text("Responde al mensaje del usuario con /ban motivo")
        return
    await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
    mark_user_expelled(chat_id, target_id, reason="ban manual", by_user_id=update.effective_user.id)
    await update.message.reply_text("🚫 Usuario baneado.")

async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    if not context.args:
        await update.message.reply_text("Uso: /unban ID_DEL_USUARIO")
        return
    try:
        target_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("ID no válido.")
        return
    await context.bot.unban_chat_member(chat_id=chat_id, user_id=target_id, only_if_banned=True)
    await update.message.reply_text("✅ Usuario desbaneado.")

async def kick_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    target_id = await moderation_reply_target(update)
    if not target_id:
        await update.message.reply_text("Responde al mensaje del usuario con /kick")
        return
    await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
    await context.bot.unban_chat_member(chat_id=chat_id, user_id=target_id, only_if_banned=True)
    mark_user_expelled(chat_id, target_id, reason="kick manual", by_user_id=update.effective_user.id)
    await update.message.reply_text("👢 Usuario expulsado.")

async def mute_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    target_id = await moderation_reply_target(update)
    if not target_id:
        await update.message.reply_text("Responde al mensaje del usuario con /mute 10m")
        return
    minutes = parse_minutes_arg(context.args[0] if context.args else "10m", 10)
    from datetime import datetime, timedelta, timezone
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    await context.bot.restrict_chat_member(chat_id=chat_id, user_id=target_id, permissions=ChatPermissions(can_send_messages=False), until_date=until)
    await update.message.reply_text(f"🔇 Usuario silenciado {minutes} minutos.")

async def unmute_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    target_id = await moderation_reply_target(update)
    if not target_id:
        await update.message.reply_text("Responde al mensaje del usuario con /unmute")
        return
    await context.bot.restrict_chat_member(chat_id=chat_id, user_id=target_id, permissions=ChatPermissions.all_permissions())
    unmark_user_muted(chat_id, target_id)
    await update.message.reply_text("🔊 Usuario desilenciado.")

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    if update.message.reply_to_message:
        await safe_delete(context.bot, chat_id, update.message.reply_to_message.message_id)
    await safe_delete(context.bot, chat_id, update.message.message_id)

async def clean_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    count = 20
    if context.args:
        try:
            count = max(1, min(100, int(context.args[0])))
        except Exception:
            count = 20
    start_id = update.message.message_id
    for mid in range(start_id, max(0, start_id - count - 1), -1):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass





# =========================
# MÓDULO: CONFIGURACIÓN PRIVADA TIPO GROUPHELP
# =========================
CONFIG_TEXT_FIELDS = {
    "validation_public_join_message": "Mensaje de bienvenida/entrada",
    "validation_intro_message": "Mensaje de preguntas",
    "validation_reminder_message": "Recordatorio",
    "validation_timeout_message": "Mensaje por timeout",
    "validation_approved_message": "Mensaje aprobado",
    "validation_rejected_message": "Mensaje rechazado",
    "rules_text": "Normas",
    "pregonero_text": "Mensaje del pregonero",
    "farewell_message": "Mensaje de despedida",
}

APPROVER_MODE_LABELS = {
    "telegram_admins": "Admins del grupo",
    "admin_ids": "Solo ADMIN_IDS",
    "creator": "Solo creador del grupo",
}


def is_global_admin_user(user_id: int) -> bool:
    return int(user_id) in ADMIN_IDS


def remember_chat_title(chat_id: int, title: str = "") -> None:
    if not chat_id:
        return
    if title:
        cfg_set(chat_id, "chat_title", title)


def config_session_key(user_id: int) -> str:
    return f"cfg:{int(user_id)}"


def set_config_pending(user_id: int, payload: Dict[str, Any]) -> None:
    PENDING_ACTIONS[config_session_key(user_id)] = payload


def pop_config_pending(user_id: int) -> Optional[Dict[str, Any]]:
    return PENDING_ACTIONS.pop(config_session_key(user_id), None)


def get_config_pending(user_id: int) -> Optional[Dict[str, Any]]:
    value = PENDING_ACTIONS.get(config_session_key(user_id))
    return value if isinstance(value, dict) else None


def known_admin_chats_for_private() -> List[int]:
    return sorted([int(chat_id) for chat_id in STATE_CACHE.keys()])


def cfg_fake_preview_values(template: str) -> str:
    return (
        str(template or "")
        .replace("{mention}", '<a href="tg://user?id=123456789">Usuario Nuevo</a>')
        .replace("{name}", "Usuario Nuevo")
        .replace("{first}", "Usuario")
        .replace("{username}", "@usuario")
        .replace("{id}", "123456789")
        .replace("{chat}", "El grupo")
    )


def parse_buttons_text(raw: str) -> List[Dict[str, str]]:
    buttons: List[Dict[str, str]] = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if " - " in line:
            text, url = line.split(" - ", 1)
        elif "|" in line:
            text, url = line.split("|", 1)
        else:
            continue
        text = text.strip()
        url = url.strip()
        if text and (url.startswith("http://") or url.startswith("https://") or url.startswith("tg://")):
            buttons.append({"text": text[:64], "url": url})
    return buttons[:12]


def build_public_join_keyboard(chat_id: int) -> Optional[InlineKeyboardMarkup]:
    raw = cfg_value(chat_id, "validation_public_join_buttons", [])
    if not isinstance(raw, list) or not raw:
        return None
    rows: List[List[InlineKeyboardButton]] = []
    for btn in raw[:12]:
        if not isinstance(btn, dict):
            continue
        text = str(btn.get("text", "")).strip()
        url = str(btn.get("url", "")).strip()
        if text and url:
            rows.append([InlineKeyboardButton(text, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None


def build_config_buttons_keyboard(chat_id: int, buttons_field: str) -> Optional[InlineKeyboardMarkup]:
    raw = cfg_value(chat_id, buttons_field, [])
    if not isinstance(raw, list) or not raw:
        return None
    rows: List[List[InlineKeyboardButton]] = []
    for btn in raw[:12]:
        if not isinstance(btn, dict):
            continue
        text = str(btn.get("text", "")).strip()
        url = str(btn.get("url", "")).strip()
        if text and url:
            rows.append([InlineKeyboardButton(text, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None


MESSAGE_PROFILES = {
    "welcome": {
        "title": "Bienvenida / entrada",
        "text": "validation_public_join_message",
        "media": "validation_public_join_media",
        "buttons": "validation_public_join_buttons",
        "position": "validation_public_join_media_position",
    },
    "questions": {
        "title": "Preguntas / presentación",
        "text": "validation_intro_message",
        "media": "validation_intro_media",
        "buttons": "validation_intro_buttons",
        "position": "validation_intro_media_position",
    },
    "reminder": {
        "title": "Recordatorio",
        "text": "validation_reminder_message",
        "media": "validation_reminder_media",
        "buttons": "validation_reminder_buttons",
        "position": "validation_reminder_media_position",
    },
    "timeout": {
        "title": "Timeout / expulsión por no contestar",
        "text": "validation_timeout_message",
        "media": "validation_timeout_media",
        "buttons": "validation_timeout_buttons",
        "position": "validation_timeout_media_position",
    },
    "approved": {
        "title": "Aprobado",
        "text": "validation_approved_message",
        "media": "validation_approved_media",
        "buttons": "validation_approved_buttons",
        "position": "validation_approved_media_position",
    },
    "rejected": {
        "title": "Rechazado / expulsado",
        "text": "validation_rejected_message",
        "media": "validation_rejected_media",
        "buttons": "validation_rejected_buttons",
        "position": "validation_rejected_media_position",
    },
    "rules": {
        "title": "Normas",
        "text": "rules_text",
        "media": "rules_media",
        "buttons": "rules_buttons",
        "position": "rules_media_position",
    },
    "pregonero": {
        "title": "Pregonero",
        "text": "pregonero_text",
        "media": "pregonero_media",
        "buttons": "pregonero_buttons",
        "position": "pregonero_media_position",
    },
    "farewell": {
        "title": "Despedida",
        "text": "farewell_message",
        "media": "farewell_media",
        "buttons": "farewell_buttons",
        "position": "farewell_media_position",
    },
}


def profile_field(profile: str, key: str) -> str:
    return MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])[key]


def fake_profile_text(chat_id: int, profile: str) -> str:
    return cfg_fake_preview_values(str(cfg_value(chat_id, profile_field(profile, "text"), "")))


async def send_media_blob(bot, chat_id: int, media: Dict[str, Any], *, caption: Optional[str] = None, reply_markup: Optional[InlineKeyboardMarkup] = None, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    mtype = media.get("type")
    fid = media.get("file_id")
    if not fid:
        return None
    kwargs = {
        "chat_id": chat_id,
        "caption": caption,
        "parse_mode": ParseMode.HTML,
        "reply_markup": reply_markup,
        "reply_to_message_id": reply_to_message_id,
        "allow_sending_without_reply": True,
    }
    if mtype == "photo":
        msg = await bot.send_photo(photo=fid, **kwargs)
    elif mtype == "video":
        msg = await bot.send_video(video=fid, **kwargs)
    elif mtype == "animation":
        msg = await bot.send_animation(animation=fid, **kwargs)
    elif mtype == "document":
        msg = await bot.send_document(document=fid, **kwargs)
    else:
        return None
    return getattr(msg, "message_id", None)


async def send_configured_profile_message(bot, chat_id: int, profile: str, text: str, *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    media = cfg_value(chat_id, profile_field(profile, "media"), None)
    markup = build_config_buttons_keyboard(chat_id, profile_field(profile, "buttons"))
    position = str(cfg_value(chat_id, profile_field(profile, "position"), "above"))

    has_media = isinstance(media, dict) and media.get("file_id")
    if has_media and position == "above":
        await send_media_blob(bot, chat_id, media, reply_to_message_id=reply_to_message_id)
        msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True, disable_web_page_preview=True)
        return getattr(msg, "message_id", None)
    if has_media and position == "below":
        msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True, disable_web_page_preview=True)
        await send_media_blob(bot, chat_id, media)
        return getattr(msg, "message_id", None)
    if has_media:
        sent_id = await send_media_blob(bot, chat_id, media, caption=text, reply_markup=markup, reply_to_message_id=reply_to_message_id)
        if sent_id:
            return sent_id
    msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True, disable_web_page_preview=True)
    return getattr(msg, "message_id", None)


async def send_profile_preview(bot, private_chat_id: int, target_chat_id: int, profile: str) -> None:
    title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
    text = fake_profile_text(target_chat_id, profile)
    if profile == "pregonero":
        text = text.replace("{mentions}", "@usuario1 @usuario2 @usuario3 @usuario4").replace("{count}", "4").replace("{total}", "12")
    await bot.send_message(private_chat_id, f"👁 <b>Vista previa completa: {h(title)}</b>\n<i>Debajo verás texto + multimedia + botones como saldría en el grupo.</i>", parse_mode=ParseMode.HTML)
    await send_configured_profile_message(bot, private_chat_id, profile, text)


def all_commands_text() -> str:
    return (
        "<b>📚 Comandos disponibles</b>\n\n"
        "<b>Comandos principales</b>\n"
        "<code>/Djplan</code> - menú DJ principal\n"
        "<code>/plan</code> - panel de administración del grupo\n"
        "<code>/ajustes</code> - panel privado de configuración\n"
        "<code>/ping</code> - comprobar que el bot está vivo\n\n"
        "<b>Validación</b>\n"
        "<code>/presentate</code> - forzar presentación respondiendo a un usuario\n"
        "<code>/preséntate</code> - alias con tilde si lo escribes manualmente\n"
        "<code>/pendientes</code> - ver usuarios respondiendo y pendientes de validar\n"
        "<code>/validacion</code> - estado del sistema\n"
        "<code>/validacionon</code> - activar validación\n"
        "<code>/validacionoff</code> - desactivar validación\n"
        "<code>/setpreguntas Nombre:|Edad:|Lugar:|¿Qué buscas?</code>\n"
        "<code>/settiempo 10</code> - minutos para responder\n"
        "<code>/setrecordatorio 3</code> - minuto del recordatorio\n"
        "<code>/setbienvenida texto</code> - mensaje público al entrar\n"
        "<code>/setintro texto</code> - mensaje inicial de preguntas\n\n"
        "<b>Control de grupo</b>\n"
        "<code>/silenciados</code> - ver usuarios silenciados/pendientes\n"
        "<code>/entradas</code> - últimas entradas detectadas\n"
        "<code>/inactivos</code> - usuarios sin actividad registrada en 10 días\n"
        "<code>/ranking</code> - ranking por mensajes registrados\n"
        "<code>/pregonero</code> - mencionar usuarios registrados\n"
        "<code>/el plan te llama</code> - pregonero con frase especial\n\n"
        "<b>Moderación</b>\n"
        "Respondiendo al mensaje de un usuario:\n"
        "<code>/ban motivo</code>\n"
        "<code>/kick</code>\n"
        "<code>/mute 10m</code> · <code>/mute 1h</code> · <code>/mute 1d</code>\n"
        "<code>/unmute</code>\n"
        "<code>/del</code> - borrar mensaje respondido\n"
        "<code>/limpiar 20</code> - limpiar últimos mensajes\n"
        "<code>/unban ID</code> - desbanear por ID\n\n"
        "<b>Reglas</b>\n"
        "<code>/reglas</code>\n"
        "<code>/setreglas texto</code>\n\n"
        "<b>DJ-PLAN</b>\n"
        "<code>/Djplan</code> - menú DJ\n"
        "<code>/dj</code> - asignar DJ respondiendo a un usuario\n\n"
        "<i>Los aliases antiguos /start, /admin y /config siguen funcionando para no romper lo que ya tenías, pero los oficiales son /Djplan, /plan y /ajustes.</i>"
    )


def validation_pending_summary_text(chat_id: int) -> str:
    state = get_state(chat_id)
    answering = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") == "answering"]
    pending = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") == "pending_admin"]
    lines = [
        "<b>👥 Usuarios pendientes</b>",
        "",
        f"Respondiendo preguntas: <b>{len(answering)}</b>",
        f"Pendientes de validar: <b>{len(pending)}</b>",
        "",
    ]
    if answering:
        lines.append("<b>📝 Aún contestando:</b>")
        for uid, r in answering[:20]:
            step = int(r.get("step", 0))
            total = len(cfg_questions(chat_id))
            lines.append(f"• {h(r.get('name') or uid)} {h(r.get('username',''))} — pregunta {step+1}/{total}")
        if len(answering) > 20:
            lines.append(f"… y {len(answering)-20} más")
        lines.append("")
    if pending:
        lines.append("<b>✅ Esperando admin:</b>")
        for uid, r in pending[:20]:
            lines.append(f"• {h(r.get('name') or uid)} {h(r.get('username',''))}")
        if len(pending) > 20:
            lines.append(f"… y {len(pending)-20} más")
    return "\n".join(lines).strip()


def admin_private_chat_list_markup() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for chat_id in known_admin_chats_for_private():
        title = str(cfg_value(chat_id, "chat_title", "")) or str(chat_id)
        rows.append([InlineKeyboardButton(f"⚙️ {title}", callback_data=f"cfg|open|{chat_id}")])
    rows.append([InlineKeyboardButton("🔄 Actualizar lista", callback_data="cfg|list")])
    return InlineKeyboardMarkup(rows)



def admin_private_main_text(chat_id: int) -> str:
    title = str(cfg_value(chat_id, "chat_title", "")) or str(chat_id)
    approver = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
    state = get_state(chat_id)
    manual_users = cfg_value(chat_id, "pregonero_manual_users", [])
    if isinstance(manual_users, str):
        manual_count = 1 if manual_users.strip() else 0
    else:
        manual_count = len(list(manual_users or []))
    pending_count = sum(1 for r in state.validation_users.values() if r.get("status") in ("answering", "pending_admin", "timeout"))
    active_profiles = sum(1 for profile in MESSAGE_PROFILES if profile_is_configured(chat_id, profile))
    return (
        f"<b>⚙️ Configuración privada · DJ-PLAN</b>\n\n"
        f"Grupo: <b>{h(title)}</b>\n"
        f"Validación: <b>{bool_label(cfg_value(chat_id, 'validation_enabled'))}</b> · "
        f"Autoaprobar: <b>{bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}</b> · "
        f"Despedida: <b>{bool_label(cfg_value(chat_id, 'farewell_enabled'))}</b>\n"
        f"Tiempo: <b>{cfg_value(chat_id, 'validation_timeout_minutes')} min</b> · "
        f"Recordatorio: <b>{cfg_value(chat_id, 'validation_reminder_minutes')} min</b> · "
        f"Valida: <b>{h(APPROVER_MODE_LABELS.get(approver, approver))}</b>\n"
        f"Pendientes: <b>{pending_count}</b> · Silenciados: <b>{len(state.muted_users)}</b> · "
        f"Expulsados: <b>{len(state.expelled_users)}</b>\n"
        f"Bloques configurados: <b>{active_profiles}/{len(MESSAGE_PROFILES)}</b> · "
        f"Pregonero manual: <b>{manual_count}</b> · "
        f"Comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n\n"
        "Pulsa un bloque para entrar en su configuración. Cada bloque se abre editando este mismo panel."
    )


ADMIN_PRIVATE_BLOCKS = [
    ("validation", "🛡️ Validación"),
    ("welcome", "👋 Bienvenida"),
    ("questions", "❓ Preguntas"),
    ("reminder", "🔔 Recordatorio"),
    ("timeout", "⛔ Timeout"),
    ("approved", "✅ Aprobado"),
    ("rejected", "❌ Rechazado"),
    ("rules", "📌 Normas"),
    ("pregonero", "📣 Pregonero"),
    ("farewell", "👋 Despedida"),
    ("recurrentes", "🔁 Mensajes recurrentes"),
    ("lists", "📊 Listados y control"),
]
ADMIN_PRIVATE_BLOCK_IDS = {key for key, _label in ADMIN_PRIVATE_BLOCKS}
PROFILE_BLOCK_IDS = set(MESSAGE_PROFILES.keys())


def profile_is_configured(chat_id: int, profile: str) -> bool:
    try:
        text = str(cfg_value(chat_id, profile_field(profile, "text"), "") or "").strip()
        media = cfg_value(chat_id, profile_field(profile, "media"), None)
        buttons = cfg_value(chat_id, profile_field(profile, "buttons"), [])
        return bool(text or media or (isinstance(buttons, list) and buttons))
    except Exception:
        return False


def admin_block_icon(chat_id: int, block: str) -> str:
    if block == "validation":
        return cfg_status(cfg_value(chat_id, "validation_enabled"))
    if block == "questions":
        return "✅" if cfg_questions(chat_id) else "❌"
    if block == "farewell":
        return cfg_status(cfg_value(chat_id, "farewell_enabled"))
    if block == "lists":
        state = get_state(chat_id)
        return "✅" if (state.validation_users or state.member_activity or state.action_log) else "❌"
    if block == "recurrentes":
        try:
            return "✅" if recurring_list(chat_id) else "❌"
        except Exception:
            return "❌"
    if block in PROFILE_BLOCK_IDS:
        return "✅" if profile_is_configured(chat_id, block) else "❌"
    return "✅"


def block_label(chat_id: int, block: str, label: str) -> str:
    return f"{admin_block_icon(chat_id, block)} {label}"


def admin_private_main_markup(chat_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    block_items = ADMIN_PRIVATE_BLOCKS[:-1]
    for i in range(0, len(block_items), 2):
        chunk = block_items[i:i+2]
        rows.append([
            InlineKeyboardButton(block_label(chat_id, key, label), callback_data=f"cfg|block|{chat_id}|{key}")
            for key, label in chunk
        ])

    # Bloque ancho específico: aquí van Pendientes, LOG, Expulsados, Ping y demás listados.
    state = get_state(chat_id)
    pending_count = sum(1 for r in state.validation_users.values() if r.get("status") in ("answering", "pending_admin", "timeout"))
    log_count = len([r for r in state.action_log if int(r.get("ts") or 0) >= _now_ts() - 3 * 86400])
    lists_label = (
        f"{admin_block_icon(chat_id, 'lists')} 📊 LISTADOS Y CONTROL "
        f"· 👥 {pending_count} · 📜 {log_count} · 🚫 {len(state.expelled_users)}"
    )
    rows.append([InlineKeyboardButton(lists_label, callback_data=f"cfg|block|{chat_id}|lists")])

    # Ping se queda dentro del bloque Listados y control, no en el menú principal.
    rows.append([
        InlineKeyboardButton("🔄 Reload", callback_data=f"cfg|reload|{chat_id}"),
        InlineKeyboardButton("💾 Guardar", callback_data=f"cfg|save|{chat_id}"),
        InlineKeyboardButton("❌ Cerrar panel", callback_data=f"cfg|close|{chat_id}"),
    ])
    rows.append([InlineKeyboardButton("🔙 Elegir otro grupo", callback_data="cfg|list")])
    return InlineKeyboardMarkup(rows)


def block_footer_rows(chat_id: int, block: str) -> List[List[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton("🔄 Reload", callback_data=f"cfg|block_reload|{chat_id}|{block}"),
            InlineKeyboardButton("💾 Guardar", callback_data=f"cfg|block_save|{chat_id}|{block}"),
            InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}"),
        ]
    ]


def profile_summary_lines(chat_id: int, profile: str) -> List[str]:
    title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
    text = str(cfg_value(chat_id, profile_field(profile, "text"), "") or "")
    buttons = cfg_value(chat_id, profile_field(profile, "buttons"), [])
    button_count = len(buttons) if isinstance(buttons, list) else 0
    preview = text.replace("\n", " ").strip()
    if len(preview) > 130:
        preview = preview[:129] + "…"
    return [
        f"<b>{h(title)}</b>",
        f"Estado: <b>{admin_block_icon(chat_id, profile)}</b>",
        f"Multimedia: <b>{media_status(chat_id, profile)}</b> · Posición: <b>{h(media_position_label(chat_id, profile))}</b>",
        f"Botones: <b>{button_count}</b>",
        "",
        f"Texto actual:\n<pre>{h(preview or 'Sin texto configurado')}</pre>",
    ]


def admin_private_block_text(chat_id: int, block: str) -> str:
    state = get_state(chat_id)
    title = str(cfg_value(chat_id, "chat_title", "")) or str(chat_id)
    if block == "validation":
        approver = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
        pending_count = sum(1 for r in state.validation_users.values() if r.get("status") in ("answering", "pending_admin", "timeout"))
        return (
            f"<b>🛡️ Bloque Validación</b>\n"
            f"Grupo: <b>{h(title)}</b>\n\n"
            f"Validación: <b>{bool_label(cfg_value(chat_id, 'validation_enabled'))}</b>\n"
            f"Autoaprobar solicitudes: <b>{bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}</b>\n"
            f"Expulsar por timeout: <b>{bool_label(cfg_value(chat_id, 'validation_kick_if_timeout'))}</b>\n"
            f"Tiempo límite: <b>{cfg_value(chat_id, 'validation_timeout_minutes')} min</b>\n"
            f"Recordatorio: <b>{cfg_value(chat_id, 'validation_reminder_minutes')} min</b>\n"
            f"Quién valida: <b>{h(APPROVER_MODE_LABELS.get(approver, approver))}</b>\n"
            f"Borrado comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n\n"
            f"Pendientes actuales: <b>{pending_count}</b>"
        )
    if block == "lists":
        pending_count = sum(1 for r in state.validation_users.values() if r.get("status") in ("answering", "pending_admin", "timeout"))
        return (
            f"<b>📊 Bloque Listados y control</b>\n"
            f"Grupo: <b>{h(title)}</b>\n\n"
            f"Pendientes: <b>{pending_count}</b>\n"
            f"Silenciados: <b>{len(state.muted_users)}</b>\n"
            f"Expulsados: <b>{len(state.expelled_users)}</b>\n"
            f"Usuarios conocidos: <b>{len(state.member_activity)}</b>\n"
            f"Entradas registradas: <b>{len(state.entry_log)}</b>\n"
            f"Acciones en LOG: <b>{len(state.action_log)}</b>"
        )
    if block == "recurrentes":
        rows = recurring_list(chat_id)
        enabled = sum(1 for r in rows if bool(r.get("enabled", True)))
        pinned = sum(1 for r in rows if bool(r.get("pin", False)))
        lines = [
            "<b>🔁 Bloque Mensajes recurrentes</b>",
            f"Grupo: <b>{h(title)}</b>",
            "",
            f"Total: <b>{len(rows)}</b> · Activos: <b>{enabled}</b> · Fijados: <b>{pinned}</b>",
            "",
        ]
        if rows:
            for r in rows[:10]:
                rid = int(r.get("id", 0) or 0)
                status = "🟢" if bool(r.get("enabled", True)) else "⚫"
                pin = "📌" if bool(r.get("pin", False)) else ""
                media = "🖼" if isinstance(r.get("media"), dict) and r.get("media", {}).get("file_id") else ""
                buttons = len(r.get("buttons") or []) if isinstance(r.get("buttons"), list) else 0
                lines.append(f"{status} #{rid} {pin}{media} <b>{h(str(r.get('name') or 'Sin nombre'))}</b> · {h(str(r.get('schedule_label') or ''))} · botones: <b>{buttons}</b>")
            if len(rows) > 10:
                lines.append(f"… y {len(rows)-10} más")
        else:
            lines.append("No hay recurrentes todavía.")
        lines.extend([
            "",
            "Desde aquí puedes crear, activar/pausar, fijar, enviar, editar texto, poner multimedia y añadir botones sin escribir comandos en el grupo.",
        ])
        return "\n".join(lines)
    if block == "questions":
        qs = cfg_questions(chat_id)
        lines = [
            "<b>❓ Bloque Preguntas</b>",
            f"Grupo: <b>{h(title)}</b>",
            "",
            f"Preguntas configuradas: <b>{len(qs)}</b>",
        ]
        lines.extend(f"{i+1}. {h(q)}" for i, q in enumerate(qs[:10]))
        if len(qs) > 10:
            lines.append(f"… y {len(qs)-10} más")
        lines.append("")
        lines.extend(profile_summary_lines(chat_id, "questions"))
        return "\n".join(lines)
    if block in PROFILE_BLOCK_IDS:
        lines = [f"<b>{h(dict(ADMIN_PRIVATE_BLOCKS).get(block, block))}</b>", f"Grupo: <b>{h(title)}</b>", ""]
        if block == "approved":
            lines.append(f"Mostrar normas al aprobar: <b>{bool_label(cfg_value(chat_id, 'rules_auto_after_approve'))}</b>")
            lines.append("")
        if block == "farewell":
            lines.append(f"Despedida automática: <b>{bool_label(cfg_value(chat_id, 'farewell_enabled'))}</b>")
            lines.append("")
        if block == "pregonero":
            manual = cfg_value(chat_id, "pregonero_manual_users", [])
            manual_count = len(manual) if isinstance(manual, list) else (1 if str(manual or '').strip() else 0)
            lines.append(f"Usuarios manuales: <b>{manual_count}</b>")
            lines.append(f"Menciones por mensaje: <b>{cfg_value(chat_id, 'pregonero_max_mentions_per_message', 4)}</b>")
            lines.append("")
        lines.extend(profile_summary_lines(chat_id, block))
        return "\n".join(lines)
    return admin_private_main_text(chat_id)


def admin_private_block_markup(chat_id: int, block: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if block == "validation":
        rows.append([
            InlineKeyboardButton(f"{cfg_status(cfg_value(chat_id, 'validation_enabled'))} Validación", callback_data=f"cfg|toggle_validation|{chat_id}|validation"),
            InlineKeyboardButton(f"{cfg_status(cfg_value(chat_id, 'validation_auto_approve_join_requests'))} Autoaprobar", callback_data=f"cfg|toggle_autoapprove|{chat_id}|validation"),
            InlineKeyboardButton(f"{cfg_status(cfg_value(chat_id, 'validation_kick_if_timeout'))} Expulsar", callback_data=f"cfg|toggle_kick|{chat_id}|validation"),
        ])
        rows.append([
            InlineKeyboardButton(f"⏱ {cfg_value(chat_id, 'validation_timeout_minutes')}m -", callback_data=f"cfg|time_minus|{chat_id}|validation"),
            InlineKeyboardButton("⏱ Tiempo +", callback_data=f"cfg|time_plus|{chat_id}|validation"),
            InlineKeyboardButton("👮 Quién valida", callback_data=f"cfg|approvers|{chat_id}|validation"),
        ])
        rows.append([
            InlineKeyboardButton(f"🔔 {cfg_value(chat_id, 'validation_reminder_minutes')}m -", callback_data=f"cfg|rem_minus|{chat_id}|validation"),
            InlineKeyboardButton("🔔 Record +", callback_data=f"cfg|rem_plus|{chat_id}|validation"),
            InlineKeyboardButton(f"🧹 Cmd {command_cleanup_label(chat_id)}", callback_data=f"cfg|cleanup_mode|{chat_id}|validation"),
        ])
        rows.append([InlineKeyboardButton("🧪 Probar entrada", callback_data=f"cfg|validation_test|{chat_id}|validation")])
        rows.extend(block_footer_rows(chat_id, block))
        return InlineKeyboardMarkup(rows)

    if block == "lists":
        # Primer vistazo: las tres opciones que más necesitas tener a mano.
        rows.append([
            InlineKeyboardButton("👥 Pendientes", callback_data=f"cfg|pending|{chat_id}"),
            InlineKeyboardButton("📜 LOG 3d", callback_data=f"cfg|log|{chat_id}"),
            InlineKeyboardButton("🚫 Expulsados", callback_data=f"cfg|expelled|{chat_id}"),
        ])
        rows.append([
            InlineKeyboardButton("🔇 Silenciados", callback_data=f"cfg|muted|{chat_id}"),
            InlineKeyboardButton("🚪 Entradas", callback_data=f"cfg|entries|{chat_id}"),
            InlineKeyboardButton("🕙 Inactivos", callback_data=f"cfg|inactive|{chat_id}"),
        ])
        rows.append([
            InlineKeyboardButton("🏆 Ranking", callback_data=f"cfg|ranking|{chat_id}"),
            InlineKeyboardButton("📚 Comandos", callback_data=f"cfg|commands|{chat_id}"),
            InlineKeyboardButton("🏓 Ping", callback_data=f"cfg|ping|{chat_id}"),
        ])
        rows.extend(block_footer_rows(chat_id, block))
        return InlineKeyboardMarkup(rows)

    if block == "recurrentes":
        rows.append([
            InlineKeyboardButton("➕ Nuevo cada hora", callback_data=f"cfg|rec_new_quick|{chat_id}|i3600|recurrentes"),
            InlineKeyboardButton("🌙 Nuevo 21:30", callback_data=f"cfg|rec_new_quick|{chat_id}|d2130|recurrentes"),
        ])
        rows.append([
            InlineKeyboardButton("⏱ Horario personalizado", callback_data=f"cfg|rec_new_custom|{chat_id}|recurrentes"),
            InlineKeyboardButton("🔄 Recargar", callback_data=f"cfg|block_reload|{chat_id}|recurrentes"),
        ])
        for r in recurring_list(chat_id)[:8]:
            rid = int(r.get("id", 0) or 0)
            name = str(r.get("name") or f"Mensaje {rid}")[:22]
            status = "🟢" if bool(r.get("enabled", True)) else "⚫"
            pin = "📌" if bool(r.get("pin", False)) else ""
            rows.append([InlineKeyboardButton(f"{status} #{rid} {pin} {name}", callback_data=f"cfg|rec_edit|{chat_id}|{rid}|recurrentes")])
            rows.append([
                InlineKeyboardButton("▶️ Enviar", callback_data=f"cfg|rec_send|{chat_id}|{rid}|recurrentes"),
                InlineKeyboardButton("ON/OFF", callback_data=f"cfg|rec_toggle|{chat_id}|{rid}|recurrentes"),
                InlineKeyboardButton("📌", callback_data=f"cfg|rec_pin|{chat_id}|{rid}|recurrentes"),
                InlineKeyboardButton("🗑", callback_data=f"cfg|rec_del|{chat_id}|{rid}|recurrentes"),
            ])
        rows.extend(block_footer_rows(chat_id, block))
        return InlineKeyboardMarkup(rows)

    if block in PROFILE_BLOCK_IDS:
        rows.append(preview_button(chat_id, block, "👁 Vista previa completa"))
        rows.extend(profile_control_rows(chat_id, block, "✏️ Texto"))
        if block == "questions":
            rows.append([InlineKeyboardButton("❓ Editar preguntas", callback_data=f"cfg|questions|{chat_id}|questions")])
        if block == "approved":
            rows.append([InlineKeyboardButton(f"{cfg_status(cfg_value(chat_id, 'rules_auto_after_approve'))} Mostrar normas al aprobar", callback_data=f"cfg|toggle_rules_after_approve|{chat_id}|approved")])
        if block == "farewell":
            rows.append([InlineKeyboardButton(f"{cfg_status(cfg_value(chat_id, 'farewell_enabled'))} Despedida automática", callback_data=f"cfg|toggle_farewell|{chat_id}|farewell")])
        if block == "pregonero":
            rows.append([
                InlineKeyboardButton("➕ Users manual", callback_data=f"cfg|pregonero_manual|{chat_id}|pregonero"),
                InlineKeyboardButton("🧹 Quitar manual", callback_data=f"cfg|pregonero_clear_manual|{chat_id}|pregonero"),
                InlineKeyboardButton("🚀 Lanzar", callback_data=f"cfg|send_pregonero|{chat_id}|pregonero"),
            ])
        rows.extend(block_footer_rows(chat_id, block))
        return InlineKeyboardMarkup(rows)

    return admin_private_main_markup(chat_id)

def approver_markup(chat_id: int) -> InlineKeyboardMarkup:
    current = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
    rows = []
    for mode, label in APPROVER_MODE_LABELS.items():
        mark = "✅ " if mode == current else ""
        rows.append([InlineKeyboardButton(mark + label, callback_data=f"cfg|setapprover|{chat_id}|{mode}|validation")])
    rows.append([InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|block|{chat_id}|validation")])
    return InlineKeyboardMarkup(rows)


async def admin_private_config_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    if not is_global_admin_user(update.effective_user.id):
        await update.message.reply_text("Solo ADMIN_IDS puede abrir la configuración privada.")
        return
    if update.effective_chat and update.effective_chat.type != "private":
        await update.message.reply_text("Escríbeme /ajustes por privado para configurar el grupo sin ensuciar el chat.")
        return
    load_all_states()
    if not known_admin_chats_for_private():
        await update.message.reply_text("Todavía no tengo grupos registrados. Usa /plan una vez dentro del grupo o espera a que entre alguien.")
        return
    await update.message.reply_html("<b>Elige el grupo que quieres configurar:</b>", reply_markup=admin_private_chat_list_markup())


async def send_public_join_preview(bot, private_chat_id: int, target_chat_id: int) -> None:
    text = cfg_fake_preview_values(str(cfg_value(target_chat_id, "validation_public_join_message", VALIDATION_PUBLIC_JOIN_MESSAGE)))
    markup = build_public_join_keyboard(target_chat_id)
    media = cfg_value(target_chat_id, "validation_public_join_media", None)
    if isinstance(media, dict) and media.get("file_id"):
        mtype = media.get("type")
        fid = media.get("file_id")
        if mtype == "photo":
            await bot.send_photo(private_chat_id, photo=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        if mtype == "video":
            await bot.send_video(private_chat_id, video=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        if mtype == "animation":
            await bot.send_animation(private_chat_id, animation=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
        if mtype == "document":
            await bot.send_document(private_chat_id, document=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return
    await bot.send_message(private_chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def send_configured_public_join(bot, chat_id: int, user, *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    text = validation_format_template(str(cfg_value(chat_id, "validation_public_join_message", VALIDATION_PUBLIC_JOIN_MESSAGE)), user)
    return await send_configured_profile_message(bot, chat_id, "welcome", text, reply_to_message_id=reply_to_message_id)


def private_pending_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    candidates = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") in ("answering", "pending_admin", "timeout")]
    for uid, record in candidates[:12]:
        name = str(record.get("name") or uid)
        rows.append([InlineKeyboardButton(f"👤 {name[:32]}", callback_data=f"cfg|noop|{chat_id}")])
        rows.append([
            InlineKeyboardButton("✅ Validar ya", callback_data=f"cfg|pendapprove|{chat_id}|{uid}"),
            InlineKeyboardButton("🚫 Expulsar", callback_data=f"cfg|pendkick|{chat_id}|{uid}"),
        ])
        rows.append([
            InlineKeyboardButton("🔔 Recordar", callback_data=f"cfg|pendremind|{chat_id}|{uid}"),
            InlineKeyboardButton("🔇 Silenciar", callback_data=f"cfg|pendmute|{chat_id}|{uid}"),
        ])
    rows.append([InlineKeyboardButton("🔄 Actualizar", callback_data=f"cfg|pending|{chat_id}"), InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|block|{chat_id}|lists")])
    return InlineKeyboardMarkup(rows)



async def validation_test_flow(context: ContextTypes.DEFAULT_TYPE, chat_id: int, admin_user) -> str:
    """Prueba no destructiva: verifica permisos y manda mensajes de prueba en el grupo."""
    bot_user = await context.bot.get_me()
    try:
        bot_member = await context.bot.get_chat_member(chat_id, bot_user.id)
    except Exception:
        return "❌ No puedo leer mis permisos en ese grupo. Revisa que el bot siga dentro."
    status = getattr(bot_member, "status", "")
    can_restrict = bool(getattr(bot_member, "can_restrict_members", False)) or status == "creator"
    can_delete = bool(getattr(bot_member, "can_delete_messages", False)) or status == "creator"
    can_invite = bool(getattr(bot_member, "can_invite_users", False)) or status == "creator"
    can_pin = bool(getattr(bot_member, "can_pin_messages", False)) or status == "creator"
    lines = [
        "🧪 <b>Prueba de validación</b>",
        "",
        f"Validación activa: <b>{bool_label(validation_is_active_for_chat(chat_id))}</b>",
        f"Bot admin: <b>{'Sí' if status in ('administrator', 'creator') else 'No'}</b>",
        f"Puede silenciar/restringir: <b>{'Sí' if can_restrict else 'No'}</b>",
        f"Puede borrar mensajes: <b>{'Sí' if can_delete else 'No'}</b>",
        f"Puede aprobar solicitudes de entrada: <b>{'Sí' if can_invite else 'No'}</b>",
        f"Puede fijar mensajes: <b>{'Sí' if can_pin else 'No'}</b>",
        "",
        "He mandado al grupo una simulación visual de entrada. No he silenciado ni creado pendiente real.",
    ]
    if not can_restrict:
        lines.append("⚠️ Para que todos pasen por validación, el bot necesita permiso de <b>Banear usuarios / restringir miembros</b>.")
    if not can_invite:
        lines.append("⚠️ Para enlaces con solicitud de aprobación, necesita permiso de <b>Añadir usuarios / aprobar solicitudes</b>.")

    # Simulación visual no destructiva en el grupo.
    try:
        intro = (
            "🧪 <b>PRUEBA DE ENTRADA</b>\n\n"
            f"Usuario simulado: {admin_user.mention_html()}\n"
            "Si esto fuera una entrada real, el bot lo restringiría y empezaría las preguntas."
        )
        msg = await context.bot.send_message(chat_id, intro, parse_mode=ParseMode.HTML)
        await register_bot_message(chat_id, msg.message_id)
        questions = cfg_questions(chat_id)
        q_text = (
            "🧪 <b>PRUEBA - Primera pregunta de validación</b>\n\n"
            f"<b>Pregunta 1/{len(questions)}</b>\n{h(questions[0] if questions else 'Nombre:')}"
        )
        msg2 = await context.bot.send_message(chat_id, q_text, parse_mode=ParseMode.HTML)
        await register_bot_message(chat_id, msg2.message_id)
        asyncio.create_task(delete_later(context.bot, chat_id, msg.message_id, 90))
        asyncio.create_task(delete_later(context.bot, chat_id, msg2.message_id, 90))
    except Exception:
        logger.exception("No pude enviar simulación de validación")
        lines.append("❌ No pude mandar la simulación al grupo. Revisa permisos para enviar mensajes.")
    return "\n".join(lines)


async def admin_private_config_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("cfg|"):
        return
    await query.answer()
    if not is_global_admin_user(update.effective_user.id):
        await query.answer("Solo ADMIN_IDS.", show_alert=True)
        return
    parts = data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    if action == "list":
        await query.edit_message_text("<b>Elige el grupo que quieres configurar:</b>", parse_mode=ParseMode.HTML, reply_markup=admin_private_chat_list_markup())
        return
    if len(parts) < 3:
        await query.answer("Acción inválida.", show_alert=True)
        return
    try:
        chat_id = int(parts[2])
    except Exception:
        await query.answer("Grupo inválido.", show_alert=True)
        return

    def return_block_from_parts(default: str = "") -> str:
        for part in reversed(parts[3:]):
            if part in ADMIN_PRIVATE_BLOCK_IDS:
                return part
        return default

    async def show_main() -> None:
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))

    async def show_block(block_name: str) -> None:
        await query.edit_message_text(
            admin_private_block_text(chat_id, block_name),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_private_block_markup(chat_id, block_name),
        )

    if action == "noop":
        await query.answer("Bloque de configuración")
        return
    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            await query.edit_message_text("Panel cerrado.")
        return
    if action == "open":
        await show_main()
        return
    if action == "block" and len(parts) >= 4:
        block = parts[3]
        if block in ADMIN_PRIVATE_BLOCK_IDS:
            await show_block(block)
        else:
            await show_main()
        return
    if action == "block_reload" and len(parts) >= 4:
        block = parts[3]
        load_all_states()
        await show_block(block if block in ADMIN_PRIVATE_BLOCK_IDS else "validation")
        await query.answer("Bloque recargado.")
        return
    if action == "block_save" and len(parts) >= 4:
        block = parts[3]
        save_all_states()
        add_action_log(chat_id, "guardar configuración", f"Guardado manual desde bloque {block}", user_id=update.effective_user.id)
        await show_block(block if block in ADMIN_PRIVATE_BLOCK_IDS else "validation")
        await query.answer("Cambios guardados.")
        return
    if action == "reload":
        load_all_states()
        await show_main()
        await query.answer("Recargado.")
        return
    if action == "save":
        save_all_states()
        add_action_log(chat_id, "guardar configuración", "Guardado manual desde panel privado", user_id=update.effective_user.id)
        await show_main()
        await query.answer("Cambios guardados.")
        return
    if action == "ping":
        await query.message.reply_html(ping_text())
        return
    if action == "validation_test":
        report = await validation_test_flow(context, chat_id, update.effective_user)
        await query.message.reply_html(report, reply_markup=cfg_back_markup(chat_id, "validation"))
        await query.answer("Prueba lanzada")
        return

    target_block = return_block_from_parts("")

    if action == "toggle_validation":
        cfg_set(chat_id, "validation_enabled", not bool(cfg_value(chat_id, "validation_enabled")))
        add_action_log(chat_id, "config", "Cambió Validación", user_id=update.effective_user.id)
        target_block = target_block or "validation"
    elif action == "toggle_kick":
        cfg_set(chat_id, "validation_kick_if_timeout", not bool(cfg_value(chat_id, "validation_kick_if_timeout")))
        add_action_log(chat_id, "config", "Cambió Expulsar timeout", user_id=update.effective_user.id)
        target_block = target_block or "validation"
    elif action == "toggle_autoapprove":
        cfg_set(chat_id, "validation_auto_approve_join_requests", not bool(cfg_value(chat_id, "validation_auto_approve_join_requests")))
        add_action_log(chat_id, "config", "Cambió Autoaprobar", user_id=update.effective_user.id)
        target_block = target_block or "validation"
    elif action == "toggle_farewell":
        cfg_set(chat_id, "farewell_enabled", not bool(cfg_value(chat_id, "farewell_enabled")))
        add_action_log(chat_id, "config", "Cambió Despedida automática", user_id=update.effective_user.id)
        target_block = target_block or "farewell"
    elif action == "toggle_rules_after_approve":
        cfg_set(chat_id, "rules_auto_after_approve", not bool(cfg_value(chat_id, "rules_auto_after_approve")))
        add_action_log(chat_id, "config", "Cambió Mostrar normas al aprobar", user_id=update.effective_user.id)
        target_block = target_block or "approved"
    elif action == "time_plus":
        cfg_set(chat_id, "validation_timeout_minutes", int(cfg_value(chat_id, "validation_timeout_minutes", 10)) + 1)
        target_block = target_block or "validation"
    elif action == "time_minus":
        cfg_set(chat_id, "validation_timeout_minutes", max(1, int(cfg_value(chat_id, "validation_timeout_minutes", 10)) - 1))
        target_block = target_block or "validation"
    elif action == "rem_plus":
        cfg_set(chat_id, "validation_reminder_minutes", int(cfg_value(chat_id, "validation_reminder_minutes", 3)) + 1)
        target_block = target_block or "validation"
    elif action == "rem_minus":
        cfg_set(chat_id, "validation_reminder_minutes", max(1, int(cfg_value(chat_id, "validation_reminder_minutes", 3)) - 1))
        target_block = target_block or "validation"
    elif action == "pos" and len(parts) >= 4:
        profile = parts[3]
        field = profile_field(profile, "position")
        current = str(cfg_value(chat_id, field, "above"))
        cfg_set(chat_id, field, "below" if current != "below" else "above")
        target_block = profile if profile in ADMIN_PRIVATE_BLOCK_IDS else target_block
    elif action == "preview" and len(parts) >= 4:
        await send_profile_preview(context.bot, query.message.chat.id, chat_id, parts[3])
        await query.answer("Vista previa enviada.")
        return
    elif action == "commands":
        await query.edit_message_text(all_commands_text(), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "cleanup_mode":
        cfg_set(chat_id, "command_cleanup_mode", next_command_cleanup_mode(chat_id))
        target_block = target_block or "validation"
    elif action == "muted":
        await query.edit_message_text(admin_muted_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "entries":
        await query.edit_message_text(admin_last_entries_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "inactive":
        await query.edit_message_text(admin_inactive_users_text(chat_id, 10), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "ranking":
        await query.edit_message_text(admin_ranking_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "expelled":
        await query.edit_message_text(admin_expelled_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "log":
        await query.edit_message_text(admin_action_log_text(chat_id, 3), parse_mode=ParseMode.HTML, reply_markup=cfg_back_markup(chat_id, "lists"))
        return
    elif action == "pending":
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=private_pending_markup(chat_id))
        return
    elif action == "pendapprove" and len(parts) >= 4:
        target_id = int(parts[3])
        record = validation_get_record(chat_id, target_id) or {}
        try:
            await validation_unrestrict(context, chat_id, target_id)
            record["status"] = "validated"
            record["validated_by"] = update.effective_user.id
            record["validated_ts"] = _now_ts()
            validation_set_record(chat_id, target_id, record)
            unmark_user_muted(chat_id, target_id)
            target_mention = f'<a href="tg://user?id={target_id}">{h(record.get("name") or target_id)}</a>'
            approved_text = str(cfg_value(chat_id, "validation_approved_message", VALIDATION_APPROVED_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
            await send_configured_profile_message(context.bot, chat_id, "approved", approved_text)
            if bool(cfg_value(chat_id, "rules_auto_after_approve", False)):
                rules_text = cfg_fake_preview_values(str(cfg_value(chat_id, "rules_text", ""))).replace("Usuario Nuevo", h(record.get("name", "Usuario")))
                await send_configured_profile_message(context.bot, chat_id, "rules", rules_text)
            add_action_log(chat_id, "validación", f"Validado directamente desde pendientes: {record.get('name') or target_id}", user_id=update.effective_user.id)
            await query.answer("Usuario validado directamente ✅")
        except Exception:
            logger.exception("No se pudo validar directamente desde pendientes")
            await query.answer("No pude validar. Revisa permisos del bot.", show_alert=True)
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=private_pending_markup(chat_id))
        return
    elif action == "pendkick" and len(parts) >= 4:
        target_id = int(parts[3])
        record = validation_get_record(chat_id, target_id) or {}
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
            record["status"] = "kicked_from_pending"
            validation_set_record(chat_id, target_id, record)
            mark_user_expelled(chat_id, target_id, record=record, reason="expulsado desde pendientes", by_user_id=update.effective_user.id)
            target_mention = f'<a href="tg://user?id={target_id}">{h(record.get("name") or target_id)}</a>'
            timeout_text = str(cfg_value(chat_id, "validation_timeout_message", VALIDATION_TIMEOUT_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
            await send_configured_profile_message(context.bot, chat_id, "timeout", timeout_text)
            await query.answer("Expulsado.")
        except Exception:
            logger.exception("No se pudo expulsar desde pendientes")
            await query.answer("No pude expulsar. Revisa permisos.", show_alert=True)
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=private_pending_markup(chat_id))
        return
    elif action == "pendremind" and len(parts) >= 4:
        target_id = int(parts[3])
        record = validation_get_record(chat_id, target_id) or {}
        target_mention = f'<a href="tg://user?id={target_id}">{h(record.get("name") or target_id)}</a>'
        reminder_text = str(cfg_value(chat_id, "validation_reminder_message", VALIDATION_REMINDER_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
        await send_configured_profile_message(context.bot, chat_id, "reminder", reminder_text)
        add_action_log(chat_id, "recordatorio", f"Recordado desde pendientes: {record.get('name') or target_id}", user_id=update.effective_user.id)
        await query.answer("Recordatorio enviado.")
        return
    elif action == "pendmute" and len(parts) >= 4:
        target_id = int(parts[3])
        try:
            await validation_mute_bot(context.bot, chat_id, target_id)
            record = validation_get_record(chat_id, target_id) or {}
            mark_user_muted(chat_id, target_id, reason="silenciado_desde_pendientes")
            add_action_log(chat_id, "silenciar", f"Silenciado desde pendientes: {record.get('name') or target_id}", user_id=update.effective_user.id)
            await query.answer("Silenciado hasta contestar.")
        except Exception:
            await query.answer("No pude silenciar. Revisa permisos.", show_alert=True)
        return
    elif action == "rec_new_quick" and len(parts) >= 4:
        code = parts[3]
        try:
            sched = recurring_schedule_from_code(code)
        except Exception as e:
            await query.answer(str(e), show_alert=True)
            return
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_new_text", "chat_id": chat_id, "return_block": "recurrentes", "schedule": sched})
        await query.edit_message_text(
            "➕ <b>Nuevo mensaje recurrente</b>\n\n"
            f"Horario elegido: <b>{h(str(sched.get('label') or ''))}</b>\n\n"
            "Envíame ahora el texto del mensaje.\n\n"
            "Si quieres ponerle nombre desde el principio, usa:\n<code>Nombre del recurrente | Texto del mensaje</code>\n\n"
            "También puedes enviar una foto, vídeo, GIF o documento con texto en el pie/caption y quedará guardado con multimedia.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|recurrentes")]])
        )
        return
    elif action == "rec_new_custom":
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_new_custom", "chat_id": chat_id, "return_block": "recurrentes"})
        await query.edit_message_text(
            "⏱ <b>Horario personalizado</b>\n\n"
            "Envíame el horario así:\n"
            "<code>cada 30m</code>\n<code>cada 2h</code>\n<code>diario 21:30</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|recurrentes")]])
        )
        return
    elif action == "rec_edit" and len(parts) >= 4:
        rid = int(parts[3])
        await query.edit_message_text(recurring_detail_text(chat_id, rid), parse_mode=ParseMode.HTML, reply_markup=recurring_detail_markup(chat_id, rid))
        return
    elif action in ("rec_send", "rec_toggle", "rec_pin", "rec_del", "rec_clear_media") and len(parts) >= 4:
        rid = int(parts[3])
        rows = recurring_list(chat_id)
        row = recurring_find(chat_id, rid)
        if not row:
            await query.answer("No encuentro ese recurrente.", show_alert=True)
            await show_block("recurrentes")
            return
        if action == "rec_send":
            await send_recurring_message(context, chat_id, row)
            await query.answer("Enviado ✅")
            await query.edit_message_text(recurring_detail_text(chat_id, rid), parse_mode=ParseMode.HTML, reply_markup=recurring_detail_markup(chat_id, rid))
            return
        if action == "rec_toggle":
            row["enabled"] = not bool(row.get("enabled", True))
            save_all_states(); await query.answer("ON/OFF actualizado ✅")
        elif action == "rec_pin":
            row["pin"] = not bool(row.get("pin", False))
            save_all_states(); await query.answer("Fijado ON/OFF actualizado ✅")
        elif action == "rec_clear_media":
            row["media"] = None
            save_all_states(); await query.answer("Multimedia quitada ✅")
        elif action == "rec_del":
            rows.remove(row)
            save_all_states(); await query.answer("Borrado ✅")
            await show_block("recurrentes")
            return
        await query.edit_message_text(recurring_detail_text(chat_id, rid), parse_mode=ParseMode.HTML, reply_markup=recurring_detail_markup(chat_id, rid))
        return
    elif action == "rec_edit_name" and len(parts) >= 4:
        rid = int(parts[3])
        row = recurring_find(chat_id, rid)
        if not row:
            await query.answer("No encuentro ese recurrente.", show_alert=True); return
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_edit_name", "chat_id": chat_id, "rid": rid, "return_block": "recurrentes"})
        await query.edit_message_text(
            (
                f"✏️ <b>Editar nombre recurrente #{rid}</b>\n\n"
                f"Actual: <b>{h(str(row.get('name') or 'Sin nombre'))}</b>\n\n"
                "Envíame el nuevo nombre."
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|rec_edit|{chat_id}|{rid}|recurrentes")]])
        )
        return
    elif action == "rec_edit_text" and len(parts) >= 4:
        rid = int(parts[3])
        row = recurring_find(chat_id, rid)
        if not row:
            await query.answer("No encuentro ese recurrente.", show_alert=True); return
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_edit_text", "chat_id": chat_id, "rid": rid, "return_block": "recurrentes"})
        await query.edit_message_text(
            f"✏️ <b>Editar texto recurrente #{rid}</b>\n\nActual:\n<pre>{h(str(row.get('text') or ''))}</pre>\n\nEnvíame el nuevo texto.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|rec_edit|{chat_id}|{rid}|recurrentes")]])
        )
        return
    elif action == "rec_edit_buttons" and len(parts) >= 4:
        rid = int(parts[3])
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_edit_buttons", "chat_id": chat_id, "rid": rid, "return_block": "recurrentes"})
        await query.edit_message_text(
            "⌨️ <b>Botones del recurrente</b>\n\nEnvíalos así:\n<code>Canal=https://t.me/tu_canal | Web=https://ejemplo.com</code>\n\nPara borrar todos escribe <code>QUITAR</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|rec_edit|{chat_id}|{rid}|recurrentes")]])
        )
        return
    elif action == "rec_edit_media" and len(parts) >= 4:
        rid = int(parts[3])
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_edit_media", "chat_id": chat_id, "rid": rid, "return_block": "recurrentes"})
        await query.edit_message_text(
            "🖼 <b>Multimedia del recurrente</b>\n\nEnvíame ahora una foto, vídeo, GIF o documento.\nPara quitarla usa el botón ‘Quitar media’.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|rec_edit|{chat_id}|{rid}|recurrentes")]])
        )
        return
    elif action == "rec_schedule" and len(parts) >= 5:
        rid = int(parts[3]); code = parts[4]
        row = recurring_find(chat_id, rid)
        if not row:
            await query.answer("No encuentro ese recurrente.", show_alert=True); return
        try:
            sched = recurring_schedule_from_code(code)
        except Exception as e:
            await query.answer(str(e), show_alert=True); return
        row["schedule"] = sched; row["schedule_label"] = sched.get("label", ""); row["last_sent_ts"] = 0; row.pop("last_day", None)
        save_all_states(); await query.answer("Horario actualizado ✅")
        await query.edit_message_text(recurring_detail_text(chat_id, rid), parse_mode=ParseMode.HTML, reply_markup=recurring_detail_markup(chat_id, rid))
        return
    elif action == "rec_schedule_custom" and len(parts) >= 4:
        rid = int(parts[3])
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_edit_schedule", "chat_id": chat_id, "rid": rid, "return_block": "recurrentes"})
        await query.edit_message_text(
            "⏱ <b>Nuevo horario</b>\n\nEnvíame algo así:\n<code>cada 30m</code>\n<code>cada 2h</code>\n<code>diario 21:30</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|rec_edit|{chat_id}|{rid}|recurrentes")]])
        )
        return
    elif action == "edit_text" and len(parts) >= 4:
        field = parts[3]
        return_block = return_block_from_parts("")
        if not return_block:
            for _profile, _info in MESSAGE_PROFILES.items():
                if _info.get("text") == field:
                    return_block = _profile
                    break
        set_config_pending(update.effective_user.id, {"kind": "cfg_text", "chat_id": chat_id, "field": field, "return_block": return_block})
        label = CONFIG_TEXT_FIELDS.get(field, field)
        current = str(cfg_value(chat_id, field, ""))
        cancel_target = f"cfg|block|{chat_id}|{return_block}" if return_block else f"cfg|open|{chat_id}"
        await query.edit_message_text(
            f"✏️ <b>{h(label)}</b>\n\nActual:\n<pre>{h(current)}</pre>\n\nEnvíame ahora el nuevo texto por aquí.\n\nVariables: <code>{{mention}}</code>, <code>{{name}}</code>, <code>{{first}}</code>, <code>{{username}}</code>, <code>{{id}}</code>, <code>{{chat}}</code>, <code>{{mentions}}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=cancel_target)]])
        )
        return
    elif action == "questions":
        return_block = return_block_from_parts("questions")
        set_config_pending(update.effective_user.id, {"kind": "cfg_questions", "chat_id": chat_id, "return_block": return_block})
        current = "\n".join(cfg_questions(chat_id))
        await query.edit_message_text(
            f"❓ <b>Preguntas actuales</b>\n\n<pre>{h(current)}</pre>\n\nEnvíame las nuevas preguntas, una por línea o separadas por |.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|{return_block}")]])
        )
        return
    elif action == "buttons":
        profile = parts[3] if len(parts) >= 4 else "welcome"
        buttons_field = profile_field(profile, "buttons")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_buttons", "chat_id": chat_id, "buttons_field": buttons_field, "return_block": profile})
        await query.edit_message_text(
            f"⌨️ <b>Botones: {h(title)}</b>\n\nEnvíame botones así, uno por línea:\n\n<code>Texto del botón - https://enlace.com</code>\n<code>Normas - https://t.me/...</code>\n\nPara borrar todos escribe: <code>QUITAR</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|{profile}")]])
        )
        return
    elif action == "media":
        profile = parts[3] if len(parts) >= 4 else "welcome"
        media_field = profile_field(profile, "media")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_media", "chat_id": chat_id, "media_field": media_field, "return_block": profile})
        await query.edit_message_text(
            f"🖼 <b>Multimedia: {h(title)}</b>\n\nEnvíame ahora una foto, vídeo, GIF o documento.\n\nPara quitar multimedia escribe: <code>QUITAR</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|{profile}")]])
        )
        return
    elif action == "pregonero_manual":
        current = cfg_value(chat_id, "pregonero_manual_users", [])
        if isinstance(current, list):
            current_text = "\n".join(str(x) for x in current)
        else:
            current_text = str(current or "")
        set_config_pending(update.effective_user.id, {"kind": "cfg_pregonero_manual", "chat_id": chat_id, "return_block": "pregonero"})
        await query.edit_message_text(
            f"➕ <b>Usuarios manuales del pregonero</b>\n\nActual:\n<pre>{h(current_text)}</pre>\n\nEnvíame @usuarios, IDs o enlaces tg://user?id=... separados por espacios o líneas.\nPara borrar todos escribe <code>QUITAR</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|pregonero")]])
        )
        return
    elif action == "pregonero_clear_manual":
        cfg_set(chat_id, "pregonero_manual_users", [])
        await query.answer("Usuarios manuales quitados.")
        target_block = "pregonero"
    elif action == "send_pregonero":
        await query.answer("Lanzando pregonero…")
        await send_pregonero(context, chat_id)
        return
    elif action == "approvers":
        await query.edit_message_text(
            "👮 <b>Quién puede validar presentaciones</b>\n\nElige quién podrá pulsar ✅ Validar o ❌ Rechazar.",
            parse_mode=ParseMode.HTML,
            reply_markup=approver_markup(chat_id)
        )
        return
    elif action == "setapprover" and len(parts) >= 4:
        mode = parts[3]
        if mode in APPROVER_MODE_LABELS:
            cfg_set(chat_id, "validation_approver_mode", mode)
        await show_block("validation")
        return

    if target_block and target_block in ADMIN_PRIVATE_BLOCK_IDS:
        await show_block(target_block)
    else:
        await show_main()


async def admin_private_config_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user or not update.message or not update.message.text:
        return False
    if update.effective_chat and update.effective_chat.type != "private":
        return False
    pending = get_config_pending(update.effective_user.id)
    if not pending:
        return False
    text = update.message.text.strip()
    html_text = telegram_html_from_message(update.message).strip()
    kind = pending.get("kind")
    chat_id = int(pending.get("target_chat_id", pending.get("chat_id", 0)))
    return_block = str(pending.get("return_block") or "")

    # Entradas privadas de módulos fuera del panel global: permiten configurar HOT/DJ
    # por privado aunque el admin del grupo no esté en ADMIN_IDS.
    if kind in ("hot_pin_text", "hot_pin_button"):
        if not await is_admin(context, chat_id, update.effective_user.id):
            pop_config_pending(update.effective_user.id)
            await update.message.reply_text("No tienes permisos de admin en ese grupo.")
            return True
        if kind == "hot_pin_text":
            cfg_set(chat_id, "hot_pin_text", html_text or h(text))
            await update.message.reply_html("✅ Texto fijado actualizado.\n\n" + hot_config_text(chat_id), reply_markup=hot_config_markup(chat_id))
        else:
            cfg_set(chat_id, "hot_pin_button_text", text[:64] or "🎲 Enviar preguntita / retito")
            await update.message.reply_html("✅ Texto del botón actualizado.\n\n" + hot_config_text(chat_id), reply_markup=hot_config_markup(chat_id))
        pop_config_pending(update.effective_user.id)
        return True

    if kind in ("djpriv_savequeue_name", "dj_music_pin_text", "dj_music_pin_button"):
        if not await is_admin(context, chat_id, update.effective_user.id):
            pop_config_pending(update.effective_user.id)
            await update.message.reply_text("No tienes permisos de admin en ese grupo.")
            return True
        state = get_state(chat_id)
        if kind == "djpriv_savequeue_name":
            name = text[:80].strip()
            if not name:
                await update.message.reply_text("Nombre no válido. Envíame un nombre para la lista o CANCELAR.")
                return True
            if not state.queue:
                pop_config_pending(update.effective_user.id)
                await update.message.reply_text("La cola está vacía; no hay nada que guardar.")
                return True
            state.saved_lists[name] = [dict(item) for item in state.queue]
            save_all_states(); pop_config_pending(update.effective_user.id)
            await update.message.reply_html(f"✅ Lista guardada como <b>{h(name)}</b>.\n\n" + dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id))
            return True
        if kind == "dj_music_pin_text":
            cfg_set(chat_id, "dj_music_pin_text", html_text or h(text))
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("✅ Texto del mensaje de música actualizado.\n\n" + dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id))
            return True
        if kind == "dj_music_pin_button":
            cfg_set(chat_id, "dj_music_pin_button_text", text[:64] or "🎧 Escuchar música")
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("✅ Texto del botón de música actualizado.\n\n" + dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id))
            return True

    if not is_global_admin_user(update.effective_user.id):
        return True

    async def reply_current_panel(prefix: str) -> None:
        if return_block in ADMIN_PRIVATE_BLOCK_IDS:
            await update.message.reply_html(prefix + "\n\n" + admin_private_block_text(chat_id, return_block), reply_markup=admin_private_block_markup(chat_id, return_block))
        else:
            await update.message.reply_html(prefix + "\n\n" + admin_private_main_text(chat_id), reply_markup=admin_private_main_markup(chat_id))

    if text.upper() in ("CANCELAR", "/CANCELAR"):
        pop_config_pending(update.effective_user.id)
        await reply_current_panel("❌ Cancelado.")
        return True
    if kind == "cfg_rec_new_custom":
        try:
            sched = parse_recurring_when(text)
        except ValueError as e:
            await update.message.reply_text(str(e))
            return True
        set_config_pending(update.effective_user.id, {"kind": "cfg_rec_new_text", "chat_id": chat_id, "return_block": "recurrentes", "schedule": sched})
        await update.message.reply_html("✅ Horario guardado: <b>" + h(str(sched.get("label") or "")) + "</b>\n\nAhora envíame el texto del mensaje recurrente. También puedes enviar foto/vídeo/documento con caption.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|block|{chat_id}|recurrentes")]]))
        return True
    if kind == "cfg_rec_new_text":
        sched = pending.get("schedule") if isinstance(pending.get("schedule"), dict) else {"type": "interval", "seconds": 3600, "label": "cada 1h"}
        row = recurring_create_row(chat_id, html_text or h(text), sched)
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html(f"✅ Recurrente creado: <b>#{int(row.get('id'))}</b>\n\n" + admin_private_block_text(chat_id, "recurrentes"), reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
        return True
    if kind == "cfg_rec_edit_name":
        rid = int(pending.get("rid", 0) or 0)
        row = recurring_find(chat_id, rid)
        if not row:
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("No encuentro ese recurrente.", reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
            return True
        name = text.replace("\n", " ").strip()[:60]
        if not name:
            await update.message.reply_text("Nombre vacío. Envíame un nombre o CANCELAR.")
            return True
        row["name"] = name
        save_all_states(); pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Nombre actualizado.\n\n" + recurring_detail_text(chat_id, rid), reply_markup=recurring_detail_markup(chat_id, rid))
        return True
    if kind == "cfg_rec_edit_text":
        rid = int(pending.get("rid", 0) or 0)
        row = recurring_find(chat_id, rid)
        if not row:
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("No encuentro ese recurrente.", reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
            return True
        row["text"] = html_text or h(text)
        if not str(row.get("name") or "").strip():
            row["name"] = text.replace("\n", " ")[:60]
        save_all_states(); pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Texto actualizado.\n\n" + recurring_detail_text(chat_id, rid), reply_markup=recurring_detail_markup(chat_id, rid))
        return True
    if kind == "cfg_rec_edit_buttons":
        rid = int(pending.get("rid", 0) or 0)
        row = recurring_find(chat_id, rid)
        if not row:
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("No encuentro ese recurrente.", reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
            return True
        if text.upper() == "QUITAR":
            row["buttons"] = []
        else:
            buttons = recurring_parse_buttons_private(text)
            if not buttons:
                await update.message.reply_html("No he detectado botones válidos. Usa: <code>Texto=https://enlace.com | Otro=https://t.me/...</code>")
                return True
            row["buttons"] = buttons
        save_all_states(); pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Botones actualizados.\n\n" + recurring_detail_text(chat_id, rid), reply_markup=recurring_detail_markup(chat_id, rid))
        return True
    if kind == "cfg_rec_edit_schedule":
        rid = int(pending.get("rid", 0) or 0)
        row = recurring_find(chat_id, rid)
        if not row:
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("No encuentro ese recurrente.", reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
            return True
        try:
            sched = parse_recurring_when(text)
        except ValueError as e:
            await update.message.reply_text(str(e))
            return True
        row["schedule"] = sched; row["schedule_label"] = sched.get("label", ""); row["last_sent_ts"] = 0; row.pop("last_day", None)
        save_all_states(); pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Horario actualizado.\n\n" + recurring_detail_text(chat_id, rid), reply_markup=recurring_detail_markup(chat_id, rid))
        return True
    if kind == "cfg_text":
        field = str(pending.get("field"))
        cfg_set(chat_id, field, html_text or h(text))
        pop_config_pending(update.effective_user.id)
        await reply_current_panel(f"✅ Texto actualizado: <b>{h(CONFIG_TEXT_FIELDS.get(field, field))}</b>")
        return True
    if kind == "cfg_questions":
        questions = [q.strip() for q in text.replace("|", "\n").splitlines() if q.strip()]
        if not questions:
            await update.message.reply_text("No he detectado preguntas válidas.")
            return True
        cfg_set(chat_id, "validation_questions", questions)
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Preguntas actualizadas:\n" + "\n".join(f"{i+1}. {h(q)}" for i, q in enumerate(questions)), reply_markup=admin_private_main_markup(chat_id))
        return True
    if kind == "cfg_buttons":
        buttons_field = str(pending.get("buttons_field") or "validation_public_join_buttons")
        if text.upper() == "QUITAR":
            cfg_set(chat_id, buttons_field, [])
            pop_config_pending(update.effective_user.id)
            await reply_current_panel("✅ Botones quitados.")
            return True
        buttons = parse_buttons_text(text)
        if not buttons:
            await update.message.reply_html("No he detectado botones válidos. Usa: <code>Texto - https://enlace.com</code>")
            return True
        cfg_set(chat_id, buttons_field, buttons)
        pop_config_pending(update.effective_user.id)
        await reply_current_panel(f"✅ Botones guardados: <b>{len(buttons)}</b>")
        return True
    if kind == "cfg_media" and text.upper() == "QUITAR":
        media_field = str(pending.get("media_field") or "validation_public_join_media")
        cfg_set(chat_id, media_field, None)
        pop_config_pending(update.effective_user.id)
        await reply_current_panel("✅ Multimedia quitada.")
        return True
    if kind == "cfg_pregonero_manual":
        if text.upper() == "QUITAR":
            users = []
        else:
            import re as _re
            users = [x.strip() for x in _re.split(r"[\s,;]+", text) if x.strip()]
        cfg_set(chat_id, "pregonero_manual_users", users[:300])
        pop_config_pending(update.effective_user.id)
        add_action_log(chat_id, "config", f"Pregonero manual actualizado: {len(users[:300])} usuarios", user_id=update.effective_user.id)
        await reply_current_panel(f"✅ Usuarios manuales del pregonero guardados: <b>{len(users[:300])}</b>")
        return True
    return False


async def admin_private_config_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.effective_chat or update.effective_chat.type != "private":
        return
    pending = get_config_pending(update.effective_user.id)
    if not pending:
        return
    kind = str(pending.get("kind") or "")
    if kind not in ("cfg_media", "cfg_rec_new_text", "cfg_rec_edit_media"):
        return
    if not is_global_admin_user(update.effective_user.id):
        return
    chat_id = int(pending.get("target_chat_id", pending.get("chat_id", 0)))
    return_block = str(pending.get("return_block") or "")
    media: Optional[Dict[str, str]] = None
    if update.message.photo:
        media = {"type": "photo", "file_id": update.message.photo[-1].file_id}
    elif update.message.video:
        media = {"type": "video", "file_id": update.message.video.file_id}
    elif update.message.animation:
        media = {"type": "animation", "file_id": update.message.animation.file_id}
    elif update.message.document:
        media = {"type": "document", "file_id": update.message.document.file_id}
    if not media:
        await update.message.reply_text("Ese tipo de archivo no está soportado. Envía foto, vídeo, GIF o documento.")
        return

    if kind == "cfg_rec_new_text":
        sched = pending.get("schedule") if isinstance(pending.get("schedule"), dict) else {"type": "interval", "seconds": 3600, "label": "cada 1h"}
        caption = telegram_html_from_message(update.message, caption=True).strip()
        row = recurring_create_row(chat_id, caption, sched, media=media)
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html(f"✅ Recurrente con multimedia creado: <b>#{int(row.get('id'))}</b>\n\n" + admin_private_block_text(chat_id, "recurrentes"), reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
        return

    if kind == "cfg_rec_edit_media":
        rid = int(pending.get("rid", 0) or 0)
        row = recurring_find(chat_id, rid)
        if not row:
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("No encuentro ese recurrente.", reply_markup=admin_private_block_markup(chat_id, "recurrentes"))
            return
        row["media"] = media
        if update.message.caption:
            row["text"] = telegram_html_from_message(update.message, caption=True).strip()
        save_all_states()
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Multimedia actualizada.\n\n" + recurring_detail_text(chat_id, rid), reply_markup=recurring_detail_markup(chat_id, rid))
        return

    media_field = str(pending.get("media_field") or "validation_public_join_media")
    cfg_set(chat_id, media_field, media)
    pop_config_pending(update.effective_user.id)
    await update.message.reply_html("✅ Multimedia guardada.\n\n" + (admin_private_block_text(chat_id, return_block) if return_block in ADMIN_PRIVATE_BLOCK_IDS else admin_private_main_text(chat_id)), reply_markup=(admin_private_block_markup(chat_id, return_block) if return_block in ADMIN_PRIVATE_BLOCK_IDS else admin_private_main_markup(chat_id)))

async def can_validate_presentation(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    mode = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
    if mode == "admin_ids":
        return int(user_id) in ADMIN_IDS
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        if mode == "creator":
            return member.status == "creator"
        return member.status in ("administrator", "creator") or int(user_id) in ADMIN_IDS
    except Exception:
        return int(user_id) in ADMIN_IDS

# =========================
# MÓDULO: PRESENTACIÓN + VALIDACIÓN ADMIN
# =========================
def _now_ts() -> int:
    import time as _time
    return int(_time.time())


def validation_is_active_for_chat(chat_id: int) -> bool:
    return bool(cfg_value(chat_id, "validation_enabled", VALIDATION_ENABLED)) and chat_is_allowed(chat_id)


def validation_format_template(template: str, user) -> str:
    username = f"@{user.username}" if getattr(user, "username", None) else ""
    mention = user.mention_html() if hasattr(user, "mention_html") else h(display_name(user))
    return (
        template
        .replace("{mention}", mention)
        .replace("{name}", h(display_name(user)))
        .replace("{first}", h(getattr(user, "first_name", "") or display_name(user)))
        .replace("{username}", h(username))
        .replace("{id}", h(getattr(user, "id", "")))
    )


def validation_get_record(chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    state = get_state(chat_id)
    data = state.validation_users.get(str(user_id))
    return data if isinstance(data, dict) else None


def validation_set_record(chat_id: int, user_id: int, record: Dict[str, Any]) -> None:
    state = get_state(chat_id)
    state.validation_users[str(user_id)] = record
    save_all_states()


async def validation_restrict_answering(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    await context.bot.restrict_chat_member(
        chat_id=chat_id,
        user_id=user_id,
        permissions=ChatPermissions(
            can_send_messages=True,
            can_send_audios=False,
            can_send_documents=False,
            can_send_photos=False,
            can_send_videos=False,
            can_send_video_notes=False,
            can_send_voice_notes=False,
            can_send_polls=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
            can_change_info=False,
            can_invite_users=False,
            can_pin_messages=False,
            can_manage_topics=False,
        ),
    )


async def validation_mute(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    await validation_mute_bot(context.bot, chat_id, user_id)


async def validation_mute_bot(bot, chat_id: int, user_id: int) -> None:
    await bot.restrict_chat_member(
        chat_id=chat_id,
        user_id=user_id,
        permissions=ChatPermissions(
            can_send_messages=False,
            can_send_audios=False,
            can_send_documents=False,
            can_send_photos=False,
            can_send_videos=False,
            can_send_video_notes=False,
            can_send_voice_notes=False,
            can_send_polls=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
            can_change_info=False,
            can_invite_users=False,
            can_pin_messages=False,
            can_manage_topics=False,
        ),
    )
    mark_user_muted(chat_id, user_id, reason="pendiente_validacion")


async def validation_unrestrict(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    await context.bot.restrict_chat_member(
        chat_id=chat_id,
        user_id=user_id,
        permissions=ChatPermissions.all_permissions(),
    )
    unmark_user_muted(chat_id, user_id)


async def validation_retry_restrict(bot, chat_id: int, user_id: int, attempts: int = 5) -> None:
    for attempt in range(max(1, attempts)):
        await asyncio.sleep(1 + attempt)
        try:
            await validation_mute_bot(bot, chat_id, user_id)
            return
        except Exception:
            continue
    logger.warning("No se pudo reintentar restricción de validación para %s en %s", user_id, chat_id)


async def start_validation_for_user(
    update_or_context,
    context: Optional[ContextTypes.DEFAULT_TYPE],
    chat_id: int,
    user,
    *,
    reply_to_message_id: Optional[int] = None,
    source: str = "new_chat_member",
    force: bool = False,
) -> bool:
    if not user or getattr(user, "is_bot", False):
        return False
    if not validation_is_active_for_chat(chat_id) and not force:
        return False
    state = get_state(chat_id)
    existing = validation_get_record(chat_id, user.id)
    if existing and existing.get("status") in ("answering", "pending_admin") and not force:
        return False

    bot = context.bot if context else update_or_context.bot
    questions = cfg_questions(chat_id)
    joined = _now_ts()
    record = {
        "user_id": user.id,
        "name": display_name(user),
        "username": f"@{user.username}" if getattr(user, "username", None) else "",
        "status": "answering",
        "step": 0,
        "answers": [],
        "joined_ts": existing.get("joined_ts", joined) if existing else joined,
        "deadline_ts": joined + int(cfg_value(chat_id, "validation_timeout_minutes", VALIDATION_TIMEOUT_MINUTES)) * 60,
        "reminder_ts": joined + int(cfg_value(chat_id, "validation_reminder_minutes", VALIDATION_REMINDER_MINUTES)) * 60,
        "reminded": False,
        "public_message_id": None,
        "question_message_id": None,
        "review_message_id": None,
        "source": source,
        "forced": bool(force),
    }
    validation_set_record(chat_id, user.id, record)
    remember_entry(chat_id, user, source=source)

    try:
        await validation_restrict_answering(context, chat_id, user.id)  # type: ignore[arg-type]
        mark_user_muted(chat_id, user.id, user=user, reason="pendiente_presentacion")
    except Exception:
        logger.exception("No se pudo restringir al usuario %s en chat %s", user.id, chat_id)
        try:
            asyncio.create_task(validation_retry_restrict(bot, chat_id, user.id))
        except Exception:
            pass

    if not force:
        try:
            public_message_id = await send_configured_public_join(
                bot,
                chat_id,
                user,
                reply_to_message_id=reply_to_message_id,
            )
            record["public_message_id"] = public_message_id
        except Exception:
            logger.exception("No se pudo enviar mensaje público de entrada")

    try:
        intro_text = (
            validation_format_template(str(cfg_value(chat_id, "validation_intro_message", VALIDATION_INTRO_MESSAGE)), user)
            + f"\n\n<b>Pregunta 1/{len(questions)}</b>\n{h(questions[0])}"
        )
        q_id = await send_configured_profile_message(bot, chat_id, "questions", intro_text, reply_to_message_id=reply_to_message_id)
        record["question_message_id"] = q_id
    except Exception:
        logger.exception("No se pudo enviar primera pregunta")

    validation_set_record(chat_id, user.id, record)
    return True


async def validation_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not validation_is_active_for_chat(update.effective_chat.id):
        return
    chat_id = update.effective_chat.id
    remember_chat_title(chat_id, update.effective_chat.title or "")
    for user in update.message.new_chat_members:
        await start_validation_for_user(
            update,
            context,
            chat_id,
            user,
            reply_to_message_id=update.message.message_id,
            source="new_chat_member",
            force=False,
        )


async def validation_chat_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    req = update.chat_join_request
    if not req or not req.chat or not req.from_user:
        return
    chat_id = req.chat.id
    remember_chat_title(chat_id, getattr(req.chat, "title", "") or "")
    if not validation_is_active_for_chat(chat_id):
        return
    user = req.from_user

    if bool(cfg_value(chat_id, "validation_auto_approve_join_requests", True)):
        try:
            await context.bot.approve_chat_join_request(chat_id=chat_id, user_id=user.id)
        except Exception:
            logger.exception("No pude aprobar la solicitud de entrada de %s en %s", user.id, chat_id)
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ Solicitud detectada de {user.mention_html()}, pero no pude aprobarla. Revisa que el bot sea admin con permiso para aprobar solicitudes.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return
        await start_validation_for_user(
            update,
            context,
            chat_id,
            user,
            reply_to_message_id=None,
            source="join_request_auto_approved",
            force=False,
        )
    else:
        remember_entry(chat_id, user, source="join_request_pending_admin")
        key = f"{chat_id}:{user.id}"
        VALIDATION_JOIN_REQUESTS[key] = {
            "chat_id": int(chat_id),
            "user_id": int(user.id),
            "first_name": getattr(user, "first_name", "") or display_name(user),
            "last_name": getattr(user, "last_name", "") or "",
            "username": getattr(user, "username", None),
            "name": display_name(user),
            "ts": _now_ts(),
        }
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Aprobar y validar", callback_data=f"valreq|approve|{user.id}"),
            InlineKeyboardButton("❌ Rechazar", callback_data=f"valreq|decline|{user.id}"),
        ]])
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🕵️ <b>Solicitud de entrada detectada</b>\n\n"
                    f"Usuario: {user.mention_html()}\n"
                    "Autoaprobación está desactivada. Si pulsas <b>Aprobar y validar</b>, "
                    "el bot aprobará la solicitud e iniciará igualmente la validación."
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        except Exception:
            pass


class _ValidationRequestedUser:
    def __init__(self, data: Dict[str, Any]):
        self.id = int(data.get("user_id"))
        self.first_name = str(data.get("first_name") or data.get("name") or "Usuario")
        self.last_name = str(data.get("last_name") or "")
        self.username = data.get("username") or None
        self.is_bot = False

    def mention_html(self) -> str:
        return f'<a href="tg://user?id={int(self.id)}">{h(display_name(self))}</a>'


async def validation_join_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("valreq|"):
        return
    chat_id = int(update.effective_chat.id)
    if not await can_validate_presentation(context, chat_id, update.effective_user.id):
        await query.answer("No tienes permiso para aprobar solicitudes.", show_alert=True)
        return
    parts = data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    try:
        target_id = int(parts[2])
    except Exception:
        await query.answer("Usuario inválido.", show_alert=True)
        return
    key = f"{chat_id}:{target_id}"
    row = VALIDATION_JOIN_REQUESTS.get(key) or {"user_id": target_id, "first_name": str(target_id), "name": str(target_id)}
    user = _ValidationRequestedUser(row)
    if action == "approve":
        try:
            await context.bot.approve_chat_join_request(chat_id=chat_id, user_id=target_id)
        except Exception:
            logger.exception("No pude aprobar solicitud desde botón")
            await query.answer("No pude aprobar. Revisa permisos del bot para aprobar solicitudes.", show_alert=True)
            return
        await start_validation_for_user(update, context, chat_id, user, reply_to_message_id=None, source="join_request_button_approved", force=True)
        VALIDATION_JOIN_REQUESTS.pop(key, None)
        try:
            await query.edit_message_text(f"✅ Solicitud aprobada y validación iniciada para {user.mention_html()}.", parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await query.answer("Aprobado y enviado a validación ✅")
        return
    if action == "decline":
        try:
            await context.bot.decline_chat_join_request(chat_id=chat_id, user_id=target_id)
        except Exception:
            logger.exception("No pude rechazar solicitud desde botón")
            await query.answer("No pude rechazar. Revisa permisos del bot.", show_alert=True)
            return
        VALIDATION_JOIN_REQUESTS.pop(key, None)
        try:
            await query.edit_message_text(f"❌ Solicitud rechazada para {user.mention_html()}.", parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await query.answer("Solicitud rechazada")
        return
    await query.answer("Acción no válida", show_alert=True)


async def force_presentate_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    if not update.message.reply_to_message or not update.message.reply_to_message.from_user:
        await update.message.reply_text("Responde al usuario con /presentate o /preséntate para obligarle a presentarse.")
        return
    target = update.message.reply_to_message.from_user
    started = await start_validation_for_user(
        update,
        context,
        chat_id,
        target,
        reply_to_message_id=update.message.reply_to_message.message_id,
        source="forced_presentate",
        force=True,
    )
    if started:
        await update.message.reply_html(f"🔇 {target.mention_html()} queda silenciado y debe responder la presentación.")
    else:
        await update.message.reply_text("No pude iniciar la presentación forzada. Revisa que el grupo esté permitido y que el bot tenga permisos.")


async def validation_handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message or not update.message.text:
        return False
    if not validation_is_active_for_chat(update.effective_chat.id):
        return False
    chat_id = update.effective_chat.id
    user = update.effective_user
    record = validation_get_record(chat_id, user.id)
    if not record:
        return False
    status = record.get("status")
    if status == "validated":
        return False
    if status == "pending_admin":
        if bool(cfg_value(chat_id, "validation_delete_wrong_messages", VALIDATION_DELETE_WRONG_MESSAGES)):
            await safe_delete(context.bot, chat_id, update.message.message_id)
        else:
            await update.message.reply_text("⏳ Tu presentación ya está enviada. Espera validación de un administrador.")
        return True
    if status != "answering":
        return False
    text = update.message.text.strip()
    if not text:
        return True
    step = int(record.get("step", 0))
    answers = list(record.get("answers", []))
    questions = cfg_questions(chat_id)
    current_question = questions[step] if 0 <= step < len(questions) else "Respuesta"
    answers.append({"question": current_question, "answer": text})
    step += 1
    record["answers"] = answers
    record["step"] = step
    if step < len(questions):
        record["status"] = "answering"
        validation_set_record(chat_id, user.id, record)
        await update.message.reply_html(
            f"✅ Recibido.\n\n<b>Pregunta {step + 1}/{len(questions)}</b>\n{h(questions[step])}"
        )
        return True
    record["status"] = "pending_admin"
    validation_set_record(chat_id, user.id, record)
    try:
        await validation_mute(context, chat_id, user.id)
    except Exception:
        logger.exception("No se pudo silenciar tras completar presentación")
    lines = [f"<b>{h(item.get('question', 'Pregunta'))}</b> {h(item.get('answer', ''))}" for item in answers]
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Validar", callback_data=f"val|ok|{user.id}"),
        InlineKeyboardButton("❌ Rechazar", callback_data=f"val|no|{user.id}"),
    ]])
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"📋 <b>Presentación pendiente de validar</b>\n\n"
            f"Usuario: {user.mention_html()}\n\n"
            "\n".join(lines) + "\n\n"
            "Un administrador debe aprobar para que pueda hablar."
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    record["review_message_id"] = msg.message_id
    validation_set_record(chat_id, user.id, record)
    return True


async def validation_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return False
    data = query.data or ""
    if not data.startswith("val|"):
        return False
    chat_id = update.effective_chat.id
    if not validation_is_active_for_chat(chat_id):
        await query.answer("Validación desactivada.", show_alert=True)
        return True
    parts = data.split("|")
    if len(parts) < 3:
        await query.answer("Acción inválida.", show_alert=True)
        return True
    action = parts[1]
    try:
        target_id = int(parts[2])
    except Exception:
        await query.answer("Usuario inválido.", show_alert=True)
        return True
    if not await can_validate_presentation(context, chat_id, update.effective_user.id):
        await query.answer("No tienes permiso para validar presentaciones.", show_alert=True)
        return True
    record = validation_get_record(chat_id, target_id)
    if not record:
        await query.answer("No encuentro esta presentación.", show_alert=True)
        return True
    if action == "ok":
        try:
            await validation_unrestrict(context, chat_id, target_id)
        except Exception:
            logger.exception("No se pudo desbloquear al usuario %s", target_id)
            await query.answer("No pude quitar el silencio. Revisa permisos del bot.", show_alert=True)
            return True
        record["status"] = "validated"
        record["validated_by"] = update.effective_user.id
        record["validated_ts"] = _now_ts()
        validation_set_record(chat_id, target_id, record)
        target_mention = f"<a href=\"tg://user?id={target_id}\">{h(record.get('name') or target_id)}</a>"
        try:
            await query.edit_message_text(
                f"✅ <b>Presentación validada</b>\n\nUsuario: {target_mention}\nAdmin: {update.effective_user.mention_html()}",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        approved_text = str(cfg_value(chat_id, "validation_approved_message", VALIDATION_APPROVED_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
        await send_configured_profile_message(context.bot, chat_id, "approved", approved_text)
        if bool(cfg_value(chat_id, "rules_auto_after_approve", False)):
            rules_text = cfg_fake_preview_values(str(cfg_value(chat_id, "rules_text", ""))).replace("Usuario Nuevo", h(record.get("name", "Usuario")))
            await send_configured_profile_message(context.bot, chat_id, "rules", rules_text)
        add_action_log(chat_id, "validación", f"Aprobado: {record.get('name') or target_id}", user_id=update.effective_user.id)
        await query.answer("Usuario validado.")
        return True
    if action == "no":
        record["status"] = "rejected"
        mark_user_expelled(chat_id, target_id, record=record, reason="rechazado en presentación", by_user_id=update.effective_user.id)
        unmark_user_muted(chat_id, target_id)
        record["rejected_by"] = update.effective_user.id
        record["rejected_ts"] = _now_ts()
        validation_set_record(chat_id, target_id, record)
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
        except Exception:
            logger.exception("No se pudo expulsar al usuario %s", target_id)
            await query.answer("No pude expulsar. Revisa permisos del bot.", show_alert=True)
            return True
        try:
            await query.edit_message_text("❌ Presentación rechazada. Usuario expulsado.", parse_mode=ParseMode.HTML)
        except Exception:
            pass
        target_mention = f"<a href=\"tg://user?id={target_id}\">{h(record.get('name') or target_id)}</a>"
        rejected_text = str(cfg_value(chat_id, "validation_rejected_message", VALIDATION_REJECTED_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
        await send_configured_profile_message(context.bot, chat_id, "rejected", rejected_text)
        await query.answer("Usuario rechazado.")
        return True
    await query.answer("Acción no reconocida.", show_alert=True)
    return True


async def validation_watchdog_loop(application: Application) -> None:
    while True:
        try:
            await asyncio.sleep(VALIDATION_WATCHDOG_SECONDS)
            if not VALIDATION_ENABLED:
                continue
            now = _now_ts()
            for chat_id, state in list(STATE_CACHE.items()):
                if not chat_is_allowed(chat_id):
                    continue
                for user_id_str, record in list(state.validation_users.items()):
                    status = record.get("status")
                    if status not in ("answering", "pending_admin"):
                        continue
                    user_id = int(user_id_str)
                    mention = f"<a href=\"tg://user?id={user_id}\">{h(record.get('name') or user_id)}</a>"
                    if status == "answering" and not record.get("reminded") and now >= int(record.get("reminder_ts") or 0):
                        record["reminded"] = True
                        validation_set_record(chat_id, user_id, record)
                        try:
                            reminder_text = str(cfg_value(chat_id, "validation_reminder_message", VALIDATION_REMINDER_MESSAGE)).replace("{mention}", mention).replace("{name}", h(record.get("name", "")))
                            await send_configured_profile_message(application.bot, chat_id, "reminder", reminder_text)
                            add_action_log(chat_id, "recordatorio automático", record.get("name", ""))
                        except Exception:
                            logger.exception("No se pudo enviar recordatorio de validación")
                    if status == "answering" and now >= int(record.get("deadline_ts") or 0):
                        record["status"] = "timeout"
                        validation_set_record(chat_id, user_id, record)
                        try:
                            await validation_mute_bot(application.bot, chat_id, user_id)
                        except Exception:
                            pass
                        try:
                            timeout_text = str(cfg_value(chat_id, "validation_timeout_message", VALIDATION_TIMEOUT_MESSAGE)).replace("{mention}", mention).replace("{name}", h(record.get("name", "")))
                            await send_configured_profile_message(application.bot, chat_id, "timeout", timeout_text)
                            add_action_log(chat_id, "timeout", record.get("name", ""))
                        except Exception:
                            pass
                        if bool(cfg_value(chat_id, "validation_kick_if_timeout", VALIDATION_KICK_IF_TIMEOUT)):
                            try:
                                await application.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                                record["status"] = "kicked_timeout"
                                validation_set_record(chat_id, user_id, record)
                                mark_user_expelled(chat_id, user_id, record=record, reason="timeout presentación")
                            except Exception:
                                logger.exception("No se pudo expulsar por timeout")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Error en watchdog de validación")


async def validation_pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    state = get_state(chat_id)
    answering = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") == "answering"]
    pending = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") == "pending_admin"]
    if not answering and not pending:
        await update.message.reply_text("No hay usuarios pendientes de contestar ni pendientes de validar.")
        return
    if answering:
        lines = ["📝 <b>Pendientes de contestar preguntas</b>\n"]
        for uid, record in answering[:30]:
            step = int(record.get("step", 0))
            total = len(cfg_questions(chat_id))
            mention = f"<a href=\"tg://user?id={uid}\">{h(record.get('name') or uid)}</a>"
            lines.append(f"• {mention} — pregunta <b>{step+1}/{total}</b>")
        if len(answering) > 30:
            lines.append(f"… y {len(answering)-30} más")
        await update.message.reply_html("\n".join(lines))
    for uid, record in pending:
        answers = record.get("answers", [])
        lines = [f"<b>{h(a.get('question','Pregunta'))}</b> {h(a.get('answer',''))}" for a in answers]
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Validar", callback_data=f"val|ok|{uid}"),
            InlineKeyboardButton("❌ Rechazar", callback_data=f"val|no|{uid}"),
        ]])
        mention = f"<a href=\"tg://user?id={uid}\">{h(record.get('name') or uid)}</a>"
        await update.message.reply_html(
            f"📋 <b>Pendiente de validar</b>\n\nUsuario: {mention}\n\n" + "\n".join(lines),
            reply_markup=keyboard,
        )

async def validation_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    state = get_state(chat_id)
    total = len(state.validation_users)
    answering = sum(1 for r in state.validation_users.values() if r.get("status") == "answering")
    pending = sum(1 for r in state.validation_users.values() if r.get("status") == "pending_admin")
    validated = sum(1 for r in state.validation_users.values() if r.get("status") == "validated")
    await update.message.reply_text(
        "⚙️ Validación de nuevos\n\n"
        f"Activo: {cfg_value(chat_id, 'validation_enabled')}\n"
        f"Preguntas: {len(cfg_questions(chat_id))}\n"
        f"Tiempo límite: {cfg_value(chat_id, 'validation_timeout_minutes')} min\n"
        f"Recordatorio: {cfg_value(chat_id, 'validation_reminder_minutes')} min\n"
        f"Expulsar al agotar tiempo: {cfg_value(chat_id, 'validation_kick_if_timeout')}\n\n"
        f"Total registrados: {total}\n"
        f"Respondiendo: {answering}\n"
        f"Pendientes admin: {pending}\n"
        f"Validados: {validated}"
    )

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not chat_is_allowed(update.effective_chat.id):
        return
    if update.message:
        await register_bot_message(update.effective_chat.id, update.message.message_id)
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="<b>DJ-PLAN</b>",
        reply_markup=main_menu_markup(),
        parse_mode=ParseMode.HTML,
    )
    await register_temp_message(update.effective_chat.id, msg.message_id)
    await register_bot_message(update.effective_chat.id, msg.message_id)


async def assign_dj_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message or not chat_is_allowed(update.effective_chat.id):
        return
    if not await is_controller(context, update.effective_chat.id, update.effective_user.id):
        await update.message.reply_text("Solo el DJ asignado o el ID registrado puede asignar DJ.")
        return
    if not update.message.reply_to_message or not update.message.reply_to_message.from_user:
        await update.message.reply_text("Responde al usuario con /dj para asignarlo como DJ.")
        return

    target = update.message.reply_to_message.from_user
    state = get_state(update.effective_chat.id)
    state.assigned_dj_id = target.id
    state.assigned_dj_name = display_name(target)
    state.dj_mode = True
    save_all_states()
    await ensure_panel(context.bot, update.effective_chat.id)
    await ensure_control_panel(context.bot, update.effective_chat.id)
    await send_temp_message(context.bot, update.effective_chat.id, f"✅ DJ asignado: <b>{h(state.assigned_dj_name)}</b>")


async def maybe_handle_pending_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message or not update.message.text or not chat_is_allowed(update.effective_chat.id):
        return False
    key = f"{update.effective_chat.id}:{update.effective_user.id}"
    pending = PENDING_ACTIONS.get(key)
    if not pending:
        return False

    text = update.message.text.strip()
    state = get_state(update.effective_chat.id)
    kind = pending.get("kind")
    prompt_id = pending.get("prompt_id")

    if kind == "save_list":
        if not text:
            await send_temp_message(context.bot, update.effective_chat.id, "❌ Nombre no válido.")
        else:
            state.saved_lists[text] = [dict(item) for item in state.queue]
            save_all_states()
            await send_temp_message(context.bot, update.effective_chat.id, f"💾 Lista guardada: <b>{h(text)}</b>")
        PENDING_ACTIONS.pop(key, None)
        await safe_delete(context.bot, update.effective_chat.id, prompt_id)
        await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)
        return True

    if kind == "temp_pin":
        minutes = int(pending.get("minutes", 1))
        if not text:
            await send_temp_message(context.bot, update.effective_chat.id, "❌ Texto no válido.")
        else:
            await create_temporary_pin(context.bot, update.effective_chat.id, text, minutes)
            await send_temp_message(context.bot, update.effective_chat.id, f"📌 Fijado temporal creado durante <b>{minutes} min</b>.", ttl=20)
        PENDING_ACTIONS.pop(key, None)
        await safe_delete(context.bot, update.effective_chat.id, prompt_id)
        await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)
        return True

    return False


async def text_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat and update.effective_user:
        remember_member_activity(update.effective_chat.id, update.effective_user, kind="message", source="text")
    if await admin_private_config_text(update, context):
        return
    if await validation_handle_text(update, context):
        return
    if await maybe_handle_pending_text(update, context):
        return
    if not update.effective_chat or not update.message or not update.message.text or not chat_is_allowed(update.effective_chat.id):
        return

    text = update.message.text.strip().lower()
    if text == "dj plan" and update.message.reply_to_message:
        track = extract_track_from_message(update.message.reply_to_message)
        if track:
            state = get_state(update.effective_chat.id)
            if not state.dj_mode or state.assigned_dj_id != getattr(update.effective_user, "id", None):
                await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)
                return
            register_detected_track(update.effective_chat.id, update.message.reply_to_message.message_id, track)
            await show_track_actions(context, update.effective_chat.id, update.message.reply_to_message.message_id)
            await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)


async def music_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat and update.effective_user:
        remember_member_activity(update.effective_chat.id, update.effective_user, kind="message", source="media")
    if not update.effective_chat or not update.message or not chat_is_allowed(update.effective_chat.id):
        return
    state = get_state(update.effective_chat.id)
    if not state.dj_mode:
        return
    if not update.message.from_user or update.message.from_user.id != state.assigned_dj_id:
        return
    track = extract_track_from_message(update.message)
    if not track:
        return
    if update.message.from_user and update.message.from_user.id == context.bot.id:
        return
    register_detected_track(update.effective_chat.id, update.message.message_id, track)
    await show_track_actions(context, update.effective_chat.id, update.message.message_id)


async def add_to_library(chat_id: int, track: Track) -> bool:
    state = get_state(chat_id)
    key = library_item_key(track)
    existing = {library_item_key_from_dict(item) for item in state.library}
    if key in existing:
        return False
    state.library.append(asdict(track))
    state.library = dedupe_library_items(state.library)
    save_all_states()
    return True


async def queue_track(chat_id: int, track: Track) -> None:
    state = get_state(chat_id)
    state.queue.append(asdict(track))
    save_all_states()


async def queue_track_first(chat_id: int, track: Track) -> None:
    state = get_state(chat_id)
    state.queue.insert(0, asdict(track))
    save_all_states()


async def play_selected_track(context: ContextTypes.DEFAULT_TYPE, chat_id: int, track: Track, *, push_current_to_history: bool = True) -> None:
    state = get_state(chat_id)
    if push_current_to_history and state.now_playing:
        old_key = track_key(state.now_playing)
        new_key = track.file_unique_id or track.file_id
        if old_key and old_key != new_key:
            state.history.append(dict(state.now_playing))
            state.history = state.history[-25:]
            save_all_states()
    await VOICE.play_track(context.bot, chat_id, track)


async def close_dj_session(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    saved_lists = {name: dedupe_track_items(items) for name, items in state.saved_lists.items()}
    library = dedupe_library_items(state.library)
    auto_track_enabled = state.auto_track_enabled
    auto_sig_seconds = state.auto_sig_seconds
    volume = state.volume

    await cancel_temporary_pin(chat_id)
    await cancel_auto_next(chat_id)
    scan_task = SCAN_TASKS.pop(chat_id, None)
    if scan_task and not scan_task.done():
        scan_task.cancel()

    # Cortamos el estado antes de limpiar para que nada recree paneles durante el cierre.
    state.dj_mode = False
    state.live_enabled = False
    state.now_playing = None
    state.paused = False
    state.play_started_at = None
    state.paused_remaining = None
    save_all_states()

    await VOICE.leave(chat_id, end_videochat=True)

    for pin_mid in [state.temp_pin_message_id, state.panel_message_id]:
        if pin_mid:
            try:
                await bot.unpin_chat_message(chat_id=chat_id, message_id=pin_mid)
            except Exception:
                pass

    await cleanup_track_controls(bot, chat_id)
    await cleanup_all_bot_messages(bot, chat_id)

    TRACK_REGISTRY.pop(chat_id, None)
    TRACK_CONTROL_REGISTRY.pop(chat_id, None)
    for key in list(PENDING_ACTIONS.keys()):
        if key.startswith(f"{chat_id}:"):
            PENDING_ACTIONS.pop(key, None)

    STATE_CACHE[chat_id] = ChatState(
        dj_mode=False,
        assigned_dj_id=None,
        assigned_dj_name="",
        panel_message_id=None,
        control_message_id=None,
        control_view="home",
        control_page=0,
        paused=False,
        now_playing=None,
        queue=[],
        history=[],
        library=library,
        saved_lists=saved_lists,
        temp_message_ids=[],
        bot_message_ids=[],
        panel_override_text="",
        panel_override_until=None,
        temp_pin_message_id=None,
        live_enabled=False,
        auto_track_enabled=auto_track_enabled,
        auto_sig_seconds=auto_sig_seconds,
        volume=volume,
        play_started_at=None,
        paused_remaining=None,
        validation_users=state.validation_users,
        admin_config=state.admin_config,
        member_activity=state.member_activity,
        muted_users=state.muted_users,
        entry_log=state.entry_log,
    )
    save_all_states()
    await cleanup_old_files(chat_id)


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user or not chat_is_allowed(update.effective_chat.id):
        return
    await query.answer()

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    state = get_state(chat_id)
    data = query.data or ""

    if await admin_callback_router(update, context):
        return

    if await validation_callback_router(update, context):
        return

    if data == "bot_ping":
        await query.answer("PONG ✅", show_alert=False)
        await send_temp_message(context.bot, chat_id, ping_text(), ttl=35)
        return

    if data == "menu_panel":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o un administrador pueden abrir el panel.", show_alert=True)
            return
        state.dj_mode = True
        if state.assigned_dj_id is None:
            state.assigned_dj_id = user_id
            state.assigned_dj_name = display_name(update.effective_user)
        save_all_states()
        try:
            await query.message.delete()
        except Exception:
            pass
        await cleanup_track_controls(context.bot, chat_id)
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await cleanup_bot_messages_keep_core(context.bot, chat_id)
        return

    if data == "menu_search_help":
        await send_temp_message(
            context.bot,
            chat_id,
            "<b>Búsqueda externa</b>\n\nPulsa copiar y pega abajo para buscar.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("📋 Copiar @VoiceShazamBot", copy_text=CopyTextButton(text="@VoiceShazamBot "))]]
            ),
            ttl=120,
        )
        return

    if data == "panel_search_help":
        await render_control_view(
            context.bot,
            chat_id,
            "<b>Búsqueda externa</b>\n\nPulsa copiar y pega abajo para buscar.",
            control_back_markup(
                [[InlineKeyboardButton("📋 Copiar @VoiceShazamBot", copy_text=CopyTextButton(text="@VoiceShazamBot "))]]
            ),
        )
        return

    if data == "panel_home":
        await render_control_home(context.bot, chat_id)
        return

    if data in ("panel_pin_edit", "panel_pin_temp"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o el ID registrado pueden fijar mensajes.", show_alert=True)
            return
        await render_control_view(
            context.bot,
            chat_id,
            "<b>Fijar mensaje temporal</b>\n\nElige cuánto tiempo quieres mantener el texto temporal.",
            control_back_markup([
                [
                    InlineKeyboardButton("1 min", callback_data="pin|t|1"),
                    InlineKeyboardButton("3 min", callback_data="pin|t|3"),
                    InlineKeyboardButton("10 min", callback_data="pin|t|10"),
                ]
            ]),
        )
        return

    if data == "panel_users":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o el ID registrado pueden ver los permisos.", show_alert=True)
            return
        body = await controller_users_text(context, chat_id)
        await render_control_view(
            context.bot,
            chat_id,
            body,
            control_back_markup(),
        )
        return

    if data in ("panel_join_live", "panel_voice_info"):
        url = await build_live_join_url(context.bot, chat_id)
        if url:
            await query.answer(url=url)
        else:
            await query.answer("Este grupo necesita username público o VOICE_CHAT_LINKS configurado para abrir el videochat actual.", show_alert=True)
        return

    if data == "panel_queue":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar el cuadro de mandos.", show_alert=True)
            return
        set_control_view(state, "queue", 0)
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return

    if data == "panel_library":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar el cuadro de mandos.", show_alert=True)
            return
        set_control_view(state, "library", 0)
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return

    if data == "panel_load_lists":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar el cuadro de mandos.", show_alert=True)
            return
        set_control_view(state, "saved_lists", 0)
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return

    if data == "panel_scan":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede rastrear canciones.", show_alert=True)
            return
        started = await start_background_scan(chat_id, limit=SCAN_LIMIT)
        body = (
            "<b>🧭 Rastreando canciones del grupo...</b>\n\n"
            "Puedes volver al panel mientras trabaja. Cuando termine, DJ-PLAN avisará en el chat."
            if started
            else "<b>🧭 Ya hay un rastreo en marcha.</b>\n\nPuedes volver al panel y seguir usando el bot."
        )
        await render_control_view(
            context.bot,
            chat_id,
            body,
            control_back_markup([[InlineKeyboardButton("📚 Ver biblioteca", callback_data="panel_library")]]),
        )
        return

    if data in ("panel_live_toggle", "panel_pause_resume"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o el ID registrado pueden controlar el directo.", show_alert=True)
            return
        try:
            live_result = await VOICE.toggle_live(chat_id)
            await cleanup_bot_messages_keep_core(context.bot, chat_id)
            if live_result:
                url = await build_live_join_url(context.bot, chat_id)
                if url:
                    await send_temp_message(
                        context.bot,
                        chat_id,
                        "<b>LIVE ON</b>\n\nPulsa para entrar al videochat actual.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎧 Unirse al directo", url=url)]]),
                        ttl=60,
                    )
            await query.answer("LIVE ON" if live_result else "LIVE OFF")
        except Exception:
            logger.exception("Fallo al ejecutar LIVE ON/OFF en chat %s", chat_id)
            await query.answer("Error al cambiar LIVE.", show_alert=True)
        return

    if data == "panel_next":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede controlar el directo.", show_alert=True)
            return
        if not state.queue:
            await query.answer("No hay canciones en cola.", show_alert=True)
            return
        ok = await simulate_panel_next(chat_id, reason="manual_button")
        if not ok:
            await query.answer("No se pudo reproducir la primera de la cola.", show_alert=True)
        return

    if data == "panel_auto_track":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cambiar AUTO-TRACK.", show_alert=True)
            return
        state.auto_track_enabled = not state.auto_track_enabled
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        if state.live_enabled and state.auto_track_enabled and state.queue and not state.now_playing:
            await simulate_panel_next(chat_id, reason="auto_track_toggled_on")
        return

    if data == "panel_shuffle":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede activar aleatorio.", show_alert=True)
            return
        state.dj_shuffle_enabled = not getattr(state, "dj_shuffle_enabled", False)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Aleatorio " + ("ON" if state.dj_shuffle_enabled else "OFF"))
        return

    if data in ("panel_auto_sig", "panel_auto_next", "panel_autoplay"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cambiar AUTO-SIG.", show_alert=True)
            return
        current_idx = AUTO_SIG_OPTIONS.index(state.auto_sig_seconds) if state.auto_sig_seconds in AUTO_SIG_OPTIONS else 0
        state.auto_sig_seconds = AUTO_SIG_OPTIONS[(current_idx + 1) % len(AUTO_SIG_OPTIONS)]
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return

    if data == "panel_save_list":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede guardar listas.", show_alert=True)
            return
        prompt = await context.bot.send_message(
            chat_id=chat_id,
            text="Escribe el nombre de la lista que quieres guardar:",
            reply_markup=ForceReply(selective=True),
        )
        await register_temp_message(chat_id, prompt.message_id)
        PENDING_ACTIONS[f"{chat_id}:{user_id}"] = {"kind": "save_list", "prompt_id": prompt.message_id}
        return

    if data == "panel_refresh":
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await cleanup_temp_messages(context.bot, chat_id)
        await cleanup_track_controls(context.bot, chat_id)
        await cleanup_bot_messages_keep_core(context.bot, chat_id)
        await cleanup_old_files(chat_id)
        return
    if data == "panel_clean":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o el ID registrado pueden limpiar.", show_alert=True)
            return
        await cleanup_track_controls(context.bot, chat_id)
        await cleanup_bot_messages_keep_core(context.bot, chat_id)
        await render_control_home(context.bot, chat_id)
        await ensure_panel(context.bot, chat_id)
        await query.answer("Mensajes del bot limpiados.")
        return


    if data.startswith("panel_vol_set|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cambiar volumen.", show_alert=True)
            return
        try:
            vol = int(data.split("|", 1)[1])
        except Exception:
            vol = 100
        new_vol = await VOICE.set_volume(chat_id, vol)
        await query.answer(f"Volumen {new_vol}")
        return

    if data == "panel_vol_up":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cambiar volumen.", show_alert=True)
            return
        await VOICE.change_volume(chat_id, 10)
        return

    if data == "panel_vol_down":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cambiar volumen.", show_alert=True)
            return
        await VOICE.change_volume(chat_id, -10)
        return

    if data == "panel_close":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o el ID registrado pueden cerrar.", show_alert=True)
            return
        await close_dj_session(context.bot, chat_id)
        return

    if data.startswith("pin|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado o el ID registrado pueden fijar mensajes.", show_alert=True)
            return
        _, action, value = data.split("|")
        if action == "t":
            minutes = max(1, int(value))
            prompt = await context.bot.send_message(
                chat_id=chat_id,
                text=f"Escribe el texto del mensaje temporal que se fijará arriba ({minutes} min):",
                reply_markup=ForceReply(selective=True),
            )
            await register_temp_message(chat_id, prompt.message_id)
            await register_bot_message(chat_id, prompt.message_id)
            PENDING_ACTIONS[f"{chat_id}:{user_id}"] = {"kind": "temp_pin", "prompt_id": prompt.message_id, "minutes": minutes}
        return

    if data.startswith("det|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar estas acciones.", show_alert=True)
            return
        _, action, source_message_id_str = data.split("|")
        source_message_id = int(source_message_id_str)
        track = get_detected_track(chat_id, source_message_id)
        if not track:
            await query.answer("No encuentro esa canción. Vuelve a responder con 'Dj plan'.", show_alert=True)
            return
        track.added_by_id = user_id
        track.added_by_name = display_name(update.effective_user)
        control_message_id = TRACK_CONTROL_REGISTRY.setdefault(chat_id, {}).pop(source_message_id, None)
        if action == "p":
            await play_selected_track(context, chat_id, track)
            await send_temp_message(context.bot, chat_id, f"▶️ Ahora suena: <b>{h(track.title)}</b>", ttl=20)
        elif action == "q":
            await queue_track(chat_id, track)
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, f"➕ Añadida a cola: <b>{h(track.title)}</b>", ttl=20)
        elif action == "f":
            await queue_track_first(chat_id, track)
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, f"⏭️ Añadida primera en cola: <b>{h(track.title)}</b>", ttl=20)
        elif action == "l":
            added = await add_to_library(chat_id, track)
            txt = f"📚 Guardada en biblioteca: <b>{h(track.title)}</b>" if added else "ℹ️ Esa canción ya estaba en la biblioteca."
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, txt, ttl=20)
        if control_message_id:
            await safe_delete(context.bot, chat_id, control_message_id)
        return

    if data.startswith("q|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede tocar la cola.", show_alert=True)
            return
        parts = data.split("|")
        action = parts[1]
        idx = int(parts[2]) if len(parts) > 2 else 0
        page = int(parts[3]) if len(parts) > 3 else 0
        if action == "noop":
            return
        if action == "r":
            await render_control_home(context.bot, chat_id)
            return
        if action == "pg":
            set_control_view(state, "queue", idx)
            save_all_states()
            await ensure_control_panel(context.bot, chat_id)
            return
        if action == "c":
            state.queue = []
        elif 0 <= idx < len(state.queue):
            if action == "u" and idx > 0:
                state.queue[idx - 1], state.queue[idx] = state.queue[idx], state.queue[idx - 1]
            elif action == "d" and idx < len(state.queue) - 1:
                state.queue[idx + 1], state.queue[idx] = state.queue[idx], state.queue[idx + 1]
            elif action == "x":
                state.queue.pop(idx)
            elif action == "p":
                chosen = Track(**state.queue.pop(idx))
                save_all_states()
                await play_selected_track(context, chat_id, chosen)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        page = clamp_page(page, len(state.queue))
        set_control_view(state, "queue", page)
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return

    if data.startswith("lib|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede tocar la biblioteca.", show_alert=True)
            return
        parts = data.split("|")
        action = parts[1]
        idx = int(parts[2]) if len(parts) > 2 else 0
        page = int(parts[3]) if len(parts) > 3 else 0
        if action == "noop":
            return
        if action == "r":
            await render_control_home(context.bot, chat_id)
            return
        if action == "pg":
            set_control_view(state, "library", idx)
            save_all_states()
            await ensure_control_panel(context.bot, chat_id)
            return
        if action == "qa":
            for item in state.library:
                await queue_track(chat_id, Track(**item))
            await ensure_panel(context.bot, chat_id)
            set_control_view(state, "library", page)
            save_all_states()
            await ensure_control_panel(context.bot, chat_id)
            return
        if action == "pa":
            if not state.library:
                return
            first = Track(**state.library[0])
            for item in state.library[1:]:
                await queue_track(chat_id, Track(**item))
            await play_selected_track(context, chat_id, first)
            await ensure_panel(context.bot, chat_id)
            set_control_view(state, "library", page)
            save_all_states()
            await ensure_control_panel(context.bot, chat_id)
            return
        if not (0 <= idx < len(state.library)):
            return
        chosen = Track(**state.library[idx])
        if action == "p":
            await play_selected_track(context, chat_id, chosen)
        elif action == "q":
            await queue_track(chat_id, chosen)
        elif action == "f":
            await queue_track_first(chat_id, chosen)
        elif action == "x":
            state.library.pop(idx)
            save_all_states()
        await ensure_panel(context.bot, chat_id)
        page = clamp_page(page, len(state.library))
        set_control_view(state, "library", page)
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return

    if data.startswith("lst|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cargar listas.", show_alert=True)
            return
        _, action, idx_str = data.split("|")
        idx = int(idx_str)
        names = sorted(state.saved_lists.keys())
        if action == "r":
            await render_control_home(context.bot, chat_id)
            return
        if not (0 <= idx < len(names)):
            return
        name = names[idx]
        items = [dict(item) for item in state.saved_lists.get(name, [])]
        if action == "a":
            for item in items:
                state.queue.append(dict(item))
            save_all_states()
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, f"➕ Lista añadida a cola: <b>{h(name)}</b>", ttl=20)
        elif action == "p":
            if items:
                first = Track(**items[0])
                for item in items[1:]:
                    state.queue.append(dict(item))
                save_all_states()
                await play_selected_track(context, chat_id, first)
                await ensure_panel(context.bot, chat_id)
                await ensure_control_panel(context.bot, chat_id)
                await send_temp_message(context.bot, chat_id, f"▶️ Reproduciendo lista: <b>{h(name)}</b>", ttl=20)
        elif action == "x":
            state.saved_lists.pop(name, None)
            save_all_states()
        set_control_view(state, "saved_lists", 0)
        save_all_states()
        await ensure_control_panel(context.bot, chat_id)
        return



# =========================
# DJ PRIVATE CONFIG MODULE
# Configuración DJ por privado sin ensuciar el grupo.
# =========================
DJ_PRIVATE_GROUPS_PATH = Path(os.getenv("DJ_PRIVATE_GROUPS_PATH", "/data/dj_private_groups.json"))
DJ_PRIVATE_GROUPS: Dict[str, Any] = {}


def dj_load_private_groups() -> None:
    global DJ_PRIVATE_GROUPS
    try:
        if DJ_PRIVATE_GROUPS_PATH.exists():
            raw = json.loads(DJ_PRIVATE_GROUPS_PATH.read_text(encoding="utf-8"))
            DJ_PRIVATE_GROUPS = raw if isinstance(raw, dict) else {}
        else:
            DJ_PRIVATE_GROUPS = {}
    except Exception:
        logger.exception("No se pudo cargar DJ_PRIVATE_GROUPS")
        DJ_PRIVATE_GROUPS = {}


def dj_save_private_groups() -> None:
    try:
        DJ_PRIVATE_GROUPS_PATH.parent.mkdir(parents=True, exist_ok=True)
        DJ_PRIVATE_GROUPS_PATH.write_text(json.dumps(DJ_PRIVATE_GROUPS, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logger.exception("No se pudo guardar DJ_PRIVATE_GROUPS")


def dj_link_private_group(user_id: int, chat_id: int, title: str) -> None:
    key = str(int(user_id))
    rec = DJ_PRIVATE_GROUPS.setdefault(key, {"selected": None, "groups": {}})
    groups = rec.setdefault("groups", {})
    groups[str(int(chat_id))] = title or str(chat_id)
    rec["selected"] = int(chat_id)
    dj_save_private_groups()


def dj_private_groups_for(user_id: int) -> Dict[int, str]:
    rec = DJ_PRIVATE_GROUPS.get(str(int(user_id)), {})
    groups = rec.get("groups", {}) if isinstance(rec, dict) else {}
    out: Dict[int, str] = {}
    for k, v in groups.items():
        try:
            out[int(k)] = str(v)
        except Exception:
            pass
    return out


def dj_selected_private_group(user_id: int) -> Optional[int]:
    rec = DJ_PRIVATE_GROUPS.get(str(int(user_id)), {})
    if not isinstance(rec, dict):
        return None
    selected = rec.get("selected")
    try:
        if selected is not None:
            return int(selected)
    except Exception:
        return None
    groups = dj_private_groups_for(user_id)
    return next(iter(groups.keys()), None) if groups else None


def dj_select_private_group(user_id: int, chat_id: int) -> bool:
    groups = dj_private_groups_for(user_id)
    if int(chat_id) not in groups:
        return False
    rec = DJ_PRIVATE_GROUPS.setdefault(str(int(user_id)), {"selected": None, "groups": {}})
    rec["selected"] = int(chat_id)
    dj_save_private_groups()
    return True


def dj_target_chat_id(update: Update) -> Optional[int]:
    if not update.effective_chat or not update.effective_user:
        return None
    if update.effective_chat.type == "private":
        return dj_selected_private_group(update.effective_user.id)
    return int(update.effective_chat.id)


def dj_group_selector_markup(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    selected = dj_selected_private_group(user_id)
    for chat_id, title in dj_private_groups_for(user_id).items():
        mark = "✅ " if selected == chat_id else ""
        rows.append([InlineKeyboardButton(mark + truncated_button_title(title, 40), callback_data=f"djgroup|select|{chat_id}")])
    rows.append([InlineKeyboardButton("❌ Cerrar", callback_data="djgroup|close|0")])
    return InlineKeyboardMarkup(rows)



async def dj_pin_music_prompt(context, chat_id: int) -> None:
    cfg = hot_cfg(chat_id)
    text = str(cfg.get("dj_music_pin_text") or "🎧 <b>Música en directo</b>\n\nPulsa el botón para unirte al directo musical del grupo.")
    button_text = str(cfg.get("dj_music_pin_button_text") or "🎧 Escuchar música").strip()[:64] or "🎧 Escuchar música"
    url = await build_live_join_url(context.bot, chat_id)
    if url:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=url)]])
    else:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, callback_data=f"djlisten|join|{chat_id}")]])
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup)
    await register_bot_message(chat_id, msg.message_id)
    try:
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        logger.exception("No se pudo fijar mensaje de música en chat %s", chat_id)


async def dj_listen_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    if not data.startswith("djlisten|"):
        return
    try:
        chat_id = int(data.split("|")[2])
    except Exception:
        await query.answer("Grupo no válido", show_alert=True)
        return
    url = await build_live_join_url(context.bot, chat_id)
    if url:
        await query.answer("Abre el enlace del directo.", url=url)
    else:
        await query.answer("No tengo enlace del directo. Configura VOICE_CHAT_LINKS o un username público del grupo.", show_alert=True)

def dj_private_text(chat_id: int) -> str:
    state = get_state(chat_id)
    current = Track(**state.now_playing).title if state.now_playing else "Nada"
    nxt = Track(**state.queue[0]).title if state.queue else "Nada"
    return (
        "🎛️ <b>DJ-PLAN · Config privado</b>\n\n"
        f"Grupo: <code>{chat_id}</code>\n"
        f"▶️ Actual: <b>{h(shorten_title(current, 40))}</b>\n"
        f"⏭️ Siguiente: <b>{h(shorten_title(nxt, 40))}</b>\n"
        f"📋 Cola: <b>{len(state.queue)}</b>\n"
        f"📚 Biblioteca: <b>{len(state.library)}</b>\n"
        f"🔀 Aleatorio: <b>{'ON' if getattr(state, 'dj_shuffle_enabled', False) else 'OFF'}</b>\n\n"
        "Puedes mandar aquí una canción/audio/documento de audio y te daré botones para meterla en cola sin molestar al grupo."
    )


def dj_private_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔀 Aleatorio " + ("ON" if getattr(state, "dj_shuffle_enabled", False) else "OFF"), callback_data=f"djpriv|shuffle|{chat_id}")],
        [InlineKeyboardButton("📋 Ver cola", callback_data=f"djpriv|queue|{chat_id}"), InlineKeyboardButton("🧹 Vaciar cola", callback_data=f"djpriv|clear|{chat_id}")],
        [InlineKeyboardButton("🌐 Cambiar grupo", callback_data="djpriv|groups|0")],
        [InlineKeyboardButton("💾 Guardar y cerrar", callback_data=f"djpriv|close|{chat_id}")],
    ])


async def djgrupo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type == "private":
        await update.message.reply_html("Usa <code>/djgrupo</code> dentro del grupo que quieres configurar por privado.")
        return
    chat_id = int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    dj_link_private_group(update.effective_user.id, chat_id, update.effective_chat.title or str(chat_id))
    msg = await update.message.reply_html("✅ Grupo vinculado para configurar DJ-PLAN por privado. Abre el bot en privado y usa <code>/djconfig</code>.")
    await register_bot_message(chat_id, msg.message_id)
    asyncio.create_task(delete_later(context.bot, chat_id, msg.message_id, 20))


async def djconfig_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type == "private":
        groups = dj_private_groups_for(update.effective_user.id)
        if not groups:
            await update.message.reply_html("Primero entra en el grupo y ejecuta <code>/djgrupo</code>. Luego vuelve aquí y usa <code>/djconfig</code>.")
            return
        if len(groups) > 1:
            await update.message.reply_html("🎛️ <b>Elige el grupo DJ que quieres configurar</b>", reply_markup=dj_group_selector_markup(update.effective_user.id))
            return
    chat_id = dj_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html("Primero vincula un grupo con <code>/djgrupo</code>.")
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores del grupo vinculado.")
        return
    await update.message.reply_html(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id))


async def cola_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = dj_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html("Primero vincula un grupo con <code>/djgrupo</code>.")
        return
    if not (await is_admin(context, chat_id, update.effective_user.id) or await is_controller(context, chat_id, update.effective_user.id)):
        await update.message.reply_text("Solo admin o DJ asignado.")
        return
    source = update.message.reply_to_message
    if not source:
        await update.message.reply_html("Responde a una canción/audio/documento de audio con <code>/cola</code> para ponerla la siguiente.")
        return
    track = extract_track_from_message(source)
    if not track:
        await update.message.reply_text("No detecto una canción en el mensaje respondido.")
        return
    track.added_by_id = update.effective_user.id
    track.added_by_name = display_name(update.effective_user)
    await queue_track_first(chat_id, track)
    await ensure_panel(context.bot, chat_id)
    await ensure_control_panel(context.bot, chat_id)
    await update.message.reply_html(f"⏭️ Añadida como siguiente canción: <b>{h(track.title)}</b>")


async def djgroup_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    parts = (query.data or "").split("|")
    action = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else "0"
    if action == "close":
        await query.answer("Cerrado")
        try:
            await query.message.delete()
        except Exception:
            pass
        return
    if action != "select":
        await query.answer("Acción no válida", show_alert=True)
        return
    try:
        chat_id = int(value)
    except Exception:
        await query.answer("Grupo no válido", show_alert=True)
        return
    if not dj_select_private_group(update.effective_user.id, chat_id):
        await query.answer("Grupo no vinculado. Usa /djgrupo en el grupo.", show_alert=True)
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Ya no eres admin de ese grupo.", show_alert=True)
        return
    await query.answer("Grupo seleccionado ✅")
    await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)


async def djfijar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    await dj_pin_music_prompt(context, chat_id)
    try:
        await safe_delete(context.bot, chat_id, update.message.message_id)
    except Exception:
        pass


async def djprivate_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    parts = (query.data or "").split("|")
    action = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else "0"
    if action == "groups":
        await query.answer("Elige grupo")
        await query.edit_message_text("🎛️ <b>Elige el grupo DJ que quieres configurar</b>", reply_markup=dj_group_selector_markup(update.effective_user.id), parse_mode=ParseMode.HTML)
        return
    if action == "close":
        save_all_states()
        await query.answer("Guardado ✅")
        try:
            await query.message.delete()
        except Exception:
            pass
        return
    try:
        chat_id = int(value)
    except Exception:
        await query.answer("Grupo no válido", show_alert=True)
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores del grupo vinculado.", show_alert=True)
        return
    state = get_state(chat_id)
    if action == "shuffle":
        state.dj_shuffle_enabled = not getattr(state, "dj_shuffle_enabled", False)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Aleatorio " + ("ON" if state.dj_shuffle_enabled else "OFF"))
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "queue":
        lines = ["📋 <b>Cola actual</b>", ""]
        if not state.queue:
            lines.append("Vacía.")
        else:
            for i, item in enumerate(state.queue[:20], 1):
                lines.append(f"{i}. {h(shorten_title(Track(**item).title, 45))}")
        await query.answer("Cola")
        await query.edit_message_text("\n".join(lines), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "clear":
        state.queue = []
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Cola vaciada")
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    await query.answer("Acción no válida", show_alert=True)


async def djprivate_track_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    parts = (query.data or "").split("|")
    if len(parts) < 4:
        return
    action = parts[1]
    try:
        target_chat_id = int(parts[2])
        source_message_id = int(parts[3])
    except Exception:
        await query.answer("Datos inválidos", show_alert=True)
        return
    if not (await is_admin(context, target_chat_id, update.effective_user.id) or await is_controller(context, target_chat_id, update.effective_user.id)):
        await query.answer("Solo admin o DJ asignado.", show_alert=True)
        return
    private_chat_id = int(query.message.chat_id)
    track = get_detected_track(private_chat_id, source_message_id)
    if not track:
        await query.answer("No encuentro esa canción.", show_alert=True)
        return
    track.added_by_id = update.effective_user.id
    track.added_by_name = display_name(update.effective_user)
    if action == "first":
        await queue_track_first(target_chat_id, track)
        txt = f"⏭️ Añadida primera en cola: <b>{h(track.title)}</b>"
    elif action == "queue":
        await queue_track(target_chat_id, track)
        txt = f"➕ Añadida a cola: <b>{h(track.title)}</b>"
    elif action == "lib":
        added = await add_to_library(target_chat_id, track)
        txt = f"📚 Guardada en biblioteca: <b>{h(track.title)}</b>" if added else "ℹ️ Ya estaba en biblioteca."
    else:
        await query.answer("Acción no válida", show_alert=True)
        return
    await ensure_panel(context.bot, target_chat_id)
    await ensure_control_panel(context.bot, target_chat_id)
    await query.answer("Hecho ✅")
    await query.message.reply_html(txt)


async def dj_private_music_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return
    target_chat_id = dj_selected_private_group(update.effective_user.id)
    if target_chat_id is None:
        return
    if not (await is_admin(context, target_chat_id, update.effective_user.id) or await is_controller(context, target_chat_id, update.effective_user.id)):
        return
    track = extract_track_from_message(update.message)
    if not track:
        return
    register_detected_track(update.effective_chat.id, update.message.message_id, track)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭️ Primera cola", callback_data=f"djtrack|first|{target_chat_id}|{update.message.message_id}")],
        [InlineKeyboardButton("➕ Cola final", callback_data=f"djtrack|queue|{target_chat_id}|{update.message.message_id}"), InlineKeyboardButton("📚 Biblioteca", callback_data=f"djtrack|lib|{target_chat_id}|{update.message.message_id}")],
    ])
    await update.message.reply_html(
        f"🎛️ <b>DJ-PLAN privado</b>\nDetectada: <b>{h(track.title)}</b>\nGrupo destino: <code>{target_chat_id}</code>",
        reply_markup=keyboard,
    )


async def djmesa_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Recrea la mesa/cuadro de mandos DJ sin cortar música ni tocar cola/configuración."""
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    if not (await is_admin(context, chat_id, update.effective_user.id) or await is_controller(context, chat_id, update.effective_user.id)):
        await update.message.reply_text("Solo admin o DJ asignado.")
        return
    state = get_state(chat_id)
    if not state.dj_mode:
        state.dj_mode = True
    old_id = state.control_message_id
    if old_id:
        try:
            await safe_delete(context.bot, chat_id, old_id)
        except Exception:
            pass
    state.control_message_id = None
    set_control_view(state, "home", 0)
    save_all_states()
    await ensure_panel(context.bot, chat_id)
    await ensure_control_panel(context.bot, chat_id)
    try:
        await safe_delete(context.bot, chat_id, update.message.message_id)
    except Exception:
        pass



# =========================
# DJ PRIVATE CONFIG V6 - panel privado funcional con cola, biblioteca y listas
# =========================
def dj_private_text(chat_id: int) -> str:
    state = get_state(chat_id)
    return (
        "🎛️ <b>DJ-PLAN privado</b>\n\n"
        f"Grupo: <code>{chat_id}</code>\n"
        f"Modo DJ: <b>{'ON' if state.dj_mode else 'OFF'}</b>\n"
        f"Actual: <b>{h(shorten_title(Track(**state.now_playing).title, 45)) if state.now_playing else 'Nada sonando'}</b>\n"
        f"Cola: <b>{len(state.queue)}</b> · Biblioteca: <b>{len(state.library)}</b> · Listas: <b>{len(state.saved_lists)}</b>\n"
        f"Aleatorio: <b>{'ON' if getattr(state, 'dj_shuffle_enabled', False) else 'OFF'}</b>\n\n"
        "Manda aquí un audio/canción/documento de audio y te saldrán botones para ponerlo primero, al final o guardarlo en biblioteca sin molestar al grupo."
    )


def dj_private_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔀 Aleatorio " + ("ON" if getattr(state, "dj_shuffle_enabled", False) else "OFF"), callback_data=f"djpriv|shuffle|{chat_id}")],
        [InlineKeyboardButton("📋 Cola", callback_data=f"djpriv|queue|{chat_id}"), InlineKeyboardButton("📚 Biblioteca", callback_data=f"djpriv|library|{chat_id}")],
        [InlineKeyboardButton("💾 Guardar cola", callback_data=f"djpriv|savequeue|{chat_id}"), InlineKeyboardButton("📂 Listas", callback_data=f"djpriv|lists|{chat_id}")],
        [InlineKeyboardButton("📌 Fijar escuchar música", callback_data=f"djpriv|pinmusic|{chat_id}"), InlineKeyboardButton("✏️ Texto música", callback_data=f"djpriv|pintext|{chat_id}")],
        [InlineKeyboardButton("🔘 Botón música", callback_data=f"djpriv|pinbutton|{chat_id}"), InlineKeyboardButton("🔄 Refrescar mesa", callback_data=f"djpriv|mesa|{chat_id}")],
        [InlineKeyboardButton("🧹 Vaciar cola", callback_data=f"djpriv|clear|{chat_id}")],
        [InlineKeyboardButton("🌐 Cambiar grupo", callback_data="djpriv|groups|0")],
        [InlineKeyboardButton("💾 Guardar y cerrar", callback_data=f"djpriv|close|{chat_id}")],
    ])


def dj_private_back_markup(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver DJ privado", callback_data=f"djpriv|home|{chat_id}")]])


def dj_private_queue_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    for i, item in enumerate(state.queue[:20]):
        title = shorten_title(Track(**item).title, 35)
        rows.append([InlineKeyboardButton(f"{i+1}. {title}", callback_data=f"djpriv|noop|{chat_id}")])
        rows.append([
            InlineKeyboardButton("▶️", callback_data=f"djpriv|qplay|{chat_id}|{i}"),
            InlineKeyboardButton("⏭️ Primera", callback_data=f"djpriv|qfirst|{chat_id}|{i}"),
            InlineKeyboardButton("🗑️", callback_data=f"djpriv|qdel|{chat_id}|{i}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Volver DJ privado", callback_data=f"djpriv|home|{chat_id}")])
    return InlineKeyboardMarkup(rows)


def dj_private_library_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    for i, item in enumerate(state.library[:20]):
        title = shorten_title(Track(**item).title, 35)
        rows.append([InlineKeyboardButton(f"{i+1}. {title}", callback_data=f"djpriv|noop|{chat_id}")])
        rows.append([
            InlineKeyboardButton("➕ Cola", callback_data=f"djpriv|libq|{chat_id}|{i}"),
            InlineKeyboardButton("⏭️ Primera", callback_data=f"djpriv|libfirst|{chat_id}|{i}"),
            InlineKeyboardButton("🗑️", callback_data=f"djpriv|libdel|{chat_id}|{i}"),
        ])
    rows.append([InlineKeyboardButton("➕ Toda a cola", callback_data=f"djpriv|liball|{chat_id}")])
    rows.append([InlineKeyboardButton("🔙 Volver DJ privado", callback_data=f"djpriv|home|{chat_id}")])
    return InlineKeyboardMarkup(rows)


def dj_private_lists_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    names = sorted(state.saved_lists.keys())[:30]
    for i, name in enumerate(names):
        rows.append([
            InlineKeyboardButton(f"📂 {truncated_button_title(name, 28)}", callback_data=f"djpriv|loadlist|{chat_id}|{i}"),
            InlineKeyboardButton("🗑️", callback_data=f"djpriv|dellist|{chat_id}|{i}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Volver DJ privado", callback_data=f"djpriv|home|{chat_id}")])
    return InlineKeyboardMarkup(rows)


async def djprivate_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    parts = (query.data or "").split("|")
    action = parts[1] if len(parts) > 1 else ""
    if action == "groups":
        await query.answer("Elige grupo")
        await query.edit_message_text("🎛️ <b>Elige el grupo DJ que quieres configurar</b>", reply_markup=dj_group_selector_markup(update.effective_user.id), parse_mode=ParseMode.HTML)
        return
    if action == "close":
        save_all_states()
        await query.answer("Guardado ✅")
        try:
            await query.message.delete()
        except Exception:
            pass
        return
    try:
        chat_id = int(parts[2]) if len(parts) > 2 else int(dj_selected_private_group(update.effective_user.id) or 0)
    except Exception:
        await query.answer("Grupo no válido", show_alert=True)
        return
    if not chat_id:
        await query.answer("Primero vincula un grupo con /djgrupo.", show_alert=True)
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores del grupo vinculado.", show_alert=True)
        return
    state = get_state(chat_id)
    try:
        idx = int(parts[3]) if len(parts) > 3 else -1
    except Exception:
        idx = -1

    if action in ("home", "noop"):
        await query.answer("DJ privado")
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "shuffle":
        state.dj_shuffle_enabled = not getattr(state, "dj_shuffle_enabled", False)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Aleatorio " + ("ON" if state.dj_shuffle_enabled else "OFF"))
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "mesa":
        old_id = state.control_message_id
        if old_id:
            await safe_delete(context.bot, chat_id, old_id)
        state.control_message_id = None
        set_control_view(state, "home", 0)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Mesa refrescada en el grupo ✅")
        return
    if action == "queue":
        lines = ["📋 <b>Cola actual</b>", ""]
        if not state.queue:
            lines.append("Vacía.")
        else:
            for i, item in enumerate(state.queue[:20], 1):
                lines.append(f"{i}. {h(shorten_title(Track(**item).title, 45))}")
        await query.answer("Cola")
        await query.edit_message_text("\n".join(lines), reply_markup=dj_private_queue_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "library":
        lines = ["📚 <b>Biblioteca</b>", ""]
        if not state.library:
            lines.append("Vacía. Manda música por privado para guardarla.")
        else:
            for i, item in enumerate(state.library[:20], 1):
                lines.append(f"{i}. {h(shorten_title(Track(**item).title, 45))}")
        await query.answer("Biblioteca")
        await query.edit_message_text("\n".join(lines), reply_markup=dj_private_library_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "lists":
        lines = ["📂 <b>Listas guardadas</b>", ""]
        names = sorted(state.saved_lists.keys())[:30]
        if not names:
            lines.append("No hay listas guardadas.")
        else:
            for i, name in enumerate(names, 1):
                lines.append(f"{i}. {h(name)} ({len(state.saved_lists.get(name, []))})")
        await query.answer("Listas")
        await query.edit_message_text("\n".join(lines), reply_markup=dj_private_lists_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "clear":
        state.queue = []
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Cola vaciada")
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "pinmusic":
        await dj_pin_music_prompt(context, chat_id)
        await query.answer("Mensaje fijado en el grupo ✅")
        return
    if action == "pintext":
        set_config_pending(update.effective_user.id, {"kind": "dj_music_pin_text", "chat_id": chat_id})
        await query.message.reply_html("✏️ Envíame el texto del mensaje fijado de música. Puedes usar HTML y emojis de Telegram si llegan como entidad.")
        await query.answer("Envíame el texto")
        return
    if action == "pinbutton":
        set_config_pending(update.effective_user.id, {"kind": "dj_music_pin_button", "chat_id": chat_id})
        await query.message.reply_html("🔘 Envíame el texto del botón de música. Ejemplo: <code>🎧 Escuchar música</code>")
        await query.answer("Envíame el botón")
        return
    if action == "savequeue":
        if not state.queue:
            await query.answer("La cola está vacía.", show_alert=True)
            return
        set_config_pending(update.effective_user.id, {"kind": "djpriv_savequeue_name", "chat_id": chat_id})
        await query.message.reply_html("💾 Envíame el nombre de la lista que quieres guardar.\n\nEjemplo: <code>Plan viernes</code>")
        await query.answer("Envíame el nombre por privado")
        return
    if action in ("qplay", "qfirst", "qdel") and 0 <= idx < len(state.queue):
        item = state.queue.pop(idx)
        if action == "qplay":
            await play_selected_track(context, chat_id, Track(**item))
        elif action == "qfirst":
            state.queue.insert(0, item)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Hecho ✅")
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action in ("libq", "libfirst", "libdel") and 0 <= idx < len(state.library):
        item = state.library[idx]
        if action == "libq":
            await queue_track(chat_id, Track(**item))
        elif action == "libfirst":
            await queue_track_first(chat_id, Track(**item))
        elif action == "libdel":
            state.library.pop(idx)
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Hecho ✅")
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action == "liball":
        for item in state.library:
            await queue_track(chat_id, Track(**item))
        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
        await query.answer("Biblioteca añadida a cola ✅")
        await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
        return
    if action in ("loadlist", "dellist"):
        names = sorted(state.saved_lists.keys())[:30]
        if 0 <= idx < len(names):
            name = names[idx]
            if action == "loadlist":
                state.queue.extend(list(state.saved_lists.get(name, [])))
                await query.answer("Lista cargada en cola ✅")
            else:
                state.saved_lists.pop(name, None)
                await query.answer("Lista borrada ✅")
            save_all_states()
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await query.edit_message_text(dj_private_text(chat_id), reply_markup=dj_private_markup(chat_id), parse_mode=ParseMode.HTML)
            return
    await query.answer("Acción no válida", show_alert=True)

# =========================
# FIN DJ PRIVATE CONFIG V6
# =========================

# =========================
# FIN DJ PRIVATE CONFIG MODULE
# =========================


# =========================
# HOT MODULE - Preguntita
# Módulo aislado: no toca DJ-PLAN ni seguridad.
# =========================
import time

HOT_AUTO_TASK: Optional[asyncio.Task] = None
HOT_ACTIVE_QUESTIONS: Dict[int, Dict[int, Dict[str, Any]]] = {}
HOT_RECENT_ACTIVITY: Dict[int, Dict[int, Dict[str, Any]]] = {}
HOT_CHAT_ACTIVITY_LOG: Dict[int, List[Dict[str, int]]] = {}
HOT_PENDING_ADD: Dict[int, Dict[str, Any]] = {}
HOT_PRIVATE_GROUPS_PATH = Path(os.getenv("HOT_PRIVATE_GROUPS_PATH", "/data/hot_private_groups.json"))
# Formato nuevo:
# {"user_id": {"selected": chat_id, "groups": {"chat_id": "Título"}}}
# Mantiene compatibilidad con el formato antiguo {"user_id": chat_id}.
HOT_PRIVATE_GROUPS: Dict[str, Any] = {}


def hot_load_private_groups() -> None:
    global HOT_PRIVATE_GROUPS
    try:
        if HOT_PRIVATE_GROUPS_PATH.exists():
            raw = json.loads(HOT_PRIVATE_GROUPS_PATH.read_text(encoding="utf-8"))
            fixed: Dict[str, Any] = {}
            if isinstance(raw, dict):
                for user_id, value in raw.items():
                    if isinstance(value, dict):
                        selected = value.get("selected")
                        groups = value.get("groups", {})
                        fixed[str(user_id)] = {
                            "selected": int(selected) if selected is not None else None,
                            "groups": {str(k): str(v) for k, v in groups.items()} if isinstance(groups, dict) else {},
                        }
                    else:
                        # Compatibilidad con la versión anterior: un único grupo vinculado.
                        chat_id = int(value)
                        fixed[str(user_id)] = {"selected": chat_id, "groups": {str(chat_id): str(chat_id)}}
            HOT_PRIVATE_GROUPS = fixed
    except Exception:
        logger.exception("No se pudo cargar HOT_PRIVATE_GROUPS")
        HOT_PRIVATE_GROUPS = {}


def hot_save_private_groups() -> None:
    try:
        HOT_PRIVATE_GROUPS_PATH.parent.mkdir(parents=True, exist_ok=True)
        HOT_PRIVATE_GROUPS_PATH.write_text(json.dumps(HOT_PRIVATE_GROUPS, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logger.exception("No se pudo guardar HOT_PRIVATE_GROUPS")


def hot_user_group_record(user_id: int) -> Dict[str, Any]:
    key = str(int(user_id))
    rec = HOT_PRIVATE_GROUPS.get(key)
    if not isinstance(rec, dict):
        rec = {"selected": None, "groups": {}}
        HOT_PRIVATE_GROUPS[key] = rec
    if not isinstance(rec.get("groups"), dict):
        rec["groups"] = {}
    return rec


def hot_link_private_group(user_id: int, chat_id: int, title: str = "") -> None:
    rec = hot_user_group_record(user_id)
    cid = str(int(chat_id))
    rec["groups"][cid] = title or cid
    rec["selected"] = int(chat_id)
    hot_save_private_groups()


def hot_private_group_for(user_id: int) -> Optional[int]:
    try:
        rec = hot_user_group_record(user_id)
        selected = rec.get("selected")
        return int(selected) if selected is not None else None
    except Exception:
        return None


def hot_private_groups_for(user_id: int) -> List[Tuple[int, str]]:
    try:
        rec = hot_user_group_record(user_id)
        groups = rec.get("groups", {})
        out: List[Tuple[int, str]] = []
        for chat_id, title in groups.items():
            try:
                out.append((int(chat_id), str(title)))
            except Exception:
                continue
        return out
    except Exception:
        return []


def hot_select_private_group(user_id: int, chat_id: int) -> bool:
    rec = hot_user_group_record(user_id)
    cid = str(int(chat_id))
    if cid not in rec.get("groups", {}):
        return False
    rec["selected"] = int(chat_id)
    hot_save_private_groups()
    return True


def hot_group_selector_markup(user_id: int) -> InlineKeyboardMarkup:
    groups = hot_private_groups_for(user_id)
    selected = hot_private_group_for(user_id)
    rows = []
    for chat_id, title in groups:
        label = f"{'✅ ' if selected == chat_id else ''}{title}"
        if len(label) > 45:
            label = label[:42] + "…"
        rows.append([InlineKeyboardButton(label, callback_data=f"hotgroup|select|{chat_id}")])
    rows.append([InlineKeyboardButton("❌ Cerrar", callback_data="hotgroup|close|0")])
    return InlineKeyboardMarkup(rows)

def hot_is_private(update: Update) -> bool:
    try:
        return bool(update.effective_chat and update.effective_chat.type == "private")
    except Exception:
        return False


def hot_target_chat_id(update: Update) -> Optional[int]:
    if not update.effective_chat or not update.effective_user:
        return None
    if hot_is_private(update):
        return hot_private_group_for(update.effective_user.id)
    return int(update.effective_chat.id)


async def hot_delete_command_if_configured(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    if not update.message:
        return
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_command_delete_mode", "off"))
    if mode == "off":
        return
    if mode == "instant":
        await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)
        return
    ttl = int(cfg.get("hot_command_delete_seconds", 20) or 20)
    asyncio.create_task(delete_later(context.bot, update.effective_chat.id, update.message.message_id, max(1, ttl)))


def hot_command_delete_label(chat_id: int) -> str:
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_command_delete_mode", "off"))
    ttl = int(cfg.get("hot_command_delete_seconds", 20) or 20)
    if mode == "instant":
        return "al ejecutar"
    if mode == "ttl":
        return f"tras {ttl}s"
    return "OFF"


def hot_answer_quality_bonus(text: str) -> int:
    clean = " ".join((text or "").split())
    words = [w for w in clean.split(" ") if w]
    # Bonus simple y seguro: premia respuestas desarrolladas, no solo 'sí/no'.
    return 1 if len(clean) >= 30 or len(words) >= 6 else 0

HOT_BASE_QUESTIONS: Dict[int, List[str]] = {
    1: [
        "¿Qué es lo primero que te atrae de alguien?",
        "¿Prefieres dar el primer paso o que lo den contigo?",
        "¿Qué tipo de mirada te gana?",
        "¿Te gusta más que te conquisten con humor o con misterio?",
        "¿Qué gesto te parece más sexy sin ser obvio?",
        "¿Te gusta tontear por mensajes o en persona?",
        "¿Qué te hace pensar 'aquí hay química'?",
        "¿Te gusta que sean directos o que haya juego?",
        "¿Qué detalle te hace fijarte en alguien?",
        "¿Te gusta provocar un poco o eres más reservado/a?",
        "¿Qué plan te parece perfecto para conocer a alguien?",
        "¿Qué voz te atrae más: dulce, seria o pícara?",
        "¿Te gustan las indirectas o prefieres claridad total?",
        "¿Qué te hace perder el interés rápido?",
        "¿Qué cumplido te gustaría recibir?",
        "¿Te gusta el contacto visual largo?",
        "¿Prefieres una conversación profunda o una con tensión?",
        "¿Qué te parece más atractivo: seguridad o ternura?",
        "¿Te gusta que te sorprendan?",
        "¿Qué emoji usarías para ligar?",
        "¿Has tenido un crush inesperado?",
        "¿Qué haría alguien para llamar tu atención hoy?",
        "¿Te gusta que haya misterio al principio?",
        "¿Qué te parece más sexy: elegancia o descaro?",
        "¿Prefieres ir lento o dejarte llevar?",
        "¿Qué canción te pone en modo coqueto/a?",
        "¿Te gusta que te busquen o buscar tú?",
        "¿Qué frase te conquistaría?",
        "¿Qué es lo más bonito que te han dicho?",
        "¿Te gusta más besar o abrazar?",
        "¿Qué lugar te parece ideal para una primera cita?",
        "¿Qué te pone nervioso/a de forma buena?",
        "¿Te gusta jugar con la tensión?",
        "¿Qué cualidad te parece irresistible?",
        "¿Te gusta que te hagan reír antes de ligar?",
        "¿Qué mirada delata que alguien te gusta?",
        "¿Te gusta más lo romántico o lo espontáneo?",
        "¿Qué detalle pequeño te derrite?",
        "¿A quién del grupo ves más misterioso/a?",
        "¿Quién del grupo crees que liga mejor?",
    ],
    2: [
        "¿A quién del grupo invitarías a una copa?",
        "¿Has tenido química con alguien nada más verlo?",
        "¿Cuál ha sido tu mejor beso?",
        "¿Prefieres besar suave o con intensidad?",
        "¿Has mandado alguna indirecta muy clara?",
        "¿Te gusta que te coqueteen delante de otros?",
        "¿Has tenido un lío que nadie esperaba?",
        "¿Te atrae alguien ahora mismo?",
        "¿Qué parte de alguien miras primero cuando te gusta?",
        "¿Te gusta que te rocen o que te lo digan claro?",
        "¿Has tenido una cita que acabó mejor de lo esperado?",
        "¿Qué te hace subir el tono en una conversación?",
        "¿Te gusta besar en público?",
        "¿A quién del grupo darías un beso de prueba?",
        "¿Te gusta que haya tensión antes del primer beso?",
        "¿Qué te parece más peligroso: una mirada o una sonrisa?",
        "¿Has tenido un rollo secreto?",
        "¿Te gusta que te reten?",
        "¿Qué te parece más atractivo: inocencia o picardía?",
        "¿Has fantaseado con alguien cercano?",
        "¿Te gusta la gente atrevida?",
        "¿Qué haría que dijeras 'hoy sí'?",
        "¿Prefieres una noche improvisada o planeada?",
        "¿Te gusta tomar el control al ligar?",
        "¿Qué mensaje te encendería el interés?",
        "¿Has tenido un beso que no esperabas?",
        "¿Te gusta el juego de celos o lo odias?",
        "¿Qué te parece más seductor: calma o intensidad?",
        "¿Has quedado con alguien solo por curiosidad?",
        "¿Qué te haría aceptar una cita ahora mismo?",
        "¿A quién del grupo le ves más peligro?",
        "¿Quién del grupo crees que besa mejor?",
        "¿Quién del grupo tendría más labia ligando?",
        "¿Qué te da más morbo: lo prohibido o lo inesperado?",
        "¿Has tenido tensión con alguien y no pasó nada?",
        "¿Te gusta que te digan exactamente lo que quieren?",
        "¿Qué frase te pondría en alerta?",
        "¿Has jugado a provocar sin intención de nada más?",
        "¿Qué te gana más: físico, actitud o conversación?",
        "¿A quién del grupo mandarías un mensaje privado?",
    ],
    3: [
        "¿Has tenido una fantasía que aún no has cumplido?",
        "¿Te gusta dominar o que te dominen?",
        "¿Has tenido algo en un sitio poco habitual?",
        "¿Te atrae más lo romántico o lo intenso?",
        "¿Has enviado alguna foto subida de tono?",
        "¿Te gusta el juego previo mental?",
        "¿Qué te da más morbo sin entrar en detalles?",
        "¿Has tenido una noche improvisada que salió muy bien?",
        "¿Te gusta experimentar o repetir lo que funciona?",
        "¿Qué límite no cruzarías?",
        "¿Te gusta que haya iniciativa fuerte?",
        "¿Qué te parece más excitante: tensión o confianza?",
        "¿Has tenido una experiencia que no contarías a cualquiera?",
        "¿Te gusta que te sorprendan en la intimidad?",
        "¿Qué te hace perder un poco el control?",
        "¿Te gusta hablar claro de lo que quieres?",
        "¿Has tenido una conexión física muy rápida?",
        "¿Qué fantasía te da curiosidad?",
        "¿Te gusta más planear o dejarte llevar?",
        "¿Has tenido algo con alguien que no era tu tipo?",
        "¿Te gusta el riesgo controlado?",
        "¿Qué detalle enciende la química?",
        "¿Has hecho algo atrevido por impulso?",
        "¿Te gusta que te digan lo que desean?",
        "¿Qué papel te sale más: mandar o dejarte llevar?",
        "¿Has tenido una noche que recordarás siempre?",
        "¿Te gusta lo prohibido o prefieres lo seguro?",
        "¿Qué te parece más intenso: silencio o palabras?",
        "¿Has sentido atracción por alguien que no debías?",
        "¿Qué te haría subir de nivel en una cita?",
        "¿A quién del grupo ves con más picardía?",
        "¿Quién del grupo tendría más secretos?",
        "¿Con quién del grupo tendrías una conversación subida de tono?",
        "¿A quién del grupo elegirías para un juego de verdad o reto?",
        "¿Qué es lo más atrevido que dirías aquí?",
        "¿Te gusta que haya reglas o romperlas un poco?",
        "¿Qué te da más curiosidad probar?",
        "¿Has tenido tensión con alguien del grupo?",
        "¿Qué prefieres: intensidad corta o calma larga?",
        "¿Qué pregunta hot te daría vergüenza responder?",
    ],
    4: [
        "¿Cuál ha sido tu experiencia más atrevida?",
        "¿Has tenido una fantasía que te cuesta admitir?",
        "¿Qué situación te parece muy peligrosa para la tentación?",
        "¿Has sentido deseo por alguien prohibido?",
        "¿Te gusta el control o perderlo?",
        "¿Qué harías si nadie se enterara?",
        "¿Has vivido una noche que parecía de película?",
        "¿Qué te parece más fuerte: deseo o curiosidad?",
        "¿Has cruzado una línea que no pensabas cruzar?",
        "¿Te gusta que la otra persona tome el mando?",
        "¿Has tenido una confesión hot que te sorprendió?",
        "¿Qué secreto no contarías en voz alta?",
        "¿Te atrae el riesgo emocional?",
        "¿Has tenido una tensión imposible de ocultar?",
        "¿Qué te hace decir 'esto se va de las manos'?",
        "¿Te gusta lo intenso desde el principio?",
        "¿Has tenido una experiencia que repetirías sin pensar?",
        "¿Qué te parece más excitante: confianza total o misterio total?",
        "¿Has jugado con fuego sabiendo que quemaba?",
        "¿Qué te hace actuar por impulso?",
        "¿Te gusta el morbo de lo secreto?",
        "¿Has tenido que fingir calma cuando había tensión?",
        "¿Qué situación te haría caer fácil?",
        "¿Te gusta provocar y ver la reacción?",
        "¿Has pensado en alguien del grupo de forma atrevida?",
        "¿Qué elegirías: una noche sin preguntas o una cita perfecta?",
        "¿Quién del grupo crees que tiene más peligro?",
        "¿Quién del grupo parece más inocente pero no lo es?",
        "¿Con quién del grupo habría más tensión?",
        "¿A quién del grupo no te conviene tener cerca?",
        "¿Qué cosa te da vergüenza admitir que te gusta?",
        "¿Te gusta hablar de fantasías o guardártelas?",
        "¿Qué sería para ti una noche perfecta?",
        "¿Has tenido una conversación que acabó demasiado intensa?",
        "¿Qué te parece más potente: deseo lento o impulso rápido?",
        "¿Qué parte de tu personalidad sale cuando hay química?",
        "¿Has tenido una tentación que casi gana?",
        "¿Qué haría alguien para desarmarte?",
        "¿Qué pregunta no quieres que te hagan?",
        "¿A quién del grupo le lanzarías una pregunta nivel 4?",
    ],
    5: [
        "¿Cuál es tu mayor secreto hot?",
        "¿Qué fantasía no has contado casi nunca?",
        "¿Qué te hace perder totalmente la compostura?",
        "¿Qué harías una sola vez si no hubiera consecuencias?",
        "¿Cuál es tu límite más claro?",
        "¿Qué deseo te cuesta admitir?",
        "¿Qué experiencia te dejó marcado/a?",
        "¿Qué te da más morbo de lo prohibido?",
        "¿Qué no responderías delante de todos?",
        "¿Qué confesión te haría sonrojar?",
        "¿Has querido repetir una noche que no debías repetir?",
        "¿Qué situación te pondría en máximo peligro?",
        "¿Qué te hace dejarte llevar demasiado?",
        "¿Qué tentación te cuesta resistir?",
        "¿Qué fantasía te gustaría cumplir con mucha confianza?",
        "¿Qué secreto guardarías aunque te pregunten?",
        "¿Qué te parece demasiado intenso incluso para ti?",
        "¿Has tenido una atracción que preferías negar?",
        "¿Qué te haría cruzar tus propios límites?",
        "¿Qué te da miedo desear?",
        "¿Qué pregunta nivel 5 esquivarías?",
        "¿Qué te gustaría que alguien adivinara de ti?",
        "¿Qué deseo oculto te sorprende?",
        "¿Qué momento te hizo perder el control?",
        "¿Qué repetirías aunque fuese mala idea?",
        "¿Qué te parece irresistible aunque sea peligroso?",
        "¿Qué te enciende más: palabras, mirada o actitud?",
        "¿Qué te gustaría confesar sin consecuencias?",
        "¿Qué te hizo pensar 'esto no debería gustarme tanto'?",
        "¿Qué harías si la noche no tuviera reglas?",
        "¿A quién del grupo le harías una pregunta nivel 5?",
        "¿Con quién del grupo tendrías una conversación sin filtros?",
        "¿Quién del grupo crees que oculta más de lo que enseña?",
        "¿Quién del grupo te parece más tentación?",
        "¿Qué elegirías: secreto seguro o riesgo intenso?",
        "¿Qué experiencia no contarías con nombres?",
        "¿Qué deseo te ha sorprendido últimamente?",
        "¿Qué te domina cuando hay química real?",
        "¿Qué te haría olvidar la prudencia?",
        "¿Cuál sería tu última confesión si este juego terminara ahora?",
    ],
}


# Generación ampliada: 500 preguntas + 500 retos por niveles (100 por nivel).
# Se generan al arrancar para mantener el archivo ligero y editable.
def _hot_generated_questions() -> Dict[int, List[str]]:
    tones = {
        1: ["suave", "coqueto", "curioso", "divertido"],
        2: ["directo", "atrevido", "con chispa", "con picardía"],
        3: ["picante", "intenso", "sin rodeos", "con tensión"],
        4: ["fuerte", "atrevido de verdad", "muy directo", "de confianza"],
        5: ["HOT", "sin filtros", "máximo nivel", "solo con confianza"],
    }
    topics = [
        "una mirada", "un mensaje privado", "una cita inesperada", "un beso", "una confesión",
        "alguien del grupo", "una noche improvisada", "una tentación", "un secreto", "una fantasía",
        "el primer paso", "una indirecta", "la química", "una conversación", "un plan de noche",
        "una persona misteriosa", "una provocación", "un reto", "un momento de tensión", "una decisión impulsiva",
    ]
    verbs = [
        "te atrae más", "te daría más vergüenza admitir", "te haría responder sin pensar", "te parece más peligroso/a",
        "te gustaría probar", "evitarías responder delante de todos", "te hace perder la calma", "te da más curiosidad",
        "te parece irresistible", "te metería en problemas",
    ]
    out: Dict[int, List[str]] = {}
    for level in range(1, 6):
        rows: List[str] = []
        for i in range(100):
            tone = tones[level][i % len(tones[level])]
            topic = topics[i % len(topics)]
            verb = verbs[(i // len(topics)) % len(verbs)]
            rows.append(f"Nivel {level} · Pregunta {i+1}: ¿Qué {verb} de {topic} en un contexto {tone}?")
        out[level] = rows
    return out


def _hot_generated_challenges() -> Dict[int, List[str]]:
    actions = {
        1: [
            "Di un cumplido elegante a alguien del grupo.",
            "Confiesa una pequeña manía graciosa.",
            "Manda un emoji que describa tu mood actual.",
            "Elige a alguien del grupo para una cita ficticia de café.",
            "Cuenta cuál sería tu plan ideal de sábado.",
        ],
        2: [
            "Etiqueta a alguien que te parezca interesante y dile por qué en una frase.",
            "Di una indirecta sin decir a quién va dirigida.",
            "Cuenta tu mejor excusa para hablar con alguien que te gusta.",
            "Elige entre beso, cita o misterio para la persona anterior.",
            "Responde con una frase que usarías para ligar.",
        ],
        3: [
            "Di una confesión picante sin dar nombres.",
            "Elige a alguien del grupo para una pregunta nivel 3.",
            "Cuenta algo atrevido que te daría curiosidad probar, sin detalles explícitos.",
            "Manda una frase con tensión, pero elegante.",
            "Di qué te da más morbo: misterio, riesgo o confianza.",
        ],
        4: [
            "Haz una confesión fuerte sin mencionar nombres.",
            "Elige a alguien que parezca peligroso/a y explica por qué.",
            "Di una verdad que normalmente esquivarías.",
            "Cuenta una tentación que casi te gana, sin detalles explícitos.",
            "Lanza una pregunta fuerte a alguien del grupo.",
        ],
        5: [
            "Confiesa un secreto HOT sin nombres y sin detalles explícitos.",
            "Elige a alguien para una conversación sin filtros y di por qué.",
            "Di una fantasía de forma elegante, sin contenido explícito.",
            "Cuenta qué pregunta nivel 5 no quieres que te hagan.",
            "Responde con una confesión máxima, pero respetuosa.",
        ],
    }
    out: Dict[int, List[str]] = {}
    for level in range(1, 6):
        base = actions[level]
        rows: List[str] = []
        for i in range(100):
            rows.append(f"Nivel {level} · Reto {i+1}: {base[i % len(base)]}")
        out[level] = rows
    return out


def _hot_extend_base_content() -> None:
    generated = _hot_generated_questions()
    for level, rows in generated.items():
        existing = HOT_BASE_QUESTIONS.setdefault(level, [])
        existing.extend([r for r in rows if r not in existing])


HOT_BASE_CHALLENGES: Dict[int, List[str]] = _hot_generated_challenges()
_hot_extend_base_content()


def hot_cfg(chat_id: int) -> Dict[str, Any]:
    cfg = admin_cfg(chat_id)
    cfg.setdefault("hot_mode", "manual")  # manual | random | auto
    cfg.setdefault("hot_level", 1)
    cfg.setdefault("hot_random_include_level5", False)
    cfg.setdefault("hot_auto_enabled", False)
    cfg.setdefault("hot_auto_interval_seconds", 180)
    cfg.setdefault("hot_auto_include_hot", False)
    cfg.setdefault("hot_auto_min_messages", 5)
    cfg.setdefault("hot_auto_min_users", 2)
    cfg.setdefault("hot_auto_activity_window_seconds", 240)
    cfg.setdefault("hot_auto_delete_enabled", True)
    cfg.setdefault("hot_auto_delete_seconds", 45)
    cfg.setdefault("hot_points_delete_seconds", 5)
    cfg.setdefault("hot_command_delete_mode", "off")  # off | instant | ttl
    cfg.setdefault("hot_command_delete_seconds", 20)
    cfg.setdefault("hot_custom_questions", {})
    cfg.setdefault("hot_custom_challenges", {})
    cfg.setdefault("hot_auto_mix_challenges", True)
    cfg.setdefault("hot_auto_challenge_every", 5)  # 4 preguntas + 1 reto
    cfg.setdefault("hot_auto_counter", 0)
    cfg.setdefault("hot_ranking", {})
    cfg.setdefault("hot_last_auto_ts", 0)
    return cfg


def hot_custom_questions(chat_id: int, level: int) -> List[str]:
    cfg = hot_cfg(chat_id)
    data = cfg.setdefault("hot_custom_questions", {})
    key = str(int(level))
    items = data.setdefault(key, [])
    if not isinstance(items, list):
        data[key] = []
        items = data[key]
    return items


def hot_custom_challenges(chat_id: int, level: int) -> List[str]:
    cfg = hot_cfg(chat_id)
    data = cfg.setdefault("hot_custom_challenges", {})
    key = str(int(level))
    items = data.setdefault(key, [])
    if not isinstance(items, list):
        data[key] = []
        items = data[key]
    return items


def hot_get_challenge(chat_id: int, level: int) -> str:
    level = max(1, min(5, int(level or 1)))
    base = HOT_BASE_CHALLENGES.get(level, [])
    custom = hot_custom_challenges(chat_id, level)
    pool = [str(x).strip() for x in (base + custom) if str(x).strip()]
    if not pool:
        return "No hay retos configurados en este nivel."
    return secrets.choice(pool)


def hot_get_item(chat_id: int, level: int, kind: str = "question") -> str:
    return hot_get_challenge(chat_id, level) if kind == "challenge" else hot_get_question(chat_id, level)


def hot_pick_auto_kind(chat_id: int) -> str:
    cfg = hot_cfg(chat_id)
    if not bool(cfg.get("hot_auto_mix_challenges", True)):
        return "question"
    every = max(2, int(cfg.get("hot_auto_challenge_every", 5) or 5))
    cfg["hot_auto_counter"] = int(cfg.get("hot_auto_counter", 0) or 0) + 1
    return "challenge" if cfg["hot_auto_counter"] % every == 0 else "question"


def hot_get_question(chat_id: int, level: int) -> str:
    level = max(1, min(5, int(level or 1)))
    base = HOT_BASE_QUESTIONS.get(level, [])
    custom = hot_custom_questions(chat_id, level)
    pool = [str(x).strip() for x in (base + custom) if str(x).strip()]
    if not pool:
        return "No hay preguntas configuradas en este nivel."
    return secrets.choice(pool)


def hot_pick_level(chat_id: int, automatic: bool = False) -> int:
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_mode", "manual"))
    if automatic:
        # En auto se respeta el nivel configurado por el admin.
        return max(1, min(5, int(cfg.get("hot_level", 1) or 1)))
    if mode == "random":
        max_level = 5 if bool(cfg.get("hot_random_include_level5", False)) else 4
        return int(secrets.choice(list(range(1, max_level + 1))))
    return max(1, min(5, int(cfg.get("hot_level", 1) or 1)))


def hot_activity_remember(chat_id: int, user) -> None:
    if not user or getattr(user, "is_bot", False):
        return
    now = int(time.time())
    users = HOT_RECENT_ACTIVITY.setdefault(int(chat_id), {})
    users[int(user.id)] = {
        "id": int(user.id),
        "name": display_name(user),
        "first_name": getattr(user, "first_name", "") or display_name(user),
        "ts": now,
    }
    log = HOT_CHAT_ACTIVITY_LOG.setdefault(int(chat_id), [])
    log.append({"ts": now, "user_id": int(user.id)})
    cutoff = now - 900
    HOT_CHAT_ACTIVITY_LOG[int(chat_id)] = [x for x in log if int(x.get("ts", 0)) >= cutoff]
    cutoff_users = now - 600
    for uid in list(users.keys()):
        if int(users[uid].get("ts", 0)) < cutoff_users:
            users.pop(uid, None)


def hot_auto_has_enough_interaction(chat_id: int, now: Optional[int] = None) -> bool:
    """Evita que el modo auto salte al primer mensaje tras un parón.

    Requiere varias intervenciones recientes y, por defecto, al menos 2 usuarios
    distintos dentro de la ventana configurada.
    """
    now = int(now or time.time())
    cfg = hot_cfg(chat_id)
    window = max(30, int(cfg.get("hot_auto_activity_window_seconds", 180) or 180))
    # Evita de verdad el disparo al primer mensaje después de un parón:
    # nunca baja de 5 mensajes y 2 usuarios recientes, aunque hubiera configuración antigua 3/2 guardada.
    min_messages = max(5, int(cfg.get("hot_auto_min_messages", 5) or 5))
    min_users = max(2, int(cfg.get("hot_auto_min_users", 2) or 2))
    cutoff = now - window
    recent = [x for x in HOT_CHAT_ACTIVITY_LOG.get(int(chat_id), []) if int(x.get("ts", 0)) >= cutoff]
    unique_users = {int(x.get("user_id", 0)) for x in recent if int(x.get("user_id", 0))}
    return len(recent) >= min_messages and len(unique_users) >= min_users


def hot_auto_activity_label(chat_id: int) -> str:
    cfg = hot_cfg(chat_id)
    return f"{max(5, int(cfg.get('hot_auto_min_messages', 5) or 5))} msgs / {max(2, int(cfg.get('hot_auto_min_users', 2) or 2))} users"


def hot_register_question(chat_id: int, message_id: int, target_user, level: int = 1, kind: str = "question") -> None:
    by_message = HOT_ACTIVE_QUESTIONS.setdefault(int(chat_id), {})
    by_message[int(message_id)] = {
        "target_id": int(target_user.id),
        "target_name": display_name(target_user),
        "level": max(1, min(5, int(level or 1))),
        "kind": "challenge" if kind == "challenge" else "question",
        "ts": int(time.time()),
    }


def hot_add_points(chat_id: int, user, points: int) -> int:
    cfg = hot_cfg(chat_id)
    ranking = cfg.setdefault("hot_ranking", {})
    key = str(int(user.id))
    row = ranking.setdefault(key, {"name": display_name(user), "points": 0})
    row["name"] = display_name(user)
    row["points"] = int(row.get("points", 0) or 0) + int(points)
    save_all_states()
    return int(row["points"])


async def hot_safe_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: Optional[int] = None) -> None:
    cfg = hot_cfg(chat_id)
    if not bool(cfg.get("hot_auto_delete_enabled", True)):
        return
    seconds = int(delay if delay is not None else cfg.get("hot_auto_delete_seconds", 45) or 45)
    if seconds <= 0:
        return
    await asyncio.sleep(seconds)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def preguntita_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    hot_activity_remember(chat_id, update.effective_user)

    target = update.message.reply_to_message.from_user if update.message.reply_to_message else update.effective_user
    level = hot_pick_level(chat_id, automatic=False)
    question = hot_get_question(chat_id, level)
    text = (
        f"🎯 {target.mention_html()}\n\n"
        f"🔥 <b>Preguntita · Nivel {level}</b>\n\n"
        f"💬 {h(question)}"
    )
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    await register_bot_message(chat_id, msg.message_id)
    hot_register_question(chat_id, msg.message_id, target, level)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id))


async def retito_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    hot_activity_remember(chat_id, update.effective_user)

    target = update.message.reply_to_message.from_user if update.message.reply_to_message else update.effective_user
    level = hot_pick_level(chat_id, automatic=False)
    challenge = hot_get_challenge(chat_id, level)
    text = (
        f"🎯 {target.mention_html()}\n\n"
        f"😈 <b>Retito · Nivel {level}</b>\n\n"
        f"🎲 {h(challenge)}"
    )
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    await register_bot_message(chat_id, msg.message_id)
    hot_register_question(chat_id, msg.message_id, target, level, kind="challenge")
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id))


async def ranking_hot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    await hot_delete_command_if_configured(update, context, chat_id)
    ranking = hot_cfg(chat_id).get("hot_ranking", {}) or {}
    rows = []
    for row in ranking.values():
        try:
            rows.append((str(row.get("name", "Usuario")), int(row.get("points", 0) or 0)))
        except Exception:
            continue
    rows.sort(key=lambda x: x[1], reverse=True)
    if not rows:
        await update.message.reply_html("🏆 <b>Ranking HOT</b>\n\nTodavía no hay puntos.")
        return
    lines = ["🏆 <b>Ranking HOT</b>", ""]
    for i, (name, points) in enumerate(rows[:15], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        lines.append(f"{medal} <b>{h(name)}</b> — {points} pts")
    await update.message.reply_html("\n".join(lines))


def hot_config_text(chat_id: int) -> str:
    cfg = hot_cfg(chat_id)
    custom_total = sum(len(hot_custom_questions(chat_id, lvl)) for lvl in range(1, 6))
    custom_retos = sum(len(hot_custom_challenges(chat_id, lvl)) for lvl in range(1, 6))
    return (
        "🔥 <b>Config Preguntita</b>\n\n"
        f"Grupo configurado: <code>{chat_id}</code>\n"
        f"Modo: <b>{h(cfg.get('hot_mode', 'manual'))}</b>\n"
        f"Nivel manual: <b>{int(cfg.get('hot_level', 1) or 1)}</b>\n"
        f"Random incluye nivel 5: <b>{bool_label(cfg.get('hot_random_include_level5', False))}</b>\n"
        f"Automático: <b>{bool_label(cfg.get('hot_auto_enabled', False))}</b>\n"
        f"Intervalo auto: <b>{int(cfg.get('hot_auto_interval_seconds', 180) or 180)}s</b>\n"
        f"Mínimo actividad auto: <b>{h(hot_auto_activity_label(chat_id))}</b>\n"
        f"Auto usa nivel configurado: <b>{int(cfg.get('hot_level', 1) or 1)}</b>\n"
        f"Auto mezcla retos: <b>{bool_label(cfg.get('hot_auto_mix_challenges', True))}</b> · cada <b>{int(cfg.get('hot_auto_challenge_every', 5) or 5)}</b> turnos\n"
        f"Borrado mensajes HOT: <b>{bool_label(cfg.get('hot_auto_delete_enabled', True))}</b>\n"
        f"Borrar preguntas tras: <b>{int(cfg.get('hot_auto_delete_seconds', 45) or 45)}s</b>\n"
        f"Borrar mensaje de puntos tras: <b>{int(cfg.get('hot_points_delete_seconds', 5) or 5)}s</b>\n"
        f"Borrado de comandos HOT: <b>{h(hot_command_delete_label(chat_id))}</b>\n"
        f"Preguntas base: <b>{sum(len(v) for v in HOT_BASE_QUESTIONS.values())}</b> · Retos base: <b>{sum(len(v) for v in HOT_BASE_CHALLENGES.values())}</b>\n"
        f"Preguntas añadidas por el grupo: <b>{custom_total}</b> · Retos añadidos: <b>{custom_retos}</b>\n\n"
        "Comandos:\n"
        "<code>/hotgrupo</code> · vincular este grupo para configurar por privado\n"
        "<code>/hotconfig</code> · configurar aquí o por privado tras /hotgrupo\n"
        "<code>/preguntita</code> · pregunta para quien lo ejecuta\n"
        "Responde a alguien con <code>/preguntita</code> · pregunta para esa persona\n"
        "<code>/rankinghot</code> · ranking\n"
        "<code>/retito</code> · reto para quien lo ejecuta o usuario respondido\n"
        "<code>/addpregunta 2 texto</code> · añadir una pregunta\n"
        "<code>/addreto 2 texto</code> · añadir un reto\n"
        "<code>/addmasivo</code> · pegar muchas preguntas y elegir nivel\n"
        "<code>/addretos</code> · pegar muchos retos y elegir nivel"
    )


def hot_config_markup(chat_id: int) -> InlineKeyboardMarkup:
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_mode", "manual"))
    auto = bool(cfg.get("hot_auto_enabled", False))
    autodel = bool(cfg.get("hot_auto_delete_enabled", True))
    include5 = bool(cfg.get("hot_random_include_level5", False))
    includehot = bool(cfg.get("hot_auto_include_hot", False))
    cmd_mode = str(cfg.get("hot_command_delete_mode", "off"))
    rows = [
        [InlineKeyboardButton(("✅ " if mode == "manual" else "") + "Manual", callback_data="hot|mode|manual"),
         InlineKeyboardButton(("✅ " if mode == "random" else "") + "Aleatorio", callback_data="hot|mode|random")],
        [InlineKeyboardButton(f"Nivel {i}" + (" ✅" if int(cfg.get("hot_level", 1) or 1) == i else ""), callback_data=f"hot|level|{i}") for i in (1, 2, 3)],
        [InlineKeyboardButton(f"Nivel {i}" + (" ✅" if int(cfg.get("hot_level", 1) or 1) == i else ""), callback_data=f"hot|level|{i}") for i in (4, 5)],
        [InlineKeyboardButton("Random N5 " + ("ON" if include5 else "OFF"), callback_data="hot|toggle|include5")],
        [InlineKeyboardButton("Auto " + ("ON ✅" if auto else "OFF ❌"), callback_data="hot|toggle|auto"),
         InlineKeyboardButton("Auto retos " + ("ON" if bool(cfg.get("hot_auto_mix_challenges", True)) else "OFF"), callback_data="hot|toggle|autoreto")],
        [InlineKeyboardButton("Mix 4P+1R", callback_data="hot|mix|5"),
         InlineKeyboardButton("Mix 6P+1R", callback_data="hot|mix|7")],
        [InlineKeyboardButton("Auto 60s", callback_data="hot|interval|60"),
         InlineKeyboardButton("180s", callback_data="hot|interval|180")],
        [InlineKeyboardButton("Auto 300s", callback_data="hot|interval|300"),
         InlineKeyboardButton("700s", callback_data="hot|interval|700")],
        [InlineKeyboardButton("Act. baja 2/1", callback_data="hot|activity|2_1"),
         InlineKeyboardButton("Act. media 3/2", callback_data="hot|activity|3_2"),
         InlineKeyboardButton("Act. alta 5/2", callback_data="hot|activity|5_2")],
        [InlineKeyboardButton("Borrado HOT " + ("ON ✅" if autodel else "OFF ❌"), callback_data="hot|toggle|delete")],
        [InlineKeyboardButton("Preguntas 30s", callback_data="hot|delete_after|30"),
         InlineKeyboardButton("90s", callback_data="hot|delete_after|90"),
         InlineKeyboardButton("700s", callback_data="hot|delete_after|700")],
        [InlineKeyboardButton("Puntos 5s", callback_data="hot|points_delete|5"),
         InlineKeyboardButton("20s", callback_data="hot|points_delete|20"),
         InlineKeyboardButton("45s", callback_data="hot|points_delete|45")],
        [InlineKeyboardButton("Cmd OFF" + (" ✅" if cmd_mode == "off" else ""), callback_data="hot|cmddelete|off"),
         InlineKeyboardButton("Cmd al ejecutar" + (" ✅" if cmd_mode == "instant" else ""), callback_data="hot|cmddelete|instant")],
        [InlineKeyboardButton("Cmd 20s", callback_data="hot|cmddelete|ttl20"),
         InlineKeyboardButton("Cmd 45s", callback_data="hot|cmddelete|ttl45")],
        [InlineKeyboardButton("🌐 Cambiar grupo", callback_data="hot|groups|0")],
        [InlineKeyboardButton("💾 Guardar y cerrar", callback_data="hot|close|save"),
         InlineKeyboardButton("🔄 Recargar", callback_data="hot|refresh|0")],
    ]
    return InlineKeyboardMarkup(rows)

async def hotconfig_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    # En privado, si hay varios grupos vinculados, primero muestra selector.
    if hot_is_private(update):
        groups = hot_private_groups_for(update.effective_user.id)
        if not groups:
            await update.message.reply_html(
                "Primero entra en el grupo que quieres configurar y ejecuta <code>/hotgrupo</code>. "
                "Luego vuelve aquí y usa <code>/hotconfig</code> por privado."
            )
            return
        if len(groups) > 1:
            await update.message.reply_html(
                "🔥 <b>Elige el grupo que quieres configurar</b>\n\n"
                "Puedes vincular varios grupos usando <code>/hotgrupo</code> dentro de cada uno.",
                reply_markup=hot_group_selector_markup(update.effective_user.id),
            )
            return

    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html(
            "Primero entra en el grupo y ejecuta <code>/hotgrupo</code>. Luego vuelve aquí y usa <code>/hotconfig</code> por privado."
        )
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores del grupo vinculado.")
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    await update.message.reply_html(hot_config_text(chat_id), reply_markup=hot_config_markup(chat_id))


async def hotgrupo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if hot_is_private(update):
        await update.message.reply_html("Este comando se usa dentro del grupo que quieres configurar. Después podrás usar <code>/hotconfig</code> por privado.")
        return
    chat_id = int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    hot_link_private_group(update.effective_user.id, chat_id, update.effective_chat.title or str(chat_id))
    await hot_delete_command_if_configured(update, context, chat_id)
    msg = await update.message.reply_html("✅ Grupo vinculado para configurar Preguntita por privado. Si tienes varios grupos, al usar <code>/hotconfig</code> por privado podrás elegir cuál configurar.")
    await register_bot_message(chat_id, msg.message_id)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=20))


async def hotgroup_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("hotgroup|"):
        return
    parts = data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else ""

    if action == "close":
        await query.answer("Cerrado")
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if action != "select":
        await query.answer("Acción no válida", show_alert=True)
        return

    try:
        chat_id = int(value)
    except Exception:
        await query.answer("Grupo no válido", show_alert=True)
        return

    if not hot_select_private_group(update.effective_user.id, chat_id):
        await query.answer("Ese grupo no está vinculado. Usa /hotgrupo dentro del grupo.", show_alert=True)
        return

    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Ya no eres administrador de ese grupo.", show_alert=True)
        return

    await query.answer("Grupo seleccionado ✅")
    try:
        await query.edit_message_text(
            hot_config_text(chat_id),
            reply_markup=hot_config_markup(chat_id),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


async def hot_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("hot|"):
        return
    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await query.answer("Primero vincula un grupo con /hotgrupo.", show_alert=True)
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores.", show_alert=True)
        return
    parts = data.split("|")
    cfg = hot_cfg(chat_id)
    try:
        action = parts[1]
        value = parts[2] if len(parts) > 2 else ""
        if action == "groups":
            groups = hot_private_groups_for(update.effective_user.id)
            if not groups:
                await query.answer("No tienes grupos vinculados. Usa /hotgrupo dentro del grupo.", show_alert=True)
                return
            await query.answer("Elige grupo")
            await query.edit_message_text(
                "🔥 <b>Elige el grupo que quieres configurar</b>",
                reply_markup=hot_group_selector_markup(update.effective_user.id),
                parse_mode=ParseMode.HTML,
            )
            return
        if action == "close":
            save_all_states()
            await query.answer("Guardado ✅")
            try:
                await query.message.delete()
            except Exception:
                try:
                    await query.edit_message_text("✅ Configuración guardada.")
                except Exception:
                    pass
            return
        if action == "mode":
            cfg["hot_mode"] = value if value in ("manual", "random", "auto") else "manual"
        elif action == "level":
            cfg["hot_level"] = max(1, min(5, int(value)))
            cfg["hot_mode"] = "manual"
        elif action == "toggle":
            if value == "include5":
                cfg["hot_random_include_level5"] = not bool(cfg.get("hot_random_include_level5", False))
            elif value == "auto":
                cfg["hot_auto_enabled"] = not bool(cfg.get("hot_auto_enabled", False))
                cfg["hot_mode"] = "auto" if cfg["hot_auto_enabled"] else cfg.get("hot_mode", "manual")
            elif value == "autohot":
                cfg["hot_auto_include_hot"] = not bool(cfg.get("hot_auto_include_hot", False))
            elif value == "autoreto":
                cfg["hot_auto_mix_challenges"] = not bool(cfg.get("hot_auto_mix_challenges", True))
            elif value == "delete":
                cfg["hot_auto_delete_enabled"] = not bool(cfg.get("hot_auto_delete_enabled", True))
        elif action == "mix":
            cfg["hot_auto_challenge_every"] = max(2, int(value))
        elif action == "interval":
            cfg["hot_auto_interval_seconds"] = max(30, int(value))
        elif action == "activity":
            try:
                min_msgs, min_users = value.split("_", 1)
                cfg["hot_auto_min_messages"] = max(1, int(min_msgs))
                cfg["hot_auto_min_users"] = max(1, int(min_users))
            except Exception:
                cfg["hot_auto_min_messages"] = 3
                cfg["hot_auto_min_users"] = 2
        elif action == "delete_after":
            cfg["hot_auto_delete_seconds"] = max(5, int(value))
        elif action == "points_delete":
            cfg["hot_points_delete_seconds"] = max(1, int(value))
        elif action == "cmddelete":
            if value == "off":
                cfg["hot_command_delete_mode"] = "off"
            elif value == "instant":
                cfg["hot_command_delete_mode"] = "instant"
            elif value.startswith("ttl"):
                cfg["hot_command_delete_mode"] = "ttl"
                try:
                    cfg["hot_command_delete_seconds"] = max(1, int(value.replace("ttl", "") or 20))
                except Exception:
                    cfg["hot_command_delete_seconds"] = 20
        save_all_states()
        await query.answer("Actualizado ✅")
        try:
            await query.edit_message_text(hot_config_text(chat_id), reply_markup=hot_config_markup(chat_id), parse_mode=ParseMode.HTML)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
    except Exception:
        logger.exception("Error en callback HOT")
        await query.answer("No se pudo actualizar.", show_alert=True)


async def addpregunta_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html("Primero vincula un grupo con <code>/hotgrupo</code>.")
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores del grupo vinculado.")
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    raw = " ".join(context.args or []).strip()
    if not raw:
        await update.message.reply_html("Uso: <code>/addpregunta 2 ¿Tu pregunta?</code>")
        return
    parts = raw.split(maxsplit=1)
    if parts and parts[0].isdigit() and len(parts) > 1:
        level = max(1, min(5, int(parts[0])))
        question = parts[1].strip()
        hot_custom_questions(chat_id, level).append(question)
        save_all_states()
        await update.message.reply_html(f"✅ Pregunta añadida al nivel <b>{level}</b>.")
        return
    HOT_PENDING_ADD[update.effective_user.id] = {"target_chat_id": chat_id, "input_chat_id": int(update.effective_chat.id), "mode": "single", "questions": [raw]}
    rows = [[InlineKeyboardButton(f"Nivel {i}", callback_data=f"hotadd|{i}") for i in (1, 2, 3)], [InlineKeyboardButton("Nivel 4", callback_data="hotadd|4"), InlineKeyboardButton("Nivel 5", callback_data="hotadd|5")]]
    await update.message.reply_html("Elige el nivel para guardar esta pregunta:", reply_markup=InlineKeyboardMarkup(rows))


async def addreto_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html("Primero vincula un grupo con <code>/hotgrupo</code>.")
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores del grupo vinculado.")
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    raw = " ".join(context.args or []).strip()
    if not raw:
        await update.message.reply_html("Uso: <code>/addreto 2 Haz una confesión...</code>")
        return
    parts = raw.split(maxsplit=1)
    if parts and parts[0].isdigit() and len(parts) > 1:
        level = max(1, min(5, int(parts[0])))
        challenge = parts[1].strip()
        hot_custom_challenges(chat_id, level).append(challenge)
        save_all_states()
        await update.message.reply_html(f"✅ Reto añadido al nivel <b>{level}</b>.")
        return
    HOT_PENDING_ADD[update.effective_user.id] = {"target_chat_id": chat_id, "input_chat_id": int(update.effective_chat.id), "mode": "single", "kind": "challenge", "questions": [raw]}
    rows = [[InlineKeyboardButton(f"Nivel {i}", callback_data=f"hotadd|{i}") for i in (1, 2, 3)], [InlineKeyboardButton("Nivel 4", callback_data="hotadd|4"), InlineKeyboardButton("Nivel 5", callback_data="hotadd|5")]]
    await update.message.reply_html("Elige el nivel para guardar este reto:", reply_markup=InlineKeyboardMarkup(rows))


async def addmasivo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html("Primero vincula un grupo con <code>/hotgrupo</code>.")
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores del grupo vinculado.")
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    HOT_PENDING_ADD[update.effective_user.id] = {"target_chat_id": chat_id, "input_chat_id": int(update.effective_chat.id), "mode": "bulk_wait_text", "kind": "question", "questions": []}
    await update.message.reply_html("📥 Pega ahora las preguntas, <b>una por línea</b>.\n\nCuando las envíes, te preguntaré el nivel.")


async def addretosmasivo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await update.message.reply_html("Primero vincula un grupo con <code>/hotgrupo</code>.")
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores del grupo vinculado.")
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    HOT_PENDING_ADD[update.effective_user.id] = {"target_chat_id": chat_id, "input_chat_id": int(update.effective_chat.id), "mode": "bulk_wait_text", "kind": "challenge", "questions": []}
    await update.message.reply_html("📥 Pega ahora los retos, <b>uno por línea</b>.\n\nCuando los envíes, te preguntaré el nivel.")


async def hotadd_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    try:
        level = int((query.data or "").split("|", 1)[1])
    except Exception:
        await query.answer("Nivel inválido.", show_alert=True)
        return
    pending = HOT_PENDING_ADD.get(update.effective_user.id)
    if not pending:
        await query.answer("No hay preguntas pendientes.", show_alert=True)
        return
    chat_id = int(pending.get("target_chat_id", pending.get("chat_id", 0)))
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores.", show_alert=True)
        return
    questions = [str(q).strip() for q in pending.get("questions", []) if str(q).strip()]
    if not questions:
        await query.answer("No hay elementos válidos.", show_alert=True)
        return
    kind = str(pending.get("kind", "question"))
    if kind == "challenge":
        hot_custom_challenges(chat_id, level).extend(questions)
        label = "retos"
    else:
        hot_custom_questions(chat_id, level).extend(questions)
        label = "preguntas"
    save_all_states()
    HOT_PENDING_ADD.pop(update.effective_user.id, None)
    await query.answer("Guardado ✅")
    await query.message.reply_html(f"✅ Añadidas <b>{len(questions)}</b> {label} al nivel <b>{level}</b>.")


async def hot_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    user = update.effective_user
    if getattr(user, "is_bot", False):
        return
    hot_activity_remember(chat_id, user)

    # Carga masiva pendiente.
    pending = HOT_PENDING_ADD.get(user.id)
    if pending and pending.get("mode") == "bulk_wait_text" and int(pending.get("input_chat_id", chat_id)) == chat_id:
        target_chat_id = int(pending.get("target_chat_id", chat_id))
        if await is_admin(context, target_chat_id, user.id):
            lines = [line.strip() for line in (update.message.text or "").replace("|", "\n").splitlines() if line.strip()]
            if not lines:
                await update.message.reply_text("No he detectado preguntas válidas.")
                return
            pending["mode"] = "bulk_choose_level"
            pending["questions"] = lines
            rows = [[InlineKeyboardButton(f"Nivel {i}", callback_data=f"hotadd|{i}") for i in (1, 2, 3)], [InlineKeyboardButton("Nivel 4", callback_data="hotadd|4"), InlineKeyboardButton("Nivel 5", callback_data="hotadd|5")]]
            label = "retos" if pending.get("kind") == "challenge" else "preguntas"
            await update.message.reply_html(f"📊 Detectados <b>{len(lines)}</b> {label}. Elige nivel:", reply_markup=InlineKeyboardMarkup(rows))
            return

    # Puntos por responder a una preguntita activa.
    reply = update.message.reply_to_message
    if not reply:
        return
    active = HOT_ACTIVE_QUESTIONS.get(chat_id, {})
    data = active.get(int(reply.message_id))
    if not data:
        return
    if int(data.get("target_id")) != int(user.id):
        return
    text = (update.message.text or "").strip()
    if len(text) < 3:
        return
    level = max(1, min(5, int(data.get("level", 1) or 1)))
    kind = str(data.get("kind", "question"))
    bonus = hot_answer_quality_bonus(text)
    points = level + bonus
    total = hot_add_points(chat_id, user, points)
    active.pop(int(reply.message_id), None)
    extra = f" + bonus {bonus}" if bonus else ""
    label = "reto" if kind == "challenge" else "pregunta"
    msg = await update.message.reply_html(f"🔥 <b>{h(display_name(user))}</b> +{points} pts <i>({label} nivel {level}{extra})</i>\n🏆 Total: <b>{total}</b>")
    await register_bot_message(chat_id, msg.message_id)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=int(hot_cfg(chat_id).get("hot_points_delete_seconds", 5) or 5)))


async def hot_auto_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(10)
        now = int(time.time())
        for chat_id in list(STATE_CACHE.keys()):
            try:
                cfg = hot_cfg(chat_id)
                if not bool(cfg.get("hot_auto_enabled", False)):
                    continue
                interval = max(30, int(cfg.get("hot_auto_interval_seconds", 180) or 180))
                if now - int(cfg.get("hot_last_auto_ts", 0) or 0) < interval:
                    continue
                if not hot_auto_has_enough_interaction(chat_id, now):
                    continue
                users = [u for u in HOT_RECENT_ACTIVITY.get(chat_id, {}).values() if now - int(u.get("ts", 0)) <= int(cfg.get("hot_auto_activity_window_seconds", 180) or 180)]
                if not users:
                    continue
                target = secrets.choice(users)
                level = hot_pick_level(chat_id, automatic=True)
                kind = hot_pick_auto_kind(chat_id)
                question = hot_get_item(chat_id, level, kind)
                mention = f"<a href=\"tg://user?id={int(target['id'])}\">{h(target.get('first_name') or target.get('name') or 'Usuario')}</a>"
                title = "Retito automático" if kind == "challenge" else "Preguntita automática"
                icon = "🎲" if kind == "challenge" else "💬"
                text = f"🎯 {mention}\n\n🤖 <b>{title} · Nivel {level}</b>\n\n{icon} {h(question)}"
                msg = await application.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                await register_bot_message(chat_id, msg.message_id)
                # Objeto mínimo para registrar target.
                class _U:
                    pass
                fake = _U()
                fake.id = int(target["id"])
                fake.first_name = str(target.get("first_name") or target.get("name") or "Usuario")
                fake.last_name = ""
                fake.username = None
                hot_register_question(chat_id, msg.message_id, fake, level, kind=kind)
                cfg["hot_last_auto_ts"] = now
                save_all_states()
                asyncio.create_task(hot_safe_delete(application, chat_id, msg.message_id))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error en hot_auto_loop para chat %s", chat_id)



# =========================
# HOT MODULE V6 - contenido natural, bloqueo, examen, ranking por periodos y botón fijado
# =========================
HOT_EXAMS: Dict[int, Dict[str, Any]] = {}


def _hot_natural_questions() -> Dict[int, List[str]]:
    """500 preguntitas orientadas a grupo de chat: 100 por nivel."""
    base: Dict[int, List[str]] = {
        1: [
            "¿A quién del grupo le mandarías un meme ahora mismo?",
            "¿Quién del grupo parece que siempre tiene buen plan?",
            "¿Qué canción mandarías ahora para levantar el grupo?",
            "¿Qué miembro del grupo te cae bien aunque habléis poco?",
            "¿Quién crees que sería el más divertido en una cena del grupo?",
            "¿Qué emoji define tu día de hoy?",
            "¿Qué foto no íntima de tu galería describe tu mood?",
            "¿Quién del grupo parece más tranquilo/a de lo que realmente es?",
            "¿Qué plan propondrías para este finde?",
            "¿Quién del grupo tiene pinta de dar buenos consejos?",
            "¿Qué te hace quedarte hablando en un chat?",
            "¿Quién del grupo respondería más rápido a un audio?",
            "¿Qué frase usarías para romper el hielo aquí?",
            "¿Quién crees que tiene mejor sentido del humor?",
            "¿Qué tema de conversación te engancha más?",
            "¿Qué miembro del grupo parece más de improvisar?",
            "¿Qué prefieres en el chat: audios, fotos o texto?",
            "¿Quién del grupo sería buen compañero/a de viaje?",
            "¿Qué cosa sencilla te pone de buen humor?",
            "¿Qué usuario crees que debería hablar más?",
        ],
        2: [
            "¿A quién del grupo invitarías a una copa sin pensarlo mucho?",
            "¿Quién del grupo te da más curiosidad conocer mejor?",
            "¿Con quién del grupo tendrías una conversación hasta tarde?",
            "¿Quién crees que liga mejor por chat?",
            "¿A quién le mandarías un privado solo para saludar?",
            "¿Quién del grupo tiene más pinta de tener secretos divertidos?",
            "¿Qué tipo de mensaje te hace contestar rápido?",
            "¿A quién del grupo elegirías para un plan improvisado?",
            "¿Quién te parece más interesante de lo que aparenta?",
            "¿Qué indirecta lanzarías hoy sin decir para quién es?",
            "¿Quién del grupo parece más peligroso/a con dos copas?",
            "¿Qué miembro del grupo tendría mejor cita de primeras?",
            "¿Qué te da más juego: una mirada, un audio o un mensaje privado?",
            "¿A quién del grupo le pedirías una recomendación musical?",
            "¿Quién crees que sorprendería si se soltase más?",
            "¿Qué plan te parece mejor: tardeo, cena o fiesta?",
            "¿Quién del grupo parece más de contestar con doble sentido?",
            "¿Qué te atrae más en un chat: humor, misterio o descaro?",
            "¿A quién del grupo ves con más labia?",
            "¿Quién podría hacer que este grupo se anime en 5 minutos?",
        ],
        3: [
            "¿A quién del grupo le harías una pregunta picante sin avisar?",
            "¿Quién del grupo te parece que tiene más picardía?",
            "¿Qué pregunta te daría vergüenza responder aquí, pero la responderías?",
            "¿Con quién del grupo tendrías una conversación con doble sentido?",
            "¿Quién crees que parece inocente pero no lo es tanto?",
            "¿Qué confesión suave podrías hacer sin dar nombres?",
            "¿Qué te da más morbo en un chat: misterio, tensión o indirectas?",
            "¿A quién del grupo mandarías una canción con mensaje oculto?",
            "¿Quién del grupo crees que guarda más historias interesantes?",
            "¿Qué mensaje privado te haría sonreír demasiado?",
            "¿Con quién del grupo harías un verdad o reto sin pensarlo?",
            "¿Qué cosa te parece sexy sin ser explícita?",
            "¿A quién del grupo le ves más peligro cuando bromea?",
            "¿Qué indirecta te gustaría recibir hoy?",
            "¿Quién del grupo crees que tiene mejor mirada de 'yo no he sido'?",
            "¿Qué te haría subir el tono en una conversación?",
            "¿A quién del grupo elegirías para una confesión anónima?",
            "¿Qué prefieres: tensión por chat o tensión en persona?",
            "¿Quién crees que sabe provocar sin hacerlo evidente?",
            "¿Qué pregunta picante le harías al grupo entero?",
        ],
        4: [
            "¿A quién del grupo no te conviene tener demasiado cerca?",
            "¿Quién del grupo podría meterte en un lío divertido?",
            "¿Qué confesión atrevida harías aquí sin decir nombres?",
            "¿Con quién del grupo tendrías una noche de conversación sin filtros?",
            "¿Quién te parece más tentación dentro del grupo?",
            "¿Qué mensaje privado te pondría nervioso/a de verdad?",
            "¿A quién del grupo le dirías 'tú tienes peligro' y por qué?",
            "¿Qué harías si este grupo tuviera modo noche sin capturas?",
            "¿Quién del grupo crees que tiene más historias subidas de tono?",
            "¿Qué pregunta fuerte responderías solo si todos responden también?",
            "¿A quién del grupo le lanzarías una indirecta muy clara?",
            "¿Qué te cuesta más admitir cuando alguien te atrae?",
            "¿Quién del grupo parece más de jugar con fuego?",
            "¿Qué conversación te gustaría tener en privado, sin detalles?",
            "¿Qué te da más peligro: curiosidad, confianza o una copa de más?",
            "¿A quién del grupo le ves más cara de secreto?",
            "¿Qué cosa te da morbo admitir, pero sin pasarte?",
            "¿Quién crees que sería más atrevido/a en un reto?",
            "¿Qué frase te dejaría pensando toda la noche?",
            "¿Con quién del grupo habría más tensión si os dejaran solos?",
        ],
        5: [
            "¿A quién del grupo le harías una pregunta HOT sin filtros?",
            "¿Quién del grupo te parece una tentación seria?",
            "¿Qué fantasía HOT confesarías sin nombres?",
            "¿Con quién del grupo tendrías una conversación totalmente privada y sin capturas?",
            "¿Qué te gustaría que alguien del grupo te dijera por privado?",
            "¿A quién del grupo elegirías para una noche de confesiones muy picantes?",
            "¿Qué deseo HOT te cuesta admitir aquí?",
            "¿Quién del grupo crees que tiene el lado más travieso?",
            "¿Qué pregunta nivel 5 te pondría contra las cuerdas?",
            "¿Qué mensaje privado te encendería bastante?",
            "¿A quién del grupo le dirías 'contigo me callo, pero pienso'?",
            "¿Qué secreto HOT contarías si nadie pudiera hacer capturas?",
            "¿Quién del grupo te parece más peligroso/a para perder el control?",
            "¿Qué te da más morbo: lo prohibido, lo secreto o lo inesperado?",
            "¿Con quién del grupo habría más química si sube el tono?",
            "¿Qué harías si esta noche el grupo jugara sin vergüenza?",
            "¿A quién del grupo mandarías una canción muy indirecta?",
            "¿Qué cosa HOT no responderías salvo que te reten?",
            "¿Quién del grupo crees que besa mejor y por qué?",
            "¿Qué confesión picante dejarías caer sin dar nombres?",
        ],
    }
    extras: Dict[int, List[str]] = {}
    for level in range(1, 6):
        rows = list(base[level])
        people = [
            "la persona más activa del grupo", "alguien que hable poco", "quien acaba de escribir", "el/la más misterioso/a", "el/la más fiestero/a",
            "alguien que te dé curiosidad", "el/la que manda más audios", "quien tiene más pinta de improvisar", "el/la más gracioso/a", "el/la más directo/a",
        ]
        actions = {
            1: ["tomarías un café", "mandarías un meme", "pedirías una canción", "harías equipo", "invitarías a un plan tranquilo"],
            2: ["mandarías un privado", "invitarías a una copa", "harías una pregunta con doble sentido", "propondrías un plan improvisado", "elegirías para una charla hasta tarde"],
            3: ["lanzarías una indirecta", "harías un verdad o reto", "mandarías una canción con mensaje", "preguntarías algo picante", "retaría a confesarse"],
            4: ["tendrías una conversación sin filtros", "dejarías que te hiciera una pregunta fuerte", "meterías en un juego atrevido", "elegirías para una noche de secretos", "dirías que tiene peligro"],
            5: ["harías una confesión HOT", "mandarías una indirecta muy clara", "tendrías una charla privada sin capturas", "elegirías para subir el tono", "le harías una pregunta nivel 5"],
        }
        moods = {
            1: ["buen rollo", "curiosidad", "risas", "plan tranquilo", "romper el hielo"],
            2: ["picardía suave", "tonteo", "curiosidad real", "chispa", "indirecta"],
            3: ["tensión", "doble sentido", "juego picante", "confesión", "morbo suave"],
            4: ["atrevimiento", "riesgo", "secreto", "tensión fuerte", "confianza"],
            5: ["HOT", "sin filtros", "muy picante", "privado", "máxima tensión"],
        }
        i = 0
        while len(rows) < 100:
            who = people[i % len(people)]
            act = actions[level][(i // len(people)) % len(actions[level])]
            mood = moods[level][(i // (len(people)*len(actions[level]))) % len(moods[level])]
            rows.append(f"¿A quién del grupo, pensando en {mood}, {act}? ¿Por qué?")
            i += 1
        extras[level] = rows[:100]
    return extras


def _hot_natural_challenges() -> Dict[int, List[str]]:
    """500 retitos orientados a hacer cosas en el grupo: 100 por nivel."""
    base: Dict[int, List[str]] = {
        1: [
            "Manda un emoji que resuma tu día.",
            "Envía una canción que pondrías ahora en el grupo.",
            "Di algo bueno de la última persona que escribió.",
            "Manda una foto NO íntima de tu galería que sea random o divertida.",
            "Etiqueta a alguien y dile un cumplido sencillo.",
            "Cuenta una manía tuya en una frase.",
            "Envía un sticker que te represente ahora mismo.",
            "Di qué plan propondrías para el grupo esta semana.",
            "Manda un audio de máximo 5 segundos saludando al grupo.",
            "Elige a alguien para que mande una canción.",
            "Di quién crees que anima más el chat.",
            "Manda una palabra que describa tu mood.",
            "Cuenta algo gracioso que te haya pasado hace poco.",
            "Pregunta al grupo algo fácil para romper el hielo.",
            "Elige a alguien para jugar la siguiente preguntita.",
            "Manda una foto de algo que tengas cerca, sin enseñar nada privado.",
            "Di qué comida pedirías ahora mismo.",
            "Etiqueta a alguien que debería hablar más.",
            "Manda una frase motivadora corta.",
            "Di si hoy eres team sofá, fiesta o paseo.",
        ],
        2: [
            "Manda una canción con indirecta suave y no digas para quién es.",
            "Etiqueta a alguien del grupo y dile por qué te cae bien.",
            "Di una indirecta graciosa sin nombres.",
            "Manda una foto NO íntima de tu galería que tenga historia y cuéntala.",
            "Elige a alguien para una cita ficticia de café.",
            "Manda un audio diciendo una frase con actitud.",
            "Di quién del grupo te parece más interesante.",
            "Escribe una frase que usarías para abrir privado.",
            "Reta a alguien a mandar una canción que le represente.",
            "Cuenta algo que te haga responder rápido un mensaje.",
            "Elige a alguien para hacer equipo en una noche de juegos.",
            "Di una verdad suave sobre ti que el grupo no sepa.",
            "Manda un sticker con doble sentido, pero sin pasarte.",
            "Etiqueta a alguien y pregúntale algo divertido.",
            "Di a quién invitarías a un plan improvisado.",
            "Manda una captura recortada de tu última canción escuchada.",
            "Dile al grupo qué plan te apetece hoy.",
            "Elige quién crees que liga mejor por chat.",
            "Manda una frase de película que te represente.",
            "Di quién del grupo tiene más pinta de ser buen cómplice.",
        ],
        3: [
            "Manda una canción con mensaje picante, sin decir para quién va.",
            "Etiqueta a alguien y dile una cosa que te dé curiosidad de esa persona.",
            "Haz una confesión picante suave, sin nombres.",
            "Manda una foto NO íntima de tu galería que parezca sospechosa y explica el contexto.",
            "Escribe una indirecta clara, pero sin mencionar a nadie.",
            "Elige a alguien para una pregunta nivel 3.",
            "Manda un audio de máximo 7 segundos diciendo 'yo no he sido' con picardía.",
            "Di quién del grupo parece inocente pero no lo es.",
            "Cuenta una anécdota con tensión, sin detalles explícitos.",
            "Reta a alguien a decir una verdad incómoda.",
            "Escribe un mensaje privado ficticio que mandarías para romper el hielo.",
            "Di qué te da más morbo en una conversación: misterio, tensión o descaro.",
            "Elige a alguien para una conversación con doble sentido.",
            "Manda un sticker que diga lo que no quieres escribir.",
            "Confiesa una indirecta que hayas tirado alguna vez.",
            "Etiqueta a alguien y pregúntale '¿te atreves con nivel 3?'.",
            "Di qué canción pondrías para subir el tono del grupo.",
            "Cuenta qué te hace pensar que alguien va con segundas.",
            "Elige a alguien que tenga pinta de guardar secretos.",
            "Manda una frase con tensión, pero elegante.",
        ],
        4: [
            "Haz una confesión atrevida sin nombres y sin detalles explícitos.",
            "Etiqueta a alguien y dile 'tú tienes peligro' explicando por qué.",
            "Manda una canción bastante directa, sin decir para quién va.",
            "Envía una foto NO íntima de tu galería que pueda malinterpretarse y explica la verdad.",
            "Escribe una indirecta muy clara y deja que el grupo adivine.",
            "Elige a alguien para una pregunta nivel 4.",
            "Manda un audio corto confesando algo que te dé vergüenza admitir.",
            "Di quién del grupo te parece más tentación.",
            "Reta a alguien a decir qué usuario le da más curiosidad.",
            "Cuenta una situación en la que casi te metes en un lío divertido.",
            "Di qué mensaje privado te pondría nervioso/a.",
            "Elige a alguien para una noche ficticia de confesiones.",
            "Manda un sticker que represente 'esto se está calentando'.",
            "Haz una pregunta fuerte al grupo, sin señalar a nadie.",
            "Di qué harías si el grupo tuviera modo noche sin capturas.",
            "Etiqueta a alguien y dile una pregunta que no te atreves a hacerle.",
            "Manda una frase que suene inocente pero tenga doble sentido.",
            "Di quién crees que se atrevería con nivel 5.",
            "Cuenta algo atrevido que hayas hecho por impulso, sin detalles explícitos.",
            "Reta a alguien a mandar una canción que le dé vergüenza admitir.",
        ],
        5: [
            "Haz una confesión HOT sin nombres y sin detalles explícitos.",
            "Etiqueta a alguien y lánzale una pregunta nivel 5, siempre con respeto.",
            "Manda una canción MUY indirecta y no digas para quién es.",
            "Envía una foto NO íntima de tu galería que tenga vibra de noche loca y cuenta solo lo justo.",
            "Escribe una frase HOT sin pasarte y deja que el grupo interprete.",
            "Elige a alguien para una conversación privada ficticia sin capturas.",
            "Di quién del grupo te parece más peligroso/a para perder la vergüenza.",
            "Manda un audio corto diciendo una confesión picante pero respetuosa.",
            "Cuenta qué pregunta nivel 5 te costaría responder.",
            "Reta a alguien a confesar una tentación sin nombres.",
            "Escribe el privado ficticio más atrevido que mandarías, sin contenido explícito.",
            "Di qué te da más morbo: secreto, tensión o prohibido.",
            "Etiqueta a alguien y dile 'contigo no juego, que pierdo'.",
            "Manda un sticker que represente tu lado más travieso.",
            "Haz una pregunta HOT al grupo entero.",
            "Di a quién del grupo le ves más cara de guardar secretos fuertes.",
            "Manda una frase que solo entendería quien va con segundas.",
            "Elige a alguien para un reto HOT elegante.",
            "Confiesa una fantasía de forma muy resumida y sin detalles explícitos.",
            "Di qué canción pondrías para cerrar una noche subida de tono.",
        ],
    }
    extras: Dict[int, List[str]] = {}
    for level in range(1, 6):
        rows = list(base[level])
        people = ["la última persona que habló", "alguien del grupo", "quien tú elijas", "la persona más activa", "alguien que hable poco"]
        things = {
            1: ["un emoji", "una canción", "un sticker", "una foto NO íntima", "una pregunta fácil"],
            2: ["una indirecta suave", "una canción con mensaje", "un cumplido", "una frase para abrir privado", "una confesión ligera"],
            3: ["una indirecta picante", "una confesión sin nombres", "una canción con doble sentido", "un audio con picardía", "una pregunta nivel 3"],
            4: ["una confesión atrevida", "una pregunta fuerte", "una canción bastante directa", "un mensaje con tensión", "una verdad incómoda"],
            5: ["una confesión HOT", "una pregunta nivel 5", "una indirecta muy clara", "una frase sin filtros", "un reto HOT elegante"],
        }
        i = 0
        while len(rows) < 100:
            who = people[i % len(people)]
            thing = things[level][(i // len(people)) % len(things[level])]
            rows.append(f"Reta a {who} a mandar {thing}. Si no quiere, que diga 'paso' y elija a otra persona.")
            i += 1
        extras[level] = rows[:100]
    return extras


HOT_BASE_QUESTIONS = _hot_natural_questions()
HOT_BASE_CHALLENGES = _hot_natural_challenges()


def hot_cfg(chat_id: int) -> Dict[str, Any]:
    cfg = admin_cfg(chat_id)
    cfg.setdefault("hot_mode", "manual")
    cfg.setdefault("hot_level", 1)
    cfg.setdefault("hot_random_include_level5", False)
    cfg.setdefault("hot_auto_enabled", False)
    cfg.setdefault("hot_auto_interval_seconds", 180)
    cfg.setdefault("hot_auto_include_hot", False)
    cfg.setdefault("hot_auto_min_messages", 5)
    cfg.setdefault("hot_auto_min_users", 2)
    cfg.setdefault("hot_auto_activity_window_seconds", 240)
    cfg.setdefault("hot_auto_delete_enabled", True)
    cfg.setdefault("hot_auto_delete_seconds", 90)
    cfg.setdefault("hot_points_delete_seconds", 5)
    cfg.setdefault("hot_command_delete_mode", "off")
    cfg.setdefault("hot_command_delete_seconds", 20)
    cfg.setdefault("hot_custom_questions", {})
    cfg.setdefault("hot_custom_challenges", {})
    cfg.setdefault("hot_auto_mix_challenges", True)
    cfg.setdefault("hot_auto_challenge_every", 5)
    cfg.setdefault("hot_auto_counter", 0)
    cfg.setdefault("hot_ranking", {})
    cfg.setdefault("hot_ranking_daily", {})
    cfg.setdefault("hot_ranking_weekly", {})
    cfg.setdefault("hot_last_auto_ts", 0)
    cfg.setdefault("hot_lock_mode", "interval")  # interval | answer
    cfg.setdefault("hot_lock_minutes", 1)
    cfg.setdefault("hot_pin_text", "🎲 <b>Juego activo</b>\n\nPulsa el botón para lanzar una preguntita o un retito. También puedes usar /preguntita, /retito o /examen.")
    cfg.setdefault("hot_pin_button_text", "🎲 Enviar preguntita / retito")
    return cfg


def hot_period_keys(now_ts: Optional[int] = None) -> Tuple[str, str]:
    now_ts = int(now_ts or time.time())
    day = time.strftime("%Y-%m-%d", time.localtime(now_ts))
    week = time.strftime("%G-W%V", time.localtime(now_ts))
    return day, week


def _ranking_add_to_bucket(bucket: Dict[str, Any], user, points: int) -> int:
    key = str(int(user.id))
    row = bucket.setdefault(key, {"name": display_name(user), "points": 0})
    row["name"] = display_name(user)
    row["points"] = int(row.get("points", 0) or 0) + int(points)
    return int(row["points"])


def hot_add_points(chat_id: int, user, points: int) -> int:
    cfg = hot_cfg(chat_id)
    total = _ranking_add_to_bucket(cfg.setdefault("hot_ranking", {}), user, points)
    day, week = hot_period_keys()
    daily = cfg.setdefault("hot_ranking_daily", {}).setdefault(day, {})
    weekly = cfg.setdefault("hot_ranking_weekly", {}).setdefault(week, {})
    _ranking_add_to_bucket(daily, user, points)
    _ranking_add_to_bucket(weekly, user, points)
    save_all_states()
    return total


def hot_active_pending(chat_id: int) -> Optional[Tuple[int, Dict[str, Any]]]:
    active = HOT_ACTIVE_QUESTIONS.get(int(chat_id), {})
    if not active:
        return None
    # Devuelve la activa más reciente.
    items = sorted(active.items(), key=lambda kv: int(kv[1].get("ts", 0)), reverse=True)
    return items[0] if items else None


def hot_prune_old_active(chat_id: int) -> None:
    cfg = hot_cfg(chat_id)
    if str(cfg.get("hot_lock_mode", "interval")) != "interval":
        return
    minutes = max(1, min(4, int(cfg.get("hot_lock_minutes", 1) or 1)))
    cutoff = int(time.time()) - minutes * 60
    active = HOT_ACTIVE_QUESTIONS.get(int(chat_id), {})
    for mid, data in list(active.items()):
        if bool(data.get("exam")):
            continue
        if int(data.get("ts", 0)) <= cutoff:
            active.pop(mid, None)


def hot_can_launch(chat_id: int) -> Tuple[bool, str]:
    hot_prune_old_active(chat_id)
    pending = hot_active_pending(chat_id)
    if not pending:
        return True, ""
    _, data = pending
    if bool(data.get("exam")):
        return False, "Hay un /examen activo. Primero debe responder la pregunta actual."
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_lock_mode", "interval"))
    if mode == "answer":
        return False, "Ya hay una preguntita/reto activo. Primero hay que responderlo."
    minutes = max(1, min(4, int(cfg.get("hot_lock_minutes", 1) or 1)))
    age = int(time.time()) - int(data.get("ts", 0) or 0)
    left = max(1, minutes * 60 - age)
    return False, f"Ya hay una preguntita/reto activo. Espera {left}s o que responda la persona marcada."


def hot_register_question(chat_id: int, message_id: int, target_user, level: int = 1, kind: str = "question", *, exam: bool = False, exam_step: int = 0) -> None:
    by_message = HOT_ACTIVE_QUESTIONS.setdefault(int(chat_id), {})
    by_message[int(message_id)] = {
        "target_id": int(target_user.id),
        "target_name": display_name(target_user),
        "level": max(1, min(5, int(level or 1))),
        "kind": "challenge" if kind == "challenge" else "question",
        "exam": bool(exam),
        "exam_step": int(exam_step or 0),
        "ts": int(time.time()),
    }


async def hot_launch_item(context, chat_id: int, target_user, *, kind: str = "question", level: Optional[int] = None, automatic: bool = False, prefix: str = ""):
    level = int(level if level is not None else hot_pick_level(chat_id, automatic=automatic))
    kind = "challenge" if kind == "challenge" else "question"
    item = hot_get_item(chat_id, level, kind)
    if kind == "challenge":
        title = "Retito automático" if automatic else "Retito"
        icon = "🎲"
    else:
        title = "Preguntita automática" if automatic else "Preguntita"
        icon = "💬"
    text = (
        f"🎯 {target_user.mention_html()}\n\n"
        f"{prefix}<b>{title} · Nivel {level}</b>\n\n"
        f"{icon} {h(item)}"
    )
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    await register_bot_message(chat_id, msg.message_id)
    hot_register_question(chat_id, msg.message_id, target_user, level, kind=kind)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id))
    return msg


async def preguntita_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    hot_activity_remember(chat_id, update.effective_user)
    ok, reason = hot_can_launch(chat_id)
    if not ok:
        msg = await update.message.reply_text(reason)
        asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=10))
        return
    target = update.message.reply_to_message.from_user if update.message.reply_to_message else update.effective_user
    await hot_launch_item(context, chat_id, target, kind="question", level=hot_pick_level(chat_id, automatic=False))


async def retito_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    hot_activity_remember(chat_id, update.effective_user)
    ok, reason = hot_can_launch(chat_id)
    if not ok:
        msg = await update.message.reply_text(reason)
        asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=10))
        return
    target = update.message.reply_to_message.from_user if update.message.reply_to_message else update.effective_user
    await hot_launch_item(context, chat_id, target, kind="challenge", level=hot_pick_level(chat_id, automatic=False))


async def examen_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    hot_activity_remember(chat_id, update.effective_user)
    ok, reason = hot_can_launch(chat_id)
    if not ok:
        msg = await update.message.reply_text(reason)
        asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=10))
        return
    target = update.message.reply_to_message.from_user if update.message.reply_to_message else update.effective_user
    HOT_EXAMS[chat_id] = {"target_id": int(target.id), "step": 1}
    question = hot_get_question(chat_id, 1)
    text = (
        f"📝 <b>EXAMEN HOT 1/5</b>\n"
        f"🎯 {target.mention_html()}\n\n"
        f"🔥 <b>Nivel 1</b>\n\n"
        f"💬 {h(question)}\n\n"
        "Responde a este mensaje para pasar a la 2/5."
    )
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    await register_bot_message(chat_id, msg.message_id)
    hot_register_question(chat_id, msg.message_id, target, 1, kind="question", exam=True, exam_step=1)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id))


async def hot_send_exam_next(context, chat_id: int, target_user, next_step: int) -> None:
    if next_step > 5:
        HOT_EXAMS.pop(chat_id, None)
        done = await context.bot.send_message(chat_id=chat_id, text=f"✅ {target_user.mention_html()} ha completado el examen 5/5.", parse_mode=ParseMode.HTML)
        await register_bot_message(chat_id, done.message_id)
        asyncio.create_task(hot_safe_delete(context, chat_id, done.message_id, delay=int(hot_cfg(chat_id).get("hot_points_delete_seconds", 5) or 5)))
        return
    question = hot_get_question(chat_id, next_step)
    suffix = f"Responde a este mensaje para pasar a la {next_step + 1}/5." if next_step < 5 else "Responde a este mensaje para terminar el examen."
    text = (
        f"📝 <b>EXAMEN HOT {next_step}/5</b>\n"
        f"🎯 {target_user.mention_html()}\n\n"
        f"🔥 <b>Nivel {next_step}</b>\n\n"
        f"💬 {h(question)}\n\n"
        f"{suffix}"
    )
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    await register_bot_message(chat_id, msg.message_id)
    hot_register_question(chat_id, msg.message_id, target_user, next_step, kind="question", exam=True, exam_step=next_step)
    HOT_EXAMS[chat_id] = {"target_id": int(target_user.id), "step": next_step}
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id))


def hot_ranking_rows(chat_id: int, scope: str) -> List[Tuple[str, int]]:
    cfg = hot_cfg(chat_id)
    if scope == "daily":
        day, _ = hot_period_keys()
        ranking = cfg.setdefault("hot_ranking_daily", {}).get(day, {})
    elif scope == "weekly":
        _, week = hot_period_keys()
        ranking = cfg.setdefault("hot_ranking_weekly", {}).get(week, {})
    else:
        ranking = cfg.get("hot_ranking", {}) or {}
    rows = []
    for row in ranking.values():
        try:
            rows.append((str(row.get("name", "Usuario")), int(row.get("points", 0) or 0)))
        except Exception:
            continue
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows


async def ranking_hot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    await hot_delete_command_if_configured(update, context, chat_id)
    arg = (context.args[0].lower() if context.args else "general") if hasattr(context, "args") else "general"
    if arg in ("dia", "día", "diario", "diaria", "daily"):
        scope, title = "daily", "Ranking HOT diario"
    elif arg in ("semana", "semanal", "weekly"):
        scope, title = "weekly", "Ranking HOT semanal"
    else:
        scope, title = "general", "Ranking HOT general"
    rows = hot_ranking_rows(chat_id, scope)
    if not rows:
        await update.message.reply_html(f"🏆 <b>{h(title)}</b>\n\nTodavía no hay puntos.")
        return
    lines = [f"🏆 <b>{h(title)}</b>", ""]
    for i, (name, points) in enumerate(rows[:15], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        lines.append(f"{medal} <b>{h(name)}</b> — {points} pts")
    lines.append("\nVer también: <code>/rankinghot diario</code> · <code>/rankinghot semanal</code> · <code>/rankinghot general</code>")
    await update.message.reply_html("\n".join(lines))


def hot_config_text(chat_id: int) -> str:
    cfg = hot_cfg(chat_id)
    custom_total = sum(len(hot_custom_questions(chat_id, lvl)) for lvl in range(1, 6))
    custom_retos = sum(len(hot_custom_challenges(chat_id, lvl)) for lvl in range(1, 6))
    lock_mode = "obligar respuesta" if str(cfg.get("hot_lock_mode", "interval")) == "answer" else f"intervalo {int(cfg.get('hot_lock_minutes', 1) or 1)} min"
    return (
        "🔥 <b>Config Preguntitas y Retitos</b>\n\n"
        f"Grupo configurado: <code>{chat_id}</code>\n"
        f"Modo: <b>{h(cfg.get('hot_mode', 'manual'))}</b> · Nivel: <b>{int(cfg.get('hot_level', 1) or 1)}</b>\n"
        f"Automático: <b>{bool_label(cfg.get('hot_auto_enabled', False))}</b> · Intervalo auto: <b>{int(cfg.get('hot_auto_interval_seconds', 180) or 180)}s</b>\n"
        f"Auto mezcla retos: <b>{bool_label(cfg.get('hot_auto_mix_challenges', True))}</b> · cada <b>{int(cfg.get('hot_auto_challenge_every', 5) or 5)}</b> turnos\n"
        f"Bloqueo nuevas invocaciones: <b>{h(lock_mode)}</b>\n"
        f"Mínimo actividad auto: <b>{h(hot_auto_activity_label(chat_id))}</b>\n"
        f"Borrado HOT: <b>{bool_label(cfg.get('hot_auto_delete_enabled', True))}</b> · Preguntas: <b>{int(cfg.get('hot_auto_delete_seconds', 90) or 90)}s</b> · Puntos: <b>{int(cfg.get('hot_points_delete_seconds', 5) or 5)}s</b>\n"
        f"Borrado comandos HOT: <b>{h(hot_command_delete_label(chat_id))}</b>\n"
        f"Preguntas base: <b>{sum(len(v) for v in HOT_BASE_QUESTIONS.values())}</b> · Retos base: <b>{sum(len(v) for v in HOT_BASE_CHALLENGES.values())}</b>\n"
        f"Preguntas añadidas: <b>{custom_total}</b> · Retos añadidos: <b>{custom_retos}</b>\n\n"
        "Comandos:\n"
        "<code>/preguntita</code> · <code>/retito</code> · <code>/examen</code>\n"
        "<code>/rankinghot</code> · <code>/rankinghot diario</code> · <code>/rankinghot semanal</code>\n"
        "<code>/hotfijar</code> · fija el botón de juego en el grupo\n"
        "<code>/addpregunta 2 texto</code> · <code>/addreto 2 texto</code> · <code>/addmasivo</code> · <code>/addretos</code>"
    )


def hot_config_markup(chat_id: int) -> InlineKeyboardMarkup:
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_mode", "manual"))
    auto = bool(cfg.get("hot_auto_enabled", False))
    autodel = bool(cfg.get("hot_auto_delete_enabled", True))
    cmd_mode = str(cfg.get("hot_command_delete_mode", "off"))
    lock_mode = str(cfg.get("hot_lock_mode", "interval"))
    rows = [
        [InlineKeyboardButton(("✅ " if mode == "manual" else "") + "Manual", callback_data="hot|mode|manual"),
         InlineKeyboardButton(("✅ " if mode == "random" else "") + "Aleatorio", callback_data="hot|mode|random")],
        [InlineKeyboardButton(f"Nivel {i}" + (" ✅" if int(cfg.get("hot_level", 1) or 1) == i else ""), callback_data=f"hot|level|{i}") for i in (1, 2, 3)],
        [InlineKeyboardButton(f"Nivel {i}" + (" ✅" if int(cfg.get("hot_level", 1) or 1) == i else ""), callback_data=f"hot|level|{i}") for i in (4, 5)],
        [InlineKeyboardButton("Auto " + ("ON ✅" if auto else "OFF ❌"), callback_data="hot|toggle|auto"),
         InlineKeyboardButton("Auto retos " + ("ON" if bool(cfg.get("hot_auto_mix_challenges", True)) else "OFF"), callback_data="hot|toggle|autoreto")],
        [InlineKeyboardButton("Mix 4P+1R ✅", callback_data="hot|mix|5"),
         InlineKeyboardButton("Mix 6P+1R", callback_data="hot|mix|7")],
        [InlineKeyboardButton("Auto 60s", callback_data="hot|interval|60"), InlineKeyboardButton("180s", callback_data="hot|interval|180")],
        [InlineKeyboardButton("Auto 300s", callback_data="hot|interval|300"), InlineKeyboardButton("700s", callback_data="hot|interval|700")],
        [InlineKeyboardButton("Act. normal 5/2", callback_data="hot|activity|5_2"), InlineKeyboardButton("Act. alta 8/3", callback_data="hot|activity|8_3"), InlineKeyboardButton("Act. 🔥 12/3", callback_data="hot|activity|12_3")],
        [InlineKeyboardButton("Bloq. respuesta" + (" ✅" if lock_mode == "answer" else ""), callback_data="hot|lockmode|answer"),
         InlineKeyboardButton("Bloq. intervalo" + (" ✅" if lock_mode == "interval" else ""), callback_data="hot|lockmode|interval")],
        [InlineKeyboardButton("1 min", callback_data="hot|lockmin|1"), InlineKeyboardButton("2 min", callback_data="hot|lockmin|2"), InlineKeyboardButton("3 min", callback_data="hot|lockmin|3"), InlineKeyboardButton("4 min", callback_data="hot|lockmin|4")],
        [InlineKeyboardButton("Borrado HOT " + ("ON ✅" if autodel else "OFF ❌"), callback_data="hot|toggle|delete")],
        [InlineKeyboardButton("Preguntas 30s", callback_data="hot|delete_after|30"), InlineKeyboardButton("90s", callback_data="hot|delete_after|90"), InlineKeyboardButton("700s", callback_data="hot|delete_after|700")],
        [InlineKeyboardButton("Puntos 5s", callback_data="hot|points_delete|5"), InlineKeyboardButton("20s", callback_data="hot|points_delete|20"), InlineKeyboardButton("45s", callback_data="hot|points_delete|45")],
        [InlineKeyboardButton("Cmd OFF" + (" ✅" if cmd_mode == "off" else ""), callback_data="hot|cmddelete|off"), InlineKeyboardButton("Cmd al ejecutar" + (" ✅" if cmd_mode == "instant" else ""), callback_data="hot|cmddelete|instant")],
        [InlineKeyboardButton("Cmd 20s", callback_data="hot|cmddelete|ttl20"), InlineKeyboardButton("Cmd 45s", callback_data="hot|cmddelete|ttl45")],
        [InlineKeyboardButton("✏️ Texto fijado", callback_data="hot|pintext|0"), InlineKeyboardButton("🔘 Texto botón", callback_data="hot|pinbutton|0")],
        [InlineKeyboardButton("📌 Fijar botón en grupo", callback_data="hot|pin|0")],
        [InlineKeyboardButton("🌐 Cambiar grupo", callback_data="hot|groups|0")],
        [InlineKeyboardButton("💾 Guardar y cerrar", callback_data="hot|close|save"), InlineKeyboardButton("🔄 Recargar", callback_data="hot|refresh|0")],
    ]
    return InlineKeyboardMarkup(rows)


async def hot_pin_prompt(context, chat_id: int) -> None:
    cfg = hot_cfg(chat_id)
    button_text = str(cfg.get("hot_pin_button_text") or "🎲 Enviar preguntita / retito").strip()[:64] or "🎲 Enviar preguntita / retito"
    markup = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, callback_data="hotpublic|random")]])
    msg = await context.bot.send_message(chat_id=chat_id, text=str(cfg.get("hot_pin_text")), parse_mode=ParseMode.HTML, reply_markup=markup)
    await register_bot_message(chat_id, msg.message_id)
    try:
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        logger.exception("No se pudo fijar el panel HOT")


async def hotfijar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    await hot_delete_command_if_configured(update, context, chat_id)
    await hot_pin_prompt(context, chat_id)


async def hot_public_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return
    if not (query.data or "").startswith("hotpublic|"):
        return
    chat_id = int(update.effective_chat.id)
    ok, reason = hot_can_launch(chat_id)
    if not ok:
        await query.answer(reason, show_alert=True)
        return
    kind = "challenge" if secrets.randbelow(5) == 0 else "question"  # 4 preguntas + 1 reto aprox.
    await query.answer("Lanzado ✅")
    await hot_launch_item(context, chat_id, update.effective_user, kind=kind, level=hot_pick_level(chat_id, automatic=False))


async def hot_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("hot|"):
        return
    chat_id = hot_target_chat_id(update)
    if chat_id is None:
        await query.answer("Primero vincula un grupo con /hotgrupo.", show_alert=True)
        return
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores.", show_alert=True)
        return
    parts = data.split("|")
    cfg = hot_cfg(chat_id)
    try:
        action = parts[1]
        value = parts[2] if len(parts) > 2 else ""
        if action == "groups":
            groups = hot_private_groups_for(update.effective_user.id)
            if not groups:
                await query.answer("No tienes grupos vinculados. Usa /hotgrupo dentro del grupo.", show_alert=True)
                return
            await query.answer("Elige grupo")
            await query.edit_message_text("🔥 <b>Elige el grupo que quieres configurar</b>", reply_markup=hot_group_selector_markup(update.effective_user.id), parse_mode=ParseMode.HTML)
            return
        if action == "close":
            save_all_states()
            await query.answer("Guardado ✅")
            try:
                await query.message.delete()
            except Exception:
                try:
                    await query.edit_message_text("✅ Configuración guardada.")
                except Exception:
                    pass
            return
        if action == "pintext":
            set_config_pending(update.effective_user.id, {"kind": "hot_pin_text", "chat_id": chat_id, "return_block": "hot"})
            await query.message.reply_html("✏️ Envíame el texto que quieres fijar arriba para Preguntitas/Retitos.\n\nPuedes pegar emojis normales y también emojis premium/custom; si Telegram los entrega como entidad, los guardaré en HTML.")
            await query.answer("Envíame el texto por privado")
            return
        if action == "pinbutton":
            set_config_pending(update.effective_user.id, {"kind": "hot_pin_button", "chat_id": chat_id, "return_block": "hot"})
            await query.message.reply_html("🔘 Envíame el texto del botón. Ejemplo: <code>🎲 Jugar ahora</code>\n\nNota: Telegram no permite entidades/emoji premium dentro del texto de botones inline; se guardará el texto visible/fallback.")
            await query.answer("Envíame el botón por privado")
            return
        if action == "pin":
            await hot_pin_prompt(context, chat_id)
            await query.answer("Mensaje fijado en el grupo ✅")
            return
        if action == "mode":
            cfg["hot_mode"] = value if value in ("manual", "random", "auto") else "manual"
        elif action == "level":
            cfg["hot_level"] = max(1, min(5, int(value)))
            cfg["hot_mode"] = "manual"
        elif action == "toggle":
            if value == "include5":
                cfg["hot_random_include_level5"] = not bool(cfg.get("hot_random_include_level5", False))
            elif value == "auto":
                cfg["hot_auto_enabled"] = not bool(cfg.get("hot_auto_enabled", False))
                cfg["hot_mode"] = "auto" if cfg["hot_auto_enabled"] else cfg.get("hot_mode", "manual")
            elif value == "autoreto":
                cfg["hot_auto_mix_challenges"] = not bool(cfg.get("hot_auto_mix_challenges", True))
            elif value == "delete":
                cfg["hot_auto_delete_enabled"] = not bool(cfg.get("hot_auto_delete_enabled", True))
        elif action == "mix":
            cfg["hot_auto_challenge_every"] = max(2, int(value))
        elif action == "interval":
            cfg["hot_auto_interval_seconds"] = max(30, int(value))
        elif action == "activity":
            min_msgs, min_users = value.split("_", 1)
            cfg["hot_auto_min_messages"] = max(1, int(min_msgs))
            cfg["hot_auto_min_users"] = max(1, int(min_users))
        elif action == "delete_after":
            cfg["hot_auto_delete_seconds"] = max(5, int(value))
        elif action == "points_delete":
            cfg["hot_points_delete_seconds"] = max(1, int(value))
        elif action == "lockmode":
            cfg["hot_lock_mode"] = value if value in ("interval", "answer") else "interval"
        elif action == "lockmin":
            cfg["hot_lock_minutes"] = max(1, min(4, int(value)))
            cfg["hot_lock_mode"] = "interval"
        elif action == "cmddelete":
            if value == "off":
                cfg["hot_command_delete_mode"] = "off"
            elif value == "instant":
                cfg["hot_command_delete_mode"] = "instant"
            elif value.startswith("ttl"):
                cfg["hot_command_delete_mode"] = "ttl"
                cfg["hot_command_delete_seconds"] = max(1, int(value.replace("ttl", "") or 20))
        save_all_states()
        await query.answer("Actualizado ✅")
        try:
            await query.edit_message_text(hot_config_text(chat_id), reply_markup=hot_config_markup(chat_id), parse_mode=ParseMode.HTML)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
    except Exception:
        logger.exception("Error en callback HOT")
        await query.answer("No se pudo actualizar.", show_alert=True)


async def hot_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    user = update.effective_user
    if getattr(user, "is_bot", False):
        return
    hot_activity_remember(chat_id, user)

    pending = HOT_PENDING_ADD.get(user.id)
    if pending and pending.get("mode") == "bulk_wait_text" and int(pending.get("input_chat_id", chat_id)) == chat_id:
        target_chat_id = int(pending.get("target_chat_id", chat_id))
        if await is_admin(context, target_chat_id, user.id):
            lines = [line.strip() for line in (update.message.text or "").replace("|", "\n").splitlines() if line.strip()]
            if not lines:
                await update.message.reply_text("No he detectado elementos válidos.")
                return
            pending["mode"] = "bulk_choose_level"
            pending["questions"] = lines
            rows = [[InlineKeyboardButton(f"Nivel {i}", callback_data=f"hotadd|{i}") for i in (1, 2, 3)], [InlineKeyboardButton("Nivel 4", callback_data="hotadd|4"), InlineKeyboardButton("Nivel 5", callback_data="hotadd|5")]]
            label = "retos" if pending.get("kind") == "challenge" else "preguntas"
            await update.message.reply_html(f"📊 Detectados <b>{len(lines)}</b> {label}. Elige nivel:", reply_markup=InlineKeyboardMarkup(rows))
            return

    reply = update.message.reply_to_message
    if not reply:
        return
    active = HOT_ACTIVE_QUESTIONS.get(chat_id, {})
    data = active.get(int(reply.message_id))
    if not data:
        return
    if int(data.get("target_id")) != int(user.id):
        return
    text = (update.message.text or "").strip()
    if len(text) < 3:
        return
    level = max(1, min(5, int(data.get("level", 1) or 1)))
    kind = str(data.get("kind", "question"))
    bonus = hot_answer_quality_bonus(text)
    points = level + bonus
    total = hot_add_points(chat_id, user, points)
    active.pop(int(reply.message_id), None)
    extra = f" + bonus {bonus}" if bonus else ""
    label = "reto" if kind == "challenge" else "pregunta"
    msg = await update.message.reply_html(f"🔥 <b>{h(display_name(user))}</b> +{points} pts <i>({label} nivel {level}{extra})</i>\n🏆 Total: <b>{total}</b>")
    await register_bot_message(chat_id, msg.message_id)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=int(hot_cfg(chat_id).get("hot_points_delete_seconds", 5) or 5)))

    if bool(data.get("exam")):
        step = int(data.get("exam_step", level) or level)
        # Usuario real ya tiene mention_html/display_name.
        await hot_send_exam_next(context, chat_id, user, step + 1)


async def hot_auto_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(10)
        now = int(time.time())
        for chat_id in list(STATE_CACHE.keys()):
            try:
                cfg = hot_cfg(chat_id)
                if not bool(cfg.get("hot_auto_enabled", False)):
                    continue
                interval = max(30, int(cfg.get("hot_auto_interval_seconds", 180) or 180))
                if now - int(cfg.get("hot_last_auto_ts", 0) or 0) < interval:
                    continue
                if not hot_auto_has_enough_interaction(chat_id, now):
                    continue
                ok, _reason = hot_can_launch(chat_id)
                if not ok:
                    continue
                users = [u for u in HOT_RECENT_ACTIVITY.get(chat_id, {}).values() if now - int(u.get("ts", 0)) <= int(cfg.get("hot_auto_activity_window_seconds", 180) or 180)]
                if not users:
                    continue
                target = secrets.choice(users)
                level = hot_pick_level(chat_id, automatic=True)
                kind = hot_pick_auto_kind(chat_id)
                class _U:
                    def mention_html(self_inner):
                        return f'<a href="tg://user?id={int(self_inner.id)}">{h(self_inner.first_name or "Usuario")}</a>'
                fake = _U()
                fake.id = int(target["id"])
                fake.first_name = str(target.get("first_name") or target.get("name") or "Usuario")
                fake.last_name = ""
                fake.username = None
                await hot_launch_item(application, chat_id, fake, kind=kind, level=level, automatic=True, prefix="🤖 ")
                cfg["hot_last_auto_ts"] = now
                save_all_states()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error en hot_auto_loop para chat %s", chat_id)

# =========================
# FIN HOT MODULE V6
# =========================

# =========================
# FIN HOT MODULE
# =========================



# =========================
# V7 - HOT mejorado, recurrentes, pregonero sync/auto y resumen
# =========================
import re as _re_v7
from datetime import datetime as _dt_v7, timedelta as _td_v7

RECURRING_TASK: Optional[asyncio.Task] = None
PREGONERO_AUTO_TASK: Optional[asyncio.Task] = None


def _cycle_pick(seq: List[str], index: int, default: str = "") -> str:
    if not seq:
        return default
    return seq[index % len(seq)]


def _unique_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        clean = str(item).strip()
        if not clean:
            continue
        key = _re_v7.sub(r"\s+", " ", clean.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def _build_question_level(level: int, seeds: List[str], target: int = 200) -> List[str]:
    people = [
        "alguien del grupo", "la última persona que escribió", "quien más habla por aquí", "quien menos habla del grupo",
        "la persona más misteriosa del grupo", "quien te da más curiosidad", "quien siempre aparece en el momento justo",
        "quien manda más audios", "quien tiene mejor humor", "quien parece más tranquilo/a", "quien parece más peligroso/a",
        "quien te respondería un privado rápido", "quien tiene más pinta de liarla", "quien parece más de plan improvisado",
        "quien más te sorprende cuando escribe", "quien pondría la mejor canción", "quien parece más de noche que de día",
    ]
    hooks = {
        1: [
            "¿Qué es lo primero que te llama la atención de {p}?",
            "¿Qué detalle sencillo te haría fijarte en {p}?",
            "¿Qué gesto de {p} te parecería bonito sin ser demasiado obvio?",
            "¿Qué plan tranquilo harías con {p}?",
            "¿Qué canción le dedicarías a {p} sin que sonara intensa?",
            "¿Qué mensaje amable le mandarías ahora a {p}?",
            "¿Qué te daría curiosidad saber de {p}?",
            "¿Qué emoji usarías para describir a {p}?",
            "¿Qué conversación te gustaría tener con {p}?",
            "¿Qué te haría pensar ‘esta persona tiene buen rollo’ de {p}?",
        ],
        2: [
            "¿A quién del grupo mirarías dos veces si coincidís en persona?",
            "¿Con quién del grupo tendrías una charla hasta tarde sin aburrirte?",
            "¿A quién del grupo invitarías a tomar algo si surgiera el plan?",
            "¿Qué indirecta suave le lanzarías a {p}?",
            "¿Qué te haría contestar rápido a un privado de {p}?",
            "¿Con quién del grupo tendrías más química de conversación?",
            "¿Quién del grupo te parece más interesante cuando se suelta?",
            "¿A quién le mandarías una canción con doble sentido suave?",
            "¿Quién del grupo parece que liga mejor por chat?",
            "¿Qué frase te gustaría que te dijera {p}?",
        ],
        3: [
            "¿A quién del grupo le harías una pregunta picante sin avisar?",
            "¿Con quién del grupo jugarías a verdad o reto sin pensarlo mucho?",
            "¿Qué indirecta clara le lanzarías a {p} si hoy estuvieras valiente?",
            "¿Quién del grupo parece inocente pero tiene mucha picardía?",
            "¿Qué te daría más morbo: un audio, una mirada o un mensaje privado de {p}?",
            "¿Con quién del grupo crees que habría tensión si hablarais a solas?",
            "¿Qué pregunta con doble sentido le harías a {p}?",
            "¿A quién del grupo mandarías un ‘tenemos que hablar’ con sonrisa?",
            "¿Quién del grupo te parece más de provocar sin que se note?",
            "¿Qué confesión picante harías aquí sin dar nombres?",
        ],
        4: [
            "¿A quién del grupo le dirías ‘tú tienes peligro’ y por qué?",
            "¿Con quién del grupo tendrías una conversación sin filtros?",
            "¿Qué pregunta fuerte le harías a {p} si tuviera que responder sí o sí?",
            "¿Quién del grupo sería una tentación difícil de ignorar?",
            "¿Qué mensaje privado de {p} te pondría realmente nervioso/a?",
            "¿Con quién del grupo habría más tensión si nadie pudiera hacer capturas?",
            "¿Qué secreto atrevido contarías si todos jugaran limpio?",
            "¿A quién del grupo le lanzarías una indirecta demasiado evidente?",
            "¿Quién del grupo crees que guarda historias fuertes?",
            "¿Qué harías si el grupo activara modo noche sin vergüenza?",
        ],
        5: [
            "¿A quién del grupo le mandarías una pregunta HOT sin escapatoria?",
            "¿Con quién del grupo tendrías una conversación privada muy subida de tono?",
            "¿Qué deseo HOT confesarías aquí, sin decir nombres?",
            "¿Quién del grupo te parece una tentación seria?",
            "¿Qué mensaje de {p} podría hacerte perder bastante el control?",
            "¿Con quién del grupo habría más química si la conversación se calentara?",
            "¿Qué fantasía HOT contarías solo si nadie pudiera hacer capturas?",
            "¿A quién del grupo elegirías para una noche de confesiones picantes?",
            "¿Qué pregunta nivel 5 te dejaría contra la pared?",
            "¿Qué te da más morbo: lo secreto, lo prohibido o lo inesperado con {p}?",
        ],
    }
    endings = {
        1: ["Responde sin pensarlo demasiado.", "Que sea sincero, pero suave.", "Nada de esconderse.", "El grupo quiere saberlo."],
        2: ["Sin dar más vueltas.", "Puedes decir nombre o dejar pista.", "Con una indirecta vale.", "Sé claro/a, pero elegante."],
        3: ["Sube un poco el tono.", "Si no quieres dar nombre, da pista.", "Aquí empieza el juego.", "No vale responder como político/a."],
        4: ["Contesta con valentía.", "Sin pasarte, pero sin esconderte.", "Nivel serio: toca mojarse.", "Una pista mínima, al menos."],
        5: ["Nivel HOT: responde con cabeza, pero responde.", "Sin contenido íntimo real, pero con picardía.", "Si te quemas, puedes decir ‘paso’. ", "Aquí se viene a jugar fuerte."],
    }
    rows = list(seeds)
    i = 0
    while len(rows) < target:
        template = _cycle_pick(hooks[level], i)
        p = _cycle_pick(people, i // len(hooks[level]))
        ending = _cycle_pick(endings[level], i // (len(hooks[level]) * len(people)))
        rows.append(f"{template.format(p=p)} {ending}".strip())
        i += 1
    return _unique_keep_order(rows)[:target]


def _hot_natural_questions() -> Dict[int, List[str]]:
    """1000 preguntitas: 200 por nivel, estilo informal y claramente niveladas."""
    seeds: Dict[int, List[str]] = {
        1: [
            "¿Qué es lo primero que te atrae de alguien?",
            "¿Prefieres dar el primer paso o que lo den?",
            "¿Has tenido un crush reciente?",
            "¿Te gusta el misterio o la claridad?",
            "¿Qué te hace sonreír sin querer?",
            "¿Alguna vez has coqueteado por aburrimiento?",
            "¿Qué tipo de mirada te conquista?",
            "¿Te gusta provocar o evitar?",
            "¿Quién del grupo parece más interesante?",
            "¿Te han tirado indirectas que no pillaste?",
            "¿Te gusta el contacto físico rápido o lento?",
            "¿Te consideras seductor/a?",
            "¿Qué outfit te parece más atractivo?",
            "¿Te gusta mirar o que te miren?",
            "¿Prefieres personalidad o físico?",
            "¿Has tenido amor a primera vista?",
            "¿Qué te parece irresistible?",
            "¿Te gusta la tensión previa?",
            "¿Qué te pone nervioso/a, pero bien?",
            "¿Te gusta jugar o ir directo?",
            "¿Qué miembro del grupo te da más buen rollo?",
            "¿A quién del grupo invitarías a un café sin pensarlo?",
            "¿Qué canción describe tu forma de ligar?",
            "¿Qué mensaje te hace contestar con sonrisa?",
            "¿Qué gesto simple te conquista?",
            "¿Quién del grupo parece más de plan tranquilo?",
            "¿Qué te hace perder el interés rápido?",
            "¿Qué prefieres: audio bonito o mensaje bien escrito?",
            "¿Quién del grupo crees que sería buen compañero/a de viaje?",
            "¿Qué frase te gustaría recibir hoy?",
            "¿A quién del grupo le pedirías consejo sentimental?",
            "¿Quién tiene pinta de guardar buenas historias?",
            "¿Qué te da más curiosidad de la gente del grupo?",
            "¿Qué plan te parece perfecto para romper el hielo?",
            "¿Quién del grupo tiene pinta de escuchar bien?",
            "¿Qué te gusta más: humor, misterio o ternura?",
            "¿Qué detalle pequeño te enamora un poco?",
            "¿Quién del grupo parece más dulce de lo que aparenta?",
            "¿Qué conversación te engancha sin darte cuenta?",
            "¿Qué te parece sexy sin ser obvio?",
        ],
        2: [
            "¿Te atrae alguien del grupo?",
            "¿Has besado a alguien que no esperabas?",
            "¿Cuál fue tu mejor beso?",
            "¿Te gusta besar con intensidad o suave?",
            "¿Has tenido un lío improvisado?",
            "¿Te han rechazado alguna vez?",
            "¿Te gusta el contacto físico rápido?",
            "¿Te gusta provocar tensión?",
            "¿Has tenido un rollo secreto?",
            "¿Te gusta el juego de seducción?",
            "¿Te han sorprendido con un beso?",
            "¿Te gusta besar en público?",
            "¿Prefieres pasión o control?",
            "¿Has tenido una cita rara?",
            "¿Te gusta el riesgo?",
            "¿Te gusta que te sorprendan?",
            "¿Te gusta jugar con los límites?",
            "¿Te han pillado en algo?",
            "¿Te gusta la improvisación?",
            "¿Has tenido química instantánea?",
            "¿A quién del grupo invitarías a tomar algo?",
            "¿Con quién del grupo tendrías una charla hasta tarde?",
            "¿Quién del grupo te da más curiosidad en privado?",
            "¿Qué indirecta lanzarías hoy sin decir para quién es?",
            "¿Quién del grupo parece que liga mejor?",
            "¿Qué te gana antes: una mirada o una conversación?",
            "¿A quién del grupo le mandarías una canción con mensaje?",
            "¿Quién te parece más interesante cuando se suelta?",
            "¿Qué tipo de privado te gustaría recibir?",
            "¿A quién del grupo ves con más chispa?",
            "¿Qué te haría sonrojar un poco en el chat?",
            "¿Quién del grupo tiene pinta de ser intenso/a cuando le interesa alguien?",
            "¿Qué prefieres: tonteo lento o ir dejando claras las cosas?",
            "¿A quién del grupo te gustaría conocer mejor fuera del chat?",
            "¿Qué frase te pone nervioso/a si viene de alguien que te gusta?",
            "¿Quién del grupo parece más de lanzar indirectas?",
            "¿Qué plan de dos personas te apetecería ahora?",
            "¿A quién del grupo le responderías aunque estuvieras ocupado/a?",
            "¿Qué te da más juego: audios, memes o privados?",
            "¿Quién del grupo te parece más magnético/a?",
        ],
        3: [
            "¿Has tenido sexo en sitio público?",
            "¿Cuál fue el lugar más raro?",
            "¿Has enviado fotos subidas de tono?",
            "¿Te gusta dominar o que te dominen?",
            "¿Te gusta lo prohibido?",
            "¿Has tenido una noche loca?",
            "¿Te han pillado en pleno momento?",
            "¿Has tenido algo sin planearlo?",
            "¿Te gusta experimentar?",
            "¿Te gusta lo inesperado cuando hay química?",
            "¿Te gusta romper normas?",
            "¿Te gusta el juego de poder?",
            "¿Has tenido una fantasía cumplida?",
            "¿Te gusta lo intenso o lento?",
            "¿Te gusta el control o perderlo?",
            "¿Te gusta lo atrevido?",
            "¿Te gusta lo impredecible?",
            "¿Te gusta repetir o variar?",
            "¿Te gusta improvisar?",
            "¿Te gusta provocar deseo?",
            "¿A quién del grupo le harías una pregunta picante?",
            "¿Quién del grupo parece inocente pero no lo es?",
            "¿Qué te da más morbo en un chat?",
            "¿Qué indirecta picante dejarías caer hoy?",
            "¿Con quién del grupo jugarías a verdad o reto?",
            "¿Qué mensaje privado te sacaría una sonrisa peligrosa?",
            "¿Quién del grupo tiene pinta de guardar secretos picantes?",
            "¿Qué te parece más tentador: misterio o descaro?",
            "¿A quién del grupo le mandarías un audio con doble sentido?",
            "¿Qué pregunta te daría vergüenza responder, pero responderías?",
            "¿Quién del grupo podría meterte en un lío divertido?",
            "¿Qué confesión picante harías sin dar nombres?",
            "¿Qué te sube más el tono: un mensaje directo o una indirecta fina?",
            "¿A quién del grupo elegirías para una confesión anónima?",
            "¿Qué te hace perder un poco el control cuando alguien te gusta?",
            "¿Quién del grupo sabe provocar sin hacerlo evidente?",
            "¿Qué prefieres: tensión por chat o tensión en persona?",
            "¿A quién del grupo le dirías ‘no empieces, que me conozco’?",
            "¿Qué pregunta picante le harías al grupo entero?",
            "¿Qué te da más curiosidad descubrir de alguien del grupo?",
        ],
        4: [
            "¿Has sido infiel?",
            "¿Te han sido infiel?",
            "¿Te excita lo prohibido?",
            "¿Te gusta dominar completamente?",
            "¿Has tenido algo con desconocido/a?",
            "¿Te gusta el riesgo extremo?",
            "¿Has tenido experiencias muy locas?",
            "¿Te gusta provocar situaciones?",
            "¿Te gusta el límite?",
            "¿Te gusta el juego psicológico fuerte?",
            "¿Has tenido experiencias intensas?",
            "¿Te gusta el control total?",
            "¿Te gusta lo extremo?",
            "¿Has tenido una experiencia que repetirías siempre?",
            "¿Te gusta el riesgo emocional alto?",
            "¿Te gusta la adrenalina máxima?",
            "¿Te gusta lo impredecible?",
            "¿Te gusta lo salvaje o tranquilo?",
            "¿Te gusta lo directo?",
            "¿Te gusta lo sin filtros?",
            "¿A quién del grupo le dirías ‘tú tienes peligro’?",
            "¿Con quién del grupo tendrías una conversación sin filtros?",
            "¿Qué mensaje privado te pondría nervioso/a de verdad?",
            "¿Quién del grupo sería una tentación difícil de ignorar?",
            "¿Qué pregunta fuerte responderías solo si todos responden?",
            "¿A quién del grupo le lanzarías una indirecta muy clara?",
            "¿Qué te cuesta admitir cuando alguien te atrae mucho?",
            "¿Quién del grupo parece más de jugar con fuego?",
            "¿Qué conversación te gustaría tener en privado, sin detalles?",
            "¿Qué te da más peligro: curiosidad, confianza o una copa de más?",
            "¿Quién del grupo tiene más cara de secreto?",
            "¿Qué cosa te da morbo admitir, pero sin pasarte?",
            "¿Quién crees que sería más atrevido/a en un reto?",
            "¿Qué frase te dejaría pensando toda la noche?",
            "¿Con quién del grupo habría más tensión si os dejaran solos?",
            "¿Qué harías si el grupo tuviera modo noche sin capturas?",
            "¿Quién del grupo crees que tiene historias subidas de tono?",
            "¿A quién del grupo no te conviene tener demasiado cerca?",
            "¿Qué te hace cruzar la línea de la curiosidad?",
            "¿Qué parte de ti sale cuando hay confianza de verdad?",
        ],
        5: [
            "¿Cuál es tu mayor secreto sexual?",
            "¿Qué fantasía no has contado?",
            "¿Qué te hace perder el control total?",
            "¿Te excita lo prohibido extremo?",
            "¿Qué harías sin consecuencias?",
            "¿Te gusta el riesgo absoluto?",
            "¿Has cruzado límites importantes?",
            "¿Te gusta lo sin control total?",
            "¿Qué experiencia repetirías mil veces?",
            "¿Qué experiencia no repetirías jamás?",
            "¿Qué te da más morbo?",
            "¿Qué te da más miedo y deseo?",
            "¿Qué te hace perder la cabeza?",
            "¿Qué no puedes controlar?",
            "¿Qué te vuelve loco/a?",
            "¿Qué te hace actuar sin pensar?",
            "¿Qué te rompe el control?",
            "¿Qué te hace cruzar la línea?",
            "¿Qué te empuja al límite?",
            "¿Qué te hace no parar?",
            "¿A quién del grupo le harías una pregunta HOT sin escapatoria?",
            "¿Quién del grupo te parece una tentación seria?",
            "¿Qué deseo HOT confesarías aquí sin decir nombres?",
            "¿Con quién del grupo tendrías una conversación privada muy subida de tono?",
            "¿Qué mensaje privado te encendería bastante?",
            "¿A quién del grupo elegirías para una noche de confesiones picantes?",
            "¿Qué pregunta nivel 5 te pondría contra las cuerdas?",
            "¿Quién del grupo crees que tiene el lado más travieso?",
            "¿Qué secreto HOT contarías si nadie pudiera hacer capturas?",
            "¿Con quién del grupo habría más química si sube el tono?",
            "¿A quién del grupo mandarías una canción muy indirecta?",
            "¿Qué cosa HOT no responderías salvo que te reten?",
            "¿Qué te da más morbo: lo prohibido, lo secreto o lo inesperado?",
            "¿Quién del grupo te parece más peligroso/a para perder el control?",
            "¿Qué frase HOT te gustaría recibir por privado?",
            "¿Qué fantasía con tensión, sin dar nombres, te cuesta reconocer?",
            "¿Quién del grupo crees que besa mejor y por qué?",
            "¿Qué confesión picante dejarías caer sin dar nombres?",
            "¿A quién del grupo le dirías ‘contigo me callo, pero pienso’?",
            "¿Qué harías si esta noche el grupo jugara sin vergüenza?",
        ],
    }
    return {level: _build_question_level(level, seeds[level], 200) for level in range(1, 6)}


def _build_challenge_level(level: int, seeds: List[str], target: int = 200) -> List[str]:
    people = [
        "la última persona que escribió", "alguien del grupo", "quien te dé más curiosidad", "quien mande más audios",
        "la persona más misteriosa", "quien parezca más divertido/a", "quien tenga más pinta de liarla",
        "quien te caiga bien aunque habléis poco", "quien parezca más atrevido/a", "quien elija el grupo",
    ]
    actions = {
        1: [
            "manda una canción que describa tu día", "manda un emoji y explica por qué", "di algo bueno de {p}",
            "manda una foto no íntima de tu galería que tenga historia", "elige a alguien para hacer equipo en un plan tranquilo",
            "cuenta una mini anécdota graciosa", "manda un sticker que defina al grupo", "recomienda una serie o canción a {p}",
        ],
        2: [
            "manda una canción con indirecta suave para {p}", "di qué plan harías con {p}", "manda un audio de 5 segundos saludando con flow",
            "di quién del grupo te da más curiosidad y por qué", "confiesa una manía tuya al ligar", "elige a alguien para una copa imaginaria",
            "manda un meme que usarías para romper el hielo", "di una frase que te gustaría recibir por privado",
        ],
        3: [
            "manda una canción con doble sentido y di ‘sin dar nombres’", "hazle una pregunta picante a {p}",
            "manda un audio diciendo una indirecta elegante", "di quién del grupo parece inocente pero no lo es",
            "confiesa algo que te dé vergüenza pero sea divertido", "elige a alguien para jugar verdad o reto",
            "manda una foto no íntima de tu galería que dé conversación", "di qué te pone nervioso/a en un chat",
        ],
        4: [
            "lanza una indirecta clara a {p}, sin faltar", "manda un audio de 7 segundos diciendo ‘tú tienes peligro’",
            "di una confesión atrevida sin nombres", "elige a alguien del grupo para una conversación sin filtros",
            "di qué mensaje privado te pondría nervioso/a", "manda una canción de tensión fuerte",
            "reta a {p} a responderte una pregunta atrevida", "di quién te parece más tentación del grupo",
        ],
        5: [
            "lanza una pregunta HOT a {p}, con respeto", "confiesa un deseo HOT sin dar nombres",
            "manda una canción muy picante pero sin explicar demasiado", "di qué mensaje privado te encendería bastante",
            "elige a alguien para una noche imaginaria de confesiones HOT", "di una fantasía sin detalles explícitos ni nombres",
            "manda un audio diciendo una indirecta nivel 5", "reta a {p} a contestar una pregunta HOT o decir ‘paso’",
        ],
    }
    endings = {
        1: ["Tiene que ser fácil y de buen rollo.", "Nada intenso, solo para reírnos.", "Sin pensar demasiado."],
        2: ["Con chispa, pero elegante.", "Puedes dejar pista sin dar nombre.", "Que se note el tonteo suave."],
        3: ["Con picardía, sin pasarte.", "Si hay nombre, mejor; si no, pista.", "Aquí ya toca mojarse un poco."],
        4: ["Atrevido, pero sin faltar.", "Si no puedes, di ‘paso’ y elige a otro.", "Sin contenido íntimo real."],
        5: ["Nivel HOT, siempre con respeto.", "Nada de fotos íntimas ni contenido comprometido.", "Si te quema, puedes decir ‘paso’."],
    }
    rows = list(seeds)
    i = 0
    while len(rows) < target:
        p = _cycle_pick(people, i)
        action = _cycle_pick(actions[level], i // len(people)).format(p=p)
        ending = _cycle_pick(endings[level], i // (len(people) * len(actions[level])))
        rows.append(f"Reto: {action}. {ending}")
        i += 1
    return _unique_keep_order(rows)[:target]


def _hot_natural_challenges() -> Dict[int, List[str]]:
    """1000 retitos: 200 por nivel, pensados para interacción real de grupo."""
    seeds: Dict[int, List[str]] = {
        1: [
            "Manda un emoji que resuma tu día y explica por qué.",
            "Envía una canción que pondrías ahora en el grupo.",
            "Di algo bueno de la última persona que escribió.",
            "Manda un sticker que defina al grupo hoy.",
            "Cuenta una mini anécdota graciosa de esta semana.",
            "Elige a alguien del grupo para tomar un café imaginario.",
            "Manda una foto no íntima de tu galería que tenga una historia.",
            "Di qué plan sencillo propondrías para todos.",
            "Recomienda una canción a alguien del grupo.",
            "Manda un audio corto diciendo ‘hoy hay plan’.",
            "Di quién del grupo te cae bien aunque habléis poco.",
            "Manda un meme limpio que te represente.",
            "Elige a alguien para hacer equipo en un juego.",
            "Di una cualidad que te gusta ver en la gente.",
            "Manda una frase que anime el grupo.",
            "Di qué usuario debería hablar más.",
            "Manda una palabra que defina tu mood.",
            "Elige a alguien para pedirle una recomendación musical.",
            "Cuenta qué te hace quedarte en un chat.",
            "Di qué plan no fallaría este finde.",
        ],
        2: [
            "Manda una canción con indirecta suave para alguien del grupo.",
            "Di a quién invitarías a tomar algo, sin explicar demasiado.",
            "Manda un audio de 5 segundos diciendo una frase con flow.",
            "Elige a alguien del grupo para una charla hasta tarde.",
            "Di qué tipo de mensaje te hace contestar rápido.",
            "Manda una foto no íntima que parezca de plan improvisado.",
            "Lanza una indirecta elegante sin decir para quién es.",
            "Di quién del grupo tiene más pinta de ligar bien por chat.",
            "Manda una canción que usarías para romper el hielo.",
            "Elige a alguien para una copa imaginaria.",
            "Di algo que te parezca atractivo sin ser obvio.",
            "Manda un sticker con doble sentido suave.",
            "Pregunta a alguien del grupo cuál sería su plan ideal.",
            "Di quién te da más curiosidad conocer mejor.",
            "Cuenta una cita rara o divertida, sin nombres.",
            "Manda una frase que te gustaría recibir por privado.",
            "Elige a alguien del grupo para bailar una canción.",
            "Di si prefieres tonteo lento o directo.",
            "Manda un audio diciendo ‘eso habría que verlo’. ",
            "Di quién del grupo te parece más misterioso/a.",
        ],
        3: [
            "Hazle una pregunta picante a alguien del grupo.",
            "Manda una canción con doble sentido y no digas para quién es.",
            "Di quién parece inocente pero no lo es tanto.",
            "Manda un audio con una indirecta elegante.",
            "Confiesa algo que te dé vergüenza pero sea divertido.",
            "Elige a alguien para jugar verdad o reto.",
            "Di qué te da más morbo en un chat: misterio, tensión o descaro.",
            "Manda una foto no íntima de tu galería que dé conversación.",
            "Reta a alguien a decir una indirecta sin nombres.",
            "Di qué mensaje privado te haría sonreír demasiado.",
            "Pregunta a alguien si prefiere tensión por chat o en persona.",
            "Manda un sticker que parezca inocente pero no lo sea.",
            "Di quién del grupo tiene más picardía.",
            "Cuenta una confesión suave sin dar nombres.",
            "Manda una canción que diga lo que tú no dices.",
            "Elige a alguien para una conversación con doble sentido.",
            "Di una pregunta que te daría vergüenza responder.",
            "Manda un audio diciendo ‘no voy a decir nombres’. ",
            "Reta a alguien a mandar una canción con mensaje oculto.",
            "Di quién del grupo sabe provocar sin hacerlo evidente.",
        ],
        4: [
            "Lanza una indirecta clara a alguien del grupo, con respeto.",
            "Di quién del grupo tiene peligro y por qué.",
            "Manda una canción de tensión fuerte.",
            "Elige a alguien para una conversación sin filtros.",
            "Confiesa algo atrevido sin dar nombres.",
            "Manda un audio diciendo ‘tú tienes peligro’. ",
            "Pregunta a alguien una cosa fuerte, pero sin faltar.",
            "Di qué mensaje privado te pondría nervioso/a.",
            "Reta a alguien a decir una verdad atrevida.",
            "Manda una foto no íntima que parezca de noche de lío.",
            "Di quién te parece más tentación del grupo.",
            "Elige a alguien para una noche imaginaria de secretos.",
            "Di qué pregunta fuerte responderías si todos responden.",
            "Manda una frase que dejaría pensando a alguien.",
            "Reta a alguien a responder con una pista, no con un sí/no.",
            "Di qué te da más peligro: curiosidad, confianza o una copa de más.",
            "Manda un audio diciendo una confesión breve.",
            "Elige a alguien del grupo para jugar con fuego, de broma.",
            "Di quién crees que sería más atrevido/a en un reto.",
            "Lanza una pregunta sin filtros al grupo entero.",
        ],
        5: [
            "Haz una pregunta HOT a alguien del grupo, siempre con respeto.",
            "Confiesa un deseo HOT sin decir nombres.",
            "Manda una canción muy picante y deja que el grupo interprete.",
            "Di qué mensaje privado te encendería bastante.",
            "Elige a alguien para una noche imaginaria de confesiones HOT.",
            "Di una fantasía sin detalles explícitos ni nombres.",
            "Manda un audio con una indirecta nivel 5.",
            "Reta a alguien a contestar una pregunta HOT o decir ‘paso’. ",
            "Di quién del grupo te parece una tentación seria.",
            "Manda una frase HOT que no sea vulgar.",
            "Pregunta a alguien qué le da más morbo: secreto, prohibido o inesperado.",
            "Di qué no responderías salvo que te reten.",
            "Elige a alguien para una conversación privada sin capturas, de broma.",
            "Manda una canción que suene a peligro.",
            "Di quién crees que tiene el lado más travieso.",
            "Reta a alguien a mandar una indirecta muy clara, sin contenido íntimo.",
            "Di qué pregunta nivel 5 te pondría contra las cuerdas.",
            "Manda un audio diciendo ‘esta respuesta me puede quemar’. ",
            "Elige a alguien para subir el tono del juego.",
            "Confiesa qué te da más morbo sin señalar a nadie directamente.",
        ],
    }
    return {level: _build_challenge_level(level, seeds[level], 200) for level in range(1, 6)}


HOT_BASE_QUESTIONS = _hot_natural_questions()
HOT_BASE_CHALLENGES = _hot_natural_challenges()


def hot_cfg(chat_id: int) -> Dict[str, Any]:
    cfg = admin_cfg(chat_id)
    cfg.setdefault("hot_mode", "manual")
    cfg.setdefault("hot_level", 1)
    cfg.setdefault("hot_random_include_level5", False)
    cfg.setdefault("hot_auto_enabled", False)
    cfg.setdefault("hot_auto_interval_seconds", 180)
    cfg.setdefault("hot_auto_include_hot", False)
    cfg.setdefault("hot_auto_min_messages", 5)
    cfg.setdefault("hot_auto_min_users", 2)
    cfg.setdefault("hot_auto_activity_window_seconds", 240)
    cfg.setdefault("hot_auto_delete_enabled", True)
    cfg.setdefault("hot_auto_delete_seconds", 90)
    cfg.setdefault("hot_points_delete_seconds", 5)
    cfg.setdefault("hot_command_delete_mode", "off")
    cfg.setdefault("hot_command_delete_seconds", 20)
    cfg.setdefault("hot_custom_questions", {})
    cfg.setdefault("hot_custom_challenges", {})
    cfg.setdefault("hot_auto_mix_challenges", True)
    cfg.setdefault("hot_auto_challenge_every", 5)
    cfg.setdefault("hot_auto_counter", 0)
    cfg.setdefault("hot_ranking", {})
    cfg.setdefault("hot_ranking_daily", {})
    cfg.setdefault("hot_ranking_weekly", {})
    cfg.setdefault("hot_last_auto_ts", 0)
    cfg.setdefault("hot_lock_mode", "interval")
    cfg.setdefault("hot_lock_minutes", 1)
    cfg.setdefault("hot_no_answer_penalty", True)
    cfg.setdefault("hot_no_answer_penalty_points", -2)
    cfg.setdefault("hot_used_items", {})
    cfg.setdefault("hot_pin_text", "🎲 <b>Juego activo</b>\n\nPulsa el botón para lanzar una preguntita o un retito. También puedes usar /preguntita, /retito o /examen.")
    cfg.setdefault("hot_pin_button_text", "🎲 Enviar preguntita / retito")
    cfg.setdefault("dj_music_pin_text", "🎧 <b>Música en directo</b>\n\nPulsa el botón para unirte al directo musical del grupo.")
    cfg.setdefault("dj_music_pin_button_text", "🎧 Escuchar música")
    cfg.setdefault("recurring_messages", [])
    cfg.setdefault("pregonero_auto_enabled", False)
    cfg.setdefault("pregonero_auto_time", "21:00")
    cfg.setdefault("pregonero_auto_last_day", "")
    cfg.setdefault("resumen_limit", 1000)
    return cfg


def _today_key_v7() -> str:
    return time.strftime("%Y-%m-%d", time.localtime())


def _hot_item_key(kind: str, level: int, item: str) -> str:
    base = _re_v7.sub(r"\s+", " ", str(item).strip().lower())
    return f"{kind}:{level}:{base}"


def hot_get_item(chat_id: int, level: int, kind: str = "question") -> str:
    level = max(1, min(5, int(level or 1)))
    kind = "challenge" if kind == "challenge" else "question"
    base = HOT_BASE_CHALLENGES.get(level, []) if kind == "challenge" else HOT_BASE_QUESTIONS.get(level, [])
    custom = hot_custom_challenges(chat_id, level) if kind == "challenge" else hot_custom_questions(chat_id, level)
    total = _unique_keep_order(list(base) + list(custom))
    if not total:
        return "No hay retos en este nivel." if kind == "challenge" else "No hay preguntas en este nivel."

    cfg = hot_cfg(chat_id)
    today = _today_key_v7()
    used_root = cfg.setdefault("hot_used_items", {})
    # Limpieza diaria: conserva solo el día actual para no inflar state.json.
    for day in list(used_root.keys()):
        if day != today:
            used_root.pop(day, None)
    used_day = used_root.setdefault(today, {})
    bucket_name = f"{kind}_{level}"
    used = set(used_day.setdefault(bucket_name, []))
    available = [item for item in total if _hot_item_key(kind, level, item) not in used]
    if not available:
        # Si se han gastado todas las del día en ese nivel, reinicia solo ese cubo.
        used.clear()
        available = total[:]
    item = secrets.choice(available)
    used.add(_hot_item_key(kind, level, item))
    used_day[bucket_name] = sorted(used)
    save_all_states()
    return item


def hot_get_question(chat_id: int, level: int) -> str:
    return hot_get_item(chat_id, level, "question")


def hot_get_challenge(chat_id: int, level: int) -> str:
    return hot_get_item(chat_id, level, "challenge")


def hot_penalize_no_answer(chat_id: int, message_id: int, data: Dict[str, Any]) -> None:
    if data.get("penalized"):
        return
    cfg = hot_cfg(chat_id)
    if not bool(cfg.get("hot_no_answer_penalty", True)):
        return
    try:
        target_id = int(data.get("target_id"))
    except Exception:
        return
    fake = type("_HotUser", (), {})()
    fake.id = target_id
    fake.first_name = str(data.get("target_name") or "Usuario")
    fake.last_name = ""
    fake.username = None
    hot_add_points(chat_id, fake, int(cfg.get("hot_no_answer_penalty_points", -2) or -2))
    data["penalized"] = True


def hot_prune_old_active(chat_id: int) -> None:
    cfg = hot_cfg(chat_id)
    minutes = max(1, min(4, int(cfg.get("hot_lock_minutes", 1) or 1)))
    cutoff = int(time.time()) - minutes * 60
    active = HOT_ACTIVE_QUESTIONS.get(int(chat_id), {})
    changed = False
    for mid, data in list(active.items()):
        # El examen bloquea hasta responder; no penalizo cada paso para no castigar de más.
        if bool(data.get("exam")):
            continue
        if int(data.get("ts", 0)) <= cutoff:
            hot_penalize_no_answer(chat_id, int(mid), data)
            active.pop(mid, None)
            changed = True
    if changed:
        save_all_states()


def hot_can_launch(chat_id: int) -> Tuple[bool, str]:
    hot_prune_old_active(chat_id)
    pending = hot_active_pending(chat_id)
    if not pending:
        return True, ""
    _, data = pending
    if bool(data.get("exam")):
        return False, "Hay un /examen activo. Primero debe responder la pregunta actual."
    cfg = hot_cfg(chat_id)
    mode = str(cfg.get("hot_lock_mode", "interval"))
    if mode == "answer":
        return False, "Ya hay una preguntita/reto activo. Primero hay que responderlo."
    minutes = max(1, min(4, int(cfg.get("hot_lock_minutes", 1) or 1)))
    age = int(time.time()) - int(data.get("ts", 0) or 0)
    left = max(1, minutes * 60 - age)
    return False, f"Ya hay una preguntita/reto activo. Espera {left}s o que responda la persona marcada."


def hot_answer_points_for_text(level: int, text: str) -> Tuple[int, int, str]:
    clean = (text or "").strip()
    words = [w for w in _re_v7.split(r"\s+", clean) if w]
    if len(clean) <= 4 or len(words) <= 1:
        # Monosílabo o respuesta seca: suma, pero menos de lo normal.
        return max(1, int(level) - 1), 0, "respuesta corta"
    bonus = hot_answer_quality_bonus(clean)
    return int(level) + bonus, bonus, "respuesta completa"


async def hot_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    user = update.effective_user
    if getattr(user, "is_bot", False):
        return
    hot_activity_remember(chat_id, user)

    # Carga masiva pendiente.
    pending = HOT_PENDING_ADD.get(user.id)
    if pending and pending.get("mode") == "bulk_wait_text" and int(pending.get("input_chat_id", chat_id)) == chat_id:
        target_chat_id = int(pending.get("target_chat_id", chat_id))
        if await is_admin(context, target_chat_id, user.id):
            lines = [line.strip() for line in (update.message.text or "").replace("|", "\n").splitlines() if line.strip()]
            if not lines:
                await update.message.reply_text("No he detectado elementos válidos.")
                return
            pending["mode"] = "bulk_choose_level"
            pending["questions"] = lines
            rows = [[InlineKeyboardButton(f"Nivel {i}", callback_data=f"hotadd|{i}") for i in (1, 2, 3)], [InlineKeyboardButton("Nivel 4", callback_data="hotadd|4"), InlineKeyboardButton("Nivel 5", callback_data="hotadd|5")]]
            label = "retos" if pending.get("kind") == "challenge" else "preguntas"
            await update.message.reply_html(f"📊 Detectados <b>{len(lines)}</b> {label}. Elige nivel:", reply_markup=InlineKeyboardMarkup(rows))
            return

    reply = update.message.reply_to_message
    if not reply:
        return
    active = HOT_ACTIVE_QUESTIONS.get(chat_id, {})
    data = active.get(int(reply.message_id))
    if not data:
        return
    if int(data.get("target_id")) != int(user.id):
        return
    text = (update.message.text or "").strip()
    if not text:
        return
    level = max(1, min(5, int(data.get("level", 1) or 1)))
    kind = str(data.get("kind", "question"))
    points, bonus, quality_label = hot_answer_points_for_text(level, text)
    total = hot_add_points(chat_id, user, points)
    active.pop(int(reply.message_id), None)
    extra = f" + bonus {bonus}" if bonus else ""
    label = "reto" if kind == "challenge" else "pregunta"
    msg = await update.message.reply_html(f"🔥 <b>{h(display_name(user))}</b> +{points} pts <i>({label} nivel {level}{extra} · {h(quality_label)})</i>\n🏆 Total: <b>{total}</b>")
    await register_bot_message(chat_id, msg.message_id)
    asyncio.create_task(hot_safe_delete(context, chat_id, msg.message_id, delay=int(hot_cfg(chat_id).get("hot_points_delete_seconds", 5) or 5)))

    if bool(data.get("exam")):
        step = int(data.get("exam_step", level) or level)
        await hot_send_exam_next(context, chat_id, user, step + 1)


def hot_config_text(chat_id: int) -> str:
    cfg = hot_cfg(chat_id)
    custom_total = sum(len(hot_custom_questions(chat_id, lvl)) for lvl in range(1, 6))
    custom_retos = sum(len(hot_custom_challenges(chat_id, lvl)) for lvl in range(1, 6))
    lock_mode = "obligar respuesta" if str(cfg.get("hot_lock_mode", "interval")) == "answer" else f"intervalo {int(cfg.get('hot_lock_minutes', 1) or 1)} min"
    used_today = cfg.get("hot_used_items", {}).get(_today_key_v7(), {})
    used_count = sum(len(v) for v in used_today.values() if isinstance(v, list))
    return (
        "🔥 <b>Config Preguntitas y Retitos</b>\n\n"
        f"Grupo configurado: <code>{chat_id}</code>\n"
        f"Modo: <b>{h(cfg.get('hot_mode', 'manual'))}</b> · Nivel: <b>{int(cfg.get('hot_level', 1) or 1)}</b>\n"
        f"Automático: <b>{bool_label(cfg.get('hot_auto_enabled', False))}</b> · Intervalo auto: <b>{int(cfg.get('hot_auto_interval_seconds', 180) or 180)}s</b>\n"
        f"Auto mezcla retos: <b>{bool_label(cfg.get('hot_auto_mix_challenges', True))}</b> · cada <b>{int(cfg.get('hot_auto_challenge_every', 5) or 5)}</b> turnos\n"
        f"Bloqueo nuevas invocaciones: <b>{h(lock_mode)}</b> · Penalización sin responder: <b>{int(cfg.get('hot_no_answer_penalty_points', -2) or -2)} pts</b>\n"
        f"Mínimo actividad auto: <b>{h(hot_auto_activity_label(chat_id))}</b>\n"
        f"Sin repetir hoy: <b>{used_count}</b> usadas · Reinicio automático al cambiar de día\n"
        f"Borrado HOT: <b>{bool_label(cfg.get('hot_auto_delete_enabled', True))}</b> · Preguntas: <b>{int(cfg.get('hot_auto_delete_seconds', 90) or 90)}s</b> · Puntos: <b>{int(cfg.get('hot_points_delete_seconds', 5) or 5)}s</b>\n"
        f"Borrado comandos HOT: <b>{h(hot_command_delete_label(chat_id))}</b>\n"
        f"Preguntas base: <b>{sum(len(v) for v in HOT_BASE_QUESTIONS.values())}</b> · Retos base: <b>{sum(len(v) for v in HOT_BASE_CHALLENGES.values())}</b>\n"
        f"Preguntas añadidas: <b>{custom_total}</b> · Retos añadidos: <b>{custom_retos}</b>\n\n"
        "Comandos:\n"
        "<code>/preguntita</code> · <code>/retito</code> · <code>/examen</code>\n"
        "<code>/rankinghot</code> · <code>/rankinghot diario</code> · <code>/rankinghot semanal</code>\n"
        "<code>/hotfijar</code> · fija el botón de juego en el grupo\n"
        "<code>/addpregunta 2 texto</code> · <code>/addreto 2 texto</code> · <code>/addmasivo</code> · <code>/addretos</code>"
    )


def recurring_list(chat_id: int) -> List[Dict[str, Any]]:
    cfg = hot_cfg(chat_id)
    rows = cfg.setdefault("recurring_messages", [])
    if not isinstance(rows, list):
        rows = []
        cfg["recurring_messages"] = rows
    return rows


def recurring_next_id(chat_id: int) -> int:
    rows = recurring_list(chat_id)
    used = {int(r.get("id", 0) or 0) for r in rows if isinstance(r, dict)}
    i = 1
    while i in used:
        i += 1
    return i


WEEKDAY_ALIASES = {
    "lunes": 0, "lun": 0, "monday": 0, "mon": 0,
    "martes": 1, "mar": 1, "tuesday": 1, "tue": 1,
    "miercoles": 2, "miércoles": 2, "mie": 2, "mié": 2, "wednesday": 2, "wed": 2,
    "jueves": 3, "jue": 3, "thursday": 3, "thu": 3,
    "viernes": 4, "vie": 4, "friday": 4, "fri": 4,
    "sabado": 5, "sábado": 5, "sab": 5, "sáb": 5, "saturday": 5, "sat": 5,
    "domingo": 6, "dom": 6, "sunday": 6, "sun": 6,
}
WEEKDAY_NAMES = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def _weekday_tokens(raw: str) -> List[int]:
    days: List[int] = []
    for token in _re_v7.split(r"[,/\s]+", (raw or "").strip().lower()):
        token = token.strip(" .;")
        if not token:
            continue
        if token in WEEKDAY_ALIASES:
            value = WEEKDAY_ALIASES[token]
            if value not in days:
                days.append(value)
    return days


def _format_weekday_label(days: List[int]) -> str:
    return ", ".join(WEEKDAY_NAMES[d] for d in days if 0 <= int(d) <= 6)


def parse_recurring_when(raw: str) -> Dict[str, Any]:
    s = (raw or "").strip().lower()
    if s in ("cada hora", "hora", "hourly"):
        return {"type": "interval", "seconds": 3600, "label": "cada hora"}
    if s.startswith("cada "):
        val = s.replace("cada ", "", 1).strip()
        mult = 60
        if val.endswith("h"):
            mult = 3600; val = val[:-1]
        elif val.endswith("m"):
            mult = 60; val = val[:-1]
        elif val.endswith("s"):
            mult = 1; val = val[:-1]
        try:
            n = max(30, int(float(val) * mult))
            return {"type": "interval", "seconds": n, "label": f"cada {n}s"}
        except Exception:
            pass
    m = _re_v7.search(r"(diario|día|dia|hora)\s+([0-2]?\d:[0-5]\d)", s)
    if m:
        hhmm = m.group(2)
        return {"type": "daily", "time": hhmm, "label": f"diario {hhmm}"}

    # Semanal: "lunes 10:00", "semanal lunes 10:00", "lunes,miercoles 21:30"
    m = _re_v7.search(r"(?:(?:semanal|cada semana)\s+)?([a-záéíóúñ,\s/]+)\s+([0-2]?\d:[0-5]\d)$", s)
    if m:
        days = _weekday_tokens(m.group(1))
        if days:
            hhmm = m.group(2)
            return {"type": "weekly", "days": days, "time": hhmm, "label": f"{_format_weekday_label(days)} {hhmm}"}

    # Fecha concreta de una sola vez: "2026-05-12 21:30" o "fecha 2026-05-12 21:30"
    m = _re_v7.search(r"(?:fecha\s+)?(20\d{2}-\d{2}-\d{2})\s+([0-2]?\d:[0-5]\d)$", s)
    if m:
        date_s, hhmm = m.group(1), m.group(2)
        return {"type": "date", "date": date_s, "time": hhmm, "label": f"{date_s} {hhmm}"}

    raise ValueError("Formato de tiempo no válido. Usa: cada 60m, cada 1h, diario 21:30, lunes 10:00 o 2026-05-12 21:30")


def recurring_buttons_markup(row: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
    buttons = row.get("buttons") or []
    if not isinstance(buttons, list) or not buttons:
        return None
    rows = []
    for b in buttons:
        if not isinstance(b, dict):
            continue
        text = str(b.get("text", "")).strip()
        url = str(b.get("url", "")).strip()
        if text and url:
            rows.append([InlineKeyboardButton(text[:64], url=url)])
    return InlineKeyboardMarkup(rows) if rows else None


async def send_recurring_message(context, chat_id: int, row: Dict[str, Any]) -> Optional[int]:
    text = str(row.get("text") or "").strip()
    media = row.get("media") if isinstance(row.get("media"), dict) else None
    markup = recurring_buttons_markup(row)
    msg = None
    try:
        if media and media.get("file_id"):
            fid = media.get("file_id")
            mtype = media.get("type")
            if mtype == "photo":
                msg = await context.bot.send_photo(chat_id, photo=fid, caption=text or None, parse_mode=ParseMode.HTML, reply_markup=markup)
            elif mtype == "video":
                msg = await context.bot.send_video(chat_id, video=fid, caption=text or None, parse_mode=ParseMode.HTML, reply_markup=markup)
            elif mtype == "animation":
                msg = await context.bot.send_animation(chat_id, animation=fid, caption=text or None, parse_mode=ParseMode.HTML, reply_markup=markup)
            else:
                msg = await context.bot.send_document(chat_id, document=fid, caption=text or None, parse_mode=ParseMode.HTML, reply_markup=markup)
        else:
            msg = await context.bot.send_message(chat_id, text=text or "Mensaje recurrente", parse_mode=ParseMode.HTML, reply_markup=markup)
        await register_bot_message(chat_id, msg.message_id)
        if bool(row.get("pin", False)):
            try:
                await context.bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
            except Exception:
                logger.exception("No se pudo fijar recurrente %s en chat %s", row.get("id"), chat_id)
        return int(msg.message_id)
    except Exception:
        logger.exception("No se pudo enviar recurrente %s en chat %s", row.get("id"), chat_id)
        return None


def recurrentes_text(chat_id: int) -> str:
    rows = recurring_list(chat_id)
    lines = ["🔁 <b>Mensajes recurrentes</b>", "", f"Grupo: <code>{chat_id}</code>", f"Total: <b>{len(rows)}</b>", ""]
    if not rows:
        lines.append("No hay mensajes recurrentes todavía.")
    for r in rows[:12]:
        status = "ON" if r.get("enabled", True) else "OFF"
        pin = "📌" if r.get("pin") else ""
        lines.append(f"#{int(r.get('id', 0))} · {status} {pin} · {h(str(r.get('name', 'Sin nombre')))} · {h(str(r.get('schedule_label', '')))}")
    lines.append("\nCrear rápido:")
    lines.append("<code>/recnuevo Nombre | cada 1h | Texto</code>")
    lines.append("<code>/recnuevo Nombre | diario 21:30 | Texto</code>")
    lines.append("Botones: <code>/recbotones ID Texto=https://url | Otro=https://url</code>")
    lines.append("Media: responde a una foto/video/documento con <code>/recmedia ID</code>")
    return "\n".join(lines)


def recurrentes_markup(chat_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for r in recurring_list(chat_id)[:8]:
        rid = int(r.get("id", 0) or 0)
        name = str(r.get("name", f"#{rid}"))[:18]
        rows.append([InlineKeyboardButton(f"▶️ {name}", callback_data=f"rec|send|{chat_id}|{rid}"), InlineKeyboardButton("ON/OFF", callback_data=f"rec|toggle|{chat_id}|{rid}"), InlineKeyboardButton("📌", callback_data=f"rec|pin|{chat_id}|{rid}"), InlineKeyboardButton("🗑", callback_data=f"rec|del|{chat_id}|{rid}")])
    rows.append([InlineKeyboardButton("🔄 Recargar", callback_data=f"rec|open|{chat_id}|0"), InlineKeyboardButton("Cerrar", callback_data=f"rec|close|{chat_id}|0")])
    return InlineKeyboardMarkup(rows)


def recurring_find(chat_id: int, rid: int) -> Optional[Dict[str, Any]]:
    return next((r for r in recurring_list(chat_id) if int(r.get("id", 0) or 0) == int(rid)), None)


def recurring_schedule_from_code(code: str) -> Dict[str, Any]:
    code = str(code or "").strip().lower()
    if code.startswith("i"):
        seconds = max(30, int(code[1:] or "3600"))
        if seconds % 3600 == 0:
            label = f"cada {seconds // 3600}h"
        elif seconds % 60 == 0:
            label = f"cada {seconds // 60}m"
        else:
            label = f"cada {seconds}s"
        return {"type": "interval", "seconds": seconds, "label": label}
    if code.startswith("d") and len(code) >= 5:
        hhmm = f"{int(code[1:3]):02d}:{int(code[3:5]):02d}"
        return {"type": "daily", "time": hhmm, "label": f"diario {hhmm}"}
    return parse_recurring_when(code)


def recurring_create_row(chat_id: int, text: str, schedule: Dict[str, Any], *, media: Optional[Dict[str, str]] = None, name: str = "") -> Dict[str, Any]:
    rid = recurring_next_id(chat_id)
    clean_text = str(text or "").strip()
    auto_name = name.strip() or (clean_text.replace("\n", " ")[:45].strip() if clean_text else f"Mensaje {rid}")
    row = {
        "id": rid,
        "name": auto_name[:60] or f"Mensaje {rid}",
        "text": clean_text,
        "enabled": True,
        "pin": False,
        "buttons": [],
        "media": media,
        "schedule": schedule,
        "schedule_label": schedule.get("label", ""),
        "last_sent_ts": 0,
    }
    recurring_list(chat_id).append(row)
    save_all_states()
    return row


def recurring_parse_buttons_private(text: str) -> List[Dict[str, str]]:
    buttons: List[Dict[str, str]] = []
    for raw in str(text or "").split("|"):
        part = raw.strip()
        if not part:
            continue
        if "=" in part:
            label, url = [x.strip() for x in part.split("=", 1)]
        elif " - " in part:
            label, url = [x.strip() for x in part.split(" - ", 1)]
        else:
            continue
        if url.startswith("t.me/"):
            url = "https://" + url
        if label and url.startswith(("http://", "https://", "tg://")):
            buttons.append({"text": label[:64], "url": url})
    return buttons[:20]


def recurring_detail_text(chat_id: int, rid: int) -> str:
    row = recurring_find(chat_id, rid)
    if not row:
        return "No encuentro ese mensaje recurrente."
    buttons = row.get("buttons") if isinstance(row.get("buttons"), list) else []
    media = row.get("media") if isinstance(row.get("media"), dict) else None
    text = str(row.get("text") or "").strip()
    preview = text if len(text) <= 700 else text[:700] + "…"
    return (
        f"🔁 <b>Recurrente #{rid}</b>\n\n"
        f"Nombre: <b>{h(str(row.get('name') or 'Sin nombre'))}</b>\n"
        f"Estado: <b>{'ON' if row.get('enabled', True) else 'OFF'}</b> · Fijar: <b>{bool_label(row.get('pin', False))}</b>\n"
        f"Horario: <b>{h(str(row.get('schedule_label') or ''))}</b>\n"
        f"Multimedia: <b>{'Sí' if media and media.get('file_id') else 'No'}</b> · Botones: <b>{len(buttons)}</b>\n\n"
        f"Texto:\n<pre>{h(preview or 'Sin texto')}</pre>"
    )


def recurring_detail_markup(chat_id: int, rid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✏️ Nombre", callback_data=f"cfg|rec_edit_name|{chat_id}|{rid}|recurrentes"),
            InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|rec_edit_text|{chat_id}|{rid}|recurrentes"),
        ],
        [
            InlineKeyboardButton("🖼 Media", callback_data=f"cfg|rec_edit_media|{chat_id}|{rid}|recurrentes"),
            InlineKeyboardButton("⌨️ Botones", callback_data=f"cfg|rec_edit_buttons|{chat_id}|{rid}|recurrentes"),
        ],
        [
            InlineKeyboardButton("⏱ Cada hora", callback_data=f"cfg|rec_schedule|{chat_id}|{rid}|i3600|recurrentes"),
            InlineKeyboardButton("🌙 Diario 21:30", callback_data=f"cfg|rec_schedule|{chat_id}|{rid}|d2130|recurrentes"),
            InlineKeyboardButton("⚙️ Otro horario", callback_data=f"cfg|rec_schedule_custom|{chat_id}|{rid}|recurrentes"),
        ],
        [
            InlineKeyboardButton("▶️ Enviar", callback_data=f"cfg|rec_send|{chat_id}|{rid}|recurrentes"),
            InlineKeyboardButton("ON/OFF", callback_data=f"cfg|rec_toggle|{chat_id}|{rid}|recurrentes"),
            InlineKeyboardButton("📌 Fijar", callback_data=f"cfg|rec_pin|{chat_id}|{rid}|recurrentes"),
        ],
        [
            InlineKeyboardButton("🧹 Quitar media", callback_data=f"cfg|rec_clear_media|{chat_id}|{rid}|recurrentes"),
            InlineKeyboardButton("🗑 Borrar", callback_data=f"cfg|rec_del|{chat_id}|{rid}|recurrentes"),
        ],
        [InlineKeyboardButton("🔙 Volver a recurrentes", callback_data=f"cfg|block|{chat_id}|recurrentes")],
    ])


async def recurrentes_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    await update.message.reply_html(recurrentes_text(chat_id), reply_markup=recurrentes_markup(chat_id))


async def recnuevo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    raw = " ".join(context.args or []).strip()
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 3:
        await update.message.reply_html("Uso:\n<code>/recnuevo Nombre | cada 1h | Texto</code>\n<code>/recnuevo Nombre | diario 21:30 | Texto</code>")
        return
    name, when_raw, body = parts[0], parts[1], " | ".join(parts[2:]).strip()
    try:
        sched = parse_recurring_when(when_raw)
    except ValueError as e:
        await update.message.reply_text(str(e))
        return
    rid = recurring_next_id(chat_id)
    row = {
        "id": rid,
        "name": name[:60] or f"Mensaje {rid}",
        "text": body,
        "enabled": True,
        "pin": False,
        "buttons": [],
        "media": None,
        "schedule": sched,
        "schedule_label": sched.get("label", ""),
        "last_sent_ts": 0,
    }
    recurring_list(chat_id).append(row)
    save_all_states()
    await update.message.reply_html(f"✅ Recurrente creado: <b>#{rid}</b> · {h(row['name'])}")


async def recbotones_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    raw = " ".join(context.args or []).strip()
    m = _re_v7.match(r"^(\d+)\s+(.+)$", raw)
    if not m:
        await update.message.reply_html("Uso:\n<code>/recbotones 1 Web=https://ejemplo.com | Canal=https://t.me/...</code>")
        return
    rid = int(m.group(1)); payload = m.group(2)
    row = next((r for r in recurring_list(chat_id) if int(r.get("id", 0) or 0) == rid), None)
    if not row:
        await update.message.reply_text("No encuentro ese recurrente.")
        return
    buttons = []
    for part in payload.split("|"):
        if "=" not in part:
            continue
        label, url = [x.strip() for x in part.split("=", 1)]
        if label and url.startswith(("http://", "https://", "tg://", "t.me/")):
            if url.startswith("t.me/"):
                url = "https://" + url
            buttons.append({"text": label[:64], "url": url})
    row["buttons"] = buttons
    save_all_states()
    await update.message.reply_text(f"✅ Botones actualizados en recurrente #{rid}: {len(buttons)}")


async def recmedia_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    if not context.args:
        await update.message.reply_text("Uso: responde a una foto/video/documento con /recmedia ID")
        return
    try:
        rid = int(context.args[0])
    except Exception:
        await update.message.reply_text("ID inválido.")
        return
    row = next((r for r in recurring_list(chat_id) if int(r.get("id", 0) or 0) == rid), None)
    if not row:
        await update.message.reply_text("No encuentro ese recurrente.")
        return
    src = update.message.reply_to_message
    if not src:
        await update.message.reply_text("Responde al archivo/foto/video que quieres guardar como media.")
        return
    media = None
    if src.photo:
        media = {"type": "photo", "file_id": src.photo[-1].file_id}
    elif src.video:
        media = {"type": "video", "file_id": src.video.file_id}
    elif src.animation:
        media = {"type": "animation", "file_id": src.animation.file_id}
    elif src.document:
        media = {"type": "document", "file_id": src.document.file_id}
    if not media:
        await update.message.reply_text("Ese mensaje no tiene una media compatible.")
        return
    row["media"] = media
    save_all_states()
    await update.message.reply_text(f"✅ Multimedia guardada en recurrente #{rid}.")


async def recenviar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = hot_target_chat_id(update) or int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    try:
        rid = int((context.args or [])[0])
    except Exception:
        await update.message.reply_text("Uso: /recenviar ID")
        return
    row = next((r for r in recurring_list(chat_id) if int(r.get("id", 0) or 0) == rid), None)
    if not row:
        await update.message.reply_text("No encuentro ese recurrente.")
        return
    await send_recurring_message(context, chat_id, row)
    await update.message.reply_text("✅ Enviado.")


async def recurrentes_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("rec|"):
        return
    parts = data.split("|")
    action = parts[1]
    chat_id = int(parts[2])
    rid = int(parts[3]) if len(parts) > 3 else 0
    if not await is_admin(context, chat_id, update.effective_user.id):
        await query.answer("Solo administradores.", show_alert=True); return
    rows = recurring_list(chat_id)
    row = next((r for r in rows if int(r.get("id", 0) or 0) == rid), None)
    if action == "close":
        await query.answer("Cerrado")
        try: await query.message.delete()
        except Exception: pass
        return
    if action == "send" and row:
        await send_recurring_message(context, chat_id, row)
        await query.answer("Enviado ✅")
    elif action == "toggle" and row:
        row["enabled"] = not bool(row.get("enabled", True)); save_all_states(); await query.answer("Actualizado ✅")
    elif action == "pin" and row:
        row["pin"] = not bool(row.get("pin", False)); save_all_states(); await query.answer("Fijado ON/OFF actualizado ✅")
    elif action == "del" and row:
        rows.remove(row); save_all_states(); await query.answer("Borrado ✅")
    else:
        await query.answer("Actualizado")
    try:
        await query.edit_message_text(recurrentes_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=recurrentes_markup(chat_id))
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


def recurring_due(row: Dict[str, Any], now_ts: int) -> bool:
    if not bool(row.get("enabled", True)):
        return False
    sched = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
    last = int(row.get("last_sent_ts", 0) or 0)
    stype = str(sched.get("type") or "")
    if stype == "interval":
        sec = max(30, int(sched.get("seconds", 3600) or 3600))
        return now_ts - last >= sec
    if stype == "daily":
        hhmm = str(sched.get("time", "21:00"))
        day_key = time.strftime("%Y-%m-%d", time.localtime(now_ts))
        if row.get("last_day") == day_key:
            return False
        try:
            hh, mm = [int(x) for x in hhmm.split(":", 1)]
            lt = time.localtime(now_ts)
            return (lt.tm_hour, lt.tm_min) >= (hh, mm)
        except Exception:
            return False
    if stype == "weekly":
        hhmm = str(sched.get("time", "21:00"))
        days = sched.get("days") if isinstance(sched.get("days"), list) else []
        try:
            lt = time.localtime(now_ts)
            if int(lt.tm_wday) not in [int(x) for x in days]:
                return False
            week_key = f"{time.strftime('%G-%V', lt)}-{lt.tm_wday}"
            if row.get("last_week_key") == week_key:
                return False
            hh, mm = [int(x) for x in hhmm.split(":", 1)]
            return (lt.tm_hour, lt.tm_min) >= (hh, mm)
        except Exception:
            return False
    if stype == "date":
        hhmm = str(sched.get("time", "21:00"))
        date_s = str(sched.get("date", ""))
        day_key = time.strftime("%Y-%m-%d", time.localtime(now_ts))
        if row.get("last_date_key") == date_s:
            return False
        if day_key != date_s:
            return False
        try:
            hh, mm = [int(x) for x in hhmm.split(":", 1)]
            lt = time.localtime(now_ts)
            return (lt.tm_hour, lt.tm_min) >= (hh, mm)
        except Exception:
            return False
    return False


async def recurring_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(20)
        now = int(time.time())
        for chat_id in list(STATE_CACHE.keys()):
            try:
                # También aplica penalizaciones HOT por no responder aunque nadie invoque otro comando.
                try:
                    hot_prune_old_active(chat_id)
                except Exception:
                    logger.exception("No se pudo revisar penalización HOT en chat %s", chat_id)
                for row in recurring_list(chat_id):
                    if recurring_due(row, now):
                        await send_recurring_message(application, chat_id, row)
                        row["last_sent_ts"] = now
                        sched_type = row.get("schedule", {}).get("type")
                        lt_now = time.localtime(now)
                        if sched_type == "daily":
                            row["last_day"] = time.strftime("%Y-%m-%d", lt_now)
                        elif sched_type == "weekly":
                            row["last_week_key"] = f"{time.strftime('%G-%V', lt_now)}-{lt_now.tm_wday}"
                        elif sched_type == "date":
                            row["last_date_key"] = str(row.get("schedule", {}).get("date", time.strftime("%Y-%m-%d", lt_now)))
                            row["enabled"] = False
                        save_all_states()
                        await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error en recurring_loop chat %s", chat_id)


async def pregonero_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    if not VOICE.client:
        await update.message.reply_text("Userbot/Telethon no está iniciado. No puedo sincronizar todos los miembros.")
        return
    msg = await update.message.reply_text("🔄 Sincronizando miembros visibles con Telethon...")
    state = get_state(chat_id)
    count = 0
    try:
        entity = await VOICE.client.get_entity(chat_id)
        async for p in VOICE.client.iter_participants(entity):
            if getattr(p, "bot", False):
                continue
            uid = str(int(p.id))
            first = getattr(p, "first_name", "") or ""
            last = getattr(p, "last_name", "") or ""
            username = getattr(p, "username", "") or ""
            state.member_activity[uid] = {
                "user_id": int(p.id), "id": int(p.id), "first_name": first, "last_name": last, "username": username,
                "name": (first + " " + last).strip() or username or uid, "is_bot": False, "last_seen": int(time.time()), "source": "telethon_sync",
            }
            count += 1
        save_all_states()
        await msg.edit_text(f"✅ Sincronizados {count} miembros visibles. El pregonero ya puede usarlos.")
    except Exception as e:
        logger.exception("No se pudo sincronizar pregonero")
        await msg.edit_text("No se pudo sincronizar. Asegúrate de que la cuenta USERBOT_SESSION está dentro del grupo y tiene acceso a la lista de miembros.")


def parse_pregonero_auto_schedule(raw: str) -> Dict[str, Any]:
    s = (raw or "").strip().lower()
    if not s:
        raise ValueError("Horario vacío")
    if _re_v7.match(r"^[0-2]?\d:[0-5]\d$", s):
        hhmm = s
        return {"type": "daily", "time": hhmm, "label": f"diario {hhmm}"}
    # lunes 21:30 / lunes,miercoles 21:30
    m = _re_v7.search(r"(?:(?:semanal|cada semana)\s+)?([a-záéíóúñ,\s/]+)\s+([0-2]?\d:[0-5]\d)$", s)
    if m:
        days = _weekday_tokens(m.group(1))
        if days:
            hhmm = m.group(2)
            return {"type": "weekly", "days": days, "time": hhmm, "label": f"{_format_weekday_label(days)} {hhmm}"}
    # fecha concreta: 2026-05-12 21:30
    m = _re_v7.search(r"(?:fecha\s+)?(20\d{2}-\d{2}-\d{2})\s+([0-2]?\d:[0-5]\d)$", s)
    if m:
        return {"type": "date", "date": m.group(1), "time": m.group(2), "label": f"{m.group(1)} {m.group(2)}"}
    raise ValueError("Uso: /pregoneroauto 21:30 · /pregoneroauto lunes 21:30 · /pregoneroauto lunes,miercoles 21:30 · /pregoneroauto 2026-05-12 21:30 · /pregoneroauto off")


def pregonero_auto_due(cfg: Dict[str, Any], now_ts: int) -> bool:
    if not bool(cfg.get("pregonero_auto_enabled", False)):
        return False
    sched = cfg.get("pregonero_auto_schedule") if isinstance(cfg.get("pregonero_auto_schedule"), dict) else None
    if not sched:
        # Compatibilidad con configuraciones antiguas: hora diaria simple.
        sched = {"type": "daily", "time": str(cfg.get("pregonero_auto_time", "21:00")), "label": f"diario {cfg.get('pregonero_auto_time', '21:00')}"}
    stype = str(sched.get("type") or "daily")
    lt = time.localtime(now_ts)
    try:
        if stype == "daily":
            day_key = time.strftime("%Y-%m-%d", lt)
            if cfg.get("pregonero_auto_last_key") == day_key or cfg.get("pregonero_auto_last_day") == day_key:
                return False
            hh, mm = [int(x) for x in str(sched.get("time", "21:00")).split(":", 1)]
            return (lt.tm_hour, lt.tm_min) >= (hh, mm)
        if stype == "weekly":
            days = sched.get("days") if isinstance(sched.get("days"), list) else []
            if int(lt.tm_wday) not in [int(x) for x in days]:
                return False
            key = f"{time.strftime('%G-%V', lt)}-{lt.tm_wday}"
            if cfg.get("pregonero_auto_last_key") == key:
                return False
            hh, mm = [int(x) for x in str(sched.get("time", "21:00")).split(":", 1)]
            return (lt.tm_hour, lt.tm_min) >= (hh, mm)
        if stype == "date":
            date_s = str(sched.get("date", ""))
            if cfg.get("pregonero_auto_last_key") == date_s:
                return False
            if time.strftime("%Y-%m-%d", lt) != date_s:
                return False
            hh, mm = [int(x) for x in str(sched.get("time", "21:00")).split(":", 1)]
            return (lt.tm_hour, lt.tm_min) >= (hh, mm)
    except Exception:
        return False
    return False


def mark_pregonero_auto_sent(cfg: Dict[str, Any], now_ts: int) -> None:
    sched = cfg.get("pregonero_auto_schedule") if isinstance(cfg.get("pregonero_auto_schedule"), dict) else {"type": "daily"}
    lt = time.localtime(now_ts)
    stype = str(sched.get("type") or "daily")
    if stype == "weekly":
        cfg["pregonero_auto_last_key"] = f"{time.strftime('%G-%V', lt)}-{lt.tm_wday}"
    elif stype == "date":
        cfg["pregonero_auto_last_key"] = str(sched.get("date", time.strftime("%Y-%m-%d", lt)))
        cfg["pregonero_auto_enabled"] = False
    else:
        day_key = time.strftime("%Y-%m-%d", lt)
        cfg["pregonero_auto_last_key"] = day_key
        cfg["pregonero_auto_last_day"] = day_key


async def pregonero_auto_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    arg = " ".join(context.args or []).strip().lower()
    cfg = hot_cfg(chat_id)
    if arg in ("off", "desactivar", "no"):
        cfg["pregonero_auto_enabled"] = False
        save_all_states()
        await update.message.reply_text("📣 Pregonero automático desactivado.")
        return
    try:
        sched = parse_pregonero_auto_schedule(arg)
    except ValueError as e:
        await update.message.reply_html(h(str(e)))
        return
    cfg["pregonero_auto_enabled"] = True
    cfg["pregonero_auto_schedule"] = sched
    cfg["pregonero_auto_time"] = sched.get("time", "21:00")  # compatibilidad con datos antiguos
    cfg.pop("pregonero_auto_last_key", None)
    cfg.pop("pregonero_auto_last_day", None)
    save_all_states()
    await update.message.reply_text(f"📣 Pregonero automático activado: {sched.get('label', arg)}.")


async def pregonero_auto_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(30)
        now = int(time.time())
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        lt = time.localtime(now)
        for chat_id in list(STATE_CACHE.keys()):
            try:
                cfg = hot_cfg(chat_id)
                if not pregonero_auto_due(cfg, now):
                    continue
                class _Ctx:
                    pass
                fake_context = _Ctx()
                fake_context.bot = application.bot
                await send_pregonero(fake_context, chat_id)
                mark_pregonero_auto_sent(cfg, now)
                save_all_states()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error en pregonero_auto_loop chat %s", chat_id)


async def resumen_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not chat_is_allowed(chat_id):
        return
    if not VOICE.client:
        await update.message.reply_text("No puedo leer historial porque Telethon/userbot no está iniciado.")
        return
    limit = 1000
    try:
        if context.args:
            limit = max(50, min(1000, int(context.args[0])))
    except Exception:
        limit = 1000
    wait = await update.message.reply_text(f"🧠 Leyendo últimos {limit} mensajes para hacer resumen...")
    users: Dict[str, int] = {}
    words: Dict[str, int] = {}
    questions = 0; media = 0; texts: List[str] = []
    stop = set("que de la el en y a los las un una unos unas por para con del al se es no si lo me te tu yo mi su nos os como más mas pero porque ya hoy ayer mañana aqui aquí este esta esto eso esa ese hay muy".split())
    try:
        async for m in VOICE.client.iter_messages(chat_id, limit=limit):
            sender = await m.get_sender() if hasattr(m, "get_sender") else None
            name = (getattr(sender, "first_name", "") or getattr(sender, "username", "") or "Usuario") if sender else "Usuario"
            users[name] = users.get(name, 0) + 1
            if getattr(m, "media", None):
                media += 1
            txt = (getattr(m, "raw_text", "") or "").strip()
            if txt:
                texts.append(txt[:240])
                questions += txt.count("?") + txt.count("¿")
                for w in _re_v7.findall(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]{4,}", txt.lower()):
                    if w not in stop:
                        words[w] = words.get(w, 0) + 1
        top_users = sorted(users.items(), key=lambda kv: kv[1], reverse=True)[:5]
        top_words = sorted(words.items(), key=lambda kv: kv[1], reverse=True)[:10]
        sample = [t for t in texts if len(t) > 20][:5]
        lines = ["🧾 <b>Resumen rápido del grupo</b>", "", f"He revisado <b>{sum(users.values())}</b> mensajes recientes."]
        if top_users:
            lines.append("\n🗣️ <b>Los que más han dado guerra:</b>")
            for name, n in top_users:
                lines.append(f"• {h(name)} — {n} mensajes")
        if top_words:
            lines.append("\n🔥 <b>Temas/palabras que más han salido:</b>")
            lines.append(", ".join(h(w) for w, _ in top_words[:10]))
        lines.append(f"\n❓ Preguntas detectadas: <b>{questions}</b>")
        lines.append(f"🖼️ Mensajes con multimedia: <b>{media}</b>")
        if sample:
            lines.append("\n🎭 <b>Momentos/frases que resumen el ambiente:</b>")
            for s in sample[:4]:
                clean = _re_v7.sub(r"\s+", " ", s)
                lines.append(f"• {h(clean[:120])}")
        lines.append("\n😂 Diagnóstico del bot: el grupo está vivo; solo falta que alguien tire una /preguntita o un /retito.")
        await wait.edit_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception:
        logger.exception("No se pudo generar resumen")
        await wait.edit_text("No pude generar el resumen. Revisa que USERBOT_SESSION tenga acceso al historial del grupo.")

# =========================
# FIN V7
# =========================

async def presentate_accent_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat and update.effective_user:
        remember_member_activity(update.effective_chat.id, update.effective_user, kind="command", source="/preséntate")
    await force_presentate_command(update, context)
    await cleanup_command_invocation(update, context)


async def farewell_left_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not getattr(update.message, "left_chat_member", None):
        return
    chat_id = update.effective_chat.id
    remember_chat_title(chat_id, update.effective_chat.title or "")
    if not bool(cfg_value(chat_id, "farewell_enabled", True)):
        return
    user = update.message.left_chat_member
    if getattr(user, "is_bot", False):
        return
    record = user_record_from_user(user)
    mention = user.mention_html()
    text = validation_format_template(str(cfg_value(chat_id, "farewell_message", "👋 {mention} ha salido del grupo.")), user)
    try:
        await send_configured_profile_message(context.bot, chat_id, "farewell", text, reply_to_message_id=update.message.message_id)
        add_action_log(chat_id, "despedida", display_name(user), user_id=getattr(user, "id", None))
    except Exception:
        logger.exception("No se pudo enviar despedida")



async def on_startup(application: Application) -> None:
    global VALIDATION_WATCHDOG_TASK, HOT_AUTO_TASK, RECURRING_TASK, PREGONERO_AUTO_TASK
    hot_load_private_groups()
    dj_load_private_groups()
    load_all_states()
    await VOICE.start(application)
    if VALIDATION_ENABLED and (VALIDATION_WATCHDOG_TASK is None or VALIDATION_WATCHDOG_TASK.done()):
        VALIDATION_WATCHDOG_TASK = asyncio.create_task(validation_watchdog_loop(application))
    if HOT_AUTO_TASK is None or HOT_AUTO_TASK.done():
        HOT_AUTO_TASK = asyncio.create_task(hot_auto_loop(application))
    if RECURRING_TASK is None or RECURRING_TASK.done():
        RECURRING_TASK = asyncio.create_task(recurring_loop(application))
    if PREGONERO_AUTO_TASK is None or PREGONERO_AUTO_TASK.done():
        PREGONERO_AUTO_TASK = asyncio.create_task(pregonero_auto_loop(application))
    for chat_id, state in STATE_CACHE.items():
        try:
            state.library = dedupe_library_items(state.library)
            state.saved_lists = {name: dedupe_track_items(items) for name, items in state.saved_lists.items()}
            save_all_states()
            if state.dj_mode and state.now_playing:
                track = Track(**state.now_playing)
                await schedule_auto_next(chat_id, track.duration, state.auto_sig_seconds)
        except Exception:
            logger.exception("No se pudo rearmar el auto-siguiente en chat %s al iniciar", chat_id)
    logger.info("DJ-PLAN iniciado")


async def on_shutdown(application: Application) -> None:
    global VALIDATION_WATCHDOG_TASK, HOT_AUTO_TASK, RECURRING_TASK, PREGONERO_AUTO_TASK
    if VALIDATION_WATCHDOG_TASK and not VALIDATION_WATCHDOG_TASK.done():
        VALIDATION_WATCHDOG_TASK.cancel()
    VALIDATION_WATCHDOG_TASK = None
    if HOT_AUTO_TASK and not HOT_AUTO_TASK.done():
        HOT_AUTO_TASK.cancel()
    HOT_AUTO_TASK = None
    if RECURRING_TASK and not RECURRING_TASK.done():
        RECURRING_TASK.cancel()
    RECURRING_TASK = None
    if PREGONERO_AUTO_TASK and not PREGONERO_AUTO_TASK.done():
        PREGONERO_AUTO_TASK.cancel()
    PREGONERO_AUTO_TASK = None
    await VOICE.stop()




async def on_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Error no controlado del bot", exc_info=context.error)


def build_application() -> Application:
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    # Comandos oficiales nuevos. Mantengo aliases antiguos para no romper instalaciones ya usadas.
    application.add_handler(command_handler(["djplan", "start"], start_command))
    application.add_handler(command_handler("dj", assign_dj_command))
    application.add_handler(command_handler(["plan", "paneladmin"], admin_command))
    application.add_handler(command_handler(["ajustes", "configuracion", "config", "admin"], admin_private_config_command))
    application.add_handler(command_handler("ping", ping_command))
    application.add_handler(command_handler("djgrupo", djgrupo_command))
    application.add_handler(command_handler("djconfig", djconfig_command))
    application.add_handler(command_handler(["djmesa", "DJmesa"], djmesa_command))
    application.add_handler(command_handler("djfijar", djfijar_command))
    application.add_handler(command_handler("cola", cola_command))
    application.add_handler(command_handler(["presentate"], force_presentate_command))
    application.add_handler(MessageHandler(filters.Regex(r"^/preséntate(?:@\w+)?(?:\s|$)"), presentate_accent_message))

    application.add_handler(command_handler("pregonero", pregonero_command))
    application.add_handler(command_handler("pregonerosync", pregonero_sync_command))
    application.add_handler(command_handler("pregoneroauto", pregonero_auto_command))
    application.add_handler(command_handler("resumen", resumen_command))
    application.add_handler(command_handler("recurrentes", recurrentes_command))
    application.add_handler(command_handler("recnuevo", recnuevo_command))
    application.add_handler(command_handler("recbotones", recbotones_command))
    application.add_handler(command_handler("recmedia", recmedia_command))
    application.add_handler(command_handler("recenviar", recenviar_command))
    application.add_handler(command_handler("el", pregonero_command))
    application.add_handler(command_handler("silenciados", silenciados_command))
    application.add_handler(command_handler("entradas", entradas_command))
    application.add_handler(command_handler("inactivos", inactivos_command))
    application.add_handler(command_handler("ranking", ranking_command))

    # HOT MODULE: preguntita, ranking y configuración.
    application.add_handler(command_handler("preguntita", preguntita_command))
    application.add_handler(command_handler(["retito", "retohot"], retito_command))
    application.add_handler(command_handler("examen", examen_command))
    application.add_handler(command_handler("rankinghot", ranking_hot_command))
    application.add_handler(command_handler("hotfijar", hotfijar_command))
    application.add_handler(command_handler("hotconfig", hotconfig_command))
    application.add_handler(command_handler("hotgrupo", hotgrupo_command))
    application.add_handler(command_handler("addpregunta", addpregunta_command))
    application.add_handler(command_handler("addreto", addreto_command))
    application.add_handler(command_handler("addmasivo", addmasivo_command))
    application.add_handler(command_handler("addretos", addretosmasivo_command))

    application.add_handler(command_handler("setpreguntas", set_questions_command))
    application.add_handler(command_handler("settiempo", set_time_command))
    application.add_handler(command_handler("setrecordatorio", set_reminder_command))
    application.add_handler(command_handler("validacionon", validation_toggle_command))
    application.add_handler(command_handler("validacionoff", validation_toggle_command))
    application.add_handler(command_handler("setreglas", set_rules_command))
    application.add_handler(command_handler("reglas", rules_command))
    application.add_handler(command_handler("setbienvenida", set_join_message_command))
    application.add_handler(command_handler("setintro", set_intro_message_command))
    application.add_handler(command_handler("ban", ban_command))
    application.add_handler(command_handler("unban", unban_command))
    application.add_handler(command_handler("kick", kick_command))
    application.add_handler(command_handler("mute", mute_command))
    application.add_handler(command_handler("unmute", unmute_command))
    application.add_handler(command_handler("del", delete_command))
    application.add_handler(command_handler("limpiar", clean_command))
    application.add_handler(command_handler("pendientes", validation_pending_command))
    application.add_handler(command_handler("validacion", validation_status_command))

    # Entrada con y sin aprobación.
    application.add_handler(ChatJoinRequestHandler(validation_chat_join_request))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, validation_new_member))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, farewell_left_member))

    application.add_handler(CallbackQueryHandler(validation_join_request_callback, pattern="^valreq\\|"))
    application.add_handler(CallbackQueryHandler(admin_private_config_callback, pattern="^cfg\\|"))
    application.add_handler(CallbackQueryHandler(djgroup_callback_router, pattern="^djgroup\\|"))
    application.add_handler(CallbackQueryHandler(dj_listen_callback_router, pattern="^djlisten\\|"))
    application.add_handler(CallbackQueryHandler(djprivate_callback_router, pattern="^djpriv\\|"))
    application.add_handler(CallbackQueryHandler(djprivate_track_callback, pattern="^djtrack\\|"))
    application.add_handler(CallbackQueryHandler(hotgroup_callback_router, pattern="^hotgroup\\|"))
    application.add_handler(CallbackQueryHandler(hot_public_callback_router, pattern="^hotpublic\\|"))
    application.add_handler(CallbackQueryHandler(hot_callback_router, pattern="^hot\\|"))
    application.add_handler(CallbackQueryHandler(hotadd_callback, pattern="^hotadd\\|"))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, hot_text_router), group=-1)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_router))
    music_filter = filters.AUDIO | filters.VOICE | filters.Document.ALL
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & music_filter, dj_private_music_router), group=-2)
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL), admin_private_config_media))
    application.add_handler(MessageHandler(music_filter, music_message_router))
    application.add_error_handler(on_application_error)
    return application


def main() -> None:
    app = build_application()
    app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()
