
import asyncio
import json
import logging
import os
import secrets
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from html import escape as html_escape
from typing import Any, Dict, List, Optional

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
    volume: int = 100
    play_started_at: Optional[int] = None
    paused_remaining: Optional[int] = None
    validation_users: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    admin_config: Dict[str, Any] = field(default_factory=dict)
    member_activity: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    muted_users: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    entry_log: List[Dict[str, Any]] = field(default_factory=list)
    expelled_users: List[Dict[str, Any]] = field(default_factory=list)
    bot_log: List[Dict[str, Any]] = field(default_factory=list)


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
                state_data.setdefault("expelled_users", [])
                state_data.setdefault("bot_log", [])
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
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(live_label, callback_data="panel_live_toggle"),
                InlineKeyboardButton("⏭️ PROX.", callback_data="panel_next"),
                InlineKeyboardButton(auto_track_label, callback_data="panel_auto_track"),
            ],
            [
                InlineKeyboardButton(auto_sig_label, callback_data="panel_auto_sig"),
                InlineKeyboardButton("📋 Ver lista", callback_data="panel_queue"),
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
            [InlineKeyboardButton("📚 Biblioteca", callback_data=f"det|l|{source_message_id}")],
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


    async def change_volume(self, chat_id: int, delta: int) -> int:
        state = get_state(chat_id)
        state.volume = max(1, min(200, state.volume + delta))
        save_all_states()
        await self._apply_volume(chat_id, state.volume)
        if self.application:
            await ensure_control_panel(self.application.bot, chat_id)
        return state.volume

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
    "validation_show_rules_after_approve": False,
    "validation_public_join_media": None,
    "validation_public_join_media_position": "top",  # top | bottom
    "validation_public_join_buttons": [],
    "validation_approved_media": None,
    "validation_approved_media_position": "top",
    "validation_approved_buttons": [],
    "validation_rejected_media": None,
    "validation_rejected_media_position": "top",
    "validation_rejected_buttons": [],
    "validation_reminder_media": None,
    "validation_reminder_media_position": "top",
    "validation_reminder_buttons": [],
    "validation_timeout_media": None,
    "validation_timeout_media_position": "top",
    "validation_timeout_buttons": [],
    "farewell_enabled": True,
    "farewell_message": "👋 {mention} ha salido del grupo.",
    "farewell_media": None,
    "farewell_media_position": "top",
    "farewell_buttons": [],
    "pregonero_message": "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}",
    "pregonero_media": None,
    "pregonero_media_position": "top",
    "pregonero_buttons": [],
    "pregonero_manual_users": [],
    "rules_text": "📌 Normas del grupo\n\n1. Respeta al resto.\n2. No spam.\n3. Preséntate al entrar.",
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


def log_bot_action(chat_id: int, action: str, detail: str = "", *, user_id: Optional[int] = None) -> None:
    """Guarda acciones recientes del bot para el botón LOG 3d."""
    try:
        state = get_state(chat_id)
        now = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
        state.bot_log.append({
            "ts": now,
            "action": str(action or "accion"),
            "detail": str(detail or "")[:500],
            "user_id": int(user_id) if user_id else None,
        })
        cutoff = now - 3 * 86400
        state.bot_log = [item for item in state.bot_log[-500:] if int(item.get("ts") or 0) >= cutoff]
        save_all_states()
    except Exception:
        logger.exception("No se pudo guardar LOG del bot")


def admin_bot_log_text(chat_id: int, days: int = 3, limit: int = 80) -> str:
    state = get_state(chat_id)
    now = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
    cutoff = now - days * 86400
    items = [item for item in state.bot_log if int(item.get("ts") or 0) >= cutoff]
    items = list(reversed(items[-limit:]))
    if not items:
        return f"<b>📜 LOG {days}d</b>\n\nAún no hay acciones recientes registradas."
    lines = [f"<b>📜 LOG {days}d</b>", ""]
    for item in items:
        detail = str(item.get("detail") or "")
        suffix = f" — {h(detail)}" if detail else ""
        lines.append(f"• <b>{fmt_ts(item.get('ts'))}</b> · {h(item.get('action') or 'acción')}{suffix}")
    return "\n".join(lines)


def media_position_label(value: Any) -> str:
    return "⬆️ Arriba" if str(value or "top") == "top" else "⬇️ Debajo"


def toggle_media_position(chat_id: int, field: str) -> None:
    current = str(cfg_value(chat_id, field, "top") or "top")
    cfg_set(chat_id, field, "bottom" if current == "top" else "top")

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


def remember_expelled_user(chat_id: int, user_id: int, *, record: Optional[Dict[str, Any]] = None, reason: str = "") -> None:
    state = get_state(chat_id)
    base = dict(record or {})
    base.setdefault("user_id", int(user_id))
    if not base.get("name"):
        base["name"] = state.member_activity.get(str(user_id), {}).get("name", str(user_id))
    base["reason"] = reason or base.get("reason", "salida/expulsión")
    base["expelled_ts"] = _now_ts() if "_now_ts" in globals() else int(__import__("time").time())
    state.expelled_users.append(base)
    state.expelled_users = state.expelled_users[-300:]
    save_all_states()


def admin_expelled_users_text(chat_id: int, limit: int = 60) -> str:
    state = get_state(chat_id)
    items = list(reversed(state.expelled_users[-limit:]))
    if not items:
        return "<b>🚫 Usuarios expulsados / salidos</b>\n\nAún no tengo salidas o expulsiones registradas."
    lines = ["<b>🚫 Usuarios expulsados / salidos</b>", ""]
    for item in items:
        uid = int(item.get("user_id") or 0)
        reason = item.get("reason") or "salida"
        lines.append(f"• {mention_from_known_user(uid, item)} · {fmt_ts(item.get('expelled_ts'))} · <i>{h(reason)}</i>")
    return "\n".join(lines)


def manual_pregonero_mentions(chat_id: int) -> List[str]:
    raw = cfg_value(chat_id, "pregonero_manual_users", [])
    if isinstance(raw, str):
        items = [x.strip() for x in raw.replace(",", "\n").splitlines() if x.strip()]
    elif isinstance(raw, list):
        items = [str(x).strip() for x in raw if str(x).strip()]
    else:
        items = []
    mentions: List[str] = []
    for item in items[:500]:
        original = item.strip()
        if not original:
            continue
        # Formatos admitidos: @user | 123456 | Nombre - 123456 | Nombre|123456 | tg://user?id=123456
        user_id = ""
        name = ""
        if original.startswith("@"):
            mentions.append(h(original))
            continue
        if "tg://user?id=" in original:
            user_id = original.split("tg://user?id=", 1)[1].split("\"", 1)[0].split("'", 1)[0].split()[0]
            name = original.split(" - ", 1)[0].strip() if " - " in original else user_id
        elif " - " in original:
            name, user_id = [x.strip() for x in original.rsplit(" - ", 1)]
        elif "|" in original:
            name, user_id = [x.strip() for x in original.rsplit("|", 1)]
        elif original.lstrip("-").isdigit():
            user_id = original
            name = original
        if user_id.lstrip("-").isdigit():
            mentions.append(f'<a href="tg://user?id={int(user_id)}">{h(name or user_id)}</a>')
    return mentions


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
    mentions = []
    seen = set()
    for uid, record in users.items():
        try:
            if str(uid) in seen:
                continue
            seen.add(str(uid))
            mentions.append(mention_from_known_user(int(uid), record))
        except Exception:
            continue
    for mention in manual_pregonero_mentions(chat_id):
        key = mention.lower()
        if key not in seen:
            mentions.append(mention)
            seen.add(key)
    return mentions


async def send_pregonero(context: ContextTypes.DEFAULT_TYPE, chat_id: int, *, title: str = "") -> None:
    mentions = known_pregonero_mentions(chat_id)
    if not mentions:
        await send_temp_message(context.bot, chat_id, "📣 No tengo usuarios registrados todavía para mencionar.", ttl=40)
        return
    max_per_message = int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4) or 4)
    max_per_message = max(1, min(4, max_per_message))
    template = str(cfg_value(chat_id, "pregonero_message", "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}"))
    total_chunks = page_total(len(mentions), max_per_message)
    for index, start in enumerate(range(0, len(mentions), max_per_message), start=1):
        chunk = mentions[start:start + max_per_message]
        text = render_template_for_user(
            template,
            None,
            chat_title=str(cfg_value(chat_id, "chat_title", "")),
            extra={
                "mentions": " ".join(chunk),
                "total": str(len(mentions)),
                "bloque": str(index),
                "bloques": str(total_chunks),
            },
        )
        if "{mentions}" not in template:
            text += "\n\n" + " ".join(chunk)
        await send_configured_profile_message(context.bot, chat_id, "pregonero", text)
        await asyncio.sleep(0.8)
    log_bot_action(chat_id, "pregonero", f"{len(mentions)} menciones en bloques de {max_per_message}")



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
    elif kind == "expelled":
        await update.message.reply_html(admin_expelled_users_text(chat_id))
    elif kind == "log":
        await update.message.reply_html(admin_bot_log_text(chat_id))


async def silenciados_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "muted")


async def entradas_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "entries")


async def inactivos_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "inactive")


async def ranking_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "ranking")


async def expulsados_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "expelled")


async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stats_command(update, context, "log")


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
        [InlineKeyboardButton("🏓 Ping", callback_data="adm|ping"), InlineKeyboardButton("📣 Pregonero", callback_data="adm|pregonero"), InlineKeyboardButton("🔄 Reload", callback_data="adm|reload")],
        [InlineKeyboardButton(f"Validación {bool_label(cfg.get('validation_enabled'))}", callback_data="adm|toggle_validation"), InlineKeyboardButton(f"Autoaprobar {bool_label(cfg.get('validation_auto_approve_join_requests'))}", callback_data="adm|toggle_autoapprove"), InlineKeyboardButton(f"Timeout {bool_label(cfg.get('validation_kick_if_timeout'))}", callback_data="adm|toggle_kick_timeout")],
        [InlineKeyboardButton("👥 Pendientes", callback_data="adm|pendientes"), InlineKeyboardButton("🔇 Silenciados", callback_data="adm|muted"), InlineKeyboardButton("🚫 Expulsados", callback_data="adm|expelled")],
        [InlineKeyboardButton("🚪 Entradas", callback_data="adm|entries"), InlineKeyboardButton("🕙 Inactivos", callback_data="adm|inactive"), InlineKeyboardButton("🏆 Ranking", callback_data="adm|ranking")],
        [InlineKeyboardButton("⏱ -", callback_data="adm|time_minus"), InlineKeyboardButton(f"⏱ {cfg.get('validation_timeout_minutes')}m", callback_data="adm|noop"), InlineKeyboardButton("⏱ +", callback_data="adm|time_plus")],
        [InlineKeyboardButton("🔔 -", callback_data="adm|rem_minus"), InlineKeyboardButton(f"🔔 {cfg.get('validation_reminder_minutes')}m", callback_data="adm|noop"), InlineKeyboardButton("🔔 +", callback_data="adm|rem_plus")],
        [InlineKeyboardButton("Comandos OFF", callback_data="adm|cleanup_set|off"), InlineKeyboardButton("Al ejecutar", callback_data="adm|cleanup_set|instant"), InlineKeyboardButton("Tras 15s", callback_data="adm|cleanup_set|ttl")],
        [InlineKeyboardButton("📌 Normas", callback_data="adm|reglas"), InlineKeyboardButton("📜 LOG 3d", callback_data="adm|log"), InlineKeyboardButton("❌ Cerrar", callback_data="adm|close")],
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


async def admin_deprecated_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Evita que /admin abra paneles viejos: lo redirige al panel oficial /plan."""
    if not update.effective_chat or not update.effective_user or not update.message or not chat_is_allowed(update.effective_chat.id):
        return
    chat_id = update.effective_chat.id
    remember_chat_title(chat_id, update.effective_chat.title or "")
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    await update.message.reply_html("⚠️ <b>/admin ya no se usa.</b> Abro el panel oficial <code>/plan</code>.")
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
    if action == "expelled":
        await query.edit_message_text(admin_expelled_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "log":
        await query.edit_message_text(admin_bot_log_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "reload":
        load_all_states()
        await query.answer("Recargado ✅")
        await query.edit_message_text(admin_panel_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup(chat_id))
        return True
    if action.startswith("cleanup_set|"):
        mode = action.split("|", 1)[1]
        if mode in ("off", "instant", "ttl"):
            cfg_set(chat_id, "command_cleanup_mode", mode)
        await query.edit_message_text(admin_panel_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup(chat_id))
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
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=validation_pending_admin_markup(chat_id, "adm"))
        return True
    elif action == "noop":
        await query.answer()
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
    remember_expelled_user(chat_id, target_id, reason="ban_manual")
    log_bot_action(chat_id, "ban manual", str(target_id), user_id=update.effective_user.id)
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
    remember_expelled_user(chat_id, target_id, reason="kick_manual")
    log_bot_action(chat_id, "kick manual", str(target_id), user_id=update.effective_user.id)
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
    "validation_public_join_message": "Mensaje público al entrar",
    "validation_intro_message": "Mensaje de preguntas",
    "validation_reminder_message": "Recordatorio",
    "validation_timeout_message": "Mensaje por timeout",
    "validation_approved_message": "Mensaje aprobado",
    "validation_rejected_message": "Mensaje rechazado",
    "farewell_message": "Mensaje despedida",
    "pregonero_message": "Mensaje pregonero",
    "rules_text": "Normas",
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
        "position": "validation_public_join_media_position",
        "buttons": "validation_public_join_buttons",
    },
    "approved": {
        "title": "Aprobado",
        "text": "validation_approved_message",
        "media": "validation_approved_media",
        "position": "validation_approved_media_position",
        "buttons": "validation_approved_buttons",
    },
    "rejected": {
        "title": "Rechazado",
        "text": "validation_rejected_message",
        "media": "validation_rejected_media",
        "position": "validation_rejected_media_position",
        "buttons": "validation_rejected_buttons",
    },
    "reminder": {
        "title": "Recordatorio",
        "text": "validation_reminder_message",
        "media": "validation_reminder_media",
        "position": "validation_reminder_media_position",
        "buttons": "validation_reminder_buttons",
    },
    "timeout": {
        "title": "Timeout / expulsión",
        "text": "validation_timeout_message",
        "media": "validation_timeout_media",
        "position": "validation_timeout_media_position",
        "buttons": "validation_timeout_buttons",
    },
    "farewell": {
        "title": "Despedida",
        "text": "farewell_message",
        "media": "farewell_media",
        "position": "farewell_media_position",
        "buttons": "farewell_buttons",
    },
    "pregonero": {
        "title": "Pregonero",
        "text": "pregonero_message",
        "media": "pregonero_media",
        "position": "pregonero_media_position",
        "buttons": "pregonero_buttons",
    },
}


def profile_field(profile: str, key: str) -> str:
    return MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])[key]


def render_template_for_user(template: str, user=None, *, chat_title: str = "", extra: Optional[Dict[str, Any]] = None) -> str:
    text = str(template or "")
    if user is not None:
        username = f"@{getattr(user, 'username', '')}" if getattr(user, "username", None) else ""
        mention = user.mention_html() if hasattr(user, "mention_html") else h(display_name(user))
        text = (
            text
            .replace("{mention}", mention)
            .replace("{name}", h(display_name(user)))
            .replace("{first}", h(getattr(user, "first_name", "") or display_name(user)))
            .replace("{username}", h(username))
            .replace("{id}", h(getattr(user, "id", "")))
        )
    else:
        text = cfg_fake_preview_values(text)
    text = text.replace("{chat}", h(chat_title or "El grupo"))
    for key, value in (extra or {}).items():
        text = text.replace("{" + str(key) + "}", str(value))
    return text


def fake_profile_text(chat_id: int, profile: str) -> str:
    extra = {"mentions": '<a href="tg://user?id=123456789">Usuario Nuevo</a> @usuario2 @usuario3 @usuario4'}
    return render_template_for_user(
        str(cfg_value(chat_id, profile_field(profile, "text"), "")),
        None,
        chat_title=str(cfg_value(chat_id, "chat_title", "El grupo")),
        extra=extra,
    )


async def _send_media_only(bot, chat_id: int, media: Dict[str, Any], *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    mtype = media.get("type")
    fid = media.get("file_id")
    msg = None
    if mtype == "photo":
        msg = await bot.send_photo(chat_id, photo=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "video":
        msg = await bot.send_video(chat_id, video=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "animation":
        msg = await bot.send_animation(chat_id, animation=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "document":
        msg = await bot.send_document(chat_id, document=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    return getattr(msg, "message_id", None) if msg else None


async def send_configured_profile_message(bot, chat_id: int, profile: str, text: str, *, reply_to_message_id: Optional[int] = None, config_chat_id: Optional[int] = None) -> Optional[int]:
    """Envía texto + multimedia + botones según perfil. position=top usa caption; bottom manda texto primero y multimedia después."""
    cfg_chat_id = int(config_chat_id or chat_id)
    media = cfg_value(cfg_chat_id, profile_field(profile, "media"), None)
    position = str(cfg_value(cfg_chat_id, profile_field(profile, "position"), "top") or "top")
    markup = build_config_buttons_keyboard(cfg_chat_id, profile_field(profile, "buttons"))
    msg = None

    if isinstance(media, dict) and media.get("file_id") and position == "top":
        mtype = media.get("type")
        fid = media.get("file_id")
        if mtype == "photo":
            msg = await bot.send_photo(chat_id, photo=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
        elif mtype == "video":
            msg = await bot.send_video(chat_id, video=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
        elif mtype == "animation":
            msg = await bot.send_animation(chat_id, animation=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
        elif mtype == "document":
            msg = await bot.send_document(chat_id, document=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
        if msg is not None:
            return getattr(msg, "message_id", None)

    msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True, disable_web_page_preview=True)
    first_id = getattr(msg, "message_id", None)

    if isinstance(media, dict) and media.get("file_id") and position == "bottom":
        try:
            await _send_media_only(bot, chat_id, media, reply_to_message_id=first_id)
        except Exception:
            logger.exception("No se pudo enviar multimedia debajo para perfil %s", profile)
    return first_id


async def send_profile_preview(bot, private_chat_id: int, target_chat_id: int, profile: str) -> None:
    title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
    text = fake_profile_text(target_chat_id, profile)
    position = media_position_label(cfg_value(target_chat_id, profile_field(profile, "position"), "top"))
    await bot.send_message(private_chat_id, f"👁 <b>Vista previa completa: {h(title)}</b>\nMedia: <b>{position}</b>", parse_mode=ParseMode.HTML)
    await send_configured_profile_message(bot, private_chat_id, profile, text, config_chat_id=target_chat_id)



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
        "<i>Oficiales: /Djplan, /plan y /ajustes. /admin queda redirigido a /plan para evitar paneles antiguos.</i>"
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
    if not answering and not pending:
        lines.append("No hay usuarios pendientes.")
    return "\n".join(lines).strip()


def validation_pending_admin_markup(chat_id: int, origin: str = "cfg") -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    candidates = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") in ("answering", "pending_admin")]
    suffix = "" if origin == "adm" else f"|{chat_id}"
    for uid, record in candidates[:12]:
        label = str(record.get("name") or uid)[:28]
        rows.append([InlineKeyboardButton(f"👤 {label}", callback_data=f"val|noop|{uid}{suffix}")])
        rows.append([
            InlineKeyboardButton("🚫 Expulsar", callback_data=f"val|kick|{uid}{suffix}"),
            InlineKeyboardButton("🔔 Recordar", callback_data=f"val|remind|{uid}{suffix}"),
            InlineKeyboardButton("🔇 Silenciar", callback_data=f"val|mute|{uid}{suffix}"),
        ])
        if record.get("status") == "pending_admin":
            rows.append([
                InlineKeyboardButton("✅ Validar", callback_data=f"val|ok|{uid}{suffix}"),
                InlineKeyboardButton("❌ Rechazar", callback_data=f"val|no|{uid}{suffix}"),
            ])
    if origin == "adm":
        rows.append([InlineKeyboardButton("🔄 Reload", callback_data="adm|pendientes"), InlineKeyboardButton("🔙 Volver", callback_data="adm|back")])
    else:
        rows.append([InlineKeyboardButton("🔄 Reload", callback_data=f"cfg|pending|{chat_id}"), InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")])
    return InlineKeyboardMarkup(rows)


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

    def prof_line(profile: str, label: str) -> str:
        media = cfg_value(chat_id, profile_field(profile, "media"), None)
        buttons = cfg_value(chat_id, profile_field(profile, "buttons"), [])
        pos = media_position_label(cfg_value(chat_id, profile_field(profile, "position"), "top"))
        return f"{label}: media <b>{'Sí' if media else 'No'}</b> ({pos}) · botones <b>{len(buttons) if isinstance(buttons, list) else 0}</b>"

    manual_users = cfg_value(chat_id, "pregonero_manual_users", [])
    manual_count = len(manual_users) if isinstance(manual_users, list) else 0
    return (
        f"<b>⚙️ Configuración privada</b>\n\n"
        f"Grupo: <b>{h(title)}</b>\n"
        f"Validación: <b>{bool_label(cfg_value(chat_id, 'validation_enabled'))}</b> · "
        f"Quién valida: <b>{h(APPROVER_MODE_LABELS.get(approver, approver))}</b>\n"
        f"Preguntas: <b>{len(cfg_questions(chat_id))}</b> · "
        f"Tiempo: <b>{cfg_value(chat_id, 'validation_timeout_minutes')} min</b> · "
        f"Recordatorio: <b>{cfg_value(chat_id, 'validation_reminder_minutes')} min</b>\n"
        f"Autoaprobar: <b>{bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}</b> · "
        f"Expulsar timeout: <b>{bool_label(cfg_value(chat_id, 'validation_kick_if_timeout'))}</b> · "
        f"Despedida: <b>{bool_label(cfg_value(chat_id, 'farewell_enabled'))}</b>\n"
        f"Borrado comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n"
        f"Pregonero: <b>{cfg_value(chat_id, 'pregonero_max_mentions_per_message', 4)}</b> por mensaje · manuales <b>{manual_count}</b>\n\n"
        f"{prof_line('welcome', 'Bienvenida')}\n"
        f"{prof_line('reminder', 'Recordatorio')}\n"
        f"{prof_line('timeout', 'Timeout')}\n"
        f"{prof_line('approved', 'Aprobado')}\n"
        f"{prof_line('rejected', 'Rechazado')}\n"
        f"{prof_line('farewell', 'Despedida')}\n"
        f"{prof_line('pregonero', 'Pregonero')}\n\n"
        "Los cambios se guardan al pulsar cada botón; 💾 Guardar fuerza escritura en disco."
    )


def admin_private_main_markup(chat_id: int) -> InlineKeyboardMarkup:
    enabled = bool_label(cfg_value(chat_id, "validation_enabled"))
    kick = bool_label(cfg_value(chat_id, "validation_kick_if_timeout"))
    cleanup = command_cleanup_label(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Validación {enabled}", callback_data=f"cfg|toggle_validation|{chat_id}"), InlineKeyboardButton(f"Autoaprobar {bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}", callback_data=f"cfg|toggle_autoapprove|{chat_id}"), InlineKeyboardButton(f"Timeout {kick}", callback_data=f"cfg|toggle_kick|{chat_id}")],
        [InlineKeyboardButton("👥 Pendientes", callback_data=f"cfg|pending|{chat_id}"), InlineKeyboardButton("🔇 Silenciados", callback_data=f"cfg|muted|{chat_id}"), InlineKeyboardButton("🚫 Expulsados", callback_data=f"cfg|expelled|{chat_id}")],
        [InlineKeyboardButton("🚪 Entradas", callback_data=f"cfg|entries|{chat_id}"), InlineKeyboardButton("🕙 Inactivos", callback_data=f"cfg|inactive|{chat_id}"), InlineKeyboardButton("🏆 Ranking", callback_data=f"cfg|ranking|{chat_id}")],
        [InlineKeyboardButton("⏱ -", callback_data=f"cfg|time_minus|{chat_id}"), InlineKeyboardButton(f"⏱ {cfg_value(chat_id, 'validation_timeout_minutes')}m", callback_data=f"cfg|noop|{chat_id}"), InlineKeyboardButton("⏱ +", callback_data=f"cfg|time_plus|{chat_id}")],
        [InlineKeyboardButton("🔔 -", callback_data=f"cfg|rem_minus|{chat_id}"), InlineKeyboardButton(f"🔔 {cfg_value(chat_id, 'validation_reminder_minutes')}m", callback_data=f"cfg|noop|{chat_id}"), InlineKeyboardButton("🔔 +", callback_data=f"cfg|rem_plus|{chat_id}")],
        [InlineKeyboardButton("Comandos OFF", callback_data=f"cfg|cleanup_set|{chat_id}|off"), InlineKeyboardButton("Al ejecutar", callback_data=f"cfg|cleanup_set|{chat_id}|instant"), InlineKeyboardButton("Tras 15s", callback_data=f"cfg|cleanup_set|{chat_id}|ttl")],
        [InlineKeyboardButton("👁 Bienvenida", callback_data=f"cfg|preview|{chat_id}|welcome"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|validation_public_join_message"), InlineKeyboardButton("🖼 Media", callback_data=f"cfg|media|{chat_id}|welcome")],
        [InlineKeyboardButton("⌨️ Botones", callback_data=f"cfg|buttons|{chat_id}|welcome"), InlineKeyboardButton(f"Media {media_position_label(cfg_value(chat_id, 'validation_public_join_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|welcome"), InlineKeyboardButton("❓ Preguntas", callback_data=f"cfg|questions|{chat_id}")],
        [InlineKeyboardButton("👁 Recordatorio", callback_data=f"cfg|preview|{chat_id}|reminder"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|validation_reminder_message"), InlineKeyboardButton("🖼 Media", callback_data=f"cfg|media|{chat_id}|reminder")],
        [InlineKeyboardButton(f"Pos record {media_position_label(cfg_value(chat_id, 'validation_reminder_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|reminder"), InlineKeyboardButton("⌨️ Botones rec.", callback_data=f"cfg|buttons|{chat_id}|reminder"), InlineKeyboardButton(" ", callback_data=f"cfg|noop|{chat_id}")],
        [InlineKeyboardButton("👁 Timeout", callback_data=f"cfg|preview|{chat_id}|timeout"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|validation_timeout_message"), InlineKeyboardButton("🖼 Media", callback_data=f"cfg|media|{chat_id}|timeout")],
        [InlineKeyboardButton(f"Pos timeout {media_position_label(cfg_value(chat_id, 'validation_timeout_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|timeout"), InlineKeyboardButton("⌨️ Botones time", callback_data=f"cfg|buttons|{chat_id}|timeout"), InlineKeyboardButton(" ", callback_data=f"cfg|noop|{chat_id}")],
        [InlineKeyboardButton("✅ Aprobado", callback_data=f"cfg|preview|{chat_id}|approved"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|validation_approved_message"), InlineKeyboardButton(f"Normas {bool_label(cfg_value(chat_id, 'validation_show_rules_after_approve'))}", callback_data=f"cfg|toggle_rules_after_approve|{chat_id}")],
        [InlineKeyboardButton("🖼 Media aprob.", callback_data=f"cfg|media|{chat_id}|approved"), InlineKeyboardButton(f"Pos aprob {media_position_label(cfg_value(chat_id, 'validation_approved_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|approved"), InlineKeyboardButton("⌨️ Btn aprob.", callback_data=f"cfg|buttons|{chat_id}|approved")],
        [InlineKeyboardButton("❌ Rechazo", callback_data=f"cfg|preview|{chat_id}|rejected"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|validation_rejected_message"), InlineKeyboardButton("🖼 Media", callback_data=f"cfg|media|{chat_id}|rejected")],
        [InlineKeyboardButton(f"Pos rechazo {media_position_label(cfg_value(chat_id, 'validation_rejected_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|rejected"), InlineKeyboardButton("⌨️ Btn rechazo", callback_data=f"cfg|buttons|{chat_id}|rejected"), InlineKeyboardButton(" ", callback_data=f"cfg|noop|{chat_id}")],
        [InlineKeyboardButton("👋 Despedida", callback_data=f"cfg|preview|{chat_id}|farewell"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|farewell_message"), InlineKeyboardButton(f"ON/OFF {bool_label(cfg_value(chat_id, 'farewell_enabled'))}", callback_data=f"cfg|toggle_farewell|{chat_id}")],
        [InlineKeyboardButton("🖼 Media desped.", callback_data=f"cfg|media|{chat_id}|farewell"), InlineKeyboardButton(f"Pos desped {media_position_label(cfg_value(chat_id, 'farewell_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|farewell"), InlineKeyboardButton("⌨️ Btn desped.", callback_data=f"cfg|buttons|{chat_id}|farewell")],
        [InlineKeyboardButton("📣 Pregonero", callback_data=f"cfg|preview|{chat_id}|pregonero"), InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|pregonero_message"), InlineKeyboardButton("👤 Users", callback_data=f"cfg|pregonero_users|{chat_id}")],
        [InlineKeyboardButton("🖼 Media preg.", callback_data=f"cfg|media|{chat_id}|pregonero"), InlineKeyboardButton(f"Pos preg {media_position_label(cfg_value(chat_id, 'pregonero_media_position'))}", callback_data=f"cfg|toggle_pos|{chat_id}|pregonero"), InlineKeyboardButton("4 en 4", callback_data=f"cfg|pregonero_4|{chat_id}")],
        [InlineKeyboardButton("⌨️ Btn preg.", callback_data=f"cfg|buttons|{chat_id}|pregonero"), InlineKeyboardButton("👮 Valida", callback_data=f"cfg|approvers|{chat_id}"), InlineKeyboardButton(" ", callback_data=f"cfg|noop|{chat_id}")],
        [InlineKeyboardButton("📌 Normas", callback_data=f"cfg|edit_text|{chat_id}|rules_text"), InlineKeyboardButton("📚 Comandos", callback_data=f"cfg|commands|{chat_id}"), InlineKeyboardButton("📜 LOG 3d", callback_data=f"cfg|log|{chat_id}")],
        [InlineKeyboardButton("🔄 Reload", callback_data=f"cfg|reload|{chat_id}"), InlineKeyboardButton("💾 Guardar", callback_data=f"cfg|save|{chat_id}"), InlineKeyboardButton("❌ Cerrar", callback_data=f"cfg|close|{chat_id}")],
        [InlineKeyboardButton("🔙 Grupos", callback_data="cfg|list")],
    ])

def approver_markup(chat_id: int) -> InlineKeyboardMarkup:
    current = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
    rows = []
    for mode, label in APPROVER_MODE_LABELS.items():
        mark = "✅ " if mode == current else ""
        rows.append([InlineKeyboardButton(mark + label, callback_data=f"cfg|setapprover|{chat_id}|{mode}")])
    rows.append([InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")])
    return InlineKeyboardMarkup(rows)


async def admin_private_config_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    if not is_global_admin_user(update.effective_user.id):
        await update.message.reply_text("Solo ADMIN_IDS puede abrir la configuración privada.")
        return
    if update.effective_chat and update.effective_chat.type != "private":
        await update.message.reply_text("Escríbeme /configuracion por privado para configurar el grupo sin ensuciar el chat.")
        return
    load_all_states()
    if not known_admin_chats_for_private():
        await update.message.reply_text("Todavía no tengo grupos registrados. Usa /plan una vez dentro del grupo o espera a que entre alguien.")
        return
    await update.message.reply_html("<b>Elige el grupo que quieres configurar:</b>", reply_markup=admin_private_chat_list_markup())


async def send_public_join_preview(bot, private_chat_id: int, target_chat_id: int) -> None:
    await send_profile_preview(bot, private_chat_id, target_chat_id, "welcome")


async def send_configured_public_join(bot, chat_id: int, user, *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    text = render_template_for_user(
        str(cfg_value(chat_id, "validation_public_join_message", VALIDATION_PUBLIC_JOIN_MESSAGE)),
        user,
        chat_title=str(cfg_value(chat_id, "chat_title", "")),
    )
    return await send_configured_profile_message(bot, chat_id, "welcome", text, reply_to_message_id=reply_to_message_id)


async def admin_private_config_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    data = query.data or ""
    if not data.startswith("cfg|"):
        return
    await query.answer()
    parts = data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    if action == "list":
        if not await can_use_private_config(context, update.effective_user.id):
            await query.answer("Solo el dueño o administradores.", show_alert=True)
            return
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
    if action == "open":
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            await query.edit_message_text("Panel cerrado.")
        return
    if action == "reload":
        load_all_states()
        await query.answer("Reload completo ✅")
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    if action == "save":
        save_all_states()
        await query.answer("Cambios guardados ✅", show_alert=True)
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    if action == "noop":
        await query.answer()
        return
    if action == "toggle_validation":
        cfg_set(chat_id, "validation_enabled", not bool(cfg_value(chat_id, "validation_enabled")))
    elif action == "toggle_kick":
        cfg_set(chat_id, "validation_kick_if_timeout", not bool(cfg_value(chat_id, "validation_kick_if_timeout")))
    elif action == "toggle_autoapprove":
        cfg_set(chat_id, "validation_auto_approve_join_requests", not bool(cfg_value(chat_id, "validation_auto_approve_join_requests")))
    elif action == "time_plus":
        cfg_set(chat_id, "validation_timeout_minutes", int(cfg_value(chat_id, "validation_timeout_minutes", 10)) + 1)
    elif action == "time_minus":
        cfg_set(chat_id, "validation_timeout_minutes", max(1, int(cfg_value(chat_id, "validation_timeout_minutes", 10)) - 1))
    elif action == "rem_plus":
        cfg_set(chat_id, "validation_reminder_minutes", int(cfg_value(chat_id, "validation_reminder_minutes", 3)) + 1)
    elif action == "rem_minus":
        cfg_set(chat_id, "validation_reminder_minutes", max(1, int(cfg_value(chat_id, "validation_reminder_minutes", 3)) - 1))
    elif action == "preview_public":
        await send_public_join_preview(context.bot, query.message.chat.id, chat_id)
        await query.answer("Vista previa enviada.")
        return
    elif action == "preview" and len(parts) >= 4:
        await send_profile_preview(context.bot, query.message.chat.id, chat_id, parts[3])
        await query.answer("Vista previa enviada.")
        return
    elif action == "commands":
        await query.edit_message_text(all_commands_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "cleanup_mode":
        cfg_set(chat_id, "command_cleanup_mode", next_command_cleanup_mode(chat_id))
    elif action == "cleanup_set" and len(parts) >= 4:
        mode = parts[3]
        if mode in ("off", "instant", "ttl"):
            cfg_set(chat_id, "command_cleanup_mode", mode)
    elif action == "toggle_rules_after_approve":
        cfg_set(chat_id, "validation_show_rules_after_approve", not bool(cfg_value(chat_id, "validation_show_rules_after_approve")))
    elif action == "toggle_farewell":
        cfg_set(chat_id, "farewell_enabled", not bool(cfg_value(chat_id, "farewell_enabled")))
    elif action == "pregonero_4":
        cfg_set(chat_id, "pregonero_max_mentions_per_message", 4)
        await query.answer("Pregonero configurado 4 en 4 ✅")
    elif action == "toggle_pos" and len(parts) >= 4:
        profile = parts[3]
        if profile in MESSAGE_PROFILES:
            toggle_media_position(chat_id, profile_field(profile, "position"))
    elif action == "muted":
        await query.edit_message_text(admin_muted_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "entries":
        await query.edit_message_text(admin_last_entries_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "inactive":
        await query.edit_message_text(admin_inactive_users_text(chat_id, 10), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "ranking":
        await query.edit_message_text(admin_ranking_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "expelled":
        await query.edit_message_text(admin_expelled_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "log":
        await query.edit_message_text(admin_bot_log_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "pending":
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=validation_pending_admin_markup(chat_id, "cfg"))
        return
    elif action == "pregonero_users":
        current = cfg_value(chat_id, "pregonero_manual_users", [])
        current_text = "\n".join(str(x) for x in current) if isinstance(current, list) else str(current or "")
        set_config_pending(update.effective_user.id, {"kind": "cfg_pregonero_users", "chat_id": chat_id})
        await query.edit_message_text(
            "👤 <b>Usuarios manuales del pregonero</b>\n\n"
            "Envíame uno por línea. Formatos admitidos:\n"
            "<code>@usuario</code>\n"
            "<code>123456789</code>\n"
            "<code>Nombre - 123456789</code>\n\n"
            "Para borrar todos escribe <code>QUITAR</code>.\n\n"
            f"Actual:\n<pre>{h(current_text or '—')}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "edit_text" and len(parts) >= 4:
        field = parts[3]
        set_config_pending(update.effective_user.id, {"kind": "cfg_text", "chat_id": chat_id, "field": field})
        label = CONFIG_TEXT_FIELDS.get(field, field)
        current = str(cfg_value(chat_id, field, ""))
        await query.edit_message_text(
            f"✏️ <b>{h(label)}</b>\n\nActual:\n<pre>{h(current)}</pre>\n\nEnvíame ahora el nuevo texto por aquí.\n\nVariables: <code>{{mention}}</code>, <code>{{name}}</code>, <code>{{first}}</code>, <code>{{username}}</code>, <code>{{id}}</code>, <code>{{chat}}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "questions":
        set_config_pending(update.effective_user.id, {"kind": "cfg_questions", "chat_id": chat_id})
        current = "\n".join(cfg_questions(chat_id))
        await query.edit_message_text(
            f"❓ <b>Preguntas actuales</b>\n\n<pre>{h(current)}</pre>\n\nEnvíame las nuevas preguntas, una por línea o separadas por |.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "buttons":
        profile = parts[3] if len(parts) >= 4 else "welcome"
        buttons_field = profile_field(profile, "buttons")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_buttons", "chat_id": chat_id, "buttons_field": buttons_field})
        await query.edit_message_text(
            f"⌨️ <b>Botones: {h(title)}</b>\n\nEnvíame botones así, uno por línea:\n\n<code>Texto del botón - https://enlace.com</code>\n<code>Normas - https://t.me/...</code>\n\nPara borrar todos escribe: <code>QUITAR</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "media":
        profile = parts[3] if len(parts) >= 4 else "welcome"
        media_field = profile_field(profile, "media")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_media", "chat_id": chat_id, "media_field": media_field})
        await query.edit_message_text(
            f"🖼 <b>Multimedia: {h(title)}</b>\n\nEnvíame ahora una foto, vídeo, GIF o documento.\n\nPara quitar multimedia escribe: <code>QUITAR</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
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
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))


async def admin_private_config_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user or not update.message or not update.message.text:
        return False
    if update.effective_chat and update.effective_chat.type != "private":
        return False
    pending = get_config_pending(update.effective_user.id)
    if not pending:
        return False
    if not is_global_admin_user(update.effective_user.id):
        return True
    text = update.message.text.strip()
    kind = pending.get("kind")
    chat_id = int(pending.get("chat_id"))
    if text.upper() in ("CANCELAR", "/CANCELAR"):
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html(admin_private_main_text(chat_id), reply_markup=admin_private_main_markup(chat_id))
        return True
    if kind == "cfg_text":
        field = str(pending.get("field"))
        cfg_set(chat_id, field, text)
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html(f"✅ Texto actualizado: <b>{h(CONFIG_TEXT_FIELDS.get(field, field))}</b>", reply_markup=admin_private_main_markup(chat_id))
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
    if kind == "cfg_pregonero_users":
        if text.upper() == "QUITAR":
            cfg_set(chat_id, "pregonero_manual_users", [])
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("✅ Usuarios manuales del pregonero borrados.", reply_markup=admin_private_main_markup(chat_id))
            return True
        users = [line.strip() for line in text.replace(",", "\n").splitlines() if line.strip()]
        cfg_set(chat_id, "pregonero_manual_users", users[:500])
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html(f"✅ Usuarios manuales guardados: <b>{len(users[:500])}</b>", reply_markup=admin_private_main_markup(chat_id))
        return True
    if kind == "cfg_buttons":
        buttons_field = str(pending.get("buttons_field") or "validation_public_join_buttons")
        if text.upper() == "QUITAR":
            cfg_set(chat_id, buttons_field, [])
            pop_config_pending(update.effective_user.id)
            await update.message.reply_html("✅ Botones quitados.", reply_markup=admin_private_main_markup(chat_id))
            return True
        buttons = parse_buttons_text(text)
        if not buttons:
            await update.message.reply_html("No he detectado botones válidos. Usa: <code>Texto - https://enlace.com</code>")
            return True
        cfg_set(chat_id, buttons_field, buttons)
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html(f"✅ Botones guardados: <b>{len(buttons)}</b>", reply_markup=admin_private_main_markup(chat_id))
        return True
    if kind == "cfg_media" and text.upper() == "QUITAR":
        media_field = str(pending.get("media_field") or "validation_public_join_media")
        cfg_set(chat_id, media_field, None)
        pop_config_pending(update.effective_user.id)
        await update.message.reply_html("✅ Multimedia quitada.", reply_markup=admin_private_main_markup(chat_id))
        return True
    return False


async def admin_private_config_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.effective_chat or update.effective_chat.type != "private":
        return
    pending = get_config_pending(update.effective_user.id)
    if not pending or pending.get("kind") != "cfg_media":
        return
    if not is_global_admin_user(update.effective_user.id):
        return
    chat_id = int(pending.get("chat_id"))
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
    media_field = str(pending.get("media_field") or "validation_public_join_media")
    cfg_set(chat_id, media_field, media)
    pop_config_pending(update.effective_user.id)
    await update.message.reply_html("✅ Multimedia guardada.", reply_markup=admin_private_main_markup(chat_id))


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
        q_msg = await bot.send_message(
            chat_id=chat_id,
            text=(
                validation_format_template(str(cfg_value(chat_id, "validation_intro_message", VALIDATION_INTRO_MESSAGE)), user)
                + f"\n\n<b>Pregunta 1/{len(questions)}</b>\n{h(questions[0])}"
            ),
            parse_mode=ParseMode.HTML,
            reply_to_message_id=reply_to_message_id,
            allow_sending_without_reply=True,
        )
        record["question_message_id"] = q_msg.message_id
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


async def farewell_left_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not update.message.left_chat_member:
        return
    chat_id = update.effective_chat.id
    if not chat_is_allowed(chat_id):
        return
    user = update.message.left_chat_member
    if getattr(user, "is_bot", False):
        return
    remember_chat_title(chat_id, update.effective_chat.title or "")
    source = "salida"
    if update.message.from_user and update.message.from_user.id != user.id:
        source = "eliminado_por_admin"
    remember_expelled_user(chat_id, user.id, record=user_record_from_user(user), reason=source)
    log_bot_action(chat_id, "usuario salió del grupo", f"{user.id} · {source}")
    if not bool(cfg_value(chat_id, "farewell_enabled", True)):
        return
    try:
        text = render_template_for_user(
            str(cfg_value(chat_id, "farewell_message", "👋 {mention} ha salido del grupo.")),
            user,
            chat_title=str(cfg_value(chat_id, "chat_title", "")),
        )
        await send_configured_profile_message(context.bot, chat_id, "farewell", text, reply_to_message_id=update.message.message_id)
    except Exception:
        logger.exception("No se pudo enviar despedida")


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
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🕵️ Solicitud de entrada detectada: {user.mention_html()}\nAutoaprobación desactivada en /ajustes.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


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
        # Limpia la pregunta anterior del bot para que el chat no se llene.
        old_qid = record.get("question_message_id")
        if old_qid:
            try:
                await safe_delete(context.bot, chat_id, int(old_qid))
            except Exception:
                pass
        record["status"] = "answering"
        validation_set_record(chat_id, user.id, record)
        q_msg = await update.message.reply_html(
            f"✅ Recibido.\n\n<b>Pregunta {step + 1}/{len(questions)}</b>\n{h(questions[step])}"
        )
        record["question_message_id"] = q_msg.message_id
        validation_set_record(chat_id, user.id, record)
        return True
    old_qid = record.get("question_message_id")
    if old_qid:
        try:
            await safe_delete(context.bot, chat_id, int(old_qid))
        except Exception:
            pass
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
    chat_id = update.effective_chat.id
    if len(parts) >= 4:
        try:
            chat_id = int(parts[3])
        except Exception:
            chat_id = update.effective_chat.id
    if not validation_is_active_for_chat(chat_id):
        await query.answer("Validación desactivada.", show_alert=True)
        return True
    if not await can_validate_presentation(context, chat_id, update.effective_user.id):
        await query.answer("No tienes permiso para validar presentaciones.", show_alert=True)
        return True
    record = validation_get_record(chat_id, target_id)
    if not record:
        await query.answer("No encuentro esta presentación.", show_alert=True)
        return True
    target_mention = f'<a href="tg://user?id={target_id}">{h(record.get("name") or target_id)}</a>'
    if action == "noop":
        await query.answer(str(record.get("name") or target_id), show_alert=False)
        return True
    if action == "remind":
        reminder_text = str(cfg_value(chat_id, "validation_reminder_message", VALIDATION_REMINDER_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
        await send_configured_profile_message(context.bot, chat_id, "reminder", reminder_text)
        record["reminded"] = True
        validation_set_record(chat_id, target_id, record)
        log_bot_action(chat_id, "recordatorio manual", str(target_id), user_id=update.effective_user.id)
        await query.answer("Recordatorio enviado ✅")
        return True
    if action == "mute":
        try:
            await validation_mute_bot(context.bot, chat_id, target_id)
            mark_user_muted(chat_id, target_id, reason="silenciado_hasta_contestar")
            log_bot_action(chat_id, "silenciado hasta contestar", str(target_id), user_id=update.effective_user.id)
            await query.answer("Usuario silenciado hasta que conteste ✅")
        except Exception:
            await query.answer("No pude silenciar. Revisa permisos.", show_alert=True)
        return True
    if action == "kick":
        timeout_text = str(cfg_value(chat_id, "validation_timeout_message", VALIDATION_TIMEOUT_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
        try:
            await send_configured_profile_message(context.bot, chat_id, "timeout", timeout_text)
        except Exception:
            logger.exception("No se pudo enviar mensaje timeout antes de expulsar")
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=target_id, only_if_banned=True)
            record["status"] = "kicked_manual"
            record["kicked_by"] = update.effective_user.id
            record["kicked_ts"] = _now_ts()
            validation_set_record(chat_id, target_id, record)
            unmark_user_muted(chat_id, target_id)
            remember_expelled_user(chat_id, target_id, record=record, reason="expulsado_desde_pendientes")
            log_bot_action(chat_id, "expulsado desde pendientes", str(target_id), user_id=update.effective_user.id)
            try:
                await query.edit_message_text(f"🚫 Usuario expulsado desde pendientes: {target_mention}", parse_mode=ParseMode.HTML)
            except Exception:
                pass
            await query.answer("Usuario expulsado ✅")
        except Exception:
            logger.exception("No se pudo expulsar desde pendientes")
            await query.answer("No pude expulsar. Revisa permisos del bot.", show_alert=True)
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
        if bool(cfg_value(chat_id, "validation_show_rules_after_approve", False)):
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=str(cfg_value(chat_id, "rules_text", "")),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception:
                logger.exception("No se pudieron mostrar normas tras aprobar")
        log_bot_action(chat_id, "usuario aprobado", str(target_id), user_id=update.effective_user.id)
        await query.answer("Usuario validado.")
        return True
    if action == "no":
        record["status"] = "rejected"
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
        remember_expelled_user(chat_id, target_id, record=record, reason="presentacion_rechazada")
        log_bot_action(chat_id, "usuario rechazado/expulsado", str(target_id), user_id=update.effective_user.id)
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
                            log_bot_action(chat_id, "recordatorio automático", str(user_id))
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
                            log_bot_action(chat_id, "timeout presentación", str(user_id))
                        except Exception:
                            pass
                        if bool(cfg_value(chat_id, "validation_kick_if_timeout", VALIDATION_KICK_IF_TIMEOUT)):
                            try:
                                await application.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                                record["status"] = "kicked_timeout"
                                validation_set_record(chat_id, user_id, record)
                                remember_expelled_user(chat_id, user_id, record=record, reason="timeout")
                                log_bot_action(chat_id, "expulsado por timeout", str(user_id))
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
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 Expulsar", callback_data=f"val|kick|{uid}"), InlineKeyboardButton("🔔 Recordar", callback_data=f"val|remind|{uid}"), InlineKeyboardButton("🔇 Silenciar", callback_data=f"val|mute|{uid}")],
            [InlineKeyboardButton("✅ Validar", callback_data=f"val|ok|{uid}"), InlineKeyboardButton("❌ Rechazar", callback_data=f"val|no|{uid}")],
        ])
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
        expelled_users=state.expelled_users,
        bot_log=state.bot_log,
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

    control_prefixes = ("menu_", "panel_", "pin|", "det|", "q|", "lib|", "lst|")
    if (data == "bot_ping" or data.startswith(control_prefixes)) and not await can_control_bot(context, chat_id, user_id):
        await query.answer("Solo el dueño o administradores pueden controlar el bot.", show_alert=True)
        return

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


async def presentate_accent_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat and update.effective_user:
        remember_member_activity(update.effective_chat.id, update.effective_user, kind="command", source="/preséntate")
    await force_presentate_command(update, context)
    await cleanup_command_invocation(update, context)


# ============================================================
# PARCHE 2: VALIDACIÓN PRO + PREGONERO 4x4 + LOG + MULTIMEDIA
# ============================================================
DEFAULT_ADMIN_CONFIG.update({
    "validation_reminder_media": None,
    "validation_reminder_buttons": [],
    "validation_reminder_media_position": "top",  # top | bottom
    "validation_timeout_media": None,
    "validation_timeout_buttons": [],
    "validation_timeout_media_position": "top",  # top | bottom
    "validation_public_join_media_position": "top",
    "validation_approved_media_position": "top",
    "validation_rejected_media_position": "top",
    "validation_show_rules_after_approval": False,
    "pregonero_text": "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}",
    "pregonero_media": None,
    "pregonero_buttons": [],
    "pregonero_media_position": "top",
    "pregonero_max_mentions_per_message": 4,
})

MESSAGE_PROFILES = {
    "welcome": {
        "title": "Bienvenida / entrada",
        "text": "validation_public_join_message",
        "media": "validation_public_join_media",
        "buttons": "validation_public_join_buttons",
        "position": "validation_public_join_media_position",
    },
    "approved": {
        "title": "Aprobado",
        "text": "validation_approved_message",
        "media": "validation_approved_media",
        "buttons": "validation_approved_buttons",
        "position": "validation_approved_media_position",
    },
    "rejected": {
        "title": "Rechazado",
        "text": "validation_rejected_message",
        "media": "validation_rejected_media",
        "buttons": "validation_rejected_buttons",
        "position": "validation_rejected_media_position",
    },
    "reminder": {
        "title": "Recordatorio",
        "text": "validation_reminder_message",
        "media": "validation_reminder_media",
        "buttons": "validation_reminder_buttons",
        "position": "validation_reminder_media_position",
    },
    "timeout": {
        "title": "Timeout / expulsión",
        "text": "validation_timeout_message",
        "media": "validation_timeout_media",
        "buttons": "validation_timeout_buttons",
        "position": "validation_timeout_media_position",
    },
    "pregonero": {
        "title": "Pregonero",
        "text": "pregonero_text",
        "media": "pregonero_media",
        "buttons": "pregonero_buttons",
        "position": "pregonero_media_position",
    },
}

CONFIG_TEXT_FIELDS.update({
    "validation_reminder_message": "Texto recordatorio",
    "validation_timeout_message": "Texto timeout / expulsión",
    "pregonero_text": "Texto pregonero",
})


def ensure_state_v2(chat_id: int) -> ChatState:
    state = get_state(chat_id)
    if not hasattr(state, "expelled_users") or state.expelled_users is None:
        state.expelled_users = []
    if not hasattr(state, "bot_log") or state.bot_log is None:
        state.bot_log = []
    return state


def add_bot_log(chat_id: int, action: str, details: str = "", *, user_id: Optional[int] = None) -> None:
    try:
        state = ensure_state_v2(chat_id)
        item = {
            "ts": _now_ts() if "_now_ts" in globals() else int(__import__("time").time()),
            "action": str(action),
            "details": str(details or ""),
        }
        if user_id is not None:
            item["user_id"] = int(user_id)
        state.bot_log.append(item)
        cutoff = item["ts"] - 7 * 86400
        state.bot_log = [x for x in state.bot_log[-700:] if int(x.get("ts") or 0) >= cutoff]
        save_all_states()
    except Exception:
        logger.exception("No se pudo escribir LOG del bot")


def bot_log_text(chat_id: int, days: int = 3, limit: int = 80) -> str:
    state = ensure_state_v2(chat_id)
    cutoff = (_now_ts() if "_now_ts" in globals() else int(__import__("time").time())) - days * 86400
    rows = [x for x in state.bot_log if int(x.get("ts") or 0) >= cutoff]
    rows = list(reversed(rows[-limit:]))
    if not rows:
        return f"<b>📜 LOG últimos {days} días</b>\n\nAún no hay acciones registradas."
    lines = [f"<b>📜 LOG últimos {days} días</b>", ""]
    for item in rows:
        uid = item.get("user_id")
        who = f" · usuario <code>{h(uid)}</code>" if uid else ""
        lines.append(f"• <b>{fmt_ts(item.get('ts'))}</b> · {h(item.get('action',''))}{who}\n  <i>{h(item.get('details',''))}</i>")
    return "\n".join(lines)


def remember_expelled(chat_id: int, user_id: int, record: Optional[Dict[str, Any]] = None, *, reason: str = "") -> None:
    state = ensure_state_v2(chat_id)
    record = record or {}
    item = {
        "user_id": int(user_id),
        "name": record.get("name", "") or state.member_activity.get(str(user_id), {}).get("name", "") or str(user_id),
        "username": record.get("username", "") or state.member_activity.get(str(user_id), {}).get("username", ""),
        "reason": reason or record.get("status", "expulsado"),
        "ts": _now_ts() if "_now_ts" in globals() else int(__import__("time").time()),
    }
    state.expelled_users.append(item)
    state.expelled_users = state.expelled_users[-300:]
    save_all_states()
    add_bot_log(chat_id, "Usuario expulsado", f"{item['name']} · {item['reason']}", user_id=user_id)


def admin_expelled_users_text(chat_id: int, limit: int = 80) -> str:
    state = ensure_state_v2(chat_id)
    rows = list(reversed(state.expelled_users[-limit:]))
    if not rows:
        return "<b>🚫 Usuarios expulsados</b>\n\nAún no hay expulsiones registradas por el bot."
    lines = ["<b>🚫 Usuarios expulsados</b>", ""]
    for item in rows:
        uid = int(item.get("user_id") or 0)
        lines.append(f"• {mention_from_known_user(uid, item)} · <b>{h(item.get('reason','expulsado'))}</b> · {fmt_ts(item.get('ts'))}")
    return "\n".join(lines)


def profile_field(profile: str, key: str) -> str:
    prof = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])
    if key in prof:
        return prof[key]
    return MESSAGE_PROFILES["welcome"].get(key, "")


def media_position_label(chat_id: int, profile: str) -> str:
    pos_field = profile_field(profile, "position")
    pos = str(cfg_value(chat_id, pos_field, "top")) if pos_field else "top"
    return "arriba" if pos != "bottom" else "debajo"


def toggle_media_position(chat_id: int, profile: str) -> None:
    field = profile_field(profile, "position")
    current = str(cfg_value(chat_id, field, "top"))
    cfg_set(chat_id, field, "bottom" if current != "bottom" else "top")


def fake_profile_text(chat_id: int, profile: str) -> str:
    return cfg_fake_preview_values(str(cfg_value(chat_id, profile_field(profile, "text"), ""))).replace("{mentions}", "@usuario1 @usuario2 @usuario3 @usuario4")


async def _send_media_only(bot, chat_id: int, media: Dict[str, Any], *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    if not isinstance(media, dict) or not media.get("file_id"):
        return None
    mtype = media.get("type")
    fid = media.get("file_id")
    msg = None
    if mtype == "photo":
        msg = await bot.send_photo(chat_id, photo=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "video":
        msg = await bot.send_video(chat_id, video=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "animation":
        msg = await bot.send_animation(chat_id, animation=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "document":
        msg = await bot.send_document(chat_id, document=fid, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    return getattr(msg, "message_id", None) if msg else None


async def _send_media_with_caption(bot, chat_id: int, media: Dict[str, Any], text: str, markup: Optional[InlineKeyboardMarkup], *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    if not isinstance(media, dict) or not media.get("file_id"):
        return None
    mtype = media.get("type")
    fid = media.get("file_id")
    msg = None
    if mtype == "photo":
        msg = await bot.send_photo(chat_id, photo=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "video":
        msg = await bot.send_video(chat_id, video=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "animation":
        msg = await bot.send_animation(chat_id, animation=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    elif mtype == "document":
        msg = await bot.send_document(chat_id, document=fid, caption=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    return getattr(msg, "message_id", None) if msg else None


async def send_configured_profile_message(bot, chat_id: int, profile: str, text: str, *, reply_to_message_id: Optional[int] = None, config_chat_id: Optional[int] = None) -> Optional[int]:
    cfg_chat_id = int(config_chat_id) if config_chat_id is not None else chat_id
    media = cfg_value(cfg_chat_id, profile_field(profile, "media"), None)
    markup = build_config_buttons_keyboard(cfg_chat_id, profile_field(profile, "buttons"))
    position = str(cfg_value(cfg_chat_id, profile_field(profile, "position"), "top"))
    if isinstance(media, dict) and media.get("file_id"):
        if position == "bottom":
            msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
            try:
                await _send_media_only(bot, chat_id, media, reply_to_message_id=getattr(msg, "message_id", None))
            except Exception:
                logger.exception("No se pudo enviar multimedia debajo del texto")
            return getattr(msg, "message_id", None)
        media_msg_id = await _send_media_with_caption(bot, chat_id, media, text, markup, reply_to_message_id=reply_to_message_id)
        if media_msg_id:
            return media_msg_id
    msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    return getattr(msg, "message_id", None)


async def send_configured_public_join(bot, chat_id: int, user, *, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    text = validation_format_template(str(cfg_value(chat_id, "validation_public_join_message", VALIDATION_PUBLIC_JOIN_MESSAGE)), user)
    media = cfg_value(chat_id, "validation_public_join_media", None)
    markup = build_public_join_keyboard(chat_id)
    position = str(cfg_value(chat_id, "validation_public_join_media_position", "top"))
    if isinstance(media, dict) and media.get("file_id"):
        if position == "bottom":
            msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
            try:
                await _send_media_only(bot, chat_id, media, reply_to_message_id=getattr(msg, "message_id", None))
            except Exception:
                logger.exception("No se pudo enviar multimedia de bienvenida debajo")
            return getattr(msg, "message_id", None)
        media_msg_id = await _send_media_with_caption(bot, chat_id, media, text, markup, reply_to_message_id=reply_to_message_id)
        if media_msg_id:
            return media_msg_id
    msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=markup, reply_to_message_id=reply_to_message_id, allow_sending_without_reply=True)
    return getattr(msg, "message_id", None)


async def send_profile_preview(bot, private_chat_id: int, target_chat_id: int, profile: str) -> None:
    title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
    text = fake_profile_text(target_chat_id, profile)
    await bot.send_message(private_chat_id, f"👁 <b>Vista previa: {h(title)}</b>", parse_mode=ParseMode.HTML)
    await send_configured_profile_message(bot, private_chat_id, profile, text)


def validation_user_keyboard(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Expulsar", callback_data=f"val|kick|{uid}"), InlineKeyboardButton("✅ Validar", callback_data=f"val|ok|{uid}")],
        [InlineKeyboardButton("🔔 Recordar", callback_data=f"val|remind|{uid}"), InlineKeyboardButton("🔇 Silenciar", callback_data=f"val|mute|{uid}")],
        [InlineKeyboardButton("❌ Rechazar", callback_data=f"val|no|{uid}")],
    ])


def validation_pending_summary_text(chat_id: int) -> str:
    state = get_state(chat_id)
    answering = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") == "answering"]
    pending = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") == "pending_admin"]
    lines = ["<b>👥 Usuarios pendientes</b>", "", f"Respondiendo preguntas: <b>{len(answering)}</b>", f"Pendientes de validar: <b>{len(pending)}</b>", ""]
    if answering:
        lines.append("<b>📝 Aún contestando:</b>")
        for uid, r in answering[:30]:
            step = int(r.get("step", 0))
            total = len(cfg_questions(chat_id))
            lines.append(f"• {mention_from_known_user(int(uid), r)} — pregunta <b>{step+1}/{total}</b>")
        lines.append("")
    if pending:
        lines.append("<b>✅ Esperando admin:</b>")
        for uid, r in pending[:30]:
            lines.append(f"• {mention_from_known_user(int(uid), r)} — usa los botones individuales abajo")
    if not answering and not pending:
        lines.append("No hay usuarios pendientes.")
    return "\n".join(lines).strip()


def validation_pending_config_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    items = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") in ("answering", "pending_admin", "timeout")]
    for uid, r in items[:20]:
        label = (r.get("name") or str(uid))[:24]
        rows.append([InlineKeyboardButton(f"🚫 Expulsar {label}", callback_data=f"cfg|pendkick|{chat_id}|{uid}")])
        rows.append([
            InlineKeyboardButton("🔔 Recordar", callback_data=f"cfg|pendremind|{chat_id}|{uid}"),
            InlineKeyboardButton("🔇 Silenciar", callback_data=f"cfg|pendmute|{chat_id}|{uid}"),
        ])
    rows.append([InlineKeyboardButton("🔄 Actualizar", callback_data=f"cfg|pending|{chat_id}"), InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")])
    return InlineKeyboardMarkup(rows)


def validation_pending_group_markup(chat_id: int) -> InlineKeyboardMarkup:
    state = get_state(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    items = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") in ("answering", "pending_admin", "timeout")]
    for uid, r in items[:20]:
        label = (r.get("name") or str(uid))[:24]
        rows.append([InlineKeyboardButton(f"🚫 Expulsar {label}", callback_data=f"val|kick|{uid}")])
        rows.append([
            InlineKeyboardButton("🔔 Recordar", callback_data=f"val|remind|{uid}"),
            InlineKeyboardButton("🔇 Silenciar", callback_data=f"val|mute|{uid}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Volver", callback_data="adm|back")])
    return InlineKeyboardMarkup(rows)


async def validation_send_reminder(bot, chat_id: int, user_id: int, record: Dict[str, Any]) -> None:
    mention = f"<a href=\"tg://user?id={user_id}\">{h(record.get('name') or user_id)}</a>"
    text = str(cfg_value(chat_id, "validation_reminder_message", VALIDATION_REMINDER_MESSAGE)).replace("{mention}", mention).replace("{name}", h(record.get("name", "")))
    await send_configured_profile_message(bot, chat_id, "reminder", text)
    add_bot_log(chat_id, "Recordatorio enviado", record.get("name", str(user_id)), user_id=user_id)


async def validation_send_timeout_message(bot, chat_id: int, user_id: int, record: Dict[str, Any]) -> None:
    mention = f"<a href=\"tg://user?id={user_id}\">{h(record.get('name') or user_id)}</a>"
    text = str(cfg_value(chat_id, "validation_timeout_message", VALIDATION_TIMEOUT_MESSAGE)).replace("{mention}", mention).replace("{name}", h(record.get("name", "")))
    await send_configured_profile_message(bot, chat_id, "timeout", text)


async def validation_kick_pending(bot, chat_id: int, user_id: int, record: Dict[str, Any], *, reason: str = "expulsado_pendiente") -> bool:
    try:
        await validation_send_timeout_message(bot, chat_id, user_id, record)
    except Exception:
        logger.exception("No se pudo enviar mensaje de timeout antes de expulsar")
    try:
        await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        try:
            await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        except Exception:
            pass
        record["status"] = reason
        record["kicked_ts"] = _now_ts()
        validation_set_record(chat_id, user_id, record)
        unmark_user_muted(chat_id, user_id)
        remember_expelled(chat_id, user_id, record, reason=reason)
        return True
    except Exception:
        logger.exception("No se pudo expulsar usuario pendiente %s", user_id)
        return False


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
        "question_message_ids": [],
        "review_message_id": None,
        "source": source,
        "forced": bool(force),
    }
    validation_set_record(chat_id, user.id, record)
    remember_entry(chat_id, user, source=source)
    add_bot_log(chat_id, "Inicio validación", f"{display_name(user)} · {source}", user_id=user.id)
    try:
        await validation_restrict_answering(context, chat_id, user.id)  # type: ignore[arg-type]
        mark_user_muted(chat_id, user.id, user=user, reason="pendiente_presentacion")
    except Exception:
        logger.exception("No se pudo restringir al usuario %s en chat %s", user.id, chat_id)
    if not force:
        try:
            public_message_id = await send_configured_public_join(bot, chat_id, user, reply_to_message_id=reply_to_message_id)
            record["public_message_id"] = public_message_id
        except Exception:
            logger.exception("No se pudo enviar mensaje público de entrada")
    try:
        q_msg = await bot.send_message(
            chat_id=chat_id,
            text=(validation_format_template(str(cfg_value(chat_id, "validation_intro_message", VALIDATION_INTRO_MESSAGE)), user) + f"\n\n<b>Pregunta 1/{len(questions)}</b>\n{h(questions[0])}"),
            parse_mode=ParseMode.HTML,
            reply_to_message_id=reply_to_message_id,
            allow_sending_without_reply=True,
        )
        record["question_message_id"] = q_msg.message_id
        record["question_message_ids"] = [q_msg.message_id]
    except Exception:
        logger.exception("No se pudo enviar primera pregunta")
    validation_set_record(chat_id, user.id, record)
    return True


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
    # Borra la pregunta anterior del bot para que no se acumulen mensajes.
    prev_q = record.get("question_message_id")
    if prev_q:
        await safe_delete(context.bot, chat_id, int(prev_q))
    step += 1
    record["answers"] = answers
    record["step"] = step
    if step < len(questions):
        record["status"] = "answering"
        validation_set_record(chat_id, user.id, record)
        q_msg = await update.message.reply_html(f"✅ Recibido.\n\n<b>Pregunta {step + 1}/{len(questions)}</b>\n{h(questions[step])}")
        record["question_message_id"] = q_msg.message_id
        ids = list(record.get("question_message_ids", []))
        ids.append(q_msg.message_id)
        record["question_message_ids"] = ids[-10:]
        validation_set_record(chat_id, user.id, record)
        return True
    record["status"] = "pending_admin"
    record["question_message_id"] = None
    validation_set_record(chat_id, user.id, record)
    try:
        await validation_mute(context, chat_id, user.id)
    except Exception:
        logger.exception("No se pudo silenciar tras completar presentación")
    lines = [f"<b>{h(item.get('question', 'Pregunta'))}</b> {h(item.get('answer', ''))}" for item in answers]
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=(f"📋 <b>Presentación pendiente de validar</b>\n\nUsuario: {user.mention_html()}\n\n" + "\n".join(lines) + "\n\nUn administrador debe aprobar para que pueda hablar."),
        parse_mode=ParseMode.HTML,
        reply_markup=validation_user_keyboard(user.id),
    )
    record["review_message_id"] = msg.message_id
    validation_set_record(chat_id, user.id, record)
    add_bot_log(chat_id, "Presentación completada", display_name(user), user_id=user.id)
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
    target_mention = f"<a href=\"tg://user?id={target_id}\">{h(record.get('name') or target_id)}</a>"
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
        try:
            await query.edit_message_text(f"✅ <b>Presentación validada</b>\n\nUsuario: {target_mention}\nAdmin: {update.effective_user.mention_html()}", parse_mode=ParseMode.HTML)
        except Exception:
            pass
        approved_text = str(cfg_value(chat_id, "validation_approved_message", VALIDATION_APPROVED_MESSAGE)).replace("{mention}", target_mention).replace("{name}", h(record.get("name", "")))
        await send_configured_profile_message(context.bot, chat_id, "approved", approved_text)
        if bool(cfg_value(chat_id, "validation_show_rules_after_approval", False)):
            try:
                await context.bot.send_message(chat_id=chat_id, text=str(cfg_value(chat_id, "rules_text", "")), parse_mode=ParseMode.HTML)
            except Exception:
                logger.exception("No se pudieron enviar normas tras aprobación")
        add_bot_log(chat_id, "Usuario aprobado", record.get("name", str(target_id)), user_id=target_id)
        await query.answer("Usuario validado.")
        return True
    if action in ("no", "kick"):
        ok = await validation_kick_pending(context.bot, chat_id, target_id, record, reason="rechazado" if action == "no" else "expulsado_pendiente")
        try:
            await query.edit_message_text(("❌ Presentación rechazada. Usuario expulsado." if action == "no" else "🚫 Usuario pendiente expulsado."), parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await query.answer("Usuario expulsado." if ok else "No pude expulsar. Revisa permisos.", show_alert=not ok)
        return True
    if action == "remind":
        try:
            await validation_send_reminder(context.bot, chat_id, target_id, record)
            record["reminded"] = True
            validation_set_record(chat_id, target_id, record)
            await query.answer("Recordatorio enviado.")
        except Exception:
            logger.exception("No se pudo recordar a usuario pendiente")
            await query.answer("No pude enviar recordatorio.", show_alert=True)
        return True
    if action == "mute":
        try:
            await validation_mute(context, chat_id, target_id)
            mark_user_muted(chat_id, target_id, reason="silenciado_hasta_presentarse")
            add_bot_log(chat_id, "Usuario silenciado", record.get("name", str(target_id)), user_id=target_id)
            await query.answer("Silenciado hasta que conteste.")
        except Exception:
            logger.exception("No se pudo silenciar a usuario pendiente")
            await query.answer("No pude silenciar. Revisa permisos.", show_alert=True)
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
                    if status == "answering" and not record.get("reminded") and now >= int(record.get("reminder_ts") or 0):
                        record["reminded"] = True
                        validation_set_record(chat_id, user_id, record)
                        try:
                            await validation_send_reminder(application.bot, chat_id, user_id, record)
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
                            await validation_send_timeout_message(application.bot, chat_id, user_id, record)
                        except Exception:
                            pass
                        if bool(cfg_value(chat_id, "validation_kick_if_timeout", VALIDATION_KICK_IF_TIMEOUT)):
                            await validation_kick_pending(application.bot, chat_id, user_id, record, reason="kicked_timeout")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Error en watchdog de validación")


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
    mentions = []
    for uid, record in users.items():
        try:
            mentions.append(mention_from_known_user(int(uid), record))
        except Exception:
            continue
    return mentions


async def send_pregonero(context: ContextTypes.DEFAULT_TYPE, chat_id: int, *, title: str = "") -> None:
    mentions = known_pregonero_mentions(chat_id)
    if not mentions:
        await send_temp_message(context.bot, chat_id, "📣 No tengo usuarios registrados todavía para mencionar.", ttl=40)
        return
    max_per_message = int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4) or 4)
    max_per_message = max(1, min(10, max_per_message))
    template = str(cfg_value(chat_id, "pregonero_text", "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}"))
    for start in range(0, len(mentions), max_per_message):
        chunk = mentions[start:start + max_per_message]
        chunk_text = " ".join(chunk)
        text = template.replace("{mentions}", chunk_text)
        if "{mentions}" not in template and chunk_text not in text:
            text = text + "\n\n" + chunk_text
        await send_configured_profile_message(context.bot, chat_id, "pregonero", text)
        await asyncio.sleep(0.8)
    add_bot_log(chat_id, "Pregonero lanzado", f"Menciones: {len(mentions)} · bloque: {max_per_message}")


def admin_panel_text(chat_id: int) -> str:
    cfg = admin_cfg(chat_id)
    state = ensure_state_v2(chat_id)
    pending = sum(1 for r in state.validation_users.values() if r.get("status") == "pending_admin")
    answering = sum(1 for r in state.validation_users.values() if r.get("status") == "answering")
    expelled = len(state.expelled_users)
    return (
        "<b>🛡️ ADMIN PLAN</b>\n\n"
        f"Validación: <b>{bool_label(cfg.get('validation_enabled'))}</b>\n"
        f"Preguntas: <b>{len(cfg_questions(chat_id))}</b>\n"
        f"Tiempo límite: <b>{cfg.get('validation_timeout_minutes')} min</b>\n"
        f"Recordatorio: <b>{cfg.get('validation_reminder_minutes')} min</b>\n"
        f"Expulsar si no responde: <b>{bool_label(cfg.get('validation_kick_if_timeout'))}</b>\n"
        f"Autoaprobar solicitudes: <b>{bool_label(cfg.get('validation_auto_approve_join_requests'))}</b>\n"
        f"Normas tras aprobar: <b>{bool_label(cfg.get('validation_show_rules_after_approval'))}</b>\n"
        f"Pregonero: <b>{cfg.get('pregonero_max_mentions_per_message',4)} en {cfg.get('pregonero_max_mentions_per_message',4)}</b>\n\n"
        f"Respondiendo: <b>{answering}</b> | Pendientes admin: <b>{pending}</b> | Expulsados: <b>{expelled}</b>\n"
        f"Borrado de comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n\n"
        "Comandos rápidos:\n"
        "<code>/plan</code> · <code>/Djplan</code> · <code>/ajustes</code>\n"
        "<code>/presentate</code> respondiendo a un usuario\n"
        "<code>/el plan te llama</code> · <code>/pregonero</code>"
    )


def admin_panel_markup(chat_id: int) -> InlineKeyboardMarkup:
    cfg = admin_cfg(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏓 Ping", callback_data="adm|ping"), InlineKeyboardButton("📣 Pregonero", callback_data="adm|pregonero")],
        [InlineKeyboardButton(f"Validación {bool_label(cfg.get('validation_enabled'))}", callback_data="adm|toggle_validation")],
        [InlineKeyboardButton(f"🚪 Autoaprobar {bool_label(cfg.get('validation_auto_approve_join_requests'))}", callback_data="adm|toggle_autoapprove")],
        [InlineKeyboardButton("📋 Pendientes", callback_data="adm|pendientes"), InlineKeyboardButton("📌 Normas", callback_data="adm|reglas")],
        [InlineKeyboardButton("🔇 Silenciados", callback_data="adm|muted"), InlineKeyboardButton("🚪 Últimas entradas", callback_data="adm|entries")],
        [InlineKeyboardButton("🚫 Expulsados", callback_data="adm|expelled"), InlineKeyboardButton("📜 LOG 3d", callback_data="adm|log")],
        [InlineKeyboardButton("🕙 Inactivos 10d", callback_data="adm|inactive"), InlineKeyboardButton("🏆 Ranking", callback_data="adm|ranking")],
        [InlineKeyboardButton("⏱️ Tiempo -", callback_data="adm|time_minus"), InlineKeyboardButton("⏱️ Tiempo +", callback_data="adm|time_plus")],
        [InlineKeyboardButton("🔔 Record -", callback_data="adm|rem_minus"), InlineKeyboardButton("🔔 Record +", callback_data="adm|rem_plus")],
        [InlineKeyboardButton(f"Expulsar timeout {bool_label(cfg.get('validation_kick_if_timeout'))}", callback_data="adm|toggle_kick_timeout")],
        [InlineKeyboardButton(f"Normas tras aprobar {bool_label(cfg.get('validation_show_rules_after_approval'))}", callback_data="adm|toggle_rules_after_approval")],
        [InlineKeyboardButton(f"🧹 Comandos: {command_cleanup_label(chat_id)}", callback_data="adm|cleanup_mode")],
        [InlineKeyboardButton("❌ Cerrar", callback_data="adm|close")],
    ])


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
        await query.edit_message_text(ping_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]]))
        return True
    if action == "pregonero":
        await query.answer("Lanzando pregonero 4x4…")
        await send_pregonero(context, chat_id)
        return True
    view_actions = {
        "muted": admin_muted_users_text,
        "entries": admin_last_entries_text,
        "inactive": lambda cid: admin_inactive_users_text(cid, 10),
        "ranking": admin_ranking_text,
        "expelled": admin_expelled_users_text,
        "log": lambda cid: bot_log_text(cid, 3),
        "pendientes": validation_pending_summary_text,
    }
    if action in view_actions:
        markup = validation_pending_group_markup(chat_id) if action == "pendientes" else InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]])
        await query.edit_message_text(view_actions[action](chat_id), parse_mode=ParseMode.HTML, reply_markup=markup)
        return True
    if action == "cleanup_mode":
        cfg_set(chat_id, "command_cleanup_mode", next_command_cleanup_mode(chat_id))
    elif action == "toggle_validation":
        cfg_set(chat_id, "validation_enabled", not bool(cfg.get("validation_enabled")))
        add_bot_log(chat_id, "Ajuste cambiado", "Validación")
    elif action == "toggle_kick_timeout":
        cfg_set(chat_id, "validation_kick_if_timeout", not bool(cfg.get("validation_kick_if_timeout")))
    elif action == "toggle_autoapprove":
        cfg_set(chat_id, "validation_auto_approve_join_requests", not bool(cfg.get("validation_auto_approve_join_requests")))
    elif action == "toggle_rules_after_approval":
        cfg_set(chat_id, "validation_show_rules_after_approval", not bool(cfg.get("validation_show_rules_after_approval")))
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
    elif action == "back":
        pass
    else:
        await query.answer("Acción no reconocida.", show_alert=True)
        return True
    await query.edit_message_text(admin_panel_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup(chat_id))
    return True


def admin_private_main_text(chat_id: int) -> str:
    title = str(cfg_value(chat_id, "chat_title", "")) or str(chat_id)
    approver = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
    return (
        f"<b>⚙️ Configuración privada</b>\n\n"
        f"Grupo: <b>{h(title)}</b>\n"
        f"Validación: <b>{bool_label(cfg_value(chat_id, 'validation_enabled'))}</b>\n"
        f"Preguntas: <b>{len(cfg_questions(chat_id))}</b>\n"
        f"Tiempo: <b>{cfg_value(chat_id, 'validation_timeout_minutes')} min</b> · Recordatorio: <b>{cfg_value(chat_id, 'validation_reminder_minutes')} min</b>\n"
        f"Quién valida: <b>{h(APPROVER_MODE_LABELS.get(approver, approver))}</b>\n"
        f"Autoaprobar solicitudes: <b>{bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}</b>\n"
        f"Normas tras aprobar: <b>{bool_label(cfg_value(chat_id, 'validation_show_rules_after_approval'))}</b>\n"
        f"Pregonero: <b>{cfg_value(chat_id, 'pregonero_max_mentions_per_message', 4)} menciones por mensaje</b> · media <b>{'Sí' if cfg_value(chat_id, 'pregonero_media') else 'No'}</b> · posición <b>{media_position_label(chat_id, 'pregonero')}</b>\n"
        f"Timeout media: <b>{'Sí' if cfg_value(chat_id, 'validation_timeout_media') else 'No'}</b> · posición <b>{media_position_label(chat_id, 'timeout')}</b>\n"
        f"Recordatorio media: <b>{'Sí' if cfg_value(chat_id, 'validation_reminder_media') else 'No'}</b> · posición <b>{media_position_label(chat_id, 'reminder')}</b>\n"
        f"Borrado comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n\n"
        "Elige qué quieres configurar. Todo se hace aquí por privado."
    )


def admin_private_main_markup(chat_id: int) -> InlineKeyboardMarkup:
    enabled = bool_label(cfg_value(chat_id, "validation_enabled"))
    kick = bool_label(cfg_value(chat_id, "validation_kick_if_timeout"))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Validación {enabled}", callback_data=f"cfg|toggle_validation|{chat_id}"), InlineKeyboardButton("📚 Ver comandos", callback_data=f"cfg|commands|{chat_id}")],
        [InlineKeyboardButton(f"🚪 Autoaprobar {bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}", callback_data=f"cfg|toggle_autoapprove|{chat_id}")],
        [InlineKeyboardButton("👥 Pendientes", callback_data=f"cfg|pending|{chat_id}"), InlineKeyboardButton("👮 Quién valida", callback_data=f"cfg|approvers|{chat_id}")],
        [InlineKeyboardButton("🔇 Silenciados", callback_data=f"cfg|muted|{chat_id}"), InlineKeyboardButton("🚫 Expulsados", callback_data=f"cfg|expelled|{chat_id}")],
        [InlineKeyboardButton("🚪 Entradas", callback_data=f"cfg|entries|{chat_id}"), InlineKeyboardButton("📜 LOG 3d", callback_data=f"cfg|log|{chat_id}")],
        [InlineKeyboardButton("🕙 Inactivos 10d", callback_data=f"cfg|inactive|{chat_id}"), InlineKeyboardButton("🏆 Ranking", callback_data=f"cfg|ranking|{chat_id}")],
        [InlineKeyboardButton(f"🧹 Comandos {command_cleanup_label(chat_id)}", callback_data=f"cfg|cleanup_mode|{chat_id}")],
        [InlineKeyboardButton(f"📌 Normas tras aprobar {bool_label(cfg_value(chat_id, 'validation_show_rules_after_approval'))}", callback_data=f"cfg|toggle_rules_after_approval|{chat_id}")],
        [InlineKeyboardButton("👁 Preview bienvenida", callback_data=f"cfg|preview|{chat_id}|welcome"), InlineKeyboardButton(f"↕️ Media bienvenida {media_position_label(chat_id, 'welcome')}", callback_data=f"cfg|position|{chat_id}|welcome")],
        [InlineKeyboardButton("✏️ Texto bienvenida", callback_data=f"cfg|edit_text|{chat_id}|validation_public_join_message")],
        [InlineKeyboardButton("🖼 Media bienvenida", callback_data=f"cfg|media|{chat_id}|welcome"), InlineKeyboardButton("⌨️ Botones bienvenida", callback_data=f"cfg|buttons|{chat_id}|welcome")],
        [InlineKeyboardButton("❓ Preguntas", callback_data=f"cfg|questions|{chat_id}"), InlineKeyboardButton("📝 Texto preguntas", callback_data=f"cfg|edit_text|{chat_id}|validation_intro_message")],
        [InlineKeyboardButton("👁 Preview aprobado", callback_data=f"cfg|preview|{chat_id}|approved"), InlineKeyboardButton(f"↕️ Media aprobado {media_position_label(chat_id, 'approved')}", callback_data=f"cfg|position|{chat_id}|approved")],
        [InlineKeyboardButton("✅ Texto aprobado", callback_data=f"cfg|edit_text|{chat_id}|validation_approved_message"), InlineKeyboardButton("🖼 Media aprobado", callback_data=f"cfg|media|{chat_id}|approved")],
        [InlineKeyboardButton("👁 Preview rechazo", callback_data=f"cfg|preview|{chat_id}|rejected"), InlineKeyboardButton(f"↕️ Media rechazo {media_position_label(chat_id, 'rejected')}", callback_data=f"cfg|position|{chat_id}|rejected")],
        [InlineKeyboardButton("❌ Texto rechazo", callback_data=f"cfg|edit_text|{chat_id}|validation_rejected_message"), InlineKeyboardButton("🖼 Media rechazo", callback_data=f"cfg|media|{chat_id}|rejected")],
        [InlineKeyboardButton("👁 Preview recordatorio", callback_data=f"cfg|preview|{chat_id}|reminder"), InlineKeyboardButton(f"↕️ Recordatorio {media_position_label(chat_id, 'reminder')}", callback_data=f"cfg|position|{chat_id}|reminder")],
        [InlineKeyboardButton("⏰ Texto recordatorio", callback_data=f"cfg|edit_text|{chat_id}|validation_reminder_message"), InlineKeyboardButton("🖼 Media recordatorio", callback_data=f"cfg|media|{chat_id}|reminder")],
        [InlineKeyboardButton("👁 Preview timeout", callback_data=f"cfg|preview|{chat_id}|timeout"), InlineKeyboardButton(f"↕️ Timeout {media_position_label(chat_id, 'timeout')}", callback_data=f"cfg|position|{chat_id}|timeout")],
        [InlineKeyboardButton("⏱ Texto timeout", callback_data=f"cfg|edit_text|{chat_id}|validation_timeout_message"), InlineKeyboardButton("🖼 Media timeout", callback_data=f"cfg|media|{chat_id}|timeout")],
        [InlineKeyboardButton("📣 Texto pregonero", callback_data=f"cfg|edit_text|{chat_id}|pregonero_text"), InlineKeyboardButton("🖼 Media pregonero", callback_data=f"cfg|media|{chat_id}|pregonero")],
        [InlineKeyboardButton(f"↕️ Pregonero {media_position_label(chat_id, 'pregonero')}", callback_data=f"cfg|position|{chat_id}|pregonero"), InlineKeyboardButton("👁 Preview pregonero", callback_data=f"cfg|preview|{chat_id}|pregonero")],
        [InlineKeyboardButton("📣 4x4 -", callback_data=f"cfg|pregonero_minus|{chat_id}"), InlineKeyboardButton("📣 4x4 +", callback_data=f"cfg|pregonero_plus|{chat_id}")],
        [InlineKeyboardButton("⏱ Tiempo -", callback_data=f"cfg|time_minus|{chat_id}"), InlineKeyboardButton("⏱ Tiempo +", callback_data=f"cfg|time_plus|{chat_id}")],
        [InlineKeyboardButton("🔔 Record -", callback_data=f"cfg|rem_minus|{chat_id}"), InlineKeyboardButton("🔔 Record +", callback_data=f"cfg|rem_plus|{chat_id}")],
        [InlineKeyboardButton(f"Expulsar timeout {kick}", callback_data=f"cfg|toggle_kick|{chat_id}")],
        [InlineKeyboardButton("📌 Normas", callback_data=f"cfg|edit_text|{chat_id}|rules_text"), InlineKeyboardButton("🔙 Grupos", callback_data="cfg|list")],
        [InlineKeyboardButton("❌ Cerrar panel", callback_data=f"cfg|close|{chat_id}")],
    ])


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
    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            await query.edit_message_text("✅ Panel de configuración cerrado.")
        return
    if action == "open":
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    if action == "toggle_validation":
        cfg_set(chat_id, "validation_enabled", not bool(cfg_value(chat_id, "validation_enabled")))
    elif action == "toggle_kick":
        cfg_set(chat_id, "validation_kick_if_timeout", not bool(cfg_value(chat_id, "validation_kick_if_timeout")))
    elif action == "toggle_autoapprove":
        cfg_set(chat_id, "validation_auto_approve_join_requests", not bool(cfg_value(chat_id, "validation_auto_approve_join_requests")))
    elif action == "toggle_rules_after_approval":
        cfg_set(chat_id, "validation_show_rules_after_approval", not bool(cfg_value(chat_id, "validation_show_rules_after_approval")))
    elif action == "time_plus":
        cfg_set(chat_id, "validation_timeout_minutes", int(cfg_value(chat_id, "validation_timeout_minutes", 10)) + 1)
    elif action == "time_minus":
        cfg_set(chat_id, "validation_timeout_minutes", max(1, int(cfg_value(chat_id, "validation_timeout_minutes", 10)) - 1))
    elif action == "rem_plus":
        cfg_set(chat_id, "validation_reminder_minutes", int(cfg_value(chat_id, "validation_reminder_minutes", 3)) + 1)
    elif action == "rem_minus":
        cfg_set(chat_id, "validation_reminder_minutes", max(1, int(cfg_value(chat_id, "validation_reminder_minutes", 3)) - 1))
    elif action == "pregonero_plus":
        cfg_set(chat_id, "pregonero_max_mentions_per_message", min(10, int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4)) + 1))
    elif action == "pregonero_minus":
        cfg_set(chat_id, "pregonero_max_mentions_per_message", max(1, int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4)) - 1))
    elif action == "position" and len(parts) >= 4:
        toggle_media_position(chat_id, parts[3])
    elif action == "commands":
        await query.edit_message_text(all_commands_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "muted":
        await query.edit_message_text(admin_muted_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "expelled":
        await query.edit_message_text(admin_expelled_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "log":
        await query.edit_message_text(bot_log_text(chat_id, 3), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "entries":
        await query.edit_message_text(admin_last_entries_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "inactive":
        await query.edit_message_text(admin_inactive_users_text(chat_id, 10), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "ranking":
        await query.edit_message_text(admin_ranking_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "pending":
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=validation_pending_config_markup(chat_id))
        return
    elif action in ("pendkick", "pendremind", "pendmute") and len(parts) >= 4:
        uid = int(parts[3])
        rec = validation_get_record(chat_id, uid) or {"name": str(uid)}
        if action == "pendkick":
            ok = await validation_kick_pending(context.bot, chat_id, uid, rec, reason="expulsado_desde_pendientes")
            await query.answer("Expulsado." if ok else "No pude expulsar.", show_alert=not ok)
        elif action == "pendremind":
            await validation_send_reminder(context.bot, chat_id, uid, rec)
            await query.answer("Recordatorio enviado.")
        elif action == "pendmute":
            await validation_mute_bot(context.bot, chat_id, uid)
            mark_user_muted(chat_id, uid, reason="silenciado_hasta_contestar")
            add_bot_log(chat_id, "Silenciado desde pendientes", rec.get("name", str(uid)), user_id=uid)
            await query.answer("Silenciado.")
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=validation_pending_config_markup(chat_id))
        return
    elif action == "preview" and len(parts) >= 4:
        await send_profile_preview(context.bot, query.message.chat.id, chat_id, parts[3])
        await query.answer("Vista previa enviada.")
        return
    elif action == "edit_text" and len(parts) >= 4:
        field = parts[3]
        set_config_pending(update.effective_user.id, {"kind": "cfg_text", "chat_id": chat_id, "field": field})
        label = CONFIG_TEXT_FIELDS.get(field, field)
        current = str(cfg_value(chat_id, field, ""))
        await query.edit_message_text(
            f"✏️ <b>{h(label)}</b>\n\nActual:\n<pre>{h(current)}</pre>\n\nEnvíame ahora el nuevo texto por aquí.\n\nVariables: <code>{{mention}}</code>, <code>{{name}}</code>, <code>{{first}}</code>, <code>{{username}}</code>, <code>{{id}}</code>, <code>{{chat}}</code>, <code>{{mentions}}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "questions":
        set_config_pending(update.effective_user.id, {"kind": "cfg_questions", "chat_id": chat_id})
        current = "\n".join(cfg_questions(chat_id))
        await query.edit_message_text(f"❓ <b>Preguntas actuales</b>\n\n<pre>{h(current)}</pre>\n\nEnvíame las nuevas preguntas, una por línea o separadas por |.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "buttons":
        profile = parts[3] if len(parts) >= 4 else "welcome"
        buttons_field = profile_field(profile, "buttons")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_buttons", "chat_id": chat_id, "buttons_field": buttons_field})
        await query.edit_message_text(f"⌨️ <b>Botones: {h(title)}</b>\n\nEnvíame botones así, uno por línea:\n\n<code>Texto del botón - https://enlace.com</code>\n\nPara borrar todos escribe: <code>QUITAR</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "media":
        profile = parts[3] if len(parts) >= 4 else "welcome"
        media_field = profile_field(profile, "media")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_media", "chat_id": chat_id, "media_field": media_field})
        await query.edit_message_text(f"🖼 <b>Multimedia: {h(title)}</b>\n\nEnvíame ahora una foto, vídeo, GIF o documento.\n\nPara quitar multimedia escribe: <code>QUITAR</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "approvers":
        await query.edit_message_text("👮 <b>Quién puede validar presentaciones</b>\n\nElige quién podrá pulsar ✅ Validar o ❌ Rechazar.", parse_mode=ParseMode.HTML, reply_markup=approver_markup(chat_id))
        return
    elif action == "setapprover" and len(parts) >= 4:
        mode = parts[3]
        if mode in APPROVER_MODE_LABELS:
            cfg_set(chat_id, "validation_approver_mode", mode)
    await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))


async def can_control_bot(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    return await is_admin(context, chat_id, user_id)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not chat_is_allowed(update.effective_chat.id) or not update.effective_user:
        return
    if not await can_control_bot(context, update.effective_chat.id, update.effective_user.id):
        if update.message:
            await update.message.reply_text("Solo el dueño o administradores pueden controlar el bot.")
        return
    if update.message:
        await register_bot_message(update.effective_chat.id, update.message.message_id)
    msg = await context.bot.send_message(chat_id=update.effective_chat.id, text="<b>DJ-PLAN</b>", reply_markup=main_menu_markup(), parse_mode=ParseMode.HTML)
    await register_temp_message(update.effective_chat.id, msg.message_id)
    await register_bot_message(update.effective_chat.id, msg.message_id)


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not chat_is_allowed(update.effective_chat.id) or not update.effective_user:
        return
    if not await can_control_bot(context, update.effective_chat.id, update.effective_user.id):
        await update.message.reply_text("Solo el dueño o administradores pueden controlar el bot.")
        return
    await update.message.reply_html(ping_text())


async def validation_pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    chat_id = update.effective_chat.id
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    state = get_state(chat_id)
    items = [(uid, r) for uid, r in state.validation_users.items() if r.get("status") in ("answering", "pending_admin", "timeout")]
    if not items:
        await update.message.reply_text("No hay usuarios pendientes.")
        return
    await update.message.reply_html(validation_pending_summary_text(chat_id))
    for uid, record in items[:30]:
        answers = record.get("answers", [])
        lines = [f"<b>{h(a.get('question','Pregunta'))}</b> {h(a.get('answer',''))}" for a in answers]
        status = h(record.get("status", "pendiente"))
        mention = f"<a href=\"tg://user?id={uid}\">{h(record.get('name') or uid)}</a>"
        body = f"📋 <b>Pendiente</b> · <b>{status}</b>\n\nUsuario: {mention}\n\n" + ("\n".join(lines) if lines else "Aún no ha terminado las preguntas.")
        await update.message.reply_html(body, reply_markup=validation_user_keyboard(int(uid)))


async def on_startup(application: Application) -> None:
    global VALIDATION_WATCHDOG_TASK
    load_all_states()
    await VOICE.start(application)
    if VALIDATION_ENABLED and (VALIDATION_WATCHDOG_TASK is None or VALIDATION_WATCHDOG_TASK.done()):
        VALIDATION_WATCHDOG_TASK = asyncio.create_task(validation_watchdog_loop(application))
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
    global VALIDATION_WATCHDOG_TASK
    if VALIDATION_WATCHDOG_TASK and not VALIDATION_WATCHDOG_TASK.done():
        VALIDATION_WATCHDOG_TASK.cancel()
    VALIDATION_WATCHDOG_TASK = None
    await VOICE.stop()




async def on_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Error no controlado del bot", exc_info=context.error)



# ============================================================
# PARCHE FINAL: UI NUEVA VISIBLE + CONFIGURACIÓN EN PRIVADO
# Motivo: había funciones duplicadas más abajo que pisaban el panel nuevo.
# Todo lo siguiente queda colocado justo antes de build_application(),
# por lo que estas son las versiones que realmente usa el bot.
# ============================================================

# Asegura perfiles finales disponibles, incluyendo despedida.
DEFAULT_ADMIN_CONFIG.update({
    "validation_public_join_media_position": DEFAULT_ADMIN_CONFIG.get("validation_public_join_media_position", "top"),
    "validation_approved_media_position": DEFAULT_ADMIN_CONFIG.get("validation_approved_media_position", "top"),
    "validation_rejected_media_position": DEFAULT_ADMIN_CONFIG.get("validation_rejected_media_position", "top"),
    "validation_reminder_media": DEFAULT_ADMIN_CONFIG.get("validation_reminder_media", None),
    "validation_reminder_buttons": DEFAULT_ADMIN_CONFIG.get("validation_reminder_buttons", []),
    "validation_reminder_media_position": DEFAULT_ADMIN_CONFIG.get("validation_reminder_media_position", "top"),
    "validation_timeout_media": DEFAULT_ADMIN_CONFIG.get("validation_timeout_media", None),
    "validation_timeout_buttons": DEFAULT_ADMIN_CONFIG.get("validation_timeout_buttons", []),
    "validation_timeout_media_position": DEFAULT_ADMIN_CONFIG.get("validation_timeout_media_position", "top"),
    "validation_show_rules_after_approval": DEFAULT_ADMIN_CONFIG.get("validation_show_rules_after_approval", False),
    "farewell_enabled": DEFAULT_ADMIN_CONFIG.get("farewell_enabled", True),
    "farewell_message": DEFAULT_ADMIN_CONFIG.get("farewell_message", "👋 {mention} ha salido del grupo."),
    "farewell_media": DEFAULT_ADMIN_CONFIG.get("farewell_media", None),
    "farewell_buttons": DEFAULT_ADMIN_CONFIG.get("farewell_buttons", []),
    "farewell_media_position": DEFAULT_ADMIN_CONFIG.get("farewell_media_position", "top"),
    "pregonero_text": DEFAULT_ADMIN_CONFIG.get("pregonero_text", "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}"),
    "pregonero_media": DEFAULT_ADMIN_CONFIG.get("pregonero_media", None),
    "pregonero_buttons": DEFAULT_ADMIN_CONFIG.get("pregonero_buttons", []),
    "pregonero_media_position": DEFAULT_ADMIN_CONFIG.get("pregonero_media_position", "top"),
    "pregonero_max_mentions_per_message": 4,
    "pregonero_manual_users": DEFAULT_ADMIN_CONFIG.get("pregonero_manual_users", []),
    "command_cleanup_mode": DEFAULT_ADMIN_CONFIG.get("command_cleanup_mode", "off"),
    "command_cleanup_ttl_seconds": DEFAULT_ADMIN_CONFIG.get("command_cleanup_ttl_seconds", 15),
})

MESSAGE_PROFILES = {
    "welcome": {
        "title": "Bienvenida / entrada",
        "text": "validation_public_join_message",
        "media": "validation_public_join_media",
        "buttons": "validation_public_join_buttons",
        "position": "validation_public_join_media_position",
    },
    "approved": {
        "title": "Aprobado",
        "text": "validation_approved_message",
        "media": "validation_approved_media",
        "buttons": "validation_approved_buttons",
        "position": "validation_approved_media_position",
    },
    "rejected": {
        "title": "Rechazado",
        "text": "validation_rejected_message",
        "media": "validation_rejected_media",
        "buttons": "validation_rejected_buttons",
        "position": "validation_rejected_media_position",
    },
    "reminder": {
        "title": "Recordatorio",
        "text": "validation_reminder_message",
        "media": "validation_reminder_media",
        "buttons": "validation_reminder_buttons",
        "position": "validation_reminder_media_position",
    },
    "timeout": {
        "title": "Timeout / expulsión",
        "text": "validation_timeout_message",
        "media": "validation_timeout_media",
        "buttons": "validation_timeout_buttons",
        "position": "validation_timeout_media_position",
    },
    "farewell": {
        "title": "Despedida",
        "text": "farewell_message",
        "media": "farewell_media",
        "buttons": "farewell_buttons",
        "position": "farewell_media_position",
    },
    "pregonero": {
        "title": "Pregonero",
        "text": "pregonero_text",
        "media": "pregonero_media",
        "buttons": "pregonero_buttons",
        "position": "pregonero_media_position",
    },
}

CONFIG_TEXT_FIELDS.update({
    "validation_public_join_message": "Texto bienvenida",
    "validation_intro_message": "Texto presentación/preguntas",
    "validation_approved_message": "Texto aprobado",
    "validation_rejected_message": "Texto rechazado",
    "validation_reminder_message": "Texto recordatorio",
    "validation_timeout_message": "Texto timeout / expulsión",
    "farewell_message": "Texto despedida",
    "pregonero_text": "Texto pregonero",
    "rules_text": "Normas",
})


def profile_field(profile: str, key: str) -> str:
    prof = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])
    return str(prof.get(key, MESSAGE_PROFILES["welcome"].get(key, "")))


def media_position_label(chat_id: int, profile: str) -> str:
    field = profile_field(profile, "position")
    return "debajo" if str(cfg_value(chat_id, field, "top")) == "bottom" else "arriba"


def toggle_media_position(chat_id: int, profile: str) -> None:
    field = profile_field(profile, "position")
    current = str(cfg_value(chat_id, field, "top"))
    cfg_set(chat_id, field, "bottom" if current != "bottom" else "top")


def _mark(current: str, value: str) -> str:
    return "✅ " if str(current) == value else ""


def _cfg_back(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg|open|{chat_id}")]])


def _adm_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data="adm|back")]])


def _three_col(rows: List[List[InlineKeyboardButton]]) -> List[List[InlineKeyboardButton]]:
    """Normaliza filas visuales para que el panel privado respire en tres columnas."""
    out: List[List[InlineKeyboardButton]] = []
    for row in rows:
        if not row:
            continue
        for i in range(0, len(row), 3):
            out.append(row[i:i + 3])
    return out


def admin_private_main_text(chat_id: int) -> str:
    cfg = admin_cfg(chat_id)
    title = str(cfg_value(chat_id, "chat_title", "")) or str(chat_id)
    approver = str(cfg_value(chat_id, "validation_approver_mode", "telegram_admins"))
    manual_users = cfg_value(chat_id, "pregonero_manual_users", [])
    manual_count = len(manual_users) if isinstance(manual_users, list) else 0

    def prof_line(profile: str, label: str) -> str:
        media = cfg_value(chat_id, profile_field(profile, "media"), None)
        buttons = cfg_value(chat_id, profile_field(profile, "buttons"), [])
        btn_count = len(buttons) if isinstance(buttons, list) else 0
        return f"{label}: media <b>{'Sí' if media else 'No'}</b> ({media_position_label(chat_id, profile)}) · botones <b>{btn_count}</b>"

    return (
        "<b>⚙️ CONFIGURACIÓN PRIVADA DJ-PLAN</b>\n\n"
        f"Grupo: <b>{h(title)}</b>\n"
        f"Validación: <b>{bool_label(cfg.get('validation_enabled'))}</b> · "
        f"Autoaprobar: <b>{bool_label(cfg.get('validation_auto_approve_join_requests'))}</b> · "
        f"Timeout expulsa: <b>{bool_label(cfg.get('validation_kick_if_timeout'))}</b>\n"
        f"Quién valida: <b>{h(APPROVER_MODE_LABELS.get(approver, approver))}</b> · "
        f"Normas tras aprobar: <b>{bool_label(cfg.get('validation_show_rules_after_approval'))}</b>\n"
        f"Preguntas: <b>{len(cfg_questions(chat_id))}</b> · "
        f"Tiempo: <b>{cfg.get('validation_timeout_minutes')} min</b> · "
        f"Recordatorio: <b>{cfg.get('validation_reminder_minutes')} min</b>\n"
        f"Borrado comandos: <b>{h(command_cleanup_label(chat_id))}</b> · "
        f"Despedida: <b>{bool_label(cfg.get('farewell_enabled'))}</b>\n"
        f"Pregonero: <b>{cfg.get('pregonero_max_mentions_per_message', 4)} por mensaje</b> · "
        f"manuales: <b>{manual_count}</b>\n\n"
        f"{prof_line('welcome', 'Bienvenida')}\n"
        f"{prof_line('reminder', 'Recordatorio')}\n"
        f"{prof_line('timeout', 'Timeout')}\n"
        f"{prof_line('approved', 'Aprobado')}\n"
        f"{prof_line('rejected', 'Rechazado')}\n"
        f"{prof_line('farewell', 'Despedida')}\n"
        f"{prof_line('pregonero', 'Pregonero')}\n\n"
        "Pulsa <b>👁</b> para ver la vista previa completa: texto + multimedia + botones."
    )


def admin_private_main_markup(chat_id: int) -> InlineKeyboardMarkup:
    cleanup = str(cfg_value(chat_id, "command_cleanup_mode", "off"))
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(f"Validación {bool_label(cfg_value(chat_id, 'validation_enabled'))}", callback_data=f"cfg|toggle_validation|{chat_id}"),
            InlineKeyboardButton(f"Autoaprobar {bool_label(cfg_value(chat_id, 'validation_auto_approve_join_requests'))}", callback_data=f"cfg|toggle_autoapprove|{chat_id}"),
            InlineKeyboardButton(f"Timeout {bool_label(cfg_value(chat_id, 'validation_kick_if_timeout'))}", callback_data=f"cfg|toggle_kick|{chat_id}"),
        ],
        [
            InlineKeyboardButton("⏱ -", callback_data=f"cfg|time_minus|{chat_id}"),
            InlineKeyboardButton(f"⏱ {cfg_value(chat_id, 'validation_timeout_minutes')}m", callback_data=f"cfg|noop|{chat_id}"),
            InlineKeyboardButton("⏱ +", callback_data=f"cfg|time_plus|{chat_id}"),
        ],
        [
            InlineKeyboardButton("🔔 -", callback_data=f"cfg|rem_minus|{chat_id}"),
            InlineKeyboardButton(f"🔔 {cfg_value(chat_id, 'validation_reminder_minutes')}m", callback_data=f"cfg|noop|{chat_id}"),
            InlineKeyboardButton("🔔 +", callback_data=f"cfg|rem_plus|{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"Normas {bool_label(cfg_value(chat_id, 'validation_show_rules_after_approval'))}", callback_data=f"cfg|toggle_rules_after_approval|{chat_id}"),
            InlineKeyboardButton(f"Despedida {bool_label(cfg_value(chat_id, 'farewell_enabled'))}", callback_data=f"cfg|toggle_farewell|{chat_id}"),
            InlineKeyboardButton("👮 Valida", callback_data=f"cfg|approvers|{chat_id}"),
        ],
        [
            InlineKeyboardButton("👥 Pendientes", callback_data=f"cfg|pending|{chat_id}"),
            InlineKeyboardButton("🔇 Silenciados", callback_data=f"cfg|muted|{chat_id}"),
            InlineKeyboardButton("🚫 Expulsados", callback_data=f"cfg|expelled|{chat_id}"),
        ],
        [
            InlineKeyboardButton("🚪 Entradas", callback_data=f"cfg|entries|{chat_id}"),
            InlineKeyboardButton("🕙 Inactivos", callback_data=f"cfg|inactive|{chat_id}"),
            InlineKeyboardButton("🏆 Ranking", callback_data=f"cfg|ranking|{chat_id}"),
        ],
        [
            InlineKeyboardButton("📜 LOG 3d", callback_data=f"cfg|log|{chat_id}"),
            InlineKeyboardButton("🔄 Reload", callback_data=f"cfg|reload|{chat_id}"),
            InlineKeyboardButton("💾 Guardar", callback_data=f"cfg|save|{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"{_mark(cleanup, 'off')}Comandos OFF", callback_data=f"cfg|cleanup_set|{chat_id}|off"),
            InlineKeyboardButton(f"{_mark(cleanup, 'instant')}Al ejecutar", callback_data=f"cfg|cleanup_set|{chat_id}|instant"),
            InlineKeyboardButton(f"{_mark(cleanup, 'ttl')}Tras 15s", callback_data=f"cfg|cleanup_set|{chat_id}|ttl"),
        ],
        [
            InlineKeyboardButton("📚 Comandos", callback_data=f"cfg|commands|{chat_id}"),
            InlineKeyboardButton("❓ Preguntas", callback_data=f"cfg|questions|{chat_id}"),
            InlineKeyboardButton("📌 Normas", callback_data=f"cfg|edit_text|{chat_id}|rules_text"),
        ],
    ]

    profiles = [
        ("welcome", "Bienvenida"),
        ("intro", "Presentación"),
        ("reminder", "Recordatorio"),
        ("timeout", "Timeout"),
        ("approved", "Aprobado"),
        ("rejected", "Rechazo"),
        ("farewell", "Despedida"),
        ("pregonero", "Pregonero"),
    ]

    for profile, label in profiles:
        if profile == "intro":
            rows.append([
                InlineKeyboardButton("👁 Presentación", callback_data=f"cfg|preview_intro|{chat_id}"),
                InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|validation_intro_message"),
                InlineKeyboardButton("❓ Preguntas", callback_data=f"cfg|questions|{chat_id}"),
            ])
            continue
        rows.append([
            InlineKeyboardButton(f"👁 {label}", callback_data=f"cfg|preview|{chat_id}|{profile}"),
            InlineKeyboardButton("✏️ Texto", callback_data=f"cfg|edit_text|{chat_id}|{profile_field(profile, 'text')}"),
            InlineKeyboardButton("🖼 Media", callback_data=f"cfg|media|{chat_id}|{profile}"),
        ])
        rows.append([
            InlineKeyboardButton(f"↕️ {media_position_label(chat_id, profile)}", callback_data=f"cfg|position|{chat_id}|{profile}"),
            InlineKeyboardButton("⌨️ Botones", callback_data=f"cfg|buttons|{chat_id}|{profile}"),
            InlineKeyboardButton("🔄 Preview", callback_data=f"cfg|preview|{chat_id}|{profile}"),
        ])

    rows.extend([
        [
            InlineKeyboardButton("📣 Manuales", callback_data=f"cfg|manual_pregonero|{chat_id}"),
            InlineKeyboardButton("📣 Lanzar", callback_data=f"cfg|run_pregonero|{chat_id}"),
            InlineKeyboardButton(f"4x4: {cfg_value(chat_id, 'pregonero_max_mentions_per_message', 4)}", callback_data=f"cfg|noop|{chat_id}"),
        ],
        [
            InlineKeyboardButton("📣 4x4 -", callback_data=f"cfg|pregonero_minus|{chat_id}"),
            InlineKeyboardButton("📣 4x4 +", callback_data=f"cfg|pregonero_plus|{chat_id}"),
            InlineKeyboardButton("🔙 Grupos", callback_data="cfg|list"),
        ],
        [InlineKeyboardButton("❌ Cerrar panel", callback_data=f"cfg|close|{chat_id}")],
    ])
    return InlineKeyboardMarkup(_three_col(rows))


def fake_profile_text(chat_id: int, profile: str) -> str:
    if profile == "pregonero":
        template = str(cfg_value(chat_id, profile_field(profile, "text"), "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}"))
        return cfg_fake_preview_values(template).replace("{mentions}", "@usuario1 @usuario2 @usuario3 @usuario4")
    template = str(cfg_value(chat_id, profile_field(profile, "text"), ""))
    return cfg_fake_preview_values(template)


async def send_profile_preview(bot, private_chat_id: int, target_chat_id: int, profile: str) -> None:
    title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
    text = fake_profile_text(target_chat_id, profile)
    await bot.send_message(private_chat_id, f"👁 <b>Vista previa completa: {h(title)}</b>", parse_mode=ParseMode.HTML)
    await send_configured_profile_message(bot, private_chat_id, profile, text, config_chat_id=target_chat_id)


async def send_intro_preview(bot, private_chat_id: int, target_chat_id: int) -> None:
    text = cfg_fake_preview_values(str(cfg_value(target_chat_id, "validation_intro_message", VALIDATION_INTRO_MESSAGE)))
    questions = cfg_questions(target_chat_id)
    if questions:
        text += "\n\n<b>Preguntas configuradas:</b>\n" + "\n".join(f"{i+1}. {h(q)}" for i, q in enumerate(questions))
    await bot.send_message(private_chat_id, "👁 <b>Vista previa completa: Presentación obligatoria</b>", parse_mode=ParseMode.HTML)
    await bot.send_message(private_chat_id, text, parse_mode=ParseMode.HTML)


def manual_pregonero_mentions(chat_id: int) -> List[str]:
    raw = cfg_value(chat_id, "pregonero_manual_users", [])
    if isinstance(raw, str):
        items = [x.strip() for x in raw.replace(",", "\n").splitlines() if x.strip()]
    elif isinstance(raw, list):
        items = [str(x).strip() for x in raw if str(x).strip()]
    else:
        items = []
    result: List[str] = []
    for item in items:
        if not item:
            continue
        if item.startswith("@") or item.startswith("<a ") or item.startswith("tg://"):
            result.append(item)
            continue
        if item.lstrip("-").isdigit():
            result.append(f'<a href="tg://user?id={int(item)}">usuario</a>')
        else:
            result.append("@" + item.lstrip("@"))
    return result


def known_pregonero_mentions(chat_id: int) -> List[str]:
    state = get_state(chat_id)
    users: Dict[str, Dict[str, Any]] = {}
    for uid, record in getattr(state, "member_activity", {}).items():
        if not record.get("is_bot"):
            users[str(uid)] = record
    for uid, record in state.validation_users.items():
        if record.get("status") in ("validated", "pending_admin", "answering"):
            users.setdefault(str(uid), record)
    for entry in getattr(state, "entry_log", []):
        uid = str(entry.get("user_id") or "")
        if uid:
            users.setdefault(uid, entry)

    mentions: List[str] = []
    seen: set[str] = set()
    for uid, record in users.items():
        try:
            mention = mention_from_known_user(int(uid), record)
        except Exception:
            continue
        if mention and mention not in seen:
            seen.add(mention)
            mentions.append(mention)
    for mention in manual_pregonero_mentions(chat_id):
        if mention not in seen:
            seen.add(mention)
            mentions.append(mention)
    return mentions


async def send_pregonero(context: ContextTypes.DEFAULT_TYPE, chat_id: int, *, title: str = "") -> None:
    mentions = known_pregonero_mentions(chat_id)
    if not mentions:
        await send_temp_message(context.bot, chat_id, "📣 No tengo usuarios registrados todavía para mencionar.", ttl=40)
        return
    max_per_message = int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4) or 4)
    max_per_message = max(1, min(10, max_per_message))
    template = str(cfg_value(chat_id, "pregonero_text", "📣 <b>EL PLAN TE LLAMA</b>\n\n{mentions}"))
    sent = 0
    for start in range(0, len(mentions), max_per_message):
        chunk = mentions[start:start + max_per_message]
        chunk_text = " ".join(chunk)
        text = template.replace("{mentions}", chunk_text) if "{mentions}" in template else template + "\n\n" + chunk_text
        await send_configured_profile_message(context.bot, chat_id, "pregonero", text)
        sent += len(chunk)
        await asyncio.sleep(0.8)
    add_bot_log(chat_id, "Pregonero lanzado", f"Menciones: {sent} · bloque: {max_per_message}")


def admin_panel_text(chat_id: int) -> str:
    cfg = admin_cfg(chat_id)
    state = get_state(chat_id)
    pending = sum(1 for r in state.validation_users.values() if r.get("status") == "pending_admin")
    answering = sum(1 for r in state.validation_users.values() if r.get("status") == "answering")
    expelled = len(getattr(state, "expelled_users", []))
    return (
        "<b>🛡️ PANEL RÁPIDO DEL GRUPO</b>\n\n"
        "La configuración completa se hace por privado con <code>/ajustes</code>.\n\n"
        f"Validación: <b>{bool_label(cfg.get('validation_enabled'))}</b> · "
        f"Autoaprobar: <b>{bool_label(cfg.get('validation_auto_approve_join_requests'))}</b>\n"
        f"Respondiendo: <b>{answering}</b> · Pendientes: <b>{pending}</b> · Expulsados: <b>{expelled}</b>\n"
        f"Borrado comandos: <b>{h(command_cleanup_label(chat_id))}</b>\n\n"
        "Usa este panel para consultas rápidas. Para textos, multimedia, botones y previsualizaciones: <b>privado</b>."
    )


def admin_panel_markup(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(_three_col([
        [InlineKeyboardButton("🏓 Ping", callback_data="adm|ping"), InlineKeyboardButton("📣 Pregonero", callback_data="adm|pregonero"), InlineKeyboardButton("⚙️ Privado", callback_data="adm|private")],
        [InlineKeyboardButton("👥 Pendientes", callback_data="adm|pendientes"), InlineKeyboardButton("🔇 Silenciados", callback_data="adm|muted"), InlineKeyboardButton("🚫 Expulsados", callback_data="adm|expelled")],
        [InlineKeyboardButton("🚪 Entradas", callback_data="adm|entries"), InlineKeyboardButton("🕙 Inactivos", callback_data="adm|inactive"), InlineKeyboardButton("🏆 Ranking", callback_data="adm|ranking")],
        [InlineKeyboardButton("📜 LOG 3d", callback_data="adm|log"), InlineKeyboardButton("🔄 Reload", callback_data="adm|back"), InlineKeyboardButton("❌ Cerrar", callback_data="adm|close")],
    ]))



async def can_use_private_config(context: ContextTypes.DEFAULT_TYPE, user_id: int, target_chat_id: Optional[int] = None) -> bool:
    if int(user_id) in ADMIN_IDS:
        return True
    if target_chat_id is not None:
        return await is_admin(context, int(target_chat_id), int(user_id))
    for cid in known_admin_chats_for_private():
        try:
            if await is_admin(context, int(cid), int(user_id)):
                return True
        except Exception:
            continue
    return False

async def _send_private_config_entry(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    load_all_states()
    try:
        chats = known_admin_chats_for_private()
        if not chats:
            await context.bot.send_message(chat_id=user_id, text="Aún no tengo grupos registrados. Usa /plan una vez dentro del grupo.")
            return True
        await context.bot.send_message(
            chat_id=user_id,
            text="<b>⚙️ Configuración privada</b>\n\nElige el grupo que quieres configurar:",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_private_chat_list_markup(),
        )
        return True
    except Exception:
        return False


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message or not chat_is_allowed(update.effective_chat.id):
        return
    chat_id = update.effective_chat.id
    remember_chat_title(chat_id, update.effective_chat.title or "")
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.message.reply_text("Solo administradores.")
        return
    await update.message.reply_html(admin_panel_text(chat_id), reply_markup=admin_panel_markup(chat_id))


async def admin_deprecated_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """El /admin antiguo ya no abre panel viejo: intenta abrir configuración privada."""
    if not update.effective_user or not update.message:
        return
    if update.effective_chat and update.effective_chat.type != "private":
        if update.effective_chat and not await is_admin(context, update.effective_chat.id, update.effective_user.id):
            await update.message.reply_text("Solo administradores.")
            return
        if update.effective_chat:
            remember_chat_title(update.effective_chat.id, update.effective_chat.title or "")
        ok = await _send_private_config_entry(context, update.effective_user.id)
        if ok:
            await update.message.reply_html("⚙️ Te he abierto la configuración por privado. <b>/admin ya no abre el panel antiguo.</b>")
        else:
            bot_username = (await context.bot.get_me()).username
            await update.message.reply_html(
                f"⚙️ La configuración se hace por privado. Ábreme chat y escribe <code>/ajustes</code>.\nhttps://t.me/{h(bot_username)}"
            )
        await cleanup_command_invocation(update, context)
        return
    await admin_private_config_command(update, context)


async def admin_private_config_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    # En privado se permite ADMIN_IDS; en grupo se exige admin del grupo y se manda el panel al privado.
    if update.effective_chat and update.effective_chat.type != "private":
        if not await is_admin(context, update.effective_chat.id, update.effective_user.id):
            await update.message.reply_text("Solo administradores.")
            return
        remember_chat_title(update.effective_chat.id, update.effective_chat.title or "")
        ok = await _send_private_config_entry(context, update.effective_user.id)
        if ok:
            await update.message.reply_html("✅ Te he enviado la configuración por privado.")
        else:
            bot_username = (await context.bot.get_me()).username
            await update.message.reply_html(
                f"No puedo escribirte aún. Abre el privado del bot y escribe <code>/ajustes</code>.\nhttps://t.me/{h(bot_username)}"
            )
        return
    if not await can_use_private_config(context, update.effective_user.id):
        await update.message.reply_text("Solo el dueño o administradores pueden abrir la configuración privada.")
        return
    load_all_states()
    if not known_admin_chats_for_private():
        await update.message.reply_text("Todavía no tengo grupos registrados. Usa /plan una vez dentro del grupo o espera a que entre alguien.")
        return
    await update.message.reply_html("<b>⚙️ Configuración privada</b>\n\nElige el grupo que quieres configurar:", reply_markup=admin_private_chat_list_markup())


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
    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            pass
        return True
    if action == "private":
        ok = await _send_private_config_entry(context, update.effective_user.id)
        await query.answer("Te lo he enviado por privado." if ok else "Ábreme privado y escribe /ajustes.", show_alert=not ok)
        return True
    if action == "ping":
        await query.edit_message_text(ping_text(), parse_mode=ParseMode.HTML, reply_markup=_adm_back())
        return True
    if action == "pregonero":
        await query.answer("Lanzando pregonero 4x4…")
        await send_pregonero(context, chat_id)
        return True
    view_actions = {
        "muted": admin_muted_users_text,
        "entries": admin_last_entries_text,
        "inactive": lambda cid: admin_inactive_users_text(cid, 10),
        "ranking": admin_ranking_text,
        "expelled": admin_expelled_users_text,
        "log": lambda cid: bot_log_text(cid, 3),
        "pendientes": validation_pending_summary_text,
    }
    if action in view_actions:
        markup = validation_pending_group_markup(chat_id) if action == "pendientes" else _adm_back()
        await query.edit_message_text(view_actions[action](chat_id), parse_mode=ParseMode.HTML, reply_markup=markup)
        return True
    if action == "reglas":
        await query.edit_message_text(str(cfg_value(chat_id, "rules_text", "Sin normas configuradas.")), parse_mode=ParseMode.HTML, reply_markup=_adm_back())
        return True
    if action == "back":
        await query.edit_message_text(admin_panel_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_panel_markup(chat_id))
        return True
    await query.answer("Esa configuración se hace por privado con /ajustes.", show_alert=True)
    return True


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

    if not await can_use_private_config(context, update.effective_user.id, chat_id):
        await query.answer("Solo el dueño o administradores de ese grupo.", show_alert=True)
        return

    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            await query.edit_message_text("✅ Panel de configuración cerrado.")
        return
    if action in ("open", "reload"):
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    if action == "save":
        save_all_states()
        await query.answer("Cambios guardados ✅", show_alert=False)
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return

    # Toggles y valores directos.
    if action == "toggle_validation":
        cfg_set(chat_id, "validation_enabled", not bool(cfg_value(chat_id, "validation_enabled")))
    elif action == "toggle_kick":
        cfg_set(chat_id, "validation_kick_if_timeout", not bool(cfg_value(chat_id, "validation_kick_if_timeout")))
    elif action == "toggle_autoapprove":
        cfg_set(chat_id, "validation_auto_approve_join_requests", not bool(cfg_value(chat_id, "validation_auto_approve_join_requests")))
    elif action == "toggle_rules_after_approval":
        cfg_set(chat_id, "validation_show_rules_after_approval", not bool(cfg_value(chat_id, "validation_show_rules_after_approval")))
    elif action == "toggle_farewell":
        cfg_set(chat_id, "farewell_enabled", not bool(cfg_value(chat_id, "farewell_enabled")))
    elif action == "time_plus":
        cfg_set(chat_id, "validation_timeout_minutes", int(cfg_value(chat_id, "validation_timeout_minutes", 10)) + 1)
    elif action == "time_minus":
        cfg_set(chat_id, "validation_timeout_minutes", max(1, int(cfg_value(chat_id, "validation_timeout_minutes", 10)) - 1))
    elif action == "rem_plus":
        cfg_set(chat_id, "validation_reminder_minutes", int(cfg_value(chat_id, "validation_reminder_minutes", 3)) + 1)
    elif action == "rem_minus":
        cfg_set(chat_id, "validation_reminder_minutes", max(1, int(cfg_value(chat_id, "validation_reminder_minutes", 3)) - 1))
    elif action == "pregonero_plus":
        cfg_set(chat_id, "pregonero_max_mentions_per_message", min(10, int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4)) + 1))
    elif action == "pregonero_minus":
        cfg_set(chat_id, "pregonero_max_mentions_per_message", max(1, int(cfg_value(chat_id, "pregonero_max_mentions_per_message", 4)) - 1))
    elif action == "position" and len(parts) >= 4:
        toggle_media_position(chat_id, parts[3])
    elif action == "cleanup_mode":
        cfg_set(chat_id, "command_cleanup_mode", next_command_cleanup_mode(chat_id))
    elif action == "cleanup_set" and len(parts) >= 4:
        mode = parts[3]
        if mode in ("off", "instant", "ttl"):
            cfg_set(chat_id, "command_cleanup_mode", mode)
            if mode == "ttl":
                cfg_set(chat_id, "command_cleanup_ttl_seconds", 15)
            await query.answer(f"Borrado de comandos: {command_cleanup_label(chat_id)}")
        else:
            await query.answer("Modo no válido.", show_alert=True)
        await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))
        return
    elif action == "commands":
        await query.edit_message_text(all_commands_text(), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "muted":
        await query.edit_message_text(admin_muted_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "expelled":
        await query.edit_message_text(admin_expelled_users_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "log":
        await query.edit_message_text(bot_log_text(chat_id, 3), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "entries":
        await query.edit_message_text(admin_last_entries_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "inactive":
        await query.edit_message_text(admin_inactive_users_text(chat_id, 10), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "ranking":
        await query.edit_message_text(admin_ranking_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=_cfg_back(chat_id))
        return
    elif action == "pending":
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=validation_pending_config_markup(chat_id))
        return
    elif action in ("pendkick", "pendremind", "pendmute") and len(parts) >= 4:
        uid = int(parts[3])
        rec = validation_get_record(chat_id, uid) or {"name": str(uid)}
        if action == "pendkick":
            ok = await validation_kick_pending(context.bot, chat_id, uid, rec, reason="expulsado_desde_pendientes")
            await query.answer("Expulsado." if ok else "No pude expulsar.", show_alert=not ok)
        elif action == "pendremind":
            await validation_send_reminder(context.bot, chat_id, uid, rec)
            await query.answer("Recordatorio enviado.")
        elif action == "pendmute":
            await validation_mute_bot(context.bot, chat_id, uid)
            mark_user_muted(chat_id, uid, reason="silenciado_hasta_contestar")
            add_bot_log(chat_id, "Silenciado desde pendientes", rec.get("name", str(uid)), user_id=uid)
            await query.answer("Silenciado.")
        await query.edit_message_text(validation_pending_summary_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=validation_pending_config_markup(chat_id))
        return
    elif action == "preview" and len(parts) >= 4:
        await send_profile_preview(context.bot, query.message.chat.id, chat_id, parts[3])
        await query.answer("Vista previa enviada.")
        return
    elif action == "preview_intro":
        await send_intro_preview(context.bot, query.message.chat.id, chat_id)
        await query.answer("Vista previa enviada.")
        return
    elif action == "edit_text" and len(parts) >= 4:
        field = parts[3]
        set_config_pending(update.effective_user.id, {"kind": "cfg_text", "chat_id": chat_id, "field": field})
        label = CONFIG_TEXT_FIELDS.get(field, field)
        current = str(cfg_value(chat_id, field, ""))
        await query.edit_message_text(
            f"✏️ <b>{h(label)}</b>\n\nActual:\n<pre>{h(current)}</pre>\n\nEnvíame ahora el nuevo texto por aquí.\n\nVariables: <code>{{mention}}</code>, <code>{{name}}</code>, <code>{{first}}</code>, <code>{{username}}</code>, <code>{{id}}</code>, <code>{{chat}}</code>, <code>{{mentions}}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "questions":
        set_config_pending(update.effective_user.id, {"kind": "cfg_questions", "chat_id": chat_id})
        current = "\n".join(cfg_questions(chat_id))
        await query.edit_message_text(f"❓ <b>Preguntas actuales</b>\n\n<pre>{h(current)}</pre>\n\nEnvíame las nuevas preguntas, una por línea o separadas por |.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "buttons" and len(parts) >= 4:
        profile = parts[3]
        buttons_field = profile_field(profile, "buttons")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_buttons", "chat_id": chat_id, "buttons_field": buttons_field})
        await query.edit_message_text(f"⌨️ <b>Botones: {h(title)}</b>\n\nEnvíame botones así, uno por línea:\n\n<code>Texto del botón - https://enlace.com</code>\n\nPara borrar todos escribe: <code>QUITAR</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "media" and len(parts) >= 4:
        profile = parts[3]
        media_field = profile_field(profile, "media")
        title = MESSAGE_PROFILES.get(profile, MESSAGE_PROFILES["welcome"])["title"]
        set_config_pending(update.effective_user.id, {"kind": "cfg_media", "chat_id": chat_id, "media_field": media_field})
        await query.edit_message_text(f"🖼 <b>Multimedia: {h(title)}</b>\n\nEnvíame ahora una foto, vídeo, GIF o documento.\n\nPara quitar multimedia escribe: <code>QUITAR</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]]))
        return
    elif action == "manual_pregonero":
        current = cfg_value(chat_id, "pregonero_manual_users", [])
        if isinstance(current, list):
            current_text = "\n".join(str(x) for x in current)
        else:
            current_text = str(current or "")
        set_config_pending(update.effective_user.id, {"kind": "cfg_pregonero_users", "chat_id": chat_id})
        await query.edit_message_text(
            "📣 <b>Usuarios manuales del pregonero</b>\n\n"
            "Envíame usuarios, uno por línea. Puedes poner <code>@usuario</code>, ID numérico o username sin @.\n"
            "Para borrar todos escribe: <code>QUITAR</code>\n\n"
            f"Actual:\n<pre>{h(current_text)}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data=f"cfg|open|{chat_id}")]])
        )
        return
    elif action == "run_pregonero":
        await query.answer("Lanzando pregonero…")
        await send_pregonero(context, chat_id)
        return
    elif action == "approvers":
        await query.edit_message_text("👮 <b>Quién puede validar presentaciones</b>\n\nElige quién podrá pulsar ✅ Validar o ❌ Rechazar.", parse_mode=ParseMode.HTML, reply_markup=approver_markup(chat_id))
        return
    elif action == "setapprover" and len(parts) >= 4:
        mode = parts[3]
        if mode in APPROVER_MODE_LABELS:
            cfg_set(chat_id, "validation_approver_mode", mode)
    elif action == "noop":
        await query.answer("Sin acción directa.")
        return

    await query.edit_message_text(admin_private_main_text(chat_id), parse_mode=ParseMode.HTML, reply_markup=admin_private_main_markup(chat_id))


def build_application() -> Application:
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    # Comandos oficiales. /admin ya no abre paneles viejos: redirige a /plan.
    application.add_handler(command_handler(["djplan", "start"], start_command))
    application.add_handler(command_handler("dj", assign_dj_command))
    application.add_handler(command_handler(["plan", "paneladmin"], admin_command))
    application.add_handler(command_handler("admin", admin_deprecated_command))
    application.add_handler(command_handler(["ajustes", "configuracion", "config"], admin_private_config_command))
    application.add_handler(command_handler("ping", ping_command))
    application.add_handler(command_handler(["presentate"], force_presentate_command))
    application.add_handler(MessageHandler(filters.Regex(r"^/preséntate(?:@\w+)?(?:\s|$)"), presentate_accent_message))

    application.add_handler(command_handler("pregonero", pregonero_command))
    application.add_handler(command_handler("el", pregonero_command))
    application.add_handler(command_handler("silenciados", silenciados_command))
    application.add_handler(command_handler("entradas", entradas_command))
    application.add_handler(command_handler("inactivos", inactivos_command))
    application.add_handler(command_handler("ranking", ranking_command))
    application.add_handler(command_handler("expulsados", expulsados_command))
    application.add_handler(command_handler("log", log_command))

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

    application.add_handler(CallbackQueryHandler(admin_private_config_callback, pattern="^cfg\\|"))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_router))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL), admin_private_config_media))
    music_filter = filters.AUDIO | filters.VOICE | filters.Document.ALL
    application.add_handler(MessageHandler(music_filter, music_message_router))
    application.add_error_handler(on_application_error)
    return application


def main() -> None:
    app = build_application()
    app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()
