import asyncio
import json
import logging
import os
import secrets
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# Importante para Python 3.14 + PyTgCalls
asyncio.set_event_loop(asyncio.new_event_loop())

from telethon import TelegramClient
from telethon.sessions import StringSession
from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update, CopyTextButton
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
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

STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger("djplan")


AUTO_NEXT_OPTIONS = [0, -5, -10, -15, -20]
AUTO_NEXT_TASKS: Dict[int, asyncio.Task] = {}
SCAN_TASKS: Dict[int, asyncio.Task] = {}
PAGE_SIZE = 15
UI_REFRESH_SECONDS = 15


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
    paused: bool = False
    now_playing: Optional[Dict[str, Any]] = None
    queue: List[Dict[str, Any]] = field(default_factory=list)
    history: List[Dict[str, Any]] = field(default_factory=list)
    library: List[Dict[str, Any]] = field(default_factory=list)
    saved_lists: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    temp_message_ids: List[int] = field(default_factory=list)
    auto_next_offset: int = 0
    volume: int = 100
    play_started_at: Optional[int] = None
    paused_remaining: Optional[int] = None


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
                # Compatibilidad con versiones antiguas que usaban autoplay_offset
                if "auto_next_offset" not in state_data and "autoplay_offset" in state_data:
                    state_data["auto_next_offset"] = state_data.pop("autoplay_offset")
                else:
                    state_data.pop("autoplay_offset", None)

            data[int(chat_id_str)] = ChatState(**state_data)
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
        if not raw:
            return 0
        return max(0, int(float(raw)))
    except Exception:
        logger.exception("No se pudo obtener duración con ffprobe: %s", file_path)
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


def panel_text(state: ChatState) -> str:
    live_on = bool(state.now_playing)
    status = "🔴 LIVE ON" if live_on else "🔵 LIVE OFF"
    dj = state.assigned_dj_name or "Sin asignar"

    if state.now_playing:
        track = Track(**state.now_playing)
        current = shorten_title(track.title, 20)
    else:
        current = "Nada sonando"

    return f"🎛️<b>DJ-PLAN:</b> {dj} | 🎶 <b>{current}</b> 🎶 | {status}"


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

    auto_label = f"{state.auto_next_offset}s"
    paused_label = "⏸️ PAUSADO" if state.paused else "▶️ ACTIVO"
    remaining = remaining_seconds(state)
    remaining_label = fmt_duration(remaining) if remaining is not None else "--:--"

    return (
        "<b>🎛️ CUADRO DE MANDOS DJ</b>\n\n"
        f"🎵 Actual: <b>{current_title}</b>\n"
        f"⏭️ Siguiente: <b>{next_title}</b>\n"
        f"⏳ Queda: <b>{remaining_label}</b>\n\n"
        f"📋 En cola: <b>{len(state.queue)}</b>\n"
        f"📚 Biblioteca: <b>{len(state.library)}</b>\n"
        f"🎧 DJ actual: <b>{state.assigned_dj_name or 'Sin asignar'}</b>\n"
        f"⏭️ Auto-siguiente: <b>{auto_label}</b>\n"
        f"🔊 Volumen: <b>{state.volume}</b>\n"
        f"⚙️ Estado: <b>{paused_label}</b>\n\n"
    )


def control_panel_text(state: ChatState) -> str:
    return control_header(state) + "Selecciona una acción del panel."


def control_panel_markup(state: ChatState) -> InlineKeyboardMarkup:
    voice_button = (
        InlineKeyboardButton("🎧 Unirse al directo", url=VOICE_CHAT_LINK)
        if VOICE_CHAT_LINK
        else InlineKeyboardButton("🎧 Unirse al directo", callback_data="panel_voice_info")
    )
    auto_label = f"⏭️ Auto-sig {state.auto_next_offset}"

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⏯️ Play/Pausa", callback_data="panel_pause_resume"),
                InlineKeyboardButton("⏭️ Siguiente", callback_data="panel_next"),
                InlineKeyboardButton("⏮️ Anterior", callback_data="panel_prev"),
            ],
            [
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
                InlineKeyboardButton("🔉 Vol -", callback_data="panel_vol_down"),
                InlineKeyboardButton("🔊 Vol +", callback_data="panel_vol_up"),
                InlineKeyboardButton(auto_label, callback_data="panel_auto_next"),
            ],
            [
                voice_button,
                InlineKeyboardButton("🔄 Actualizar", callback_data="panel_refresh"),
            ],
            [
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
        current_line = f"🔴 Sonando: <b>{current.title}</b>\n\n"
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
        row: List[InlineKeyboardButton] = [
            InlineKeyboardButton(f"▶️ {truncated_button_title(track.title)}", callback_data=f"q|p|{idx}|{page}")
        ]
        if idx > 0:
            row.append(InlineKeyboardButton("⬆️", callback_data=f"q|u|{idx}|{page}"))
        if idx < len(state.queue) - 1:
            row.append(InlineKeyboardButton("⬇️", callback_data=f"q|d|{idx}|{page}"))
        row.append(InlineKeyboardButton("🗑️", callback_data=f"q|x|{idx}|{page}"))
        rows.append(row)

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
            InlineKeyboardButton(f"▶️ {truncated_button_title(track.title)}", callback_data=f"lib|p|{idx}|{page}"),
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
        lines.append(f"{idx}. {name} ({len(state.saved_lists[name])})")
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


async def ensure_panel(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    if not state.dj_mode:
        return

    text = panel_text(state)
    markup = panel_markup()

    if state.panel_message_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=state.panel_message_id,
                text=text,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            )
            save_all_states()
            return
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                return
        except Exception:
            logger.exception("No se pudo editar el panel fijado; se recreará")

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=markup,
        parse_mode=ParseMode.HTML,
    )
    state.panel_message_id = msg.message_id
    save_all_states()

    try:
        await bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        logger.exception("No se pudo fijar el panel")


async def ensure_control_panel(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    if not state.dj_mode:
        return

    text = control_panel_text(state)
    markup = control_panel_markup(state)

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
        except Exception:
            logger.exception("No se pudo editar el cuadro de mandos; se recreará")

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=markup,
        parse_mode=ParseMode.HTML,
    )
    state.control_message_id = msg.message_id
    save_all_states()


async def render_control_home(bot, chat_id: int) -> None:
    await ensure_control_panel(bot, chat_id)


async def render_control_view(bot, chat_id: int, body_text: str, reply_markup: InlineKeyboardMarkup) -> None:
    state = get_state(chat_id)
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
        except Exception:
            logger.exception("No se pudo editar la vista del cuadro de mandos")

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )
    state.control_message_id = msg.message_id
    save_all_states()


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
    return state.assigned_dj_id == user_id


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
        audio_like = (doc.mime_type or "").startswith("audio/") or lower.endswith(
            (".mp3", ".m4a", ".ogg", ".wav", ".flac", ".opus")
        )
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

    existing_keys = {
        item.get("file_unique_id") or item.get("file_id") or f"msg:{item.get('original_message_id')}"
        for item in state.library
    }

    async for message in VOICE.client.iter_messages(chat_id, limit=limit):
        try:
            track = extract_track_from_telethon_message(message, chat_id)
            if not track:
                continue

            found += 1
            unique_key = track.file_unique_id or track.file_id or f"msg:{track.original_message_id}"
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
    task = AUTO_NEXT_TASKS.pop(chat_id, None)
    if task and not task.done():
        task.cancel()


async def execute_next_button_action(chat_id: int) -> None:
    # Misma acción que el botón:
    # InlineKeyboardButton("⏭️ Siguiente", callback_data="panel_next")
    await VOICE.play_next_from_queue(chat_id)


async def _auto_next_monitor(chat_id: int, track_token: str) -> None:
    import time as _time

    next_ui_refresh = 0
    try:
        while True:
            await asyncio.sleep(1)
            state = get_state(chat_id)

            if not state.now_playing or state.paused:
                return

            if track_key(state.now_playing) != track_token:
                return

            remaining = remaining_seconds(state)
            if remaining is None:
                return

            if VOICE.application and int(_time.time()) >= next_ui_refresh:
                next_ui_refresh = int(_time.time()) + UI_REFRESH_SECONDS
                try:
                    await ensure_panel(VOICE.application.bot, chat_id)
                    await ensure_control_panel(VOICE.application.bot, chat_id)
                except Exception:
                    logger.exception("No se pudo refrescar el panel durante auto-siguiente")

            trigger = abs(state.auto_next_offset) if state.auto_next_offset < 0 and state.queue else 0

            # Ejecuta la MISMA acción de "⏭️ Siguiente" cuando quede el tiempo marcado o menos
            if remaining <= trigger:
                await execute_next_button_action(chat_id)
                return

            if remaining <= 0:
                await execute_next_button_action(chat_id)
                return

    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("Error en monitor de auto-siguiente para chat %s", chat_id)


async def schedule_auto_next(chat_id: int, duration: int, offset: int) -> None:
    await cancel_auto_next(chat_id)
    state = get_state(chat_id)
    if duration <= 0 or not state.now_playing:
        return
    AUTO_NEXT_TASKS[chat_id] = asyncio.create_task(_auto_next_monitor(chat_id, track_key(state.now_playing)))


class VoiceEngine:
    def __init__(self) -> None:
        self.client: Optional[TelegramClient] = None
        self.calls: Optional[PyTgCalls] = None
        self.application: Optional[Application] = None

    async def start(self, application: Application) -> None:
        self.application = application
        self.client = TelegramClient(load_userbot_string_session(), API_ID, API_HASH)
        await self.client.start()
        self.calls = PyTgCalls(self.client)
        await self.calls.start()
        logger.info("Userbot + voice engine iniciados")

    async def stop(self) -> None:
        for chat_id in list(AUTO_NEXT_TASKS.keys()):
            await cancel_auto_next(chat_id)
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

        if not track.local_path or not Path(track.local_path).exists():
            track = await materialize_track(bot, chat_id, track)

        if track.duration <= 0 and track.local_path and Path(track.local_path).exists():
            track.duration = probe_duration_seconds(track.local_path)

        await self.play_file(chat_id, track.local_path)
        await self._apply_volume(chat_id, state.volume)

        state.now_playing = asdict(track)
        state.paused = False
        state.play_started_at = int(_time.time())
        state.paused_remaining = None
        save_all_states()

        await ensure_panel(bot, chat_id)
        await ensure_control_panel(bot, chat_id)
        await cleanup_old_files(chat_id)
        await schedule_auto_next(chat_id, track.duration, state.auto_next_offset)

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

    async def pause_resume(self, chat_id: int) -> None:
        import time as _time

        state = get_state(chat_id)
        if not self.calls:
            raise RuntimeError("Voice engine no iniciado")

        if not state.now_playing and state.queue:
            await self.play_next_from_queue(chat_id)
            return

        if not state.now_playing:
            return

        if state.paused:
            await self.calls.resume_stream(chat_id)
            state.paused = False
            if state.paused_remaining is not None and state.now_playing:
                track = Track(**state.now_playing)
                state.play_started_at = int(_time.time()) - max(
                    0, int(track.duration) - int(state.paused_remaining)
                )
                state.paused_remaining = None
            current = Track(**state.now_playing)
            save_all_states()
            await schedule_auto_next(chat_id, current.duration, state.auto_next_offset)
        else:
            await self.calls.pause_stream(chat_id)
            state.paused = True
            state.paused_remaining = remaining_seconds(state)
            save_all_states()
            await cancel_auto_next(chat_id)

        if self.application:
            await ensure_panel(self.application.bot, chat_id)
            await ensure_control_panel(self.application.bot, chat_id)

    async def change_volume(self, chat_id: int, delta: int) -> int:
        state = get_state(chat_id)
        state.volume = max(1, min(200, state.volume + delta))
        save_all_states()
        await self._apply_volume(chat_id, state.volume)
        if self.application:
            await ensure_control_panel(self.application.bot, chat_id)
        return state.volume

    async def leave(self, chat_id: int) -> None:
        await cancel_auto_next(chat_id)
        if self.calls:
            try:
                await self.calls.leave_call(chat_id)
            except Exception:
                pass


VOICE = VoiceEngine()


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat:
        return
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="<b>DJ-PLAN</b>",
        reply_markup=main_menu_markup(),
        parse_mode=ParseMode.HTML,
    )


async def assign_dj_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if not await is_admin(context, update.effective_chat.id, update.effective_user.id):
        await update.message.reply_text("Solo un admin puede asignar DJ.")
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
    await send_temp_message(
        context.bot,
        update.effective_chat.id,
        f"✅ DJ asignado: <b>{state.assigned_dj_name}</b>",
    )


async def maybe_handle_pending_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message or not update.message.text:
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
            await send_temp_message(
                context.bot,
                update.effective_chat.id,
                f"💾 Lista guardada: <b>{text}</b>",
            )

        PENDING_ACTIONS.pop(key, None)
        await safe_delete(context.bot, update.effective_chat.id, prompt_id)
        await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)
        return True

    return False


async def text_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await maybe_handle_pending_text(update, context):
        return

    if not update.effective_chat or not update.message or not update.message.text:
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
    if not update.effective_chat or not update.message:
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
    key = track.file_unique_id or track.file_id

    for item in state.library:
        if (item.get("file_unique_id") or item.get("file_id")) == key:
            return False

    state.library.append(asdict(track))
    save_all_states()
    return True


async def queue_track(chat_id: int, track: Track) -> None:
    state = get_state(chat_id)
    state.queue.append(asdict(track))
    save_all_states()


async def play_selected_track(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    track: Track,
    *,
    push_current_to_history: bool = True,
) -> None:
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

    saved_lists = dict(state.saved_lists)
    library = [dict(item) for item in state.library]
    auto_next_offset = state.auto_next_offset
    volume = state.volume

    await cleanup_temp_messages(bot, chat_id)
    await cleanup_track_controls(bot, chat_id)
    await cancel_auto_next(chat_id)

    scan_task = SCAN_TASKS.pop(chat_id, None)
    if scan_task and not scan_task.done():
        scan_task.cancel()

    await VOICE.leave(chat_id)

    if state.panel_message_id:
        try:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=state.panel_message_id)
        except Exception:
            pass
        await safe_delete(bot, chat_id, state.panel_message_id)

    if state.control_message_id:
        await safe_delete(bot, chat_id, state.control_message_id)

    await cleanup_temp_messages(bot, chat_id)
    await cleanup_track_controls(bot, chat_id)

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
        paused=False,
        now_playing=None,
        queue=[],
        history=[],
        library=library,
        saved_lists=saved_lists,
        temp_message_ids=[],
        auto_next_offset=auto_next_offset,
        volume=volume,
        play_started_at=None,
        paused_remaining=None,
    )
    save_all_states()

    await cleanup_old_files(chat_id)
    await send_temp_message(bot, chat_id, "<b>SESIÓN DE DJ FINALIZADA</b>", ttl=30)


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_chat or not update.effective_user:
        return

    await query.answer()

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    state = get_state(chat_id)
    data = query.data or ""

    if data == "menu_panel":
        if not await is_admin(context, chat_id, user_id):
            await query.answer("Solo un admin puede activar el modo DJ.", show_alert=True)
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

        await ensure_panel(context.bot, chat_id)
        await ensure_control_panel(context.bot, chat_id)
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

    if data == "panel_voice_info":
        await query.answer("Configura VOICE_CHAT_LINK para abrir el voice directamente.", show_alert=True)
        return

    if data == "panel_queue":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar el cuadro de mandos.", show_alert=True)
            return
        await render_control_view(context.bot, chat_id, queue_text(state, 0), queue_markup(state, 0))
        return

    if data == "panel_library":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar el cuadro de mandos.", show_alert=True)
            return
        await render_control_view(context.bot, chat_id, library_text(state, 0), library_markup(state, 0))
        return

    if data == "panel_load_lists":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede usar el cuadro de mandos.", show_alert=True)
            return
        await render_control_view(context.bot, chat_id, saved_lists_text(state), saved_lists_markup(state))
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

    if data == "panel_pause_resume":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede controlar el directo.", show_alert=True)
            return
        if not state.now_playing and not state.queue:
            await query.answer("No hay canciones ni sonando ni en cola.", show_alert=True)
            return
        await VOICE.pause_resume(chat_id)
        return

    if data == "panel_next":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede controlar el directo.", show_alert=True)
            return
        if not state.queue:
            await query.answer("No hay canciones en cola.", show_alert=True)
            return
        await execute_next_button_action(chat_id)
        return

    if data == "panel_prev":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede controlar el directo.", show_alert=True)
            return
        if not state.history:
            await query.answer("No hay canción anterior disponible.", show_alert=True)
            return

        previous = Track(**state.history.pop())
        if state.now_playing:
            state.queue.insert(0, dict(state.now_playing))

        save_all_states()
        await VOICE.play_track(context.bot, chat_id, previous)
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
        await cleanup_old_files(chat_id)
        return

    # Compatibilidad: acepta el callback viejo panel_autoplay y el nuevo panel_auto_next
    if data in ("panel_auto_next", "panel_autoplay"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cambiar auto-siguiente.", show_alert=True)
            return

        current_idx = AUTO_NEXT_OPTIONS.index(state.auto_next_offset) if state.auto_next_offset in AUTO_NEXT_OPTIONS else 0
        state.auto_next_offset = AUTO_NEXT_OPTIONS[(current_idx + 1) % len(AUTO_NEXT_OPTIONS)]
        save_all_states()

        await ensure_control_panel(context.bot, chat_id)

        if state.now_playing and not state.paused:
            current = Track(**state.now_playing)
            await schedule_auto_next(chat_id, current.duration, state.auto_next_offset)
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
        if not await is_controller(context, chat_id, user_id) and not await is_admin(context, chat_id, user_id):
            await query.answer("Solo el DJ o un admin puede cerrar.", show_alert=True)
            return
        await close_dj_session(context.bot, chat_id)
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
            await send_temp_message(context.bot, chat_id, f"▶️ Ahora suena: <b>{track.title}</b>", ttl=20)
        elif action == "q":
            await queue_track(chat_id, track)
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, f"➕ Añadida a cola: <b>{track.title}</b>", ttl=20)
        elif action == "l":
            added = await add_to_library(chat_id, track)
            txt = f"📚 Guardada en biblioteca: <b>{track.title}</b>" if added else "ℹ️ Esa canción ya estaba en la biblioteca."
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

        if action == "r":
            await render_control_home(context.bot, chat_id)
            return

        if action == "pg":
            await render_control_view(context.bot, chat_id, queue_text(state, idx), queue_markup(state, idx))
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
        await render_control_view(context.bot, chat_id, queue_text(state, page), queue_markup(state, page))
        return

    if data.startswith("lib|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede tocar la biblioteca.", show_alert=True)
            return

        parts = data.split("|")
        action = parts[1]
        idx = int(parts[2]) if len(parts) > 2 else 0
        page = int(parts[3]) if len(parts) > 3 else 0

        if action == "r":
            await render_control_home(context.bot, chat_id)
            return

        if action == "pg":
            await render_control_view(context.bot, chat_id, library_text(state, idx), library_markup(state, idx))
            return

        if action == "qa":
            for item in state.library:
                await queue_track(chat_id, Track(**item))
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await render_control_view(context.bot, chat_id, library_text(state, page), library_markup(state, page))
            return

        if action == "pa":
            if not state.library:
                return
            first = Track(**state.library[0])
            for item in state.library[1:]:
                await queue_track(chat_id, Track(**item))
            await play_selected_track(context, chat_id, first)
            await ensure_panel(context.bot, chat_id)
            await ensure_control_panel(context.bot, chat_id)
            await render_control_view(context.bot, chat_id, library_text(state, page), library_markup(state, page))
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
        await ensure_control_panel(context.bot, chat_id)

        page = clamp_page(page, len(state.library))
        await render_control_view(context.bot, chat_id, library_text(state, page), library_markup(state, page))
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
            await send_temp_message(context.bot, chat_id, f"➕ Lista añadida a cola: <b>{name}</b>", ttl=20)
        elif action == "p":
            if items:
                first = Track(**items[0])
                for item in items[1:]:
                    state.queue.append(dict(item))
                save_all_states()
                await play_selected_track(context, chat_id, first)
                await ensure_panel(context.bot, chat_id)
                await ensure_control_panel(context.bot, chat_id)
                await send_temp_message(context.bot, chat_id, f"▶️ Reproduciendo lista: <b>{name}</b>", ttl=20)
        elif action == "x":
            state.saved_lists.pop(name, None)
            save_all_states()

        await render_control_view(context.bot, chat_id, saved_lists_text(state), saved_lists_markup(state))
        return


async def on_startup(application: Application) -> None:
    load_all_states()
    await VOICE.start(application)
    logger.info("DJ-PLAN iniciado")


async def on_shutdown(application: Application) -> None:
    await VOICE.stop()


def build_application() -> Application:
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("dj", assign_dj_command))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_router))

    music_filter = filters.AUDIO | filters.VOICE | filters.Document.ALL
    application.add_handler(MessageHandler(music_filter, music_message_router))

    return application


def main() -> None:
    app = build_application()
    app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()