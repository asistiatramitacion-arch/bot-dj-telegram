import asyncio
import json
import logging
import os
import secrets
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# Important for Python 3.14 + PyTgCalls import path.
asyncio.set_event_loop(asyncio.new_event_loop())

from telethon import TelegramClient
from telethon.sessions import StringSession
from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update
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
VOICE_CHAT_LINK = os.getenv("VOICE_CHAT_LINK", "").strip()
SEARCH_TRIGGER = os.getenv("SEARCH_TRIGGER", "@sha ").strip()
STATE_PATH = Path(os.getenv("STATE_PATH", "/data/state.json"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/data/downloads"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "80"))

STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger("djplan")


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
    paused: bool = False
    now_playing: Optional[Dict[str, Any]] = None
    queue: List[Dict[str, Any]] = field(default_factory=list)
    history: List[Dict[str, Any]] = field(default_factory=list)
    library: List[Dict[str, Any]] = field(default_factory=list)
    saved_lists: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    temp_message_ids: List[int] = field(default_factory=list)


STATE_CACHE: Dict[int, ChatState] = {}
TRACK_REGISTRY: Dict[int, Dict[int, Dict[str, Any]]] = {}
TRACK_CONTROL_REGISTRY: Dict[int, Dict[int, int]] = {}
PENDING_ACTIONS: Dict[str, Dict[str, Any]] = {}


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


def track_key(data: Dict[str, Any]) -> str:
    return data.get("file_unique_id") or data.get("file_id") or data.get("title", "")


def panel_text(state: ChatState) -> str:
    live_status = "🔴 EN DIRECTO" if state.now_playing else "⚪ SIN DIRECTO"
    dj_mode_status = "🟢 DJ ON" if state.dj_mode else "🔴 DJ OFF"
    dj = state.assigned_dj_name or "Sin asignar"
    queue_count = len(state.queue)
    library_count = len(state.library)
    paused_text = "⏸️ Pausado" if state.paused else "▶️ Activo"

    if state.now_playing:
        track = Track(**state.now_playing)
        current = f"🎵 <b>{track.title}</b>"
        meta: List[str] = []
        if track.performer:
            meta.append(track.performer)
        if track.duration:
            meta.append(fmt_duration(track.duration))
        if meta:
            current += "\n" + " · ".join(meta)
    else:
        current = "🎵 Nada sonando"

    return (
        "<b>DJ-PLAN</b>\n"
        f"{live_status}\n"
        f"🎚️ Modo: <b>{dj_mode_status}</b>\n"
        f"🎧 DJ: <b>{dj}</b>\n"
        f"📋 Cola: <b>{queue_count}</b>\n"
        f"📚 Biblioteca: <b>{library_count}</b>\n"
        f"⚙️ Estado: <b>{paused_text}</b>\n\n"
        f"{current}"
    )


def panel_markup(state: ChatState) -> InlineKeyboardMarkup:
    listen_row = (
        [InlineKeyboardButton("🎧 Entrar al voice", url=VOICE_CHAT_LINK)]
        if VOICE_CHAT_LINK
        else [InlineKeyboardButton("🎧 Entrar al voice", callback_data="panel_voice_info")]
    )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📋 Lista actual", callback_data="panel_queue"),
                InlineKeyboardButton("📚 Biblioteca", callback_data="panel_library"),
            ],
            [
                InlineKeyboardButton("🧭 Rastrear grupo", callback_data="panel_scan_group"),
                InlineKeyboardButton("💾 Guardar lista", callback_data="panel_save_list"),
            ],
            [
                InlineKeyboardButton("📂 Cargar lista", callback_data="panel_load_lists"),
                InlineKeyboardButton("🔎 Buscar música", callback_data="panel_search_help"),
            ],
            [
                InlineKeyboardButton("⏯️ Pausa/Reanudar", callback_data="panel_pause_resume"),
                InlineKeyboardButton("⏮️ Anterior", callback_data="panel_prev"),
                InlineKeyboardButton("⏭️ Siguiente", callback_data="panel_next"),
            ],
            listen_row,
            [
                InlineKeyboardButton("🎤 DJ = yo", callback_data="panel_take_dj"),
                InlineKeyboardButton("🔴 DJ OFF" if state.dj_mode else "🟢 DJ ON", callback_data="panel_toggle_dj"),
            ],
            [
                InlineKeyboardButton("🔄 Actualizar", callback_data="panel_refresh"),
                InlineKeyboardButton("❌ Cerrar", callback_data="panel_close"),
            ],
        ]
    )


def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🎛️ Abrir panel DJ", callback_data="menu_panel")],
            [InlineKeyboardButton("🔎 Buscar música", callback_data="menu_search_help")],
        ]
    )


def queue_text(state: ChatState) -> str:
    lines = ["<b>Lista actual</b>"]
    if state.now_playing:
        current = Track(**state.now_playing)
        lines.append(f"\n🔴 Sonando: <b>{current.title}</b>")
    if not state.queue:
        lines.append("\nLa cola está vacía.")
        return "\n".join(lines)
    lines.append("")
    for idx, item in enumerate(state.queue, start=1):
        track = Track(**item)
        meta: List[str] = []
        if track.performer:
            meta.append(track.performer)
        if track.duration:
            meta.append(fmt_duration(track.duration))
        suffix = f" — {' · '.join(meta)}" if meta else ""
        lines.append(f"{idx}. {track.title}{suffix}")
    return "\n".join(lines)


def queue_markup(state: ChatState) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, _ in enumerate(state.queue):
        row: List[InlineKeyboardButton] = [InlineKeyboardButton(f"▶️ {idx+1}", callback_data=f"q|p|{idx}")]
        if idx > 0:
            row.append(InlineKeyboardButton("⬆️", callback_data=f"q|u|{idx}"))
        if idx < len(state.queue) - 1:
            row.append(InlineKeyboardButton("⬇️", callback_data=f"q|d|{idx}"))
        row.append(InlineKeyboardButton("🗑️", callback_data=f"q|x|{idx}"))
        rows.append(row)
    rows.append([
        InlineKeyboardButton("🧹 Vaciar cola", callback_data="q|c|0"),
        InlineKeyboardButton("❌ Cerrar", callback_data="q|r|0"),
    ])
    return InlineKeyboardMarkup(rows)


def library_text(state: ChatState) -> str:
    lines = ["<b>Biblioteca</b>"]
    if not state.library:
        lines.append("\nNo hay canciones guardadas todavía.")
        return "\n".join(lines)
    lines.append("")
    for idx, item in enumerate(state.library, start=1):
        track = Track(**item)
        meta: List[str] = []
        if track.performer:
            meta.append(track.performer)
        if track.duration:
            meta.append(fmt_duration(track.duration))
        suffix = f" — {' · '.join(meta)}" if meta else ""
        lines.append(f"{idx}. {track.title}{suffix}")
    return "\n".join(lines)


def library_markup(state: ChatState) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, _ in enumerate(state.library):
        rows.append([
            InlineKeyboardButton(f"▶️ {idx+1}", callback_data=f"lib|p|{idx}"),
            InlineKeyboardButton("➕ Cola", callback_data=f"lib|q|{idx}"),
            InlineKeyboardButton("🗑️", callback_data=f"lib|x|{idx}"),
        ])
    rows.append([InlineKeyboardButton("❌ Cerrar", callback_data="lib|r|0")])
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
            InlineKeyboardButton(f"📂 {name[:24]}", callback_data=f"lst|l|{idx}"),
            InlineKeyboardButton("🗑️", callback_data=f"lst|x|{idx}"),
        ])
    rows.append([InlineKeyboardButton("❌ Cerrar", callback_data="lst|r|0")])
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


async def send_temp_message(bot, chat_id: int, text: str, *, reply_to_message_id: Optional[int] = None,
                            reply_markup: Optional[InlineKeyboardMarkup] = None,
                            ttl: int = 90,
                            parse_mode: str = ParseMode.HTML) -> Optional[int]:
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


async def cleanup_pending_actions(bot, chat_id: int) -> None:
    for key, data in list(PENDING_ACTIONS.items()):
        if not key.startswith(f"{chat_id}:"):
            continue
        prompt_id = data.get("prompt_id")
        if prompt_id:
            await safe_delete(bot, chat_id, prompt_id)
        PENDING_ACTIONS.pop(key, None)


async def cleanup_temp_messages(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    keep = {state.panel_message_id}
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
    text = panel_text(state)
    markup = panel_markup(state)
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
            logger.exception("No se pudo editar el panel; se recreará")

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
    if await is_admin(context, chat_id, user_id):
        return True
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


def extract_track_from_telethon_message(message, chat_id: int) -> Optional[Track]:
    document = getattr(message, "document", None)
    if not document:
        return None

    file_name = ""
    mime_type = getattr(document, "mime_type", "") or ""
    duration = 0
    performer = ""
    title = ""
    is_audio = False

    for attr in getattr(document, "attributes", []) or []:
        name = attr.__class__.__name__
        if name == "DocumentAttributeFilename":
            file_name = getattr(attr, "file_name", "") or file_name
        if name == "DocumentAttributeAudio":
            is_audio = True
            duration = int(getattr(attr, "duration", 0) or 0)
            performer = getattr(attr, "performer", "") or ""
            title = getattr(attr, "title", "") or title

    lower = (file_name or "").lower()
    if not is_audio and not mime_type.startswith("audio/") and not lower.endswith((".mp3", ".m4a", ".ogg", ".wav", ".flac", ".opus")):
        return None

    if not title:
        title = file_name or f"Track {message.id}"

    ext = Path(file_name).suffix if file_name else ""
    if not ext:
        if "mpeg" in mime_type:
            ext = ".mp3"
        elif "mp4" in mime_type or "m4a" in mime_type:
            ext = ".m4a"
        elif "ogg" in mime_type:
            ext = ".ogg"
        elif "wav" in mime_type:
            ext = ".wav"
        else:
            ext = ".bin"

    local_path = str(DOWNLOAD_DIR / f"{chat_id}_scan_{message.id}{ext}")

    return Track(
        title=title,
        performer=performer,
        duration=duration,
        file_id="",
        file_unique_id=f"scanmsg:{message.id}",
        mime_type=mime_type or "audio",
        local_path=local_path,
        original_message_id=int(message.id),
    )


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
        return track
    if not track.file_id:
        raise RuntimeError("La pista no tiene file_id ni archivo local disponible.")
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
    return track


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
        if item.get("local_path") and Path(item["local_path"]).exists():
            keep_paths.add(item["local_path"])
    for path in DOWNLOAD_DIR.glob(f"{chat_id}_*"):
        try:
            if str(path) not in keep_paths and path.is_file():
                path.unlink(missing_ok=True)
        except Exception:
            logger.exception("No se pudo borrar %s", path)


class VoiceEngine:
    def __init__(self) -> None:
        self.client: Optional[TelegramClient] = None
        self.calls: Optional[PyTgCalls] = None
        self.application: Optional[Application] = None

    async def start(self, application: Application) -> None:
        self.application = application
        self.client = TelegramClient(StringSession(USERBOT_SESSION), API_ID, API_HASH)
        await self.client.start()
        self.calls = PyTgCalls(self.client)

        stream_end_registered = False
        stream_end_hook = getattr(self.calls, "on_stream_end", None)
        if callable(stream_end_hook):
            try:
                @stream_end_hook()  # type: ignore[misc]
                async def _on_stream_end(_, update):
                    try:
                        chat_id = int(getattr(update, "chat_id"))
                    except Exception:
                        logger.exception("No se pudo leer chat_id del fin de stream")
                        return
                    logger.info("Stream terminado en %s", chat_id)
                    await self.play_next_from_queue(chat_id)

                stream_end_registered = True
            except Exception:
                logger.exception("No se pudo registrar on_stream_end() en PyTgCalls")

        if not stream_end_registered:
            logger.warning(
                "PyTgCalls instalado sin on_stream_end; el bot arrancará, "
                "pero el autoplay al terminar la pista queda desactivado."
            )

        await self.calls.start()
        logger.info("Userbot + voice engine iniciados")

    async def stop(self) -> None:
        if self.client:
            await self.client.disconnect()
            self.client = None
        self.calls = None

    async def scan_group_for_tracks(self, chat_id: int, limit: int = SCAN_LIMIT) -> tuple[int, int]:
        if not self.client:
            raise RuntimeError("Userbot no iniciado")
        state = get_state(chat_id)
        added = 0
        scanned = 0
        existing_keys = {item.get("file_unique_id") or item.get("file_id") for item in state.library}

        async for msg in self.client.iter_messages(chat_id, limit=limit):
            track = extract_track_from_telethon_message(msg, chat_id)
            if not track:
                continue
            scanned += 1
            key = track.file_unique_id or track.file_id or f"scanmsg:{msg.id}"
            if key in existing_keys:
                continue
            path = Path(track.local_path)
            if not path.exists():
                try:
                    await msg.download_media(file=str(path))
                except Exception:
                    logger.exception("No se pudo descargar media del mensaje %s", msg.id)
                    continue
            state.library.append(asdict(track))
            existing_keys.add(key)
            added += 1

        save_all_states()
        return scanned, added

    async def play_file(self, chat_id: int, file_path: str) -> None:
        if not self.calls:
            raise RuntimeError("Voice engine no iniciado")
        await self.calls.play(chat_id, file_path)

    async def play_track(self, bot, chat_id: int, track: Track) -> None:
        state = get_state(chat_id)
        if not track.local_path or not Path(track.local_path).exists():
            track = await materialize_track(bot, chat_id, track)
        await self.play_file(chat_id, track.local_path)
        state.now_playing = asdict(track)
        state.paused = False
        save_all_states()
        await ensure_panel(bot, chat_id)
        await cleanup_old_files(chat_id)

    async def play_next_from_queue(self, chat_id: int) -> None:
        state = get_state(chat_id)
        bot = self.application.bot if self.application else None
        if not bot:
            return
        if not state.queue:
            state.now_playing = None
            state.paused = False
            save_all_states()
            await ensure_panel(bot, chat_id)
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
        state = get_state(chat_id)
        if not self.calls:
            raise RuntimeError("Voice engine no iniciado")
        if state.paused:
            await self.calls.resume_stream(chat_id)
            state.paused = False
        else:
            await self.calls.pause_stream(chat_id)
            state.paused = True
        save_all_states()
        if self.application:
            await ensure_panel(self.application.bot, chat_id)

    async def leave(self, chat_id: int) -> None:
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
        text=(
            "<b>DJ-PLAN</b>\n"
            "Panel DJ, cola, biblioteca y reproducción en voice.\n\n"
            f"La búsqueda se hace con <code>{SEARCH_TRIGGER}</code>.\n"
            "Cuando aparezca una canción descargada en el chat, DJ-PLAN la detectará y pondrá botones debajo."
        ),
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
    await send_temp_message(context.bot, update.effective_chat.id, f"✅ DJ asignado: <b>{state.assigned_dj_name}</b>")


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
            await send_temp_message(context.bot, update.effective_chat.id, f"💾 Lista guardada: <b>{text}</b>")
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
            if not state.dj_mode:
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


async def disable_dj_mode(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    await cleanup_temp_messages(bot, chat_id)
    await cleanup_track_controls(bot, chat_id)
    await cleanup_pending_actions(bot, chat_id)
    await VOICE.leave(chat_id)
    state.dj_mode = False
    state.assigned_dj_id = None
    state.assigned_dj_name = ""
    state.paused = False
    state.now_playing = None
    state.queue = []
    state.history = []
    save_all_states()
    await ensure_panel(bot, chat_id)
    await cleanup_old_files(chat_id)


async def close_dj_session(bot, chat_id: int) -> None:
    state = get_state(chat_id)
    await cleanup_temp_messages(bot, chat_id)
    await cleanup_track_controls(bot, chat_id)
    await cleanup_pending_actions(bot, chat_id)
    await VOICE.leave(chat_id)
    if state.panel_message_id:
        try:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=state.panel_message_id)
        except Exception:
            pass
        await safe_delete(bot, chat_id, state.panel_message_id)
    TRACK_REGISTRY.pop(chat_id, None)
    TRACK_CONTROL_REGISTRY.pop(chat_id, None)
    for key in list(PENDING_ACTIONS.keys()):
        if key.startswith(f"{chat_id}:"):
            PENDING_ACTIONS.pop(key, None)
    STATE_CACHE[chat_id] = ChatState()
    save_all_states()
    await cleanup_old_files(chat_id)


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
        await ensure_panel(context.bot, chat_id)
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if data in {"menu_search_help", "panel_search_help"}:
        await send_temp_message(
            context.bot,
            chat_id,
            (
                "<b>Búsqueda externa</b>\n"
                f"La búsqueda se hace con tu otro bot de música.\n\n"
                f"Escribe en el chat: <code>{SEARCH_TRIGGER}nombre de la canción</code>\n\n"
                "Cuando aparezca una canción descargada en el grupo, DJ-PLAN pondrá debajo:\n"
                "• ▶️ Voice ahora\n"
                "• ➕ Cola\n"
                "• 📚 Biblioteca"
            ),
        )
        return

    if data == "panel_voice_info":
        await query.answer("Entra al voice desde la cabecera del grupo o configura VOICE_CHAT_LINK.", show_alert=True)
        return

    if data == "panel_take_dj":
        if not await is_admin(context, chat_id, user_id):
            await query.answer("Solo un admin puede asignarse DJ.", show_alert=True)
            return
        state.assigned_dj_id = user_id
        state.assigned_dj_name = display_name(update.effective_user)
        state.dj_mode = True
        save_all_states()
        await ensure_panel(context.bot, chat_id)
        return

    if data == "panel_toggle_dj":
        if not await is_admin(context, chat_id, user_id):
            await query.answer("Solo un admin puede cambiar el modo DJ.", show_alert=True)
            return
        if state.dj_mode:
            await disable_dj_mode(context.bot, chat_id)
        else:
            state.dj_mode = True
            save_all_states()
            await ensure_panel(context.bot, chat_id)
        return

    if data == "panel_refresh":
        await ensure_panel(context.bot, chat_id)
        await cleanup_temp_messages(context.bot, chat_id)
        await cleanup_track_controls(context.bot, chat_id)
        await cleanup_pending_actions(context.bot, chat_id)
        await cleanup_old_files(chat_id)
        return

    if data == "panel_close":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ o un admin puede cerrar.", show_alert=True)
            return
        await close_dj_session(context.bot, chat_id)
        return

    if data == "panel_scan_group":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ o un admin puede rastrear el grupo.", show_alert=True)
            return
        info_id = await send_temp_message(context.bot, chat_id, "🧭 Rastreando canciones del grupo...", ttl=120)
        try:
            scanned, added = await VOICE.scan_group_for_tracks(chat_id, limit=SCAN_LIMIT)
            await ensure_panel(context.bot, chat_id)
            await send_temp_message(
                context.bot,
                chat_id,
                f"🧭 Rastreo completado. Revisadas: <b>{scanned}</b> · Añadidas a biblioteca: <b>{added}</b>",
                ttl=25,
            )
        except Exception:
            logger.exception("Fallo en rastreo del grupo")
            await send_temp_message(context.bot, chat_id, "❌ No se pudo rastrear el grupo.", ttl=20)
        finally:
            if info_id:
                await safe_delete(context.bot, chat_id, info_id)
        return

    if data == "panel_pause_resume":
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede controlar el directo.", show_alert=True)
            return
        if not state.now_playing:
            await query.answer("No hay ninguna canción sonando.", show_alert=True)
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
        await VOICE.play_next_from_queue(chat_id)
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

    if data == "panel_queue":
        msg_id = await send_temp_message(
            context.bot,
            chat_id,
            queue_text(state),
            reply_markup=queue_markup(state),
            ttl=1800,
        )
        if msg_id:
            await register_temp_message(chat_id, msg_id)
        return

    if data == "panel_library":
        msg_id = await send_temp_message(
            context.bot,
            chat_id,
            library_text(state),
            reply_markup=library_markup(state),
            ttl=1800,
        )
        if msg_id:
            await register_temp_message(chat_id, msg_id)
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

    if data == "panel_load_lists":
        msg_id = await send_temp_message(
            context.bot,
            chat_id,
            saved_lists_text(state),
            reply_markup=saved_lists_markup(state),
            ttl=1800,
        )
        if msg_id:
            await register_temp_message(chat_id, msg_id)
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
            await send_temp_message(context.bot, chat_id, f"➕ Añadida a cola: <b>{track.title}</b>", ttl=20)
        elif action == "l":
            added = await add_to_library(chat_id, track)
            txt = f"📚 Guardada en biblioteca: <b>{track.title}</b>" if added else "ℹ️ Esa canción ya estaba en la biblioteca."
            await ensure_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, txt, ttl=20)
        if control_message_id:
            await safe_delete(context.bot, chat_id, control_message_id)
        return

    if data.startswith("q|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede tocar la cola.", show_alert=True)
            return
        _, action, idx_str = data.split("|")
        idx = int(idx_str)
        if action == "r":
            await safe_delete(context.bot, chat_id, query.message.message_id)
            await ensure_panel(context.bot, chat_id)
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
        try:
            await query.message.edit_text(queue_text(state), reply_markup=queue_markup(state), parse_mode=ParseMode.HTML)
        except BadRequest:
            pass
        return

    if data.startswith("lib|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede tocar la biblioteca.", show_alert=True)
            return
        _, action, idx_str = data.split("|")
        idx = int(idx_str)
        if action == "r":
            await safe_delete(context.bot, chat_id, query.message.message_id)
            await ensure_panel(context.bot, chat_id)
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
        try:
            await query.message.edit_text(library_text(state), reply_markup=library_markup(state), parse_mode=ParseMode.HTML)
        except BadRequest:
            pass
        return

    if data.startswith("lst|"):
        if not await is_controller(context, chat_id, user_id):
            await query.answer("Solo el DJ asignado puede cargar listas.", show_alert=True)
            return
        _, action, idx_str = data.split("|")
        idx = int(idx_str)
        names = sorted(state.saved_lists.keys())
        if action == "r":
            await safe_delete(context.bot, chat_id, query.message.message_id)
            await ensure_panel(context.bot, chat_id)
            return
        if not (0 <= idx < len(names)):
            return
        name = names[idx]
        if action == "l":
            state.queue = [dict(item) for item in state.saved_lists.get(name, [])]
            save_all_states()
            await ensure_panel(context.bot, chat_id)
            await send_temp_message(context.bot, chat_id, f"📂 Lista cargada: <b>{name}</b>", ttl=20)
        elif action == "x":
            state.saved_lists.pop(name, None)
            save_all_states()
        try:
            await query.message.edit_text(saved_lists_text(state), reply_markup=saved_lists_markup(state), parse_mode=ParseMode.HTML)
        except BadRequest:
            pass
        return


async def on_startup(application: Application) -> None:
    load_all_states()
    await VOICE.start(application)
    logger.info("DJ-PLAN iniciado")


async def on_shutdown(application: Application) -> None:
    await VOICE.stop()


def build_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).post_init(on_startup).post_shutdown(on_shutdown).build()
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
