from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
import os

TOKEN = os.getenv("TOKEN") or "8718621245:AAFXJf1RhZucLVs9yOJIguFwbMEf3C_rkXU"
ADMIN_ID = 376338797

estado_usuario = {}
modo_dj = False
cola = []
cancion_actual = None
audio_actual = None
mensaje_id = None
dj_user_id = None

# 🔐 PERMISOS
def es_dj(user_id):
    return user_id == ADMIN_ID or user_id == dj_user_id

# =========================
# 🎛 MENÚ
# =========================
async def mostrar_menu(query):
    estado = "🟢 ON" if modo_dj else "🔴 OFF"

    keyboard = [
        [InlineKeyboardButton("🎧 Buscar música", callback_data="musica")],
        [InlineKeyboardButton(f"🔴 En directo ({estado})", callback_data="directo")],
        [InlineKeyboardButton("💌 Confesiones", url="https://t.me/MiKatBot?start=confe_-1003699439553")],
        [InlineKeyboardButton("❌ Cerrar", callback_data="cerrar")]
    ]

    if query.from_user.id == ADMIN_ID:
        keyboard.insert(2, [InlineKeyboardButton("🎛 Panel DJ", callback_data="panel")])

    await query.edit_message_text("⭐ *MENÚ EL PLAN* ⭐", parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard))

# =========================
# 🎧 BUSCAR
# =========================
async def menu_musica(query):
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data="menu")]]
    await query.edit_message_text("🎧 Escribe la canción 👇", reply_markup=InlineKeyboardMarkup(keyboard))

# =========================
# 🎛 PANEL DJ
# =========================
async def panel_dj(query):
    keyboard = [
        [InlineKeyboardButton("🟢 Activar DJ", callback_data="activar_dj")],
        [InlineKeyboardButton("🔴 Desactivar DJ", callback_data="desactivar_dj")],
        [InlineKeyboardButton("👤 Asignar DJ (/dj @usuario)", callback_data="info")],
        [InlineKeyboardButton("🔙 Volver", callback_data="menu")]
    ]
    await query.edit_message_text("🎛 PANEL DJ", reply_markup=InlineKeyboardMarkup(keyboard))

# =========================
# 📌 PANEL FIJO
# =========================
async def panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    keyboard = [[InlineKeyboardButton("🚀 Abrir menú", callback_data="menu")]]
    msg = await update.message.reply_text("🎛 PANEL PRINCIPAL\n\nPulsa 👇", reply_markup=InlineKeyboardMarkup(keyboard))

    await context.bot.pin_chat_message(update.effective_chat.id, msg.message_id, disable_notification=True)

# =========================
# 📌 DJ PLAN
# =========================
async def actualizar_directo(context, chat_id):
    global mensaje_id

    estado = "🟢 ON" if modo_dj else "🔴 OFF"

    texto = f"🎧 DJ-PLAN 🎧\n\n🔴 Estado: {estado}\n\n"
    texto += f"🎵 {cancion_actual or 'Sin canción'}\n\n"

    if cola:
        texto += "📀 COLA:\n"
        for i, c in enumerate(cola[:5]):
            texto += f"{i+1}. {c[0]}\n"
    else:
        texto += "📀 Cola vacía"

    keyboard = [
        [InlineKeyboardButton("▶️ Escuchar", callback_data="play_audio")],
        [InlineKeyboardButton("📀 Ver lista", callback_data="ver_cola")],
        [InlineKeyboardButton("🎧 Buscar música", callback_data="musica")]
    ]

    if mensaje_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=mensaje_id,
                text=texto, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        except:
            mensaje_id = None

    msg = await context.bot.send_message(chat_id, texto, reply_markup=InlineKeyboardMarkup(keyboard))
    mensaje_id = msg.message_id
    await context.bot.pin_chat_message(chat_id, mensaje_id, disable_notification=True)

# =========================
# 🧹 LIMPIAR DJ
# =========================
async def limpiar_dj(context, chat_id):
    global mensaje_id, cola, cancion_actual, audio_actual

    try:
        if mensaje_id:
            await context.bot.unpin_chat_message(chat_id, mensaje_id)
            await context.bot.delete_message(chat_id, mensaje_id)
    except:
        pass

    mensaje_id = None
    cola.clear()
    cancion_actual = None
    audio_actual = None

# =========================
# 📀 LISTA
# =========================
def generar_lista():
    botones = []
    for i, c in enumerate(cola):
        botones.append([
            InlineKeyboardButton(f"▶️ {i+1}", callback_data=f"play_{i}"),
            InlineKeyboardButton("⬆️", callback_data=f"up_{i}"),
            InlineKeyboardButton("⬇️", callback_data=f"down_{i}"),
            InlineKeyboardButton("❌", callback_data=f"del_{i}")
        ])
    botones.append([InlineKeyboardButton("🔙 Volver", callback_data="menu")])
    return botones

# =========================
# 🔘 BOTONES
# =========================
async def botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global modo_dj, cola, cancion_actual, audio_actual, dj_user_id

    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    chat_id = query.message.chat.id
    data = query.data

    if data == "menu":
        await mostrar_menu(query)

    elif data == "musica":
        if not es_dj(user_id): return
        estado_usuario[user_id] = True
        await menu_musica(query)

    elif data == "directo":
        if audio_actual:
            await context.bot.send_audio(chat_id, audio=audio_actual)

    elif data == "panel":
        if user_id == ADMIN_ID:
            await panel_dj(query)

    elif data == "cerrar":
        await query.message.delete()

    elif data == "activar_dj":
        modo_dj = True
        await actualizar_directo(context, chat_id)
        await mostrar_menu(query)

    elif data == "desactivar_dj":
        modo_dj = False
        dj_user_id = None
        await limpiar_dj(context, chat_id)
        await mostrar_menu(query)

    elif data == "play_audio":
        if audio_actual:
            await context.bot.send_audio(chat_id, audio=audio_actual)

    elif data == "ver_cola":
        if not es_dj(user_id): return
        await query.edit_message_text("📀 LISTA:", reply_markup=InlineKeyboardMarkup(generar_lista()))

    elif data.startswith("play_"):
        i = int(data.split("_")[1])
        cancion_actual, audio_actual = cola[i]
        await context.bot.send_audio(chat_id, audio=audio_actual)

    elif data.startswith("del_"):
        i = int(data.split("_")[1])
        cola.pop(i)
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(generar_lista()))
        await actualizar_directo(context, chat_id)

    elif data.startswith("up_"):
        i = int(data.split("_")[1])
        if i > 0:
            cola[i], cola[i-1] = cola[i-1], cola[i]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(generar_lista()))

    elif data.startswith("down_"):
        i = int(data.split("_")[1])
        if i < len(cola)-1:
            cola[i], cola[i+1] = cola[i+1], cola[i]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(generar_lista()))

# =========================
# 👤 DJ
# =========================
async def dj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global dj_user_id

    if update.effective_user.id != ADMIN_ID:
        return

    username = context.args[0].replace("@", "")
    admins = await context.bot.get_chat_administrators(update.effective_chat.id)

    for m in admins:
        if m.user.username == username:
            dj_user_id = m.user.id
            await update.message.reply_text(f"🎧 DJ: @{username}")
            return

# =========================
# 🎧 AUDIO
# =========================
async def mensajes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global cancion_actual, audio_actual

    msg = update.message
    chat_id = msg.chat.id

    if modo_dj and msg.audio:
        cancion_actual = msg.audio.title or "Canción"
        audio_actual = msg.audio.file_id
        cola.append((cancion_actual, audio_actual))
        await actualizar_directo(context, chat_id)

    elif msg.from_user.id in estado_usuario:
        await msg.reply_text("/search " + msg.text)
        del estado_usuario[msg.from_user.id]

# =========================
# 🚀 START
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("🚀 Abrir menú", callback_data="menu")]]
    await update.message.reply_text("⭐ *MENÚ EL PLAN* ⭐", parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard))

# =========================
# 🚀 APP
# =========================
app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("panel", panel))
app.add_handler(CommandHandler("dj", dj))

app.add_handler(CallbackQueryHandler(botones))
app.add_handler(MessageHandler(filters.ALL, mensajes))

print("Bot iniciado...")
app.run_polling()