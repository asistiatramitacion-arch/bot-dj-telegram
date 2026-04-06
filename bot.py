from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
import os
import asyncio

TOKEN = os.getenv("8718621245:AAFXJf1RhZucLVs9yOJIguFwbMEf3C_rkXU")
ADMIN_ID = 37633897

estado_usuario = {}
modo_dj = False
cola = []
mensaje_id = None
dj_user_id = None

# =========================
# 🔐 PERMISOS
# =========================
def es_dj(user_id):
    return user_id == ADMIN_ID or user_id == dj_user_id

# =========================
# 🧼 BORRADO AUTOMÁTICO
# =========================
async def borrar_despues(msg, segundos=5):
    await asyncio.sleep(segundos)
    try:
        await msg.delete()
    except:
        pass

# =========================
# 🎛 MENÚ
# =========================
async def mostrar_menu(query):
    estado = "🟢 ON" if modo_dj else "🔴 OFF"

    keyboard = [
        [InlineKeyboardButton("🎧 Buscar música", callback_data="musica")],
        [InlineKeyboardButton(f"🔴 DJ ({estado})", callback_data="panel")],
        [InlineKeyboardButton("❌ Cerrar", callback_data="cerrar")]
    ]

    await query.edit_message_text(
        "⭐ *MENÚ EL PLAN* ⭐",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# =========================
# 🎧 BUSCAR
# =========================
async def buscar(query):
    estado_usuario[query.from_user.id] = True

    msg = await query.message.reply_text(
        "🎧 Escribe la canción, playlist o artista.\n\nEjemplo:\n/search canción"
    )

    asyncio.create_task(borrar_despues(msg, 10))

# =========================
# 🎛 DJ PANEL
# =========================
async def panel_dj(query):
    if query.from_user.id != ADMIN_ID:
        return

    keyboard = [
        [InlineKeyboardButton("🟢 Activar DJ", callback_data="activar_dj")],
        [InlineKeyboardButton("🔴 Desactivar DJ", callback_data="desactivar_dj")],
        [InlineKeyboardButton("🔙 Volver", callback_data="menu")]
    ]

    await query.edit_message_text("🎛 PANEL DJ", reply_markup=InlineKeyboardMarkup(keyboard))

# =========================
# 📌 DJ PLAN
# =========================
async def actualizar_dj(context, chat_id):
    global mensaje_id

    texto = "🎧 DJ-PLAN 🎧\n\n"

    if cola:
        for i, c in enumerate(cola[:5]):
            texto += f"{i+1}. {c[0]}\n"
    else:
        texto += "📀 Cola vacía"

    if mensaje_id:
        try:
            await context.bot.edit_message_text(chat_id, mensaje_id, texto)
            return
        except:
            mensaje_id = None

    msg = await context.bot.send_message(chat_id, texto)
    mensaje_id = msg.message_id
    await context.bot.pin_chat_message(chat_id, mensaje_id)

# =========================
# 🔘 BOTONES
# =========================
async def botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global modo_dj, cola, mensaje_id

    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat.id
    user_id = query.from_user.id

    if query.data == "menu":
        await mostrar_menu(query)

    elif query.data == "musica":
        await buscar(query)

    elif query.data == "panel":
        await panel_dj(query)

    elif query.data == "activar_dj":
        modo_dj = True
        await actualizar_dj(context, chat_id)

    elif query.data == "desactivar_dj":
        modo_dj = False
        cola.clear()

        try:
            await context.bot.unpin_chat_message(chat_id, mensaje_id)
            await context.bot.delete_message(chat_id, mensaje_id)
        except:
            pass

    elif query.data.startswith("add_"):
        if not es_dj(user_id):
            return

        index = int(query.data.split("_")[1])
        cola.append(cola[index])

        msg = await query.message.reply_text("✅ Añadido a la cola")
        asyncio.create_task(borrar_despues(msg))

        await actualizar_dj(context, chat_id)

# =========================
# 🎧 MENSAJES
# =========================
async def mensajes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    chat_id = msg.chat.id

    # BUSCAR
    if msg.from_user.id in estado_usuario:
        texto = msg.text.replace("/search", "").strip()

        comando = f"/search@VoiceShazamBot {texto}"

        enviado = await msg.reply_text(comando)

        asyncio.create_task(borrar_despues(enviado))
        asyncio.create_task(borrar_despues(msg))

        del estado_usuario[msg.from_user.id]

    # DETECTAR AUDIO
    if modo_dj and msg.audio:
        nombre = msg.audio.title or "Canción"
        cola.append((nombre, msg.audio.file_id))

        if es_dj(msg.from_user.id):
            keyboard = [[InlineKeyboardButton("➕ Añadir a cola", callback_data=f"add_{len(cola)-1}")]]
            await msg.reply_text("🎧 Canción detectada", reply_markup=InlineKeyboardMarkup(keyboard))

# =========================
# 🚀 START
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("🚀 Abrir menú", callback_data="menu")]]

    await update.message.reply_text(
        "⭐ *MENÚ EL PLAN* ⭐",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# =========================
# 🚀 APP
# =========================
app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(botones))
app.add_handler(MessageHandler(filters.ALL, mensajes))

print("Bot iniciado...")
app.run_polling()
