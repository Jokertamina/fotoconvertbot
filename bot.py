# pyright: reportMissingImports=false
import os, io, logging
from typing import Tuple
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()  # Carga variables desde .env (BOT_TOKEN, etc.)

# PIL / Pillow
try:
    from PIL import Image, ImageOps  # type: ignore[import-not-found]
except ImportError as e:
    raise SystemExit("Pillow no instalado. Ejecuta:  pip install Pillow") from e

from telegram import Update
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    ContextTypes, filters
)

# -------- Config --------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Falta BOT_TOKEN en tu .env")

TARGET_KB = int(os.getenv("TARGET_KB", "200"))     # tamaño objetivo por imagen (KB)
MAX_DIM = int(os.getenv("MAX_DIMENSION", "1920"))  # lado largo máximo (px)
MIN_Q, MAX_Q = 30, 90                              # rango de calidad WebP
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))         # opcional: tu user_id

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO
)
log = logging.getLogger("webp-bot")

# Marca de tiempo del arranque (para ignorar mensajes antiguos)
START_TIME = datetime.now(timezone.utc)

# Stats en memoria
stats = {"processed": 0, "saved_bytes": 0}

# -------- Utilidades --------
async def is_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Permite cambiar ajustes solo a admins o ADMIN_ID (si se define)."""
    user = update.effective_user
    chat = update.effective_chat

    if ADMIN_ID and user and user.id == ADMIN_ID:
        return True

    try:
        if chat and user:
            member = await context.bot.get_chat_member(chat.id, user.id)
            return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False
    return False


def to_webp_optimized(img_bytes: bytes, target_kb: int, max_dim: int) -> Tuple[io.BytesIO, int]:
    """Convierte a WebP buscando una calidad que se acerque al tamaño objetivo."""
    im = Image.open(io.BytesIO(img_bytes))
    im = ImageOps.exif_transpose(im)  # corrige orientación

    # Conservar transparencia si existe; si no, RGB
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")

    # Redimensionar si excede el máximo
    w, h = im.size
    m = max(w, h)
    if m > max_dim:
        scale = max_dim / float(m)
        im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    # Búsqueda binaria de calidad
    low, high = MIN_Q, MAX_Q
    best_buf, best_q = None, MIN_Q

    for _ in range(10):
        q = (low + high) // 2
        buf = io.BytesIO()
        im.save(buf, format="WEBP", quality=q, method=6, optimize=True)
        size_kb = buf.tell() / 1024
        if size_kb <= target_kb:
            best_buf, best_q = buf, q
            low = q + 1
        else:
            high = q - 1

    if best_buf is None:
        best_buf = io.BytesIO()
        im.save(best_buf, format="WEBP", quality=MIN_Q, method=6, optimize=True)
        best_q = MIN_Q

    best_buf.seek(0)
    return best_buf, best_q


# -------- Handlers --------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "👋 Listo. Envía una foto o documento de imagen y la devuelvo en WebP optimizado.\n"
        f"Objetivo: ~{TARGET_KB} KB | Máx. {MAX_DIM}px\n"
        "Comandos: /help /settarget /setmaxdim /stats"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "/settarget <KB> — cambia tamaño objetivo (ej: /settarget 180)\n"
        "/setmaxdim <px> — cambia lado largo máximo (ej: /setmaxdim 1920)\n"
        "/stats — imágenes procesadas y ahorro acumulado."
    )


async def settarget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized(update, context):
        return await update.effective_message.reply_text("Solo admins pueden cambiar ajustes.")
    global TARGET_KB
    try:
        kb = int(context.args[0])
        if kb < 50 or kb > 2000:
            raise ValueError
        TARGET_KB = kb
        await update.effective_message.reply_text(f"TARGET_KB = {TARGET_KB} KB ✅")
    except Exception:
        await update.effective_message.reply_text("Uso: /settarget 50..2000")


async def setmaxdim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized(update, context):
        return await update.effective_message.reply_text("Solo admins pueden cambiar ajustes.")
    global MAX_DIM
    try:
        px = int(context.args[0])
        if px < 256 or px > 8192:
            raise ValueError
        MAX_DIM = px
        await update.effective_message.reply_text(f"MAX_DIMENSION = {MAX_DIM}px ✅")
    except Exception:
        await update.effective_message.reply_text("Uso: /setmaxdim 256..8192")


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized(update, context):
        return await update.effective_message.reply_text("Solo admins pueden ver /stats.")
    saved_mb = stats["saved_bytes"] / (1024 * 1024) if stats["saved_bytes"] else 0
    await update.effective_message.reply_text(
        f"📊 Procesadas: {stats['processed']}\n"
        f"💾 Ahorro acumulado: ~{saved_mb:.2f} MB"
    )


async def handle_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message

    # --- Evita procesar mensajes antiguos (anteriores al arranque) ---
    if msg and msg.date and msg.date < START_TIME:
        return

    try:
        file = None
        orig_name = None

        if msg.photo:
            file = await msg.photo[-1].get_file()
            orig_name = f"photo_{file.file_unique_id}.jpg"
        elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"):
            file = await msg.document.get_file()
            orig_name = msg.document.file_name or f"image_{file.file_unique_id}"
        else:
            return  # no es imagen

        src = io.BytesIO()
        await file.download_to_memory(out=src)
        before = src.tell()

        buf, used_q = to_webp_optimized(src.getvalue(), TARGET_KB, MAX_DIM)
        after = len(buf.getbuffer())

        stats["processed"] += 1
        if before > after:
            stats["saved_bytes"] += (before - after)

        base, _ = os.path.splitext(orig_name)
        await msg.reply_document(
            document=buf,
            filename=f"{base}.webp",
            caption=f"WebP ✅ ~{after/1024:.0f} KB (antes ~{before/1024:.0f} KB) | q={used_q} | {MAX_DIM}px máx."
        )
    except Exception as e:
        log.exception("Error procesando imagen")
        await msg.reply_text(f"Ups, no pude convertir esta imagen: {e}")


def main():
    # Timeouts más amplios y polling sin backlog
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(60)
        .write_timeout(60)
        .pool_timeout(30)
        .get_updates_read_timeout(70)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("settarget", settarget))
    app.add_handler(CommandHandler("setmaxdim", setmaxdim))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, handle_image))

    log.info("Bot iniciado. Esperando imágenes…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
