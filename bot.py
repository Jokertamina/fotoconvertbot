# pyright: reportMissingImports=false
import os, io, logging, json, time, asyncio, uuid
from dataclasses import dataclass, field
from typing import Tuple, List, Dict, Optional
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# Pillow
try:
    from PIL import Image, ImageOps  # type: ignore[import-not-found]
except ImportError as e:
    raise SystemExit("Pillow no instalado. pip install Pillow") from e

# Telegram
from telegram import Update
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    ContextTypes, filters
)

# HTTP async
import aiohttp

# Firebase Storage (GCS)
from google.oauth2 import service_account
from google.cloud import storage as gcs

# -------- Config --------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Falta BOT_TOKEN")

TARGET_KB = int(os.getenv("TARGET_KB", "200"))
MAX_DIM = int(os.getenv("MAX_DIMENSION", "1920"))
MIN_Q, MAX_Q = 30, 90
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
ALLOWED_CHAT_ID = int(os.getenv("ALLOWED_CHAT_ID", "0"))  # grupo permitido (opcional)
ALBUM_TTL_SEC = float(os.getenv("ALBUM_TTL_SEC", "4.0"))

# Endpoint de tu web
API_DRAFTS_IMPORT_URL = os.getenv("API_DRAFTS_IMPORT_URL", "https://www.morrinashop.com/api/drafts/import")
X_INGEST_TOKEN = os.getenv("X_INGEST_TOKEN")  # mismo que INGEST_TOKEN en la web
if not X_INGEST_TOKEN:
    raise SystemExit("Falta X_INGEST_TOKEN (mismo valor que INGEST_TOKEN en la web)")

# Firebase/GCS
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET")  # p.ej. morrinha.appspot.com
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
if not (FIREBASE_PROJECT_ID and FIREBASE_STORAGE_BUCKET and SERVICE_ACCOUNT_JSON):
    raise SystemExit("Faltan FIREBASE_PROJECT_ID, FIREBASE_STORAGE_BUCKET o SERVICE_ACCOUNT_JSON")

creds = service_account.Credentials.from_service_account_info(json.loads(SERVICE_ACCOUNT_JSON))
gcs_client = gcs.Client(project=FIREBASE_PROJECT_ID, credentials=creds)
bucket = gcs_client.bucket(FIREBASE_STORAGE_BUCKET)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO
)
log = logging.getLogger("ingest-bot")

START_TIME = datetime.now(timezone.utc)
stats = {"processed": 0, "saved_bytes": 0}

# -------- Utilidades --------
async def is_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
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
    im = Image.open(io.BytesIO(img_bytes))
    im = ImageOps.exif_transpose(im)
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")
    w, h = im.size
    m = max(w, h)
    if m > max_dim:
        scale = max_dim / float(m)
        im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
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

# -------- Álbumes (agrupación por media_group_id) --------
@dataclass
class AlbumBuffer:
    media_group_id: str
    chat_id: int
    caption: str = ""
    message_ids: List[int] = field(default_factory=list)
    items: List[bytes] = field(default_factory=list)  # webp bytes
    last_update: float = field(default_factory=lambda: time.time())

ALBUMS: Dict[str, AlbumBuffer] = {}

async def upload_webp_bytes(webp_bytes: bytes, path: str) -> None:
    # Ejecutar subida GCS en hilo para no bloquear
    def _upload():
        blob = bucket.blob(path)
        blob.cache_control = "public, max-age=31536000, immutable"
        blob.upload_from_file(io.BytesIO(webp_bytes), content_type="image/webp")
    await asyncio.to_thread(_upload)

async def finalize_and_send(draft_id: str, album: AlbumBuffer, reply_target):
    # Subir a Storage
    images = []
    for idx, data in enumerate(album.items):
        storage_path = f"drafts/{draft_id}/images/{idx:02d}.webp"
        await upload_webp_bytes(data, storage_path)
        images.append({"storagePath": storage_path, "index": idx})

    # POST a tu web
    payload = {
        "draftId": draft_id,
        "caption": album.caption or "",
        "chat_id": album.chat_id,
        "media_group_id": None if album.media_group_id.startswith("single_") else album.media_group_id,
        "images": images
    }
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
        async with session.post(
            API_DRAFTS_IMPORT_URL,
            headers={"X-Ingest-Token": X_INGEST_TOKEN, "Content-Type": "application/json"},
            json=payload
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"ingest HTTP {resp.status}: {text}")
            data = await resp.json()
            draft_id_resp = data.get("draftId", draft_id)
            slug_suggested = data.get("slugSuggested")

    # Responder en Telegram
    await reply_target.reply_text(f"📝 Borrador creado: {draft_id_resp}\nSlug sugerido: {slug_suggested or '—'}\nRevisar en /admin/productos/borradores")

# -------- Handlers --------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "👋 Listo. Envía un álbum con caption (Nombre, Descripción, Tallas, Precio, Categoría).\n"
        f"Optimizo a WebP ~{TARGET_KB}KB, máx {MAX_DIM}px y creo un borrador en la web."
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "/settarget <KB>\n"
        "/setmaxdim <px>\n"
        "/stats"
    )

async def settarget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized(update, context):
        return await update.effective_message.reply_text("Solo admins.")
    global TARGET_KB
    try:
        kb = int(context.args[0]); assert 50 <= kb <= 2000
        TARGET_KB = kb
        await update.effective_message.reply_text(f"TARGET_KB = {TARGET_KB} KB ✅")
    except Exception:
        await update.effective_message.reply_text("Uso: /settarget 50..2000")

async def setmaxdim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized(update, context):
        return await update.effective_message.reply_text("Solo admins.")
    global MAX_DIM
    try:
        px = int(context.args[0]); assert 256 <= px <= 8192
        MAX_DIM = px
        await update.effective_message.reply_text(f"MAX_DIMENSION = {MAX_DIM}px ✅")
    except Exception:
        await update.effective_message.reply_text("Uso: /setmaxdim 256..8192")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_authorized(update, context):
        return await update.effective_message.reply_text("Solo admins.")
    saved_mb = stats["saved_bytes"] / (1024 * 1024) if stats["saved_bytes"] else 0
    await update.effective_message.reply_text(
        f"📊 Procesadas: {stats['processed']}\n"
        f"💾 Ahorro: ~{saved_mb:.2f} MB"
    )

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or (msg.date and msg.date < START_TIME):
        return
    if ALLOWED_CHAT_ID and (not chat or chat.id != ALLOWED_CHAT_ID):
        return  # ignora otros chats

    # Grupo de álbum (o single)
    mgid = msg.media_group_id or f"single_{msg.message_id}"
    album = ALBUMS.get(mgid)
    if not album:
        album = ALBUMS[mgid] = AlbumBuffer(media_group_id=mgid, chat_id=chat.id)
    album.last_update = time.time()
    album.message_ids.append(msg.message_id)
    if msg.caption and not album.caption:
        album.caption = msg.caption

    # Descargar original
    file = None; orig_name = None
    if msg.photo:
        file = await msg.photo[-1].get_file()
        orig_name = f"photo_{file.file_unique_id}.jpg"
    elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"):
        file = await msg.document.get_file()
        orig_name = msg.document.file_name or f"image_{file.file_unique_id}"
    else:
        return

    src = io.BytesIO()
    await file.download_to_memory(out=src)
    before = src.tell()

    webp_buf, used_q = to_webp_optimized(src.getvalue(), TARGET_KB, MAX_DIM)
    after = len(webp_buf.getbuffer())
    stats["processed"] += 1
    if before > after:
        stats["saved_bytes"] += (before - after)

    album.items.append(webp_buf.getvalue())

    # Debounce: consolida tras TTL si no llegan más partes
    async def _debounce_finalize():
        await asyncio.sleep(ALBUM_TTL_SEC)
        # si no se ha actualizado dentro del TTL, consolidamos
        if mgid in ALBUMS and (time.time() - ALBUMS[mgid].last_update) >= (ALBUM_TTL_SEC - 0.1):
            try:
                draft_id = str(uuid.uuid4())
                await finalize_and_send(draft_id, ALBUMS[mgid], msg)
            except Exception as e:
                log.exception("Error finalizando álbum")
                await msg.reply_text(f"❌ Error creando borrador: {e}")
            finally:
                ALBUMS.pop(mgid, None)

    context.application.create_task(_debounce_finalize())

def main():
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
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, handle_media))

    log.info("Bot iniciado. Esperando álbumes…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
