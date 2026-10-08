import asyncio
import logging
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import asyncpg
from bs4 import BeautifulSoup
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ButtonStyle
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)

# =========================================================
# CONFIG
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
PROXY_URL = os.getenv("PROXY_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL در Environment Variables تنظیم نشده است. "
        "در Railway یک PostgreSQL اضافه کن."
    )

URL_CURRENCY = "https://alanchand.com/en/currencies-price"
URL_CRYPTO = "https://alanchand.com/en/crypto-price"
URL_GOLD = "https://alanchand.com/en/gold-price"
URL_TGJU_DOLLAR = "https://www.tgju.org/profile/price_dollar_rl"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

TEHRAN = ZoneInfo("Asia/Tehran")
dp = Dispatcher()
app = FastAPI()

# =========================================================
# DATABASE
# =========================================================

db_pool: asyncpg.Pool | None = None


async def init_db():
    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=20,
    )

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_prices (
                price_date DATE NOT NULL,
                symbol TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (price_date, symbol)
            )
            """
        )

    logging.info("PostgreSQL database is ready.")


async def save_daily_prices(data: dict):
    if db_pool is None:
        return

    today = datetime.now(TEHRAN).date()

    prices = {
        "dollar": data.get("tgju_dollar"),
        "euro": (
            data.get("currency", {}).get("euro", {}).get("sell")
            if data.get("currency")
            else None
        ),
        "usdt": data.get("crypto", {}).get("usdt"),
        "btc": data.get("crypto", {}).get("btc"),
        "gold18": data.get("gold", {}).get("gold18"),
        "coin": data.get("gold", {}).get("coin"),
        "ounce": data.get("gold", {}).get("ounce"),
    }

    async with db_pool.acquire() as conn:
        for symbol, value in prices.items():
            if value is None:
                continue

            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue

            await conn.execute(
                """
                INSERT INTO daily_prices (price_date, symbol, value, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (price_date, symbol)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                today,
                symbol,
                numeric_value,
            )


async def get_yesterday_prices() -> dict:
    if db_pool is None:
        return {}

    today = datetime.now(TEHRAN).date()

    rows = await db_pool.fetch(
        """
        SELECT symbol, value
        FROM daily_prices
        WHERE price_date < $1
          AND price_date = (
              SELECT MAX(price_date)
              FROM daily_prices
              WHERE price_date < $1
          )
        """,
        today,
    )

    return {row["symbol"]: float(row["value"]) for row in rows}


# =========================================================
# FASTAPI ENDPOINTS (WEB APP)
# =========================================================

@app.get("/api/prices")
async def get_prices_api():
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database pool is not initialized")
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, value, updated_at 
            FROM daily_prices 
            WHERE price_date = (SELECT MAX(price_date) FROM daily_prices)
        """)
        return {row["symbol"]: {"value": row["value"], "updated_at": row["updated_at"]} for row in rows}


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>مینی‌اپ قیمت‌ها فعال است 🚀</h1>"


# =========================================================
# HELPERS & SCRAPING
# =========================================================

def to_int(value: str) -> int | None:
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def irr_to_toman(cell: str) -> int | None:
    match = re.search(r"([\d,]+)\s*IRR", cell, re.IGNORECASE)
    if not match:
        return None
    number = to_int(match.group(1))
    return number // 10 if number else None


def usd_value_number(cell: str) -> float | None:
    match = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", cell)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def fmt_toman(number) -> str:
    if number is None:
        return "—"
    try:
        return f"{float(number):,.0f}"
    except (TypeError, ValueError):
        return "—"


def fmt_usd(number) -> str:
    if number is None:
        return "—"
    try:
        return f"{float(number):,.2f}"
    except (TypeError, ValueError):
        return "—"


def change_text(current, previous, percent=True) -> str:
    if current is None or previous is None:
        return "⚪ —"
    try:
        current = float(current)
        previous = float(previous)
    except (TypeError, ValueError):
        return "⚪ —"

    diff = current - previous
    if diff > 0:
        icon, sign = "🟢", "+"
    elif diff < 0:
        icon, sign = "🔴", ""
    else:
        return "⚪ 0"

    result = f"{icon} {sign}{diff:,.0f}"
    if percent and previous != 0:
        pct = (diff / previous) * 100
        result += f" ({sign}{pct:.2f}٪)"
    return result


def change_usd_text(current, previous) -> str:
    if current is None or previous is None:
        return "⚪ —"
    try:
        current = float(current)
        previous = float(previous)
    except (TypeError, ValueError):
        return "⚪ —"

    diff = current - previous
    if diff > 0:
        icon, sign = "🟢", "+"
    elif diff < 0:
        icon, sign = "🔴", ""
    else:
        return "⚪ 0"

    result = f"{icon} {sign}{diff:,.2f}"
    if previous != 0:
        pct = (diff / previous) * 100
        result += f" ({sign}{pct:.2f}٪)"
    return result


async def get_rows(session: aiohttp.ClientSession, url: str) -> list[list[str]]:
    async with session.get(url) as response:
        response.raise_for_status()
        html = await response.text()
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    return rows


def parse_currency(rows: list[list[str]]) -> dict:
    output = {}
    for row in rows:
        if len(row) < 3:
            continue
        name = row[0].strip().lower()
        if "euro" in name:
            buy, sell = to_int(row[1]), to_int(row[2])
            if buy is not None and sell is not None:
                output["euro"] = {"buy": buy // 10, "sell": sell // 10}
    return output


async def get_tgju_dollar(session: aiohttp.ClientSession) -> int:
    async with session.get(URL_TGJU_DOLLAR) as response:
        response.raise_for_status()
        html = await response.text()
    soup = BeautifulSoup(html, "html.parser")
    match = re.search(r"نرخ\s*فعلی\s*[:：]+\s*([\d,٬٫]+)", soup.get_text(" ", strip=True), re.IGNORECASE)
    if not match:
        raise ValueError("نرخ دلار پیدا نشد")
    rial_price = to_int(match.group(1))
    return rial_price // 10


def parse_crypto(rows: list[list[str]]) -> dict:
    output = {}
    for row in rows:
        if len(row) < 3:
            continue
        name = row[0].strip().upper()
        if name.endswith("USDT"):
            output["usdt"] = irr_to_toman(row[1])
        elif name.endswith("BTC"):
            output["btc"] = usd_value_number(row[2])
    return output


def parse_gold(rows: list[list[str]]) -> dict:
    output = {}
    for row in rows:
        if len(row) < 2:
            continue
        name = row[0].strip().lower()
        if name.startswith("18k gold"):
            output["gold18"] = irr_to_toman(row[1])
        elif name.startswith("full coin"):
            output["coin"] = irr_to_toman(row[1])
        elif name.startswith("gold ounce"):
            output["ounce"] = usd_value_number(row[1])
    return output


async def fetch_all() -> dict:
    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        results = await asyncio.gather(
            get_rows(session, URL_CURRENCY),
            get_rows(session, URL_CRYPTO),
            get_rows(session, URL_GOLD),
            get_tgju_dollar(session),
            return_exceptions=True,
        )

    data = {}
    parsers = (("currency", parse_currency), ("crypto", parse_crypto), ("gold", parse_gold))

    for (key, parser), result in zip(parsers, results[:3]):
        try:
            if isinstance(result, Exception):
                raise result
            data[key] = parser(result)
        except Exception:
            data[key] = None

    try:
        if isinstance(results[3], Exception):
            raise results[3]
        data["tgju_dollar"] = results[3]
    except Exception:
        data["tgju_dollar"] = None

    try:
        await save_daily_prices(data)
    except Exception:
        pass

    return data


async def build_text(data: dict) -> str:
    now = datetime.now(TEHRAN).strftime("%Y/%m/%d | %H:%M:%S")
    previous = await get_yesterday_prices()

    currency = data.get("currency") or {}
    crypto = data.get("crypto") or {}
    gold = data.get("gold") or {}
    usd = data.get("tgju_dollar")
    eur = currency.get("euro")

    lines = [
        "📊 <b>قیمت لحظه‌ای بازار</b>",
        "<i>مقایسه با آخرین قیمت ثبت‌شده روز قبل</i>",
        "",
    ]

    if usd is not None:
        lines.extend([
            "💵 <b>دلار آزاد</b>",
            f"   <b>{fmt_toman(usd)}</b> تومان  {change_text(usd, previous.get('dollar'))}",
            "",
        ])

    if eur:
        euro_sell = eur.get("sell")
        lines.extend([
            "💶 <b>یورو</b>",
            f"   خرید: <b>{fmt_toman(eur.get('buy'))}</b> تومان",
            f"   فروش: <b>{fmt_toman(euro_sell)}</b> تومان  {change_text(euro_sell, previous.get('euro'))}",
            "",
        ])

    usdt = crypto.get("usdt")
    lines.append(f"💲 <b>تتر:</b> {fmt_toman(usdt)} تومان  {change_text(usdt, previous.get('usdt'))}")

    btc = crypto.get("btc")
    if btc is not None:
        lines.append(f"₿ <b>بیت‌کوین:</b> {fmt_usd(btc)} دلار  {change_usd_text(btc, previous.get('btc'))}")

    lines.append("")

    gold18 = gold.get("gold18")
    lines.append(f"🥇 <b>طلای ۱۸ عیار:</b> {fmt_toman(gold18)} تومان  {change_text(gold18, previous.get('gold18'))}")

    coin = gold.get("coin")
    lines.append(f"🪙 <b>سکه تمام:</b> {fmt_toman(coin)} تومان  {change_text(coin, previous.get('coin'))}")

    ounce = gold.get("ounce")
    if ounce is not None:
        lines.append(f"🌍 <b>اونس جهانی طلا:</b> {fmt_usd(ounce)} دلار  {change_usd_text(ounce, previous.get('ounce'))}")

    lines.extend([
        "",
        f"🕒 <b>آخرین دریافت:</b> {now}",
        "",
        "<i>🟢 افزایش | 🔴 کاهش | ⚪ بدون تغییر</i>",
    ])

    return "\n".join(lines)


# =========================================================
# HANDLERS
# =========================================================

@dp.message(Command("start", "dollar", "price"))
async def cmd_price(message: Message):
    # گرفتن دامین از متغیرهای محیطی یا حالت پیش‌فرض برای دکمه مینی‌اپ
    web_app_url = os.environ.get("WEB_APP_URL", "https://dollar-production-82c0.up.railway.app/")
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="refresh_prices", style=ButtonStyle.SUCCESS),
                InlineKeyboardButton(text="🌐 ورود به مینی‌اپ", web_app=WebAppInfo(url=web_app_url))
            ]
        ]
    )

    wait = await message.answer("⏳ در حال دریافت آخرین قیمت‌ها...")
    try:
        data = await fetch_all()
        await wait.edit_text(await build_text(data), reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        await wait.edit_text("❌ دریافت قیمت‌ها ناموفق بود. لطفاً دوباره تلاش کنید.")


@dp.callback_query(F.data == "refresh_prices")
async def on_refresh(call: CallbackQuery):
    try:
        await call.answer("⏳ در حال بروزرسانی...")
        data = await fetch_all()
        
        web_app_url = os.environ.get("WEB_APP_URL", "https://dollar-production-82c0.up.railway.app/")
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="refresh_prices", style=ButtonStyle.SUCCESS),
                    InlineKeyboardButton(text="🌐 ورود به مینی‌اپ", web_app=WebAppInfo(url=web_app_url))
                ]
            ]
        )

        await call.message.edit_text(await build_text(data), reply_markup=keyboard, parse_mode="HTML")
        await call.answer("✅ قیمت‌ها بروزرسانی شد")
    except TelegramBadRequest:
        await call.answer("قیمت‌ها تغییری نکرده")
    except Exception:
        await call.answer("❌ خطا در بروزرسانی", show_alert=True)


# =========================================================
# RUNNERS (WEB SERVER + BOT)
# =========================================================

async def run_web_server():
    port = int(os.environ.get("PORT", 8000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await init_db()

    session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
    bot = Bot(token=BOT_TOKEN, session=session)

    try:
        logging.info("Bot and Web Server are starting concurrently...")
        await asyncio.gather(
            run_web_server(),
            dp.start_polling(bot)
        )
    finally:
        await bot.session.close()
        if db_pool is not None:
            await db_pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped")
