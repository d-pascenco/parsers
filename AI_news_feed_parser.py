# rss_feed_parser.py
import logging, os, sys, subprocess, importlib, shlex, time, csv
from datetime import datetime, timedelta

LOG_PATH = os.path.join("/kaggle/working", "rss_feed_parser.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"),
              logging.StreamHandler()]
)
log = logging.getLogger("rss")

# ────────── ЗАВИСИМОСТИ ──────────
deps = [
    "feedparser",
    "gspread",
    "oauth2client",
    "beautifulsoup4"
]
for p in deps:
    try:
        importlib.import_module(p.split("==")[0])
    except ImportError:
        log.info("Install %s", p)
        subprocess.check_call([sys.executable, "-m", "pip", "install", p, "--quiet"])

import feedparser, gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup

MAX_CELL, ELLIPSIS = 50_000, "…"
DATE_OVERRIDE = ""
# Раскомментируйте, чтобы всегда использовать вчерашнюю дату
AUTO_YESTERDAY = True
# AUTO_YESTERDAY = False
SHEET_NAME   = "rss_feed_parser"

# ────────── СПИСОК RSS ──────────
FEED_LIST_PATH = "/kaggle/input/test-feed-list/test_feed_list.txt"

def load_feeds(path: str = FEED_LIST_PATH) -> list[str]:
    try:
        with open(path, encoding="utf-8") as f:
            feeds = [line.strip() for line in f if line.strip()]
        log.info("Loaded %d feeds", len(feeds))
        return feeds
    except Exception:
        log.error("Can't read feed list at %s", path, exc_info=True)
        return []

rss_feeds = load_feeds()

trunc = lambda t: t if len(t) <= MAX_CELL else t[:MAX_CELL-len(ELLIPSIS)] + ELLIPSIS
clean = lambda h: BeautifulSoup(h, "html.parser").get_text(" ", strip=True)

# ────────────── ДАТА ──────────────
def get_date():
    if AUTO_YESTERDAY:
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    d = DATE_OVERRIDE or os.getenv("DATE", "")
    if not d:
        try:
            d = input("Введите дату YYYY-MM-DD (пусто = вчера): ").strip()
        except EOFError:
            d = ""
    try:
        if d:
            datetime.strptime(d, "%Y-%m-%d")
            return d
    except ValueError:
        log.warning("Bad date '%s' – using yesterday", d)
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ─────────── АВТЕНТИФИКАЦИЯ ──────────
def auth():
    path = "/kaggle/input/credentials-json/credentials.json"
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        path,
        ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
    )
    log.info("Google Sheets auth OK")
    return gspread.authorize(creds)

# ───────── ФЕТЧИНГ RSS ─────────
def fetch(feeds, date):
    log.info("Fetching for %s …", date)
    res, hdr = [], {"User-Agent": "MyRSSReader/1.0"}
    for u in feeds:
        log.debug("▶ %s", u)
        try:
            f = feedparser.parse(u, request_headers=hdr)
            matched = 0
            for e in f.entries:
                pp = e.get("published_parsed")
                pub = datetime(*pp[:6]).strftime("%Y-%m-%d") if pp else None
                if pub == date:
                    matched += 1
                    res.append({
                        "Title": e.get("title", ""),
                        "Link": e.get("link", ""),
                        "Summary": clean(e.get("summary", "")),
                        "Published Date": pub,
                        "Source": f.feed.get("title", "")
                    })
            log.debug("  matched %d items", matched)
        except Exception:
            log.error("Parse error: %s", u, exc_info=True)
        time.sleep(0.2)
    log.info("Total collected: %d", len(res))
    return res

# ───────── ЛИСТ ДЛЯ ДАТЫ ─────────
def ensure_sheet_for_date(ss, base, date):
    name = f"{base}_{date}"
    for w in ss.worksheets():
        if w.title == name:
            log.info("Using existing sheet %s", name)
            return w
    new_ws = ss.worksheet(base).duplicate(new_sheet_name=name)
    log.info("Created sheet copy %s", name)
    return new_ws

# ───────── ЗАПИСЬ В SHEETS ─────────
def save_sheet(arts, book, date):
    client = auth()
    try:
        ss = client.open(book)
    except gspread.SpreadsheetNotFound:
        ss = client.create(book)
        log.info("Created spreadsheet %s", book)
    ws = ensure_sheet_for_date(ss, book, date)
    ws.clear()
    ws.append_row(["Title", "Link", "Summary", "Published Date", "Source"])
    ws.append_rows(
        [[a["Title"], a["Link"], trunc(a["Summary"]),
          a["Published Date"], a["Source"]] for a in arts],
        value_input_option="RAW"
    )
    log.info("Wrote %d rows to sheet %s", len(arts), ws.title)

# ───────── CSV (опц.) ─────────
def save_csv(arts, fn=None):
    fn = fn or f"rss_feed_{datetime.now():%Y-%m-%d_%H-%M-%S}.csv"
    with open(fn, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(
            [["Title", "Link", "Summary", "Published Date", "Source"]] +
            [[a["Title"], a["Link"], a["Summary"],
              a["Published Date"], a["Source"]] for a in arts]
        )
    log.info("CSV saved: %s", fn)

# ───────── MAIN ─────────
def main():
    d = get_date()
    print("Date:", d)
    arts = fetch(rss_feeds, d)
    if not arts:
        print("No articles")
        return
    # (сокращение ссылок убрано)
    save_sheet(arts, SHEET_NAME, d)
    save_csv(arts)
    print("Done:", len(arts))
    subprocess.run(shlex.split(f"tail -n 20 {LOG_PATH}"))

if __name__ == "__main__":
    main()
