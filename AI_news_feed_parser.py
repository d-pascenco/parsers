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
SHEET_NAME   = "rss_feed_parser"

# ────────── СПИСОК RSS ──────────
rss_feeds = [
"https://www.sciencedaily.com/rss/computers_math/robotics.xml",
"https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml",
"https://www.sciencedaily.com/rss/computers_math/educational_technology.xml",
"https://techcrunch.com/feed/",
"https://www.techrepublic.com/rssfeeds/topic/artificial-intelligence/",
"https://aws.amazon.com/ru/blogs/machine-learning/feed/",
"https://openai.com/news/rss.xml",
"https://lastweekin.ai/feed",
"https://machinelearningmastery.com/blog/feed/",
"https://www.marktechpost.com/feed/",
"https://bair.berkeley.edu/blog/feed.xml",
"https://news.mit.edu/rss/topic/artificial-intelligence2",
"https://deepmind.google/blog/rss.xml",
"https://www.unite.ai/feed/",
"https://www.artificial-intelligence.blog/terminology?format=rss",
"https://www.artificial-intelligence.blog/ai-news?format=rss",
"https://ai2people.com/feed/",
"https://hanhdbrown.com/feed/",
"https://aiparabellum.com/feed/",
"https://dailyai.com/feed/",
"https://nyheter.aitool.se/feed/",
"https://www.spritle.com/blog/feed/",
"https://www.shaip.com/feed/",
"https://weam.ai/feed/",
"https://zerothprinciples.substack.com/feed",
"https://airevolution.blog/feed/",
"https://saal.ai/feed/",
"https://aicorr.com/feed/",
"https://wgmimedia.com/feed/",
"https://advancewithai.net/feed",
"https://qudata.com/en/news/rss.xml",
"https://hanhdbrown.com/category/ai/feed/",
"https://www.oreilly.com/radar/topics/ai-ml/feed/index.xml",
"https://blogs.sas.com/content/topic/artificial-intelligence/feed/",
"https://blogs.rstudio.com/ai/index.xml",
"https://www.technologyreview.com/topic/artificial-intelligence/feed",
"https://www.kdnuggets.com/feed",
"https://nanonets.com/blog/rss/",
"https://www.datarobot.com/blog/feed/",
"https://becominghuman.ai/feed",
"https://bigdataanalyticsnews.com/category/artificial-intelligence/feed/",
"https://blog.kore.ai/rss.xml",
"https://www.clarifai.com/blog/rss.xml",
"https://www.expert.ai/feed/",
"https://theaisummer.com/feed.xml",
"https://www.aiiottalk.com/feed/",
"https://www.isentia.com/feed/",
"https://blog.chatbotslife.com/feed",
"https://www.marketingaiinstitute.com/blog/rss.xml",
"https://www.topbots.com/feed/",
"https://www.artificiallawyer.com/feed/",
"https://dlabs.ai/feed/",
"https://www.aitimejournal.com/feed/",
"https://insights.fusemachines.com/feed/",
"https://aiweekly.co/issues.rss",
"https://intelligence.org/blog/feed/",
"https://deepcognition.ai/feed/",
"https://1reddrop.com/feed/",
"https://www.viact.ai/blog-feed.xml",
"https://robotwritersai.com/feed/",
"https://blog.marekrosa.org/feeds/posts/default",
"https://aihub.org/feed/?cat=-473",
"https://techspective.net/category/technology/artificial-intelligence/feed/",
"https://www.mantralabsglobal.com/blog/feed/",
"https://usmsystems.com/blog/feed/",
"https://computational-intelligence.blogspot.com/feeds/posts/default",
"https://www.aiplusinfo.com/feed/",
"https://metadevo.com/feed/",
"https://www.cogitotech.com/feed/",
"https://www.mtlc.co/category/ai/feed/",
"https://datamachina.substack.com/feed",
"https://vue.ai/blog/feed/",
"https://www.mygreatlearning.com/blog/artificial-intelligence/feed/",
"https://topmarketingai.com/feed/",
"https://yatter.in/feed/",
"https://appzoon.com/feed/",
"https://rapidtechstories.com/feed/",
"https://medium.com/feed/@securechainai",
"https://blogs.microsoft.com/ai/feed/",
"https://research.aimultiple.com/feed/",
"https://chatbotsmagazine.com/feed",
"https://findnewai.com/feed/",
"https://kavita-ganesan.com/feed/",
"https://pandio.com/feed/",
"https://aiworldschool.com/feed/",
"https://www.danrose.ai/blog?format=rss/",
"https://www.edia.nl/edia-blog?format=rss",
"https://www.eledia.org/e-air/feed/",
"https://ankit-ai.blogspot.com/feeds/posts/default?alt=rss",
"https://airoboticsprof.blogspot.com/feeds/posts/default",
"https://anotherdatum.com/feeds/all.atom.xml?format=xml",
"https://editorialia.com/feed/",
"https://blog.datumbox.com/feed/",
"https://daleonai.com/feed.xml",
"https://www.lorienpratt.com/category/artificial-intelligence/feed/",
"https://binaryinformatics.com/category/ai/feed/",
"https://www.kochartech.com/feed/",
"https://medium.com/feed/@Francesco_AI",
"https://medium.com/feed/archieai",
"https://medium.com/feed/ai-roadmap-institute",
"https://learn.microsoft.com/en-us/archive/blogs/machinelearning/feed.xml",
"https://www.tryswivl.com/blog/feed",
"https://www.jmlr.org/jmlr.xml",
"https://news.mit.edu/topic/mitartificial-intelligence2-rss.xml",
"https://venturebeat.com/category/ai/feed/",
"https://aibusiness.com/rss.xml",
"https://techcrunch.com/tag/artificial-intelligence/feed/",
"https://machinelearningmastery.com/feed/",
"https://www.aitrends.com/feed/",
"https://analyticsindiamag.com/feed/",
"https://www.reddit.com/r/MachineLearning/.rss",
"https://www.aiweirdness.com/rss/",
"https://futurism.com/categories/ai-artificial-intelligence/feed",
"https://www.wired.com/feed/tag/ai/latest/rss",
"https://insideainews.com/feed/",
"https://www.artificialintelligence-news.com/feed/",
"https://techxplore.com/rss-feed/machine-learning-ai-news/",
"https://www.technologyreview.com/topic/computing/feed/",
"https://www.datanami.com/feed/",
"https://ai-techpark.com/feed/",
"https://www.aiacceleratorinstitute.com/rss/",
"https://blogs.sas.com/content/feed/",
"https://singularityhub.com/feed/",
"https://www.topbots.com/feed/",
"https://papers.takara.ai/api/feed",
"https://research.fb.com/feed",
"https://www.research.microsoft.com/rss/news.xml",
"https://developer.nvidia.com/blog/feed",
"https://status.anthropic.com/history.rss",
"https://stability.ai/blog/rss.xml",
"https://ai-techpark.com/feed",
"https://aiacceleratorinstitute.com/rss/",
"https://aihub.org/feed?cat=-473",
"https://www.aiweirdness.com/rss/",
"https://aws.amazon.com/blogs/machine-learning/feed",
"https://www.amazon.science/index.rss",
"https://www.anaconda.com/blog/feed",
"https://analyticsindiamag.com/feed/",
"https://feeds.arstechnica.com/arstechnica/index",
"https://www.artificialintelligence-news.com/feed/rss/",
"https://arxiv.org/rss/stat.ML",
"https://arxiv.org/rss/cs.CL",
"https://arxiv.org/rss/cs.CV",
"https://arxiv.org/rss/cs.LG",
"https://www.assemblyai.com/blog/rss/",
"https://www.benzinga.com/feed",
"https://ml.berkeley.edu/blog/rss.xml",
"https://bair.berkeley.edu/blog/feed.xml",
"https://feeds.feedburner.com/PythonInsider",
"https://nicholas.carlini.com/writing/feed.xml",
"https://blog.ml.cmu.edu/feed",
"https://news.crunchbase.com/feed",
"https://dagshub.com/blog/rss/",
"https://dataconomy.com/feed",
"https://datafloq.com/feed/?post_type=post",
"https://www.datanami.com/feed/",
"https://davidstutz.de/category/blog/feed",
"https://deep-and-shallow.com/feed",
"https://deephaven.io/blog/rss.xml",
"https://deepmind.com/blog/feed/basic/",
"https://dev.to/feed",
"https://www.digitaljournal.com/feed",
"https://distill.pub/rss.xml",
"https://www.elastic.co/blog/feed",
"https://explosion.ai/feed",
"https://www.fast.ai/index.xml",
"http://www.fast.ai/atom.xml",
"https://future.com/category/data/feed",
"http://googleaiblog.blogspot.com/atom.xml",
"https://gradientflow.com/feed/",
"https://hackernoon.com/tagged/ai/feed",
"https://feeds.feedburner.com/HealthTechMagazine",
"https://huggingface.co/blog/feed.xml",
"https://huyenchip.com/feed",
"https://spectrum.ieee.org/feeds/topic/artificial-intelligence.rss",
"https://feed.infoq.com/ai-ml-data-eng/",
"https://www.infoworld.com/category/analytics/index.rss",
"https://www.infoworld.com/category/machine-learning/index.rss",
"https://insidebigdata.com/feed",
"http://feeds.feedburner.com/miriblog",
"https://jack-clark.net/feed",
"https://www.jmlr.org/jmlr.xml",
"https://www.kdnuggets.com/feed",
"https://lambdalabs.com/blog/rss/",
"https://lastweekin.ai/feed",
"https://machinelearningmastery.com/blog/feed",
"https://www.marktechpost.com/feed",
"https://medium.com/feed/@mazzanti.sam",
"https://medium.com/feed/mlearning-ai",
"https://www.microsoft.com/en-us/research/feed/",
"https://minimaxir.com/post/index.xml",
"https://news.mit.edu/topic/mitmachine-learning-rss.xml",
"https://nanonets.com/blog/rss/",
"https://www.nature.com/subjects/machine-learning.rss",
"https://neptune.ai/blog/feed",
"https://www.newscientist.com/subject/technology/feed/",
"https://developer.nvidia.com/blog/feed",
"https://openai.com/blog/rss/",
"https://hub.packtpub.com/category/data/artificial-intelligence/feed",
"https://blog.paperspace.com/rss/",
"https://www.bmc.com/blogs/categories/machine-learning-big-data/feed",
"https://www.producthunt.com/feed",
"https://pyimagesearch.com/blog/feed",
"https://www.python.org/dev/peps/peps.rss",
"https://api.quantamagazine.org/feed",
"https://feeds.feedburner.com/RBloggers",
"https://www.radiant.earth/feed/",
"https://www.reddit.com/r/datascience.json?limit=50",
"https://www.reddit.com/r/MachineLearning.json?limit=50",
"https://www.reddit.com/r/computervision.json?limit=50",
"https://www.reddit.com/r/machinelearningnews.json?limit=50",
"https://www.reddit.com/r/reinforcementlearning.json?limit=50",
"https://www.reddit.com/r/neuralnetworks.json?limit=50",
"https://www.reddit.com/r/LanguageTechnology.json?limit=50",
"https://www.reddit.com/r/artificial.json?limit=50",
"https://www.reddit.com/r/deeplearning.json?limit=50",
"https://replicate.com/blog/rss",
"https://restofworld.org/feed/latest/",
"https://www.reutersagency.com/feed/?best-topics=tech",
"https://ruder.io/rss/index.rss",
"https://www.sciencedaily.com/rss/computers_math/neural_interfaces.xml",
"https://www.sciencedaily.com/rss/computers_math/robotics.xml",
"https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml",
"http://rss.sciam.com/ScientificAmerican-Global",
"https://siliconangle.com/category/ai/feed",
"https://siliconangle.com/category/big-data/feed",
"https://stability.ai/blog?format=rss",
"http://ai.stanford.edu/blog/feed.xml",
"https://hai.stanford.edu/rss.xml",
"https://syncedreview.com/feed",
"https://tech.eu/category/deep-tech/feed",
"https://tech.eu/category/robotics/feed",
"https://techcrunch.com/feed/",
"https://www.technologyreview.com/c/computing/rss/",
"https://www.techrepublic.com/rssfeeds/topic/artificial-intelligence/",
"https://techxplore.com/rss-feed/machine-learning-ai-news/",
"https://phys.org/rss-feed/technology-news/machine-learning-ai/",
"https://the-decoder.com/feed/",
"https://theaisummer.com/feed.xml",
"https://thegradient.pub/rss/",
"https://www.theguardian.com/technology/artificialintelligenceai/rss",
"https://thenextweb.com/neural/feed",
"https://www.theregister.com/software/ai_ml/headlines.atom",
"https://www.theverge.com/rss/index.xml",
"https://pub.towardsai.net/feed",
"https://towardsdatascience.com/feed",
"https://eng.uber.com/category/articles/ai/feed",
"https://www.unite.ai/feed/",
"https://venturebeat.com/category/ai/feed/",
"https://wandb.ai/fully-connected/rss.xml",
"https://www.wired.com/feed/category/artificial-intelligence/rss",
"https://www.zdnet.com/topic/artificial-intelligence/rss.xml",
"https://www.zdnet.com/topic/big-data/rss.xml",
"https://feeds.buzzsprout.com/520474.rss",
"http://feeds.soundcloud.com/users/soundcloud:users:306749289/sounds.rss",
"https://anchor.fm/s/41286f68/podcast/rss",
"https://anchor.fm/s/32ec7408/podcast/rss",
"https://anchor.fm/s/443868ac/podcast/rss",
"https://anchor.fm/s/43e0b648/podcast/rss",
"https://bigdatabeard.com/feed",
"https://changelog.com/practicalai/feed",
"https://podcasts.files.bbci.co.uk/p02nrss1.rss",
"https://www.dataengineeringpodcast.com/feed/mp3/",
"https://feeds.buzzsprout.com/300035.rss",
"https://dataskeptic.libsyn.com/rss",
"https://datastori.es/feed/",
"https://topenddevs.com/podcasts/adventures-in-machine-learning/rss.rss",
"http://podcast.emerj.com/rss",
"http://feeds.feedburner.com/ibm-big-data-hub-podcasts",
"http://feeds.libsyn.com/363776/rss",
"https://aiandbanking.libsyn.com/rss",
"http://lexisnexisbis.libsyn.com/rss",
"https://anchor.fm/s/174cb1b8/podcast/rss",
"http://nssdeviations.com/rss",
"http://feeds.soundcloud.com/users/soundcloud:users:264034133/sounds.rss",
"http://feeds.podtrac.com/IOJSwQcdEBcg",
"https://datascienceathome.com/feed.xml",
"https://feed.podbean.com/hdsr/feed.xml",
"https://roaringelephant.org/feed/podcast/",
"https://feeds.simplecast.com/XA_851k3",
"https://api.substack.com/feed/podcast/265424/s/1354.rss",
"https://talkpython.fm/episodes/rss",
"https://feeds.fireside.fm/theartistsofdatascience/rss",
"https://thedataexchange.media/feed/",
"https://geomob-podcast.castos.com/feed",
"https://feeds.transistor.fm/the-data-engineering-show",
"https://twimlai.com/feed",
"https://feeds.captivate.fm/gradient-dissent/",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCHuiy8bXnmK5nisYHUd1J5g",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCEAbqW0HFB_UxZoUDO0kJBw",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCa0RTSXWyZdh7IciV9r-3ow",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCEqgmyWChwvt6MFGGlmUQCQ",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCbfYPyITQ-7l4upoX8nvctg",
"https://www.youtube.com/feeds/videos.xml?channel_id=UC66Ggxy8MHX9DCDohdRYDTA",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCUzGQrN-lyyc0BWTYoJM_Sg",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCobqgqE4i5Kf7wrxRxhToQA",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCHXa4OpASJEwrHrLeIzw7Yg",
"https://www.youtube.com/feeds/videos.xml?channel_id=UC0rqucBdTuFTjJiefW5t-IQ",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCV0qA-eDDICsRR9rPcnG7tw",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCVqU1Vy3HO4Ms-pbN0r2_kg",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCMLtBahI5DMrt0NPvDSoIRQ",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCAlwrsgeJavG1vw9qSFOUmA",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCtatfZMf-8EkIwASXM4ts0A",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCuHZ1UYfHRqk3-5N5oc97Kw",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCDia_lkNYKLJVLRLQl_-pFw",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCwx7Y3W30N8aS_tiCy2x-2g",
"https://www.youtube.com/feeds/videos.xml?channel_id=UC1H1NWNTG2Xi3pt85ykVSHA",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCP7jMXSY2xbc3KCAE0MHQ-A",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCq6XkhO5SZ66N04IcPbqNcw",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCSNeZleDn9c74yQc-EKnVTA",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCYoS2VT03weLA7uzvL2Vybw",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCVchfoB65aVtQiDITbGq2LQ",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCk6ONJlPzjw3DohAeMSgsng",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCMbtqTGdSsxYYhhTpV4lSTQ",
"https://www.youtube.com/feeds/videos.xml?channel_id=UC2UXDak6o7rBm23k3Vv5dww",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCj8shE7aIn4Yawwbo2FceCQ",
"https://www.youtube.com/feeds/videos.xml?channel_id=UCiT9RITQ9PW6BhXK0y2jaeg"
]

trunc = lambda t: t if len(t) <= MAX_CELL else t[:MAX_CELL-len(ELLIPSIS)] + ELLIPSIS
clean = lambda h: BeautifulSoup(h, "html.parser").get_text(" ", strip=True)

# ────────────── ДАТА ──────────────
def get_date():
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