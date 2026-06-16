#!/usr/bin/env python3
"""
WhaleWatch — a mobile-first hedge-fund 13F tracker, in ONE file.

Usage:
    python3 whalewatch.py            run the app  ->  http://localhost:8000
    python3 whalewatch.py build      build/refresh the searchable fund directory
    python3 whalewatch.py doctor     diagnose why data isn't loading
    python3 whalewatch.py ingest 1067983 [more CIKs...]   pre-load funds

Data (SQLite) is stored next to this file as whalewatch.db.
"""
import os, re, sys, time, json, sqlite3, threading
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import xml.etree.ElementTree as ET
from functools import lru_cache

try:
    import requests
    from flask import Flask, jsonify, request, Response
except ImportError:
    sys.stderr.write("\nMissing packages. Run this first:\n"
                     "    python3 -m pip install flask requests\n\n")
    sys.exit(1)

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("WW_DB", os.path.join(HERE, "whalewatch.db"))

# SEC REQUIRES a real contact email or it blocks you. Set it via:
#   export SEC_UA="WhaleWatch/1.0 (Your Name you@email.com)"
SEC_UA = os.environ.get("SEC_UA", "WhaleWatch/1.0 (Eduardo Ribeiro eduardoribeiro@uchicago.edu)")
SEC_HEADERS = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}

_last = [0.0]
_throttle_lock = threading.Lock()
def _throttle():
    # Thread-safe: enforces global SEC spacing (~7.7 req/s, under SEC's 10/s cap)
    # even when enrichment runs across a thread pool.
    with _throttle_lock:
        d = time.time() - _last[0]
        if d < 0.13:
            time.sleep(0.13 - d)
        _last[0] = time.time()

def sec_get(url, **kw):
    _throttle()
    r = requests.get(url, headers=SEC_HEADERS, timeout=30, **kw)
    r.raise_for_status()
    return r

# ---------------------------------------------------------------------------
# EDGAR: index -> universe of filers
# ---------------------------------------------------------------------------
def quarters_back(n):
    today = dt.date.today()
    y, q = today.year, (today.month - 1) // 3 + 1
    for _ in range(n):
        yield y, q
        q -= 1
        if q == 0:
            q = 4; y -= 1

def parse_master_idx(text):
    out = []
    for line in text.splitlines():
        if line.count("|") != 4:
            continue
        cik, name, form, date, fname = line.split("|")
        if not cik.strip().isdigit():
            continue
        if form.strip().startswith("13F-HR"):
            out.append({"cik": cik.strip().zfill(10), "name": name.strip(),
                        "form": form.strip(), "date": date.strip()})
    return out

def fetch_quarter_filers(year, qtr):
    url = "https://www.sec.gov/Archives/edgar/full-index/%d/QTR%d/master.idx" % (year, qtr)
    return parse_master_idx(sec_get(url).text)

@lru_cache(maxsize=512)
def get_submissions(cik):
    cik = str(cik).zfill(10)
    return sec_get("https://data.sec.gov/submissions/CIK%s.json" % cik).json()

def _safe(lst, i):
    return lst[i] if i < len(lst) else ""

def list_13f_filings(cik):
    sub = get_submissions(cik)
    r = sub.get("filings", {}).get("recent", {})
    out = []
    for i, f in enumerate(r.get("form", [])):
        if f.startswith("13F-HR"):
            out.append({"form": f, "accession": r.get("accessionNumber", [])[i],
                        "filingDate": _safe(r.get("filingDate", []), i),
                        "reportDate": _safe(r.get("reportDate", []), i)})
    return {"name": sub.get("name", str(cik)), "cik": str(cik).zfill(10), "filings": out}

# ---------------------------------------------------------------------------
# EDGAR: information table (holdings) parsing
# ---------------------------------------------------------------------------
def _strip_ns(tag):
    return tag.split("}")[-1] if "}" in tag else tag

def _text(el, name):
    for c in el.iter():
        if _strip_ns(c.tag).lower() == name.lower():
            return (c.text or "").strip()
    return ""

def parse_info_table(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        t = xml_bytes.decode("utf-8", "ignore")
        root = ET.fromstring(t[t.find("<"):])
    out = []
    for el in root.iter():
        if _strip_ns(el.tag).lower() != "infotable":
            continue
        def num(v):
            try: return float(v or 0)
            except ValueError: return 0.0
        out.append({"cusip": _text(el, "cusip").upper(), "name": _text(el, "nameOfIssuer"),
                    "class": _text(el, "titleOfClass"), "value": num(_text(el, "value")),
                    "shares": num(_text(el, "sshPrnamt")), "putCall": _text(el, "putCall")})
    return out

def fetch_info_table(cik, accession):
    cik_int = str(int(cik)); acc = accession.replace("-", "")
    base = "https://www.sec.gov/Archives/edgar/data/%s/%s/" % (cik_int, acc)
    items = sec_get(base + "index.json").json().get("directory", {}).get("item", [])
    name = None
    for it in items:
        low = it.get("name", "").lower()
        if low.endswith(".xml") and ("infotable" in low or "info_table" in low):
            name = it["name"]; break
    if not name:
        for it in items:
            low = it.get("name", "").lower()
            if low.endswith(".xml") and ("form13f" in low or "table" in low):
                name = it["name"]; break
    if not name:
        for it in items:
            low = it.get("name", "").lower()
            if low.endswith(".xml") and "primary_doc" not in low:
                name = it["name"]; break
    if not name:
        return []
    return parse_info_table(sec_get(base + name).content)

def value_multiplier(report_date):
    try:
        return 1.0 if int((report_date or "")[:4]) >= 2023 else 1000.0
    except ValueError:
        return 1000.0

def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0

def apply_value_units(raw):
    """Normalize 13F 'value' to whole dollars by DETECTING the unit from the data,
    not the filing date. Within one filing, implied price = value/shares. If values
    are in thousands, the median implied price is ~1/1000 of a real share price
    (e.g. ~$0.25 instead of $254), so we multiply by 1000. Robust to filers who
    ignore the 2023 whole-dollar rule in either direction."""
    implied = [h["value"] / h["shares"] for h in raw
               if h.get("shares") and h.get("value") and h["shares"] > 0 and h["value"] > 0]
    m = _median(implied)
    mult = 1000.0 if (m is not None and m < 1.0) else 1.0
    return [{**h, "value": h["value"] * mult} for h in raw]

def _snap_factor(implied, target):
    """Power-of-1000 factor that brings `implied` closest (in ratio) to `target`."""
    best_f, best_r = 1.0, None
    for f in (1.0, 0.001, 1000.0, 1e-6, 1e6):
        v = implied * f
        if v <= 0:
            continue
        r = v / target if v >= target else target / v
        if best_r is None or r < best_r:
            best_r, best_f = r, f
    return best_f

def _snap_agg(agg):
    """Fix value-unit errors for one filing's aggregated holdings (in place).
    Uses the filing's median implied price; snaps all values by one 1000^k factor
    so the median lands in a sane $1–$10,000 share-price range."""
    imp = [d["value"] / d["shares"] for d in agg.values()
           if d.get("shares") and d["shares"] > 0 and d.get("value") and d["value"] > 0]
    m = _median(imp)
    if not m or m <= 0:
        return agg
    factor = 1.0
    while m * factor > 10000:
        factor /= 1000.0
    while m * factor < 1.0:
        factor *= 1000.0
    if factor != 1.0:
        for d in agg.values():
            if d.get("value"):
                d["value"] *= factor
    return agg

def _snap_holders(rows):
    """Fix value-unit errors across funds holding ONE security (in place), using
    the consensus (median) implied price. Leaves % of portfolio untouched (that's
    computed from raw values and is unit-invariant)."""
    imp = [r["value"] / r["shares"] for r in rows
           if r.get("shares") and r["shares"] > 0 and r.get("value") and r["value"] > 0]
    cons = _median(imp)
    if not cons or cons <= 0:
        return rows
    for r in rows:
        if r.get("shares") and r["shares"] > 0 and r.get("value"):
            r["value"] *= _snap_factor(r["value"] / r["shares"], cons)
    return rows

def migrate_fix_values():
    """One-time, idempotent repair of already-stored values that were normalized
    with the old date-based rule. Per filing: if the median implied price is wildly
    high (a dollars filing wrongly x1000'd) divide by 1000; if wildly low (a
    thousands filing left as-is) multiply by 1000. Normal filings are left untouched."""
    con = connect()
    try:
        accs = [r["accession"] for r in con.execute("SELECT DISTINCT accession FROM holdings").fetchall()]
        fixed = 0
        for acc in accs:
            rows = con.execute("SELECT shares,value FROM holdings WHERE accession=? AND shares>0 AND value>0",
                               (acc,)).fetchall()
            implied = [r["value"] / r["shares"] for r in rows]
            m = _median(implied)
            if m is None:
                continue
            factor = 0.001 if m > 20000 else (1000.0 if m < 1.0 else None)
            if factor:
                con.execute("UPDATE holdings SET value=value*? WHERE accession=?", (factor, acc))
                fixed += 1
        con.commit()
        return fixed
    finally:
        con.close()

# ---------------------------------------------------------------------------
# Ticker <-> company name (so users can search "AAPL" or "Apple")
# 13F filings have NO ticker, only issuer name. SEC publishes a free
# ticker<->name list; we match it against holding names by normalization.
# ---------------------------------------------------------------------------
_SUFFIX = {"INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LTD",
           "LIMITED", "PLC", "LLC", "LP", "NV", "SA", "AG", "THE", "HOLDINGS",
           "HLDGS", "HLDG", "HOLDING", "GROUP", "GRP", "TRUST", "CLASS", "CL",
           "COM", "COMMON", "SHARES", "ADR", "ADS", "NEW", "SPONSORED", "PAR"}

# Common 13F name abbreviations expanded to the full words SEC's official ticker
# list uses, so e.g. "RESTAURANT BRANDS INTL" matches "Restaurant Brands
# International". Only well-known, low-ambiguity expansions (a wrong expansion
# simply fails to match — names must be exactly equal after normalizing — so the
# worst case is the same blank we already had, never a wrong ticker).
_ABBREV = {
    "INTL": "INTERNATIONAL", "INTERNATL": "INTERNATIONAL", "INTERNATL'": "INTERNATIONAL",
    "ENTMT": "ENTERTAINMENT", "ENTMNT": "ENTERTAINMENT", "ENTERTAINMNT": "ENTERTAINMENT",
    "CMNCTNS": "COMMUNICATIONS", "COMMUNICATNS": "COMMUNICATIONS", "COMMS": "COMMUNICATIONS",
    "CMNTYS": "COMMUNITIES", "CMNTY": "COMMUNITY",
    "FINL": "FINANCIAL", "SVCS": "SERVICES", "SVC": "SERVICES",
    "MGMT": "MANAGEMENT", "NATL": "NATIONAL", "PETE": "PETROLEUM",
    "HLTH": "HEALTH", "SYS": "SYSTEMS", "MTRS": "MOTORS", "TECH": "TECHNOLOGIES",
    "PHARMACEUTICAL": "PHARMACEUTICALS", "RES": "RESOURCES",
}

def _norm(s):
    s = (s or "").upper()
    s = re.sub(r"[.'`&]", "", s)              # delete apostrophes/periods/& (MOODY'S -> MOODYS)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    toks = [_ABBREV.get(t, t) for t in s.split()]            # expand known abbreviations
    toks = [t for t in toks if t and t not in _SUFFIX and len(t) > 1]
    return " ".join(toks)

# Reliable CUSIP -> ticker for common holdings whose 13F names don't match the
# official ticker list (abbreviations like "BANK AMERICA", "OCCIDENTAL PETE").
CUSIP_TICKER = {
    "060505104": "BAC", "674599105": "OXY", "H1467J104": "CB", "615369105": "MCO",
    "829933100": "SIRI", "92343E102": "VRSN", "14040H105": "COF", "650111107": "NYT",
    "02005N100": "ALLY", "546347105": "LPX", "47233W109": "JEF", "25754A201": "DPZ",
    "G0403H108": "AON", "73278L105": "POOL", "422806208": "HEI", "037833100": "AAPL",
    "025816109": "AXP", "191216100": "KO", "166764100": "CVX", "500754106": "KHC",
    "23918K108": "DVA", "501044101": "KR", "247361702": "DAL", "02079K305": "GOOGL",
    "02079K107": "GOOG", "92826C839": "V", "57636Q104": "MA", "91324P102": "UNH",
    "023135106": "AMZN", "21036P108": "STZ", "62944T105": "NVR", "670346105": "NUE",
    "526057104": "LEN", "16119P108": "CHTR", "512816109": "LAMR",
    # Names whose 13F spelling doesn't match SEC's official list (foreign issuers,
    # spinoffs, abbreviations). CUSIP is exact, so this is the most reliable match.
    "11271J107": "BN",   "76131D103": "QSR",  "812215200": "SEG",
    "90353T100": "UBER", "594918104": "MSFT", "30303M102": "META",
    "44267T102": "HHH",  "42806J700": "HTZ",
}

# Manual shares-outstanding fallback for foreign private issuers (SEC Form 40-F)
# that do NOT report the dei:EntityCommonStockSharesOutstanding cover-page tag,
# so % owned can still be computed. (shares, as_of_date). Update occasionally.
SHARES_OUT_OVERRIDE = {
    # Curated total shares outstanding (all classes) for issuers SEC's per-concept
    # XBRL API can't serve: foreign 40-F filers, and multi-class US filers that report
    # shares only with per-class dimensions (so companyconcept returns NoSuchKey).
    # Values are point-in-time from each issuer's latest 10-Q/10-K/40-F cover page and
    # drift slowly with buybacks/issuance — refresh occasionally.
    "BN":    (2450808038.0, "2026-05-15"),  # Brookfield Corp Class A (40-F; absent from SEC XBRL)
    "META":  (2538423304.0, "2026-04-24"),  # Class A 2,196,045,588 + Class B 342,377,716
    "NYT":   (161862699.0,  "2026-05-01"),  # Class A 161,081,975 + Class B 780,724
    "LEN":   (246290000.0,  "2026-04-22"),  # Class A 215.24M + Class B 31.05M
    "STZ":   (172198467.0,  "2026-04-17"),  # Class A 172,172,544 + Class 1 25,923
    "LLYVA": (92003750.0,   "2026-03-31"),  # Liberty Live Series A 25,573,685 + B 2,530,951 + C 63,899,114
}

_TMAPS = None
_NORM2T_NS = {}   # spaceless normalized name -> ticker (catches "SIRIUS XM" vs "SIRIUSXM")
def _load_company_tickers():
    path = os.path.join(HERE, "tickers.json")
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception:
            pass
    data = sec_get("https://www.sec.gov/files/company_tickers.json").json()
    try:
        json.dump(data, open(path, "w"))
    except Exception:
        pass
    return data

_T2CIK = {}
def ticker_maps():
    """Return (ticker->title, normalized_name->ticker). Cached; needs network once."""
    global _TMAPS, _T2CIK
    if _TMAPS is not None:
        return _TMAPS
    t2title, norm2t = {}, {}
    try:
        for v in _load_company_tickers().values():
            t = (v.get("ticker") or "").upper()
            title = v.get("title") or ""
            if not t:
                continue
            t2title.setdefault(t, title)
            if v.get("cik_str"):
                _T2CIK[t] = str(v["cik_str"]).zfill(10)
            n = _norm(title)
            if n and n not in norm2t:
                norm2t[n] = t
            ns = n.replace(" ", "")
            if ns and ns not in _NORM2T_NS:
                _NORM2T_NS[ns] = t
    except Exception:
        pass
    _TMAPS = (t2title, norm2t)
    return _TMAPS

def ticker_for(name, cusip=None):
    if cusip and cusip.upper() in CUSIP_TICKER:   # reliable CUSIP match first
        return CUSIP_TICKER[cusip.upper()]
    _, norm2t = ticker_maps()
    n = _norm(name)
    return norm2t.get(n) or _NORM2T_NS.get(n.replace(" ", ""))  # spaced, then spaceless

def cik_for_name(name):
    ticker_maps()
    t = ticker_for(name)
    return _T2CIK.get(t) if t else None

# ---------------------------------------------------------------------------
# Enrichment: sector (SEC SIC), shares outstanding (SEC XBRL), 12m return (price)
# All best-effort + cached. Never blocks/breaks the core 13F view.
# ---------------------------------------------------------------------------
def _http_get(url, headers=None, timeout=15):
    h = {"User-Agent": "Mozilla/5.0 (compatible; WhaleWatch/1.0)"}
    if headers:
        h.update(headers)
    r = requests.get(url, headers=h, timeout=timeout)
    r.raise_for_status()
    return r

def _company_sic(cik):
    try:
        return get_submissions(cik).get("sicDescription") or ""
    except Exception:
        return ""

def _shares_outstanding(cik):
    """Latest reported common shares outstanding via SEC XBRL. (value, asof_date)."""
    for taxo, tag in (("dei", "EntityCommonStockSharesOutstanding"),
                      ("us-gaap", "CommonStockSharesOutstanding")):
        try:
            url = "https://data.sec.gov/api/xbrl/companyconcept/CIK%s/%s/%s.json" % (
                str(int(cik)).zfill(10), taxo, tag)
            j = sec_get(url).json()
            vals = []
            for arr in j.get("units", {}).values():
                for it in arr:
                    if it.get("val") and it.get("end"):
                        vals.append((it["end"], float(it["val"])))
            if vals:
                vals.sort()
                return vals[-1][1], vals[-1][0]
        except Exception:
            continue
    return None, None

def _ret_12m(ticker):
    """Trailing ~12-month price return from a free price feed. Best-effort."""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%s?range=1y&interval=1mo" % ticker
        res = _http_get(url).json()["chart"]["result"][0]
        closes = [c for c in res["indicators"]["quote"][0]["close"] if c is not None]
        if len(closes) >= 2 and closes[0]:
            return (closes[-1] - closes[0]) / closes[0]
    except Exception:
        pass
    return None

def _cached_security(cusip):
    con = connect()
    r = con.execute("SELECT * FROM securities WHERE cusip=?", (cusip,)).fetchone()
    con.close()
    return dict(r) if r else None

def enrich_security(cusip, name, max_price_age_days=7):
    """Resolve ticker/cik, fetch sector + shares outstanding + 12m return; cache."""
    cusip = (cusip or "").upper()
    cached = _cached_security(cusip)
    fresh_price = False
    if cached and cached.get("priced_at"):
        try:
            age = dt.datetime.utcnow() - dt.datetime.fromisoformat(cached["priced_at"])
            fresh_price = age.days < max_price_age_days
        except Exception:
            pass
    # Only skip re-resolving if we already have a real ticker, fresh price, AND
    # shares outstanding — unless there's no CIK to look shares up from anyway.
    # (A blank/unmatched cache, or one missing shares we could still fetch, must
    #  be retried — that's what left names like "BANK AMERICA CORP" / "META" empty
    #  after a transient SEC hiccup during the nightly bulk enrich.)
    if (cached and cached.get("ticker") and cached.get("sector") and fresh_price
            and (cached.get("shares_out") or not cached.get("cik"))):
        return cached

    ticker = (cached or {}).get("ticker") or ticker_for(name, cusip)
    ticker_maps()  # ensure ticker->CIK map is loaded
    cik = (cached or {}).get("cik") or (_T2CIK.get(ticker) if ticker else None)
    sector = (cached or {}).get("sector")
    shares_out = (cached or {}).get("shares_out")
    shares_date = (cached or {}).get("shares_out_date")
    if cik and not sector:
        sector = _company_sic(cik)
    if cik and not shares_out:
        shares_out, shares_date = _shares_outstanding(cik)
    if not shares_out and ticker in SHARES_OUT_OVERRIDE:   # foreign filers missing from SEC XBRL
        shares_out, shares_date = SHARES_OUT_OVERRIDE[ticker]
    # Additive-only: a failed live fetch must never blank a value we already had.
    fetched_ret = _ret_12m(ticker) if ticker else None
    ret = fetched_ret if fetched_ret is not None else (cached or {}).get("ret_12m")

    con = connect()
    con.execute("""INSERT OR REPLACE INTO securities
        (cusip,ticker,cik,sector,shares_out,shares_out_date,ret_12m,enriched_at,priced_at)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (cusip, ticker, cik, sector or "", shares_out, shares_date, ret,
         _now(), _now() if fetched_ret is not None else (cached or {}).get("priced_at")))
    con.commit(); con.close()
    return _cached_security(cusip)

def aggregate(holdings):
    agg = {}
    for h in holdings:
        k = h["cusip"] or h["name"]
        a = agg.setdefault(k, {"cusip": h["cusip"], "name": h["name"], "class": h["class"],
                               "value": 0.0, "shares": 0.0, "putCall": h.get("putCall", "")})
        a["value"] += h["value"]; a["shares"] += h["shares"]
    return agg

def diff(curr_rows, prev_rows):
    rows = []
    for k, c in curr_rows.items():
        if k in prev_rows:
            ps = prev_rows[k]["shares"]
            if ps == 0:
                status, pct = "NEW", None
            else:
                ch = (c["shares"] - ps) / ps
                status = "HOLD" if abs(ch) < 1e-9 else ("ADDED" if ch > 0 else "TRIMMED")
                pct = round(ch, 4)
        else:
            status, pct = "NEW", None
        rows.append({**c, "status": status, "sharesChangePct": pct})
    for k, p in prev_rows.items():
        if k not in curr_rows:
            rows.append({**p, "shares": 0.0, "status": "SOLD", "sharesChangePct": -1.0})
    # current holdings first (by value, like Wisdom Whale); sold-out at the bottom
    rows.sort(key=lambda r: (r["status"] == "SOLD", -r["value"]))
    return rows

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def connect():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    try: con.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError: pass
    return con

def init_schema():
    con = connect()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS funds (
        cik TEXT PRIMARY KEY, name TEXT NOT NULL, last_filing_date TEXT,
        holdings_status TEXT DEFAULT 'none', ingested_at TEXT);
    CREATE TABLE IF NOT EXISTS filings (
        accession TEXT PRIMARY KEY, cik TEXT NOT NULL, form TEXT,
        filing_date TEXT, report_date TEXT);
    CREATE TABLE IF NOT EXISTS holdings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, accession TEXT NOT NULL, cik TEXT NOT NULL,
        report_date TEXT, cusip TEXT, name TEXT, class TEXT, value REAL, shares REAL, put_call TEXT);
    CREATE TABLE IF NOT EXISTS fund_filings (
        accession TEXT PRIMARY KEY, cik TEXT NOT NULL, form TEXT,
        filing_date TEXT, report_date TEXT);
    CREATE TABLE IF NOT EXISTS securities (
        cusip TEXT PRIMARY KEY, ticker TEXT, cik TEXT, sector TEXT,
        shares_out REAL, shares_out_date TEXT,
        ret_12m REAL, enriched_at TEXT, priced_at TEXT);
    CREATE INDEX IF NOT EXISTS idx_catalog_cik    ON fund_filings(cik);
    CREATE INDEX IF NOT EXISTS idx_funds_name     ON funds(name);
    CREATE INDEX IF NOT EXISTS idx_filings_cik    ON filings(cik);
    CREATE INDEX IF NOT EXISTS idx_holdings_cusip ON holdings(cusip);
    CREATE INDEX IF NOT EXISTS idx_holdings_cik   ON holdings(cik);
    CREATE INDEX IF NOT EXISTS idx_holdings_accn  ON holdings(accession);
    """)
    con.commit(); con.close()

def _now():
    return dt.datetime.utcnow().isoformat(timespec="seconds")

def upsert_fund(cik, name, last_filing_date=None):
    con = connect()
    con.execute("""INSERT INTO funds (cik,name,last_filing_date) VALUES (?,?,?)
        ON CONFLICT(cik) DO UPDATE SET name=excluded.name,
        last_filing_date=COALESCE(MAX(excluded.last_filing_date,funds.last_filing_date),
                                  excluded.last_filing_date,funds.last_filing_date)""",
        (cik, name, last_filing_date))
    con.commit(); con.close()

def upsert_funds_bulk(rows):
    con = connect()
    con.executemany("""INSERT INTO funds (cik,name,last_filing_date) VALUES (?,?,?)
        ON CONFLICT(cik) DO UPDATE SET name=excluded.name,
        last_filing_date=COALESCE(MAX(excluded.last_filing_date,funds.last_filing_date),
                                  excluded.last_filing_date,funds.last_filing_date)""", list(rows))
    con.commit(); con.close()

def search_funds(q, limit=25):
    con = connect()
    if q.strip().isdigit():
        rows = con.execute("SELECT * FROM funds WHERE cik=?", (q.strip().zfill(10),)).fetchall()
    else:
        rows = con.execute("""SELECT * FROM funds WHERE name LIKE ? COLLATE NOCASE
            ORDER BY (holdings_status='ingested') DESC, last_filing_date DESC LIMIT ?""",
            ("%" + q.strip() + "%", limit)).fetchall()
    con.close(); return [dict(r) for r in rows]

def get_fund(cik):
    con = connect()
    r = con.execute("SELECT * FROM funds WHERE cik=?", (str(cik).zfill(10),)).fetchone()
    con.close(); return dict(r) if r else None

def count_funds():
    con = connect()
    n = con.execute("SELECT COUNT(*) c FROM funds").fetchone()["c"]
    ing = con.execute("SELECT COUNT(*) c FROM funds WHERE holdings_status='ingested'").fetchone()["c"]
    con.close(); return n, ing

def funds_needing_refresh():
    con = connect()
    rows = con.execute("SELECT cik,name FROM funds WHERE holdings_status='ingested'").fetchall()
    con.close(); return [dict(r) for r in rows]

def has_filing(accession):
    con = connect()
    r = con.execute("SELECT 1 FROM filings WHERE accession=?", (accession,)).fetchone()
    con.close(); return r is not None

def save_filing(cik, accession, form, filing_date, report_date, holdings):
    cik = str(cik).zfill(10); con = connect()
    # make sure a fund row exists so cross-fund joins never silently drop data
    con.execute("INSERT OR IGNORE INTO funds (cik,name) VALUES (?,?)", (cik, cik))
    con.execute("INSERT OR REPLACE INTO filings (accession,cik,form,filing_date,report_date) VALUES (?,?,?,?,?)",
                (accession, cik, form, filing_date, report_date))
    con.execute("DELETE FROM holdings WHERE accession=?", (accession,))
    con.executemany("""INSERT INTO holdings (accession,cik,report_date,cusip,name,class,value,shares,put_call)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        [(accession, cik, report_date, h["cusip"], h["name"], h["class"], h["value"],
          h["shares"], h.get("putCall", "")) for h in holdings])
    con.execute("""UPDATE funds SET holdings_status='ingested', ingested_at=?,
        last_filing_date=COALESCE(MAX(?,last_filing_date),?) WHERE cik=?""",
        (_now(), filing_date, filing_date, cik))
    con.commit(); con.close()

def get_fund_filings(cik):
    con = connect()
    rows = con.execute("SELECT * FROM filings WHERE cik=? ORDER BY report_date DESC, filing_date DESC",
                       (str(cik).zfill(10),)).fetchall()
    con.close(); return [dict(r) for r in rows]

def get_holdings(accession):
    con = connect()
    rows = con.execute("SELECT * FROM holdings WHERE accession=?", (accession,)).fetchall()
    con.close(); return [dict(r) for r in rows]

def holders_of(cusip=None, name_like=None, limit=100):
    con = connect()
    latest = """SELECT cik,accession FROM (
        SELECT cik,accession,
          ROW_NUMBER() OVER (PARTITION BY cik ORDER BY report_date DESC, accession DESC) rn
        FROM filings) WHERE rn=1"""
    where, params = [], []
    if cusip: where.append("h.cusip=?"); params.append(cusip.upper())
    if name_like: where.append("h.name LIKE ? COLLATE NOCASE"); params.append("%" + name_like + "%")
    wc = (" AND " + " AND ".join(where)) if where else ""
    sql = ("SELECT h.cusip,h.name AS security,h.value,h.shares,fn.name AS fund,h.cik,h.report_date "
           "FROM holdings h JOIN (%s) l ON h.accession=l.accession "
           "JOIN funds fn ON fn.cik=h.cik WHERE 1=1 %s ORDER BY h.value DESC LIMIT ?" % (latest, wc))
    params.append(limit)
    rows = con.execute(sql, params).fetchall(); con.close()
    return [dict(r) for r in rows]

def distinct_securities(q, limit=20):
    con = connect()
    rows = con.execute("""SELECT cusip,name,COUNT(DISTINCT cik) AS holders,SUM(value) AS total
        FROM holdings WHERE name LIKE ? COLLATE NOCASE GROUP BY cusip
        ORDER BY holders DESC, total DESC LIMIT ?""", ("%" + q + "%", limit)).fetchall()
    con.close(); return [dict(r) for r in rows]

def stock_holders(cusip, shares_out=None, limit=100):
    """Per-fund detail for one security (Wisdom-Whale-style columns):
    value, shares, % of shares outstanding, change in shares (# and %),
    % of that fund's portfolio, and filing date."""
    cusip = cusip.upper()
    con = connect()
    # exactly ONE filing per fund (newest; amendment wins on ties) — avoids dup rows
    latest = """SELECT cik,accession,report_date FROM (
        SELECT cik,accession,report_date,
          ROW_NUMBER() OVER (PARTITION BY cik ORDER BY report_date DESC, accession DESC) rn
        FROM filings) WHERE rn=1"""
    rows = con.execute("""SELECT h.shares,h.value,fn.name AS fund,h.cik,l.accession,l.report_date
        FROM holdings h JOIN (%s) l ON h.accession=l.accession
        JOIN funds fn ON fn.cik=h.cik WHERE h.cusip=?
        ORDER BY h.value DESC LIMIT ?""" % latest, (cusip, limit)).fetchall()
    out = []
    for r in rows:
        cik, acc, rd = r["cik"], r["accession"], r["report_date"]
        ftot = con.execute("SELECT SUM(value) t FROM holdings WHERE accession=?", (acc,)).fetchone()["t"] or 0.0
        prior = con.execute("""SELECT accession FROM filings WHERE cik=? AND report_date<?
            ORDER BY report_date DESC LIMIT 1""", (cik, rd)).fetchone()
        prev = None
        if prior:
            pr = con.execute("SELECT shares FROM holdings WHERE accession=? AND cusip=?",
                             (prior["accession"], cusip)).fetchone()
            prev = pr["shares"] if pr else 0.0
        chg = (r["shares"] - prev) if prev is not None else None
        if prev is None:
            chgpct, status = None, None
        elif prev == 0:
            chgpct, status = None, "NEW"
        else:
            chgpct = (r["shares"] - prev) / prev
            status = "HOLD" if abs(chgpct) < 1e-9 else ("ADDED" if chgpct > 0 else "TRIMMED")
        out.append({
            "fund": r["fund"], "cik": cik, "shares": r["shares"], "value": r["value"],
            "pctSharesOut": (r["shares"] / shares_out) if shares_out else None,
            "changeShares": chg, "changePct": chgpct, "status": status,
            "pctPortfolio": (r["value"] / ftot) if ftot else None,
            "reportDate": rd,
        })
    con.close()
    _snap_holders(out)                       # fix any 1000x value-unit errors at read time
    out.sort(key=lambda r: -(r["value"] or 0))  # rank by corrected value (= true position size)
    return out

# ---------------------------------------------------------------------------
# Ingest + payload
# ---------------------------------------------------------------------------
def record_catalog(cik, filings_list):
    """Store metadata for ALL of a fund's 13F filings (not the holdings) so the
    period dropdown can list every quarter without re-hitting EDGAR."""
    cik = str(cik).zfill(10)
    con = connect()
    con.executemany("""INSERT OR REPLACE INTO fund_filings
        (accession,cik,form,filing_date,report_date) VALUES (?,?,?,?,?)""",
        [(f["accession"], cik, f["form"], f["filingDate"], f["reportDate"]) for f in filings_list])
    con.commit(); con.close()

def get_catalog(cik):
    """All known 13F filings for a fund, newest first."""
    con = connect()
    rows = con.execute("""SELECT accession,form,filing_date,report_date FROM fund_filings
        WHERE cik=? ORDER BY report_date DESC, filing_date DESC""", (str(cik).zfill(10),)).fetchall()
    con.close(); return [dict(r) for r in rows]

def ensure_catalog(cik):
    """Make sure the filing catalog exists; fetch from EDGAR once if empty."""
    cat = get_catalog(cik)
    if cat:
        return cat
    info = list_13f_filings(cik)
    upsert_fund(cik, info["name"])
    if info["filings"]:
        record_catalog(cik, info["filings"])
    return get_catalog(cik)

def ensure_ingested(cik, accession, form, filing_date, report_date):
    """Ingest one specific filing's holdings if we don't already have them."""
    if has_filing(accession):
        return
    raw = fetch_info_table(cik, accession)
    agg = aggregate(apply_value_units(raw))
    save_filing(cik, accession, form, filing_date, report_date, list(agg.values()))

def ingest_fund(cik, max_filings=2, force=False):
    cik = str(cik).zfill(10)
    info = list_13f_filings(cik)
    upsert_fund(cik, info["name"])
    if info["filings"]:
        record_catalog(cik, info["filings"])  # remember every quarter for the dropdown
    new = 0
    for f in info["filings"][:max_filings]:
        if not force and has_filing(f["accession"]):
            continue
        try:
            raw = fetch_info_table(cik, f["accession"])
        except Exception as e:
            sys.stderr.write("  ! %s %s: %s\n" % (cik, f["accession"], e)); continue
        agg = aggregate(apply_value_units(raw))
        save_filing(cik, f["accession"], f["form"], f["filingDate"], f["reportDate"], list(agg.values()))
        new += 1
    return new

def build_fund_payload(cik, period=None):
    """Holdings + QoQ for one quarter. period = a reportDate or accession; default = latest."""
    cik = str(cik).zfill(10)
    fund = get_fund(cik) or {"name": cik, "cik": cik}
    cat = ensure_catalog(cik)
    if not cat:
        return {"error": "No 13F-HR filings found", "name": fund["name"], "cik": cik}
    # pick the selected quarter (default newest)
    curr = None
    if period:
        curr = next((f for f in cat if f["report_date"] == period or f["accession"] == period), None)
    if curr is None:
        curr = cat[0]
    idx = cat.index(curr)
    prev = cat[idx + 1] if idx + 1 < len(cat) else None
    # make sure the holdings for current (and prior, for the diff) are loaded.
    # Each is an independent EDGAR download; fetch them concurrently so first-open
    # latency is ~one filing instead of two back-to-back.
    if prev:
        with ThreadPoolExecutor(max_workers=2) as ex:
            fc = ex.submit(ensure_ingested, cik, curr["accession"], curr["form"], curr["filing_date"], curr["report_date"])
            fp = ex.submit(ensure_ingested, cik, prev["accession"], prev["form"], prev["filing_date"], prev["report_date"])
            fc.result()                      # current-quarter errors propagate as before
            try:
                fp.result()
            except Exception:
                prev = None                  # prior is best-effort (only needed for the diff)
    else:
        ensure_ingested(cik, curr["accession"], curr["form"], curr["filing_date"], curr["report_date"])
    curr_rows = _snap_agg(aggregate(get_holdings(curr["accession"])))
    if prev:
        holdings = diff(curr_rows, _snap_agg(aggregate(get_holdings(prev["accession"]))))
    else:
        holdings = sorted([{**c, "status": "NEW", "sharesChangePct": None} for c in curr_rows.values()],
                          key=lambda r: r["value"], reverse=True)
    active = [h for h in holdings if h["status"] != "SOLD"]
    total = sum(h["value"] for h in active) or 0.0
    for h in holdings:
        h["pctPortfolio"] = (h["value"] / total) if (total and h["status"] != "SOLD") else None
        _attach_cached_enrichment(h)
    return {"name": fund["name"], "cik": cik,
            "current": {"form": curr["form"], "filingDate": curr["filing_date"], "reportDate": curr["report_date"]},
            "previous": ({"reportDate": prev["report_date"]} if prev else None),
            "totalValue": total, "positions": len(active), "holdings": holdings,
            "periods": _dedupe_periods(cat),
            "selectedPeriod": curr["report_date"]}

def _dedupe_periods(cat):
    seen, out = set(), []
    for f in cat:  # newest first; keep first (latest accession) per report date
        if f["report_date"] in seen:
            continue
        seen.add(f["report_date"])
        out.append({"reportDate": f["report_date"], "accession": f["accession"]})
    return out

def fund_value_history(cik, max_quarters=40):
    """Total reported 13F holdings value for each of a fund's quarters, oldest→newest.
    Ingests any missing filings from EDGAR once (then cached in the DB); SEC calls are
    globally rate-limited in sec_get, so the workers just parse in parallel. This is
    the sum of long US-listed positions a 13F discloses — NOT the manager's total AUM."""
    cik = str(cik).zfill(10)
    cat = ensure_catalog(cik)
    # one filing per quarter (catalog is newest-first; keep the newest accession),
    # then cap to the most recent N quarters to bound first-load cost.
    seen, picks = set(), []
    for f in cat:
        rd = f["report_date"]
        if not rd or rd in seen:
            continue
        seen.add(rd); picks.append(f)
    picks = picks[:max_quarters]
    missing = [f for f in picks if not has_filing(f["accession"])]
    if missing:
        def _ing(f):
            try:
                ensure_ingested(cik, f["accession"], f["form"], f["filing_date"], f["report_date"])
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(_ing, missing))
    out = []
    for f in picks:
        rows = get_holdings(f["accession"])
        if not rows:
            continue
        agg = _snap_agg(aggregate(rows))             # same value-unit correction as the holdings view
        total = sum((h.get("value") or 0.0) for h in agg.values())
        if total:
            out.append({"period": f["report_date"], "value": total})
    out.sort(key=lambda r: r["period"])              # oldest → newest for the chart
    return out

def _attach_cached_enrichment(h):
    """Add ticker/sector/sharesOutstanding/pctOwnership/ret12m from cache (if any)."""
    cu = (h.get("cusip") or "").upper()
    sec = _cached_security(cu)
    h["ticker"] = (sec or {}).get("ticker") or CUSIP_TICKER.get(cu) or ticker_for(h.get("name", ""), cu)
    h["sector"] = (sec or {}).get("sector") or None
    so = (sec or {}).get("shares_out")
    h["sharesOutstanding"] = so
    h["pctOwnership"] = (h.get("shares") / so) if (so and h.get("shares")) else None
    h["ret12m"] = (sec or {}).get("ret_12m")
    return h

def enrich_fund(cik, period=None, cap=40):
    """Live-enrich a fund's current holdings (best-effort, cached), then return
    the refreshed payload. Bounded to the top `cap` positions by value."""
    payload = build_fund_payload(cik, period)
    if payload.get("error"):
        return payload
    active = [h for h in payload["holdings"] if h["status"] != "SOLD"][:cap]
    def _one(h):
        try:
            enrich_security(h["cusip"], h["name"])
        except Exception:
            pass
    if active:
        # Fetch holdings concurrently. SEC stays rate-limited by _throttle();
        # the win is overlapping the slower Yahoo price calls + DB writes.
        with ThreadPoolExecutor(max_workers=6) as ex:
            list(ex.map(_one, active))
    return build_fund_payload(cik, period)  # rebuild with freshly cached values

# Official website per well-known manager, matched by NAME keyword. URLs only —
# we deliberately do NOT keep hand-written strategy/people blurbs: those aren't in
# SEC filings, can't be verified per-fund, and go stale (e.g. CEO changes). Users
# click through to the fund's own site for the real story. Only include a URL here
# if it's the firm's verified official site; otherwise leave the fund out (it falls
# back to the SEC EDGAR + web-search links).
CURATED_SITES = [
    ("BERKSHIRE", "berkshirehathaway.com"),
    ("PERSHING SQUARE", "pershingsquareholdings.com"),
    ("BRIDGEWATER", "bridgewater.com"),
    ("CITADEL", "citadel.com"),
    ("RENAISSANCE TECH", "rentec.com"),
    ("TIGER GLOBAL", "tigerglobal.com"),
    ("GREENLIGHT", "greenlightcapital.com"),
    ("THIRD POINT", "thirdpoint.com"),
    ("BAUPOST", "baupost.com"),
    ("ARK INVEST", "ark-invest.com"),
]

def _curated_site(name):
    up = (name or "").upper()
    for kw, site in CURATED_SITES:
        if kw in up:
            return site
    return None

def fund_info(cik):
    """Profile panel data: SEC-derived facts + links, plus a curated blurb if known."""
    cik = str(cik).zfill(10)
    name, location, sic = cik, None, None
    try:
        sub = get_submissions(cik)
        name = sub.get("name", cik)
        sic = sub.get("sicDescription") or None
        b = (sub.get("addresses") or {}).get("business") or {}
        location = ", ".join([x for x in [b.get("city"), b.get("stateOrCountry")] if x]) or None
    except Exception:
        f = get_fund(cik)
        if f:
            name = f["name"]
    cat = get_catalog(cik)
    return {
        "cik": cik, "name": name, "location": location, "sic": sic,
        "filingsCount": len(cat),
        "firstPeriod": cat[-1]["report_date"] if cat else None,
        "latestPeriod": cat[0]["report_date"] if cat else None,
        "edgarUrl": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=13F" % cik,
        "searchUrl": "https://www.google.com/search?q=" + requests.utils.quote((name or "") + " investment firm"),
        "website": _curated_site(name),
    }

def build_directory(quarters=8):
    init_schema()
    best = {}
    for year, qtr in quarters_back(quarters):
        try:
            rows = fetch_quarter_filers(year, qtr)
        except Exception as e:
            print("  %d QTR%d: skipped (%s)" % (year, qtr, e)); continue
        for r in rows:
            if r["cik"] not in best or r["date"] > best[r["cik"]][1]:
                best[r["cik"]] = (r["name"], r["date"])
        print("  %d QTR%d: %d filings (universe now %d funds)" % (year, qtr, len(rows), len(best)))
    upsert_funds_bulk((c, nm, d) for c, (nm, d) in best.items())
    return len(best)

# A few well-known managers to seed the Stocks/Alerts tabs with recognizable
# names. The real fund name is always re-fetched from EDGAR on ingest, so even
# if a label here is off, stored data stays correct.
CURATED_CIKS = ["0001067983", "0001649339", "0001336528", "0001350694",
                "0001423053", "0001037389", "0001167483", "0001079114",
                "0001040273", "0001061768", "0001029160", "0001697748"]

def preload(n=150):
    """Ingest curated famous funds + the N most-recently-active filers so the
    Stocks tab has real cross-fund data. Safe to re-run (skips existing)."""
    init_schema()
    con = connect()
    recent = [r["cik"] for r in con.execute(
        "SELECT cik FROM funds ORDER BY last_filing_date DESC LIMIT ?", (n,)).fetchall()]
    con.close()
    targets = list(dict.fromkeys(CURATED_CIKS + recent))  # dedupe, keep order
    print("Preloading holdings for %d funds (curated + %d recent)..." % (len(targets), n))
    done = 0
    for i, cik in enumerate(targets, 1):
        try:
            ingest_fund(cik, max_filings=2, force=False)
            done += 1
        except Exception as e:
            sys.stderr.write("  ! %s: %s\n" % (cik, e))
        if i % 25 == 0:
            print("  ...%d/%d funds processed" % (i, len(targets)))
    total, ing = count_funds()
    print("Done. %d funds now have holdings loaded (Stocks tab is ready)." % ing)
    return done

def fund_highlights(cik):
    """One fund's latest-filing summary for the Alerts feed."""
    p = build_fund_payload(cik)
    if p.get("error"):
        return {"cik": str(cik).zfill(10), "name": p.get("name", cik), "error": p["error"]}
    h = p["holdings"]
    counts = {}
    for x in h:
        counts[x["status"]] = counts.get(x["status"], 0) + 1
    def first(*statuses):
        for x in h:  # h is sorted by value desc
            if x["status"] in statuses:
                return x
        return None
    buy = first("NEW")
    add = first("ADDED")
    exit_ = first("SOLD") or first("TRIMMED")
    return {
        "cik": str(cik).zfill(10), "name": p["name"],
        "reportDate": p["current"]["reportDate"], "filingDate": p["current"]["filingDate"],
        "totalValue": p["totalValue"], "positions": p["positions"], "counts": counts,
        "topBuy": ({"name": buy["name"], "value": buy["value"]} if buy else None),
        "topAdd": ({"name": add["name"], "pct": add["sharesChangePct"]} if add else None),
        "topExit": ({"name": exit_["name"], "status": exit_["status"]} if exit_ else None),
    }

def enrich_held(limit=1200):
    """Enrich (sector / shares outstanding / 12-mo return) every distinct security
    held by an ingested fund and CACHE it in the DB, so the app serves these columns
    instantly instead of fetching them live on each page open. Run by the nightly job."""
    con = connect()
    rows = con.execute("""SELECT DISTINCT h.cusip, h.name FROM holdings h
        JOIN funds f ON f.cik=h.cik
        WHERE f.holdings_status='ingested' AND h.cusip IS NOT NULL AND h.cusip!=''
        LIMIT ?""", (limit,)).fetchall()
    con.close()
    done = {"n": 0}
    lock = threading.Lock()
    def _one(r):
        try:
            enrich_security(r["cusip"], r["name"])
            with lock:
                done["n"] += 1
        except Exception:
            pass
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(_one, rows))
    return done["n"]

_prewarm_started = [False]
def start_prewarm():
    """Warm the enrichment cache in the background on server start, so funds open
    already filled instead of waiting on live SEC/Yahoo fetches at page-open time.
    Render's disk is ephemeral (wiped on each deploy/restart), so this rebuilds the
    cache once per boot; already-cached securities are skipped by enrich_security."""
    if _prewarm_started[0]:
        return
    _prewarm_started[0] = True
    def _bg():
        try:
            time.sleep(3)  # let gunicorn finish binding first
            init_schema()
            n = enrich_held()
            print("[prewarm] cached enrichment for %d securities" % n)
        except Exception as e:
            sys.stderr.write("[prewarm] failed: %s\n" % e)
    threading.Thread(target=_bg, name="prewarm", daemon=True).start()

def refresh():
    init_schema()
    print("[%s] refresh start" % dt.datetime.now().isoformat(timespec="seconds"))
    n = migrate_fix_values()
    if n:
        print("  repaired value units in %d filing(s)" % n)
    seen = build_directory(2)
    funds = funds_needing_refresh(); updated = 0
    for f in funds:
        try:
            if ingest_fund(f["cik"], 2, force=False):
                updated += 1; print("  + %s (%s): new filing" % (f["name"], f["cik"]))
                # NOTE: fire push/email alert to watchers here.
        except Exception as e:
            sys.stderr.write("  ! %s: %s\n" % (f["cik"], e))
    print("  enriching held securities (sector / shares out / 12-mo return)…")
    en = enrich_held()
    print("  cached enrichment for %d securities" % en)
    total, ing = count_funds()
    print("[done] %d funds, %d with holdings (%d updated)" % (total, ing, updated))

# === embedded UI ===
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b0e11">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>WhaleWatch — 13F Tracker</title>
<style>
  :root{
    --bg:#0b0e11; --bg2:#11161c; --card:#161c24; --line:#222b35;
    --txt:#e8edf2; --mut:#8a97a6; --acc:#2ea1ff;
    --green:#16c784; --red:#ea3943; --amber:#f0b90b; --blue:#4b9fff;
    --r:14px;
  }
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
  html,body{margin:0;padding:0;background:var(--bg);color:var(--txt);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    -webkit-font-smoothing:antialiased}
  body{padding-bottom:78px;max-width:680px;margin:0 auto}
  a{color:var(--acc);text-decoration:none}
  .top{position:sticky;top:0;z-index:20;background:rgba(11,14,17,.92);
    backdrop-filter:blur(12px);border-bottom:1px solid var(--line);
    padding:max(env(safe-area-inset-top),12px) 16px 12px}
  .brand{display:flex;align-items:center;gap:9px;font-weight:700;font-size:19px;letter-spacing:-.3px}
  .brand .dot{font-size:22px}
  .brand small{font-weight:500;color:var(--mut);font-size:12px;margin-left:auto}
  .search{margin-top:12px;position:relative}
  .search input{width:100%;background:var(--bg2);border:1px solid var(--line);
    color:var(--txt);border-radius:12px;padding:13px 14px 13px 42px;font-size:16px;outline:none}
  .search input:focus{border-color:var(--acc)}
  .search svg{position:absolute;left:13px;top:13px;opacity:.6}
  .wrap{padding:16px}
  .muted{color:var(--mut)}
  .chips{display:flex;gap:8px;flex-wrap:wrap;margin:4px 0 0}
  .chip{background:var(--card);border:1px solid var(--line);color:var(--txt);
    padding:8px 13px;border-radius:999px;font-size:13.5px;cursor:pointer;white-space:nowrap}
  .chip.on{background:var(--acc);border-color:var(--acc);color:#fff}
  .sec-title{font-size:13px;text-transform:uppercase;letter-spacing:.08em;
    color:var(--mut);margin:22px 0 10px;font-weight:600}
  .card{background:var(--card);border:1px solid var(--line);border-radius:var(--r);
    padding:14px;margin-bottom:10px}
  .row{display:flex;align-items:center;gap:12px;cursor:pointer}
  .row .grow{flex:1;min-width:0}
  .row .nm{font-weight:600;font-size:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .row .sub{font-size:12.5px;color:var(--mut);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .star{font-size:20px;cursor:pointer;padding:4px;line-height:1;color:var(--mut)}
  .star.on{color:var(--amber)}
  /* fund header */
  .fhead{display:flex;flex-direction:column;gap:4px;margin-bottom:6px}
  .fhead .h1{font-size:22px;font-weight:700;letter-spacing:-.4px}
  .stats{display:flex;gap:10px;margin:14px 0 6px}
  .stat{flex:1;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}
  .stat .k{font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.05em}
  .stat .v{font-size:18px;font-weight:700;margin-top:3px}
  /* holding rows */
  .hrow{display:flex;align-items:center;gap:10px;padding:12px 2px;border-bottom:1px solid var(--line)}
  .hrow:last-child{border-bottom:none}
  .tkr{width:42px;height:42px;border-radius:11px;background:var(--bg2);border:1px solid var(--line);
    display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;flex-shrink:0;color:var(--acc)}
  .hrow .nm{font-weight:600;font-size:14.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .hrow .sub{font-size:12px;color:var(--mut);margin-top:2px}
  .right{text-align:right;flex-shrink:0}
  .val{font-weight:700;font-size:14.5px}
  .badge{display:inline-block;font-size:11px;font-weight:700;padding:3px 7px;border-radius:6px;margin-top:4px}
  .b-NEW{background:rgba(22,199,132,.15);color:var(--green)}
  .b-ADDED{background:rgba(22,199,132,.12);color:var(--green)}
  .b-TRIMMED{background:rgba(240,185,11,.14);color:var(--amber)}
  .b-SOLD{background:rgba(234,57,67,.15);color:var(--red)}
  .b-HOLD{background:rgba(138,151,166,.14);color:var(--mut)}
  .pct{font-size:12px;font-weight:600;margin-top:3px}
  .up{color:var(--green)} .dn{color:var(--red)}
  .scrollx{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid var(--line);border-radius:var(--r);background:var(--card)}
  .htbl{width:100%;border-collapse:collapse;font-size:12.5px;min-width:520px}
  .htbl th{font-size:9.5px;text-transform:uppercase;letter-spacing:.03em;color:var(--mut);font-weight:600;text-align:right;padding:8px 9px;border-bottom:1px solid var(--line);white-space:nowrap;position:sticky;top:0;background:var(--card);z-index:2}
  .htbl th.l{text-align:left;left:0;z-index:3}
  .htbl td{padding:9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap;vertical-align:middle}
  .htbl td.l{text-align:left;position:sticky;left:0;background:var(--card);max-width:170px;overflow:hidden;text-overflow:ellipsis}
  .htbl tr{cursor:pointer}
  .htbl tbody tr:hover td{background:var(--bg2)}
  .htbl tr:last-child td{border-bottom:none}
  .cellnm{font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis}
  .cellsub{font-size:10.5px;color:var(--mut);margin-top:1px}
  .filterbar{display:flex;gap:7px;overflow-x:auto;padding:4px 0 2px;margin:8px 0;-ms-overflow-style:none;scrollbar-width:none}
  .filterbar::-webkit-scrollbar{display:none}
  .back{display:inline-flex;align-items:center;gap:6px;color:var(--mut);font-size:14px;cursor:pointer;margin-bottom:10px}
  .empty{text-align:center;color:var(--mut);padding:48px 20px;font-size:14.5px;line-height:1.6}
  .empty .big{font-size:40px;margin-bottom:10px}
  .spin{text-align:center;color:var(--mut);padding:40px}
  .banner{background:rgba(46,161,255,.1);border:1px solid rgba(46,161,255,.3);
    color:var(--blue);border-radius:10px;padding:10px 12px;font-size:13px;margin-bottom:12px}
  .alertbtn{margin-top:14px;width:100%;padding:13px;border-radius:12px;border:1px solid var(--line);
    background:var(--bg2);color:var(--txt);font-size:15px;font-weight:600;cursor:pointer}
  .alertbtn.on{background:rgba(46,161,255,.15);border-color:var(--acc);color:var(--acc)}
  /* bottom nav */
  .nav{position:fixed;bottom:0;left:0;right:0;z-index:30;background:rgba(11,14,17,.95);
    backdrop-filter:blur(12px);border-top:1px solid var(--line);display:flex;
    max-width:680px;margin:0 auto;padding-bottom:env(safe-area-inset-bottom)}
  .nav a{flex:1;text-align:center;padding:10px 0 8px;color:var(--mut);font-size:11px;cursor:pointer}
  .nav a.on{color:var(--acc)}
  .nav .ic{font-size:21px;display:block;margin-bottom:2px}
</style>
</head>
<body>
<div class="top">
  <div class="brand"><span class="dot">🐋</span> WhaleWatch <small id="modeTag"></small></div>
  <div class="search">
    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="#8a97a6" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4-4"/></svg>
    <input id="q" placeholder="Search a fund (Berkshire, Scion, Pershing…)" autocomplete="off">
  </div>
</div>

<div class="wrap" id="app"></div>

<div class="nav">
  <a data-tab="search" class="on"><span class="ic">🔍</span>Funds</a>
  <a data-tab="stocks"><span class="ic">📈</span>Stocks</a>
  <a data-tab="watch"><span class="ic">⭐</span>Watchlist</a>
  <a data-tab="alerts"><span class="ic">🔔</span>Alerts</a>
</div>

<script>
const $=s=>document.querySelector(s), app=document.querySelector('#app');
const fmt=n=>{n=+n||0;const a=Math.abs(n);
  if(a>=1e12)return '$'+(n/1e12).toFixed(2)+'T';
  if(a>=1e9)return '$'+(n/1e9).toFixed(2)+'B';
  if(a>=1e6)return '$'+(n/1e6).toFixed(1)+'M';
  if(a>=1e3)return '$'+(n/1e3).toFixed(0)+'K';return '$'+n.toFixed(0);};
const pct=n=>(n>=0?'+':'')+(n*100).toFixed(1)+'%';
const tkr=nm=>{const w=(nm||'').replace(/[^A-Za-z ]/g,'').trim().split(/\s+/);
  return ((w[0]||'?')[0]+((w[1]||'')[0]||'')).toUpperCase();};

const POPULAR=[
  {cik:'0001067983',name:'Berkshire Hathaway'},
  {cik:'0001649339',name:'Scion Asset Mgmt (M. Burry)'},
  {cik:'0001336528',name:'Pershing Square (Ackman)'},
  {cik:'0001350694',name:'Bridgewater Associates'},
  {cik:'0001637460',name:'Renaissance Technologies'},
  {cik:'0001423053',name:'Citadel Advisors'},
];

// ---- state / storage ----
const store={
  get watch(){return JSON.parse(localStorage.getItem('ww_watch')||'[]')},
  set watch(v){localStorage.setItem('ww_watch',JSON.stringify(v))},
  get alerts(){return JSON.parse(localStorage.getItem('ww_alerts')||'{}')},
  set alerts(v){localStorage.setItem('ww_alerts',JSON.stringify(v))},
};
const inWatch=c=>store.watch.some(f=>f.cik===c);
function toggleWatch(f){let w=store.watch;
  if(inWatch(f.cik))w=w.filter(x=>x.cik!==f.cik);else w.push(f);store.watch=w;}

let tab='search', lastResults=null;
const PLACEHOLDER={search:'Search any 13F fund (Berkshire, Citadel, CIK…)',
  stocks:'Search a stock (Apple, NVDA, Coca-Cola…)'};

// ---- API ----
async function api(path){
  let r;
  try{r=await fetch(path);}
  catch(e){throw new Error('Cannot reach the backend. Are you running "python app.py" and viewing http://localhost:8000 (not the .html file)?');}
  let body; try{body=await r.json();}catch(_){body=null;}
  if(!r.ok){throw new Error((body&&body.error)?body.error:('Backend error HTTP '+r.status));}
  return body;
}
function errBox(back,msg){return `${back?`<span class="back" onclick="setTab('search')">‹ back</span>`:''}
  <div class="empty"><div class="big">⚠️</div><div style="color:var(--txt);font-weight:600;margin-bottom:6px">Couldn't load</div>
  <div style="font-size:13px;max-width:340px;margin:0 auto">${msg}</div>
  <div style="margin-top:16px"><span class="chip on" onclick="runHealth()">Run a diagnostic</span>
  <span class="chip" onclick="loadDemo()">Load sample</span></div></div>`;}
async function runHealth(){
  app.innerHTML=`<div class="spin">Checking backend + EDGAR…</div>`;
  try{const h=await api('/api/health');
    const rows=[
      ['Backend reachable','yes',true],
      ['SEC_UA (contact email) set',h.ua_ok?'yes':'NO — using placeholder',h.ua_ok],
      ['EDGAR reachable',h.edgar_reachable?('yes (HTTP '+h.edgar_status+')'):('NO — '+(h.edgar_error||'')),h.edgar_reachable],
      ['Fund directory built',h.directory_built?(h.funds+' funds'):'NO — run build_directory.py',h.directory_built],
      ['Funds with holdings loaded',String(h.ingested),h.ingested>0],
    ];
    let out=`<div class="sec-title">Diagnostic</div><div class="card">`;
    rows.forEach(([k,v,ok])=>{out+=`<div class="hrow"><div class="grow"><div class="nm">${ok?'✅':'❌'} ${k}</div>
      <div class="sub">${v}</div></div></div>`;});
    out+=`</div>`;
    if(!h.ua_ok)out+=`<div class="banner">Fix: stop the app, then run<br><code>export SEC_UA="WhaleWatch/1.0 (Your Name you@gmail.com)"</code><br>and start it again in the same terminal.</div>`;
    else if(!h.edgar_reachable)out+=`<div class="banner">EDGAR is blocking or unreachable. Check internet/VPN/firewall. Details above.</div>`;
    else if(!h.directory_built)out+=`<div class="banner">Run <code>python build_directory.py</code> once to populate the fund list.</div>`;
    app.innerHTML=out;
  }catch(e){app.innerHTML=`<div class="empty"><div class="big">⚡</div>${e.message}</div>`;}
}

// ---- views ----
function setTab(t){tab=t;document.querySelectorAll('.nav a').forEach(a=>a.classList.toggle('on',a.dataset.tab===t));
  const q=$('#q'); if(PLACEHOLDER[t]){q.placeholder=PLACEHOLDER[t];}
  if(t!=='search'&&t!=='stocks')q.value='';
  if(t==='search')renderHome();
  if(t==='stocks')renderStocksHome();
  if(t==='watch')renderWatch();
  if(t==='alerts')renderAlerts();}

function renderHome(){
  let h=`<div id="dirstat" class="muted" style="font-size:12.5px;margin-bottom:6px"></div>`;
  h+=`<div class="sec-title">Popular funds</div>`;
  h+=POPULAR.map(f=>fundCard(f)).join('');
  h+=`<div class="sec-title">Try it now</div>
    <div class="card"><div class="row" onclick="loadDemo()">
      <div class="tkr">🐋</div><div class="grow"><div class="nm">Load sample (Berkshire Q1 2025)</div>
      <div class="sub">See the full holdings + change view with zero setup</div></div>
      <span style="color:var(--acc);font-size:20px">›</span></div></div>`;
  app.innerHTML=h;
  // health banner: warn immediately if something's misconfigured
  api('/api/health').then(hl=>{
    const e=$('#dirstat'); if(!e)return;
    if(!hl.ua_ok){e.innerHTML=`⚠️ <b>SEC_UA not set</b> — clicks will fail. <a onclick="runHealth()">Fix it</a>`;e.style.color='var(--amber)';}
    else if(!hl.edgar_reachable){e.innerHTML=`⚠️ <b>Can't reach EDGAR.</b> <a onclick="runHealth()">Details</a>`;e.style.color='var(--amber)';}
    else if(!hl.directory_built){e.innerHTML=`📒 Directory empty — run <code>python build_directory.py</code>. Search still works live.`;}
    else{e.innerHTML=`📒 <b>${hl.funds.toLocaleString('en-US')}</b> funds searchable · ${hl.ingested.toLocaleString('en-US')} with holdings loaded`;}
  }).catch(()=>{const e=$('#dirstat');if(e){e.innerHTML=`⚠️ Backend not reachable. Open <b>http://localhost:8000</b> (not the .html file). <a onclick="runHealth()">Recheck</a>`;e.style.color='var(--amber)';}});
}
function fundCard(f){const on=inWatch(f.cik);const dot=f.ingested?' <span style="color:var(--green)">●</span>':'';
  return `<div class="card"><div class="row">
    <div class="tkr">${tkr(f.name)}</div>
    <div class="grow" onclick="openFund('${f.cik}','${(f.name||'').replace(/'/g,"")}')">
      <div class="nm">${f.name}${dot}</div><div class="sub">CIK ${(+f.cik)} · tap to view 13F</div></div>
    <span class="star ${on?'on':''}" onclick="evt(event);star('${f.cik}','${(f.name||'').replace(/'/g,"")}')">${on?'★':'☆'}</span>
  </div></div>`;}

// ---- Stocks (cross-fund) ----
function renderStocksHome(){
  api('/api/stats').then(s=>{
    const note=s.ingested>0
      ?`Searching across <b>${s.ingested.toLocaleString('en-US')}</b> funds with loaded holdings.`
      :`No fund holdings loaded yet. Run <code>python3 whalewatch.py preload</code> in Terminal to load the big funds, then refresh.`;
    app.innerHTML=`<div class="banner">Pick a stock to see <b>which funds hold it</b> and who's buying or selling. ${note}</div>
      <div class="empty"><div class="big">📈</div>Type a company name above<br>e.g. Apple, Nvidia, Coca-Cola.</div>`;
  }).catch(()=>{app.innerHTML=`<div class="empty"><div class="big">📈</div>Type a company name above.</div>`;});
}
async function searchStocks(q){
  app.innerHTML=`<div class="spin">Searching loaded holdings…</div>`;
  try{const d=await api('/api/stock?q='+encodeURIComponent(q));
    const secs=d.securities||[];
    if(!secs.length){app.innerHTML=`<div class="empty"><div class="big">🔍</div>No loaded holdings match “${q}”.<br>Open some funds first (or bulk-ingest) so their holdings are in the DB.</div>`;return;}
    app.innerHTML=`<div class="sec-title">Securities</div>`+secs.map(s=>`
      <div class="card"><div class="row" onclick="openStock('${s.cusip}','${(s.name||'').replace(/'/g,'')}')">
        <div class="tkr">${s.ticker||tkr(s.name)}</div>
        <div class="grow"><div class="nm">${s.name}${s.ticker?` <span style="color:var(--acc);font-weight:700">${s.ticker}</span>`:''}</div>
          <div class="sub">${s.holders} fund${s.holders>1?'s':''} · ${fmt(s.total)} total · CUSIP ${s.cusip}</div></div>
        <span style="color:var(--acc);font-size:20px">›</span></div></div>`).join('');
  }catch(e){app.innerHTML=errBox(false,e.message);}
}
async function openFundInfo(cik,name){window.scrollTo(0,0);
  app.innerHTML=`<span class="back" onclick="openFund('${cik}','${(name||'').replace(/'/g,'')}')">‹ back to holdings</span><div class="spin">Loading fund profile…</div>`;
  try{const d=await api('/api/fundinfo/'+cik);
    let h=`<span class="back" onclick="openFund('${cik}','${(name||'').replace(/'/g,'')}')">‹ back to holdings</span>
      <div class="fhead"><div class="h1">${d.name}</div></div>`;
    h+=`<div class="card"><div class="sec-title" style="margin:0 0 6px">Filer facts (SEC)</div>
      <div style="font-size:13.5px;line-height:1.8">
      ${d.location?`📍 ${d.location}<br>`:''}
      ${d.sic?`🏷️ ${d.sic}<br>`:''}
      🗂️ ${d.filingsCount} 13F filings on file${d.firstPeriod?`, back to ${d.firstPeriod}`:''}<br>
      📅 Latest: ${d.latestPeriod||'—'}</div></div>`;
    h+=`<div class="card"><div class="sec-title" style="margin:0 0 8px">Links</div>`;
    if(d.website)h+=`<a href="https://${d.website}" target="_blank" class="chip on" style="display:inline-block;margin:0 6px 6px 0">🌐 Official website</a>`;
    h+=`<a href="${d.edgarUrl}" target="_blank" class="chip" style="display:inline-block;margin:0 6px 6px 0">📄 SEC EDGAR filings</a>
        <a href="${d.searchUrl}" target="_blank" class="chip" style="display:inline-block;margin:0 6px 6px 0">🔎 Search the web</a></div>`;
    h+=`<div class="muted" style="font-size:12px;text-align:center;margin:8px 0">WhaleWatch reports verified SEC filing data. For strategy, people & background, ${d.website?'visit the fund’s official website above':'use the SEC EDGAR or web-search links above'}.</div>`;
    h+=`<button class="alertbtn" onclick="openFund('${cik}','${(name||'').replace(/'/g,'')}')">‹ View holdings</button>`;
    app.innerHTML=h;
  }catch(e){app.innerHTML=errBox(false,e.message);}
}
let curStock=null, stockFresh=true;
async function openStock(cusip,name){window.scrollTo(0,0);
  tab='stocks';document.querySelectorAll('.nav a').forEach(a=>a.classList.toggle('on',a.dataset.tab==='stocks'));
  app.innerHTML=`<span class="back" onclick="setTab('stocks')">‹ back</span><div class="spin">Loading holders & shares data…<br><span style="font-size:12px">(first load pulls company data — a few seconds)</span></div>`;
  try{const d=await api('/api/stock?cusip='+encodeURIComponent(cusip));
    d.cusip=d.cusip||cusip; curStock=d; stockFresh=true; drawStock();
  }catch(e){app.innerHTML=`<span class="back" onclick="setTab('stocks')">‹ back</span>`+errBox(false,e.message);}
}
function freshCutoff(){const c=new Date();c.setFullYear(c.getFullYear()-1);return c.toISOString().slice(0,10);}
function drawStock(){
  const d=curStock; const all=d.holders||[];
  const cut=freshCutoff();
  const shown=stockFresh?all.filter(r=>(r.reportDate||'')>=cut):all;
  const hidden=all.length-shown.length;
  let out=`<span class="back" onclick="setTab('stocks')">‹ back</span>
    <div class="fhead"><div class="h1">${d.security}${d.ticker?` <span style="color:var(--acc)">${d.ticker}</span>`:''}</div>
    <div class="muted" style="font-size:13px">CUSIP ${d.cusip} · held by ${d.count} loaded fund${d.count!==1?'s':''}${d.sharesOutstanding?` · ${fmtShares(d.sharesOutstanding)} shares outstanding`:''}${d.sector?' · '+d.sector:''}</div></div>`;
  out+=`<div class="filterbar"><span class="chip ${stockFresh?'on':''}" onclick="stockFresh=true;drawStock()">Recent filers (last 1y)</span>
    <span class="chip ${stockFresh?'':'on'}" onclick="stockFresh=false;drawStock()">All filers ${all.length}</span>
    ${stockFresh&&hidden>0?`<span class="muted" style="align-self:center;font-size:11.5px">${hidden} stale hidden</span>`:''}</div>`;
  if(!shown.length){out+=`<div class="card"><div class="empty" style="padding:30px">${all.length&&stockFresh?'No holders filed in the last year. ':''}No loaded funds hold this${all.length?' recently':' yet'}.</div></div>`;}
  else{
    out+=`<div class="scrollx"><table class="htbl"><thead><tr>
      <th class="l">Investor</th><th>Value</th><th>% Sh Out</th><th># Shares</th><th>Δ Shares</th><th>% Chg</th><th>% Port</th><th>Date</th>
      </tr></thead><tbody>`;
    shown.forEach(r=>{
      let chg='—', chgpct='—';
      if(r.status==='NEW'){chg=`<span class="up">NEW</span>`;chgpct=`<span class="up">new</span>`;}
      else if(r.changeShares!=null){
        chg=`<span class="${r.changeShares>=0?'up':'dn'}">${r.changeShares>=0?'+':''}${Math.round(r.changeShares).toLocaleString('en-US')}</span>`;
        chgpct=(r.changePct!=null)?`<span class="${r.changePct>=0?'up':'dn'}">${pct(r.changePct)}</span>`:'—';
      }
      const shOut=r.pctSharesOut!=null?`${(r.pctSharesOut*100).toFixed(r.pctSharesOut<0.01?2:1)}%`:`<span class="muted">—</span>`;
      const port=r.pctPortfolio!=null?`${(r.pctPortfolio*100).toFixed(1)}%`:`<span class="muted">—</span>`;
      out+=`<tr onclick="openFund('${r.cik}','${(r.fund||'').replace(/'/g,'')}')">
        <td class="l"><div class="cellnm">${r.fund}</div><div class="cellsub">tap → fund's 13F</div></td>
        <td>${fmt(r.value)}</td>
        <td>${shOut}</td>
        <td>${Math.round(r.shares||0).toLocaleString('en-US')}</td>
        <td>${chg}</td>
        <td>${chgpct}</td>
        <td>${port}</td>
        <td>${r.reportDate}</td>
      </tr>`;});
    out+=`</tbody></table></div>`;
  }
  out+=`<div class="muted" style="font-size:11.5px;text-align:center;margin:14px 0">Value in USD ($K/$M/$B). <b>#&nbsp;Shares and Δ&nbsp;Shares are exact share counts</b> (not thousands/millions). "Recent" = latest 13F within the last year; % Sh Out shown when company share data is available.</div>`;
  app.innerHTML=out;
}
function fmtShares(n){n=+n||0;if(n>=1e9)return (n/1e9).toFixed(2)+'B';if(n>=1e6)return (n/1e6).toFixed(1)+'M';return n.toLocaleString('en-US');}
function fmtShares(n){n=+n||0;if(n>=1e9)return (n/1e9).toFixed(2)+'B';if(n>=1e6)return (n/1e6).toFixed(1)+'M';return n.toLocaleString('en-US');}

function evt(e){e.stopPropagation();}
function star(cik,name){toggleWatch({cik,name});setTab(tab);}

async function doSearch(q){
  app.innerHTML=`<div class="spin">Searching EDGAR…</div>`;
  try{const res=await api('/api/search?q='+encodeURIComponent(q));
    if(!res.length){app.innerHTML=`<div class="empty"><div class="big">🤷</div>No 13F filers found for “${q}”.<br>Try a manager's legal name or a CIK number.</div>`;return;}
    app.innerHTML=`<div class="sec-title">Results (${res.length})</div>`+res.map(f=>fundCard(f)).join('');
  }catch(e){app.innerHTML=errBox(false,e.message);}
}

async function openFund(cik,name,period){
  window.scrollTo(0,0);
  app.innerHTML=`<span class="back" onclick="setTab('search')">‹ back</span><div class="spin">Loading ${name||'fund'} holdings from EDGAR…<br><span style="font-size:12px">(first open downloads from SEC — can take 5–15s)</span></div>`;
  try{const d=await api('/api/holdings/'+cik+(period?('?period='+encodeURIComponent(period)):''));
    renderFund(d);
    if(!d.error&&!d.demo)enrichFund(cik,d.selectedPeriod);}
  catch(e){app.innerHTML=errBox(true,e.message);}
}
let enriching=false;
async function enrichFund(cik,period){
  if(enriching)return; enriching=true;
  const tag=document.getElementById('enrichTag'); if(tag)tag.textContent='loading sector · % owned · 12-mo return…';
  try{const d=await api('/api/enrich/'+cik+(period?('?period='+encodeURIComponent(period)):''));
    if(!d.error&&curFund&&curFund.cik===d.cik&&curFund.selectedPeriod===d.selectedPeriod){
      curFund=d; drawFund(false);
    }
  }catch(e){const t=document.getElementById('enrichTag'); if(t)t.textContent='market data unavailable';}
  enriching=false;
}
function qlabel(d){if(!d)return d;const p=String(d).split('-');const q={'03':'Q1','06':'Q2','09':'Q3','12':'Q4'}[p[1]]||p[1];return q+' '+p[0];}
// --- Holdings-value-over-time chart (dependency-free SVG) ---
let histCache={};
async function loadHistory(cik){
  if(histCache[cik]!==undefined){if(curFund&&curFund.cik===cik)drawFund(curFund.demo);return;}
  try{const d=await api('/api/history/'+cik);histCache[cik]=(d&&d.series)||[];}
  catch(e){histCache[cik]=[];}
  if(curFund&&curFund.cik===cik)drawFund(curFund.demo);
}
function niceNum(x,round){if(!isFinite(x)||x<=0)return 1;const e=Math.floor(Math.log10(x)),f=x/Math.pow(10,e);let nf;if(round){nf=f<1.5?1:f<3?2:f<7?5:10;}else{nf=f<=1?1:f<=2?2:f<=5?5:10;}return nf*Math.pow(10,e);}
function histChartSVG(series){
  const n=series.length;if(!n)return'';
  const bw=22,gap=12,padL=58,padR=14,padT=10,padB=54,plotH=190;
  const W=padL+padR+n*bw+(n>1?(n-1)*gap:0),H=padT+plotH+padB;
  const max=Math.max.apply(null,series.map(d=>d.value));
  const step=niceNum(niceNum(max,false)/4,true),nm=Math.max(step,Math.ceil(max/step)*step),ticks=Math.round(nm/step);
  const y=v=>padT+plotH-(v/nm)*plotH;
  let g='';
  for(let i=0;i<=ticks;i++){const v=nm*i/ticks,yy=y(v);
    g+=`<line x1="${padL}" y1="${yy}" x2="${W-padR}" y2="${yy}" style="stroke:var(--line)" stroke-width="1"/>`;
    g+=`<text x="${padL-8}" y="${yy+4}" text-anchor="end" font-size="11" style="fill:var(--mut)">${fmt(v)}</text>`;}
  series.forEach((d,i)=>{const x=padL+i*(bw+gap),bh=Math.max(1,(d.value/nm)*plotH),yy=padT+plotH-bh;
    g+=`<g><title>${qlabel(d.period)} — ${fmt(d.value)}</title><rect x="${x}" y="${yy}" width="${bw}" height="${bh}" rx="2" style="fill:var(--acc);fill-opacity:.5;stroke:var(--acc);stroke-width:1"/></g>`;
    if(i%2===0||n<=14){const cx=x+bw/2,ty=padT+plotH+14;
      g+=`<text x="${cx}" y="${ty}" text-anchor="end" font-size="10.5" style="fill:var(--mut)" transform="rotate(-45 ${cx} ${ty})">${qlabel(d.period)}</text>`;}});
  return `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" style="display:block" xmlns="http://www.w3.org/2000/svg">${g}</svg>`;
}
async function loadDemo(){window.scrollTo(0,0);
  app.innerHTML=`<div class="spin">Loading sample…</div>`;
  try{renderFund(await api('/api/demo'),true);}catch(e){app.innerHTML=`<div class="empty">Sample unavailable.</div>`;}
}

let curFund=null, filter='CURRENT';
function renderFund(d,demo){
  if(d.error){app.innerHTML=`<span class="back" onclick="setTab('search')">‹ back</span><div class="empty"><div class="big">📭</div>${d.error} for ${d.name||''}.</div>`;return;}
  curFund=d; filter='CURRENT';
  drawFund(demo);
}
function drawFund(demo){
  const d=curFund, on=inWatch(d.cik), al=store.alerts[d.cik];
  const counts={NEW:0,ADDED:0,TRIMMED:0,SOLD:0,HOLD:0};
  d.holdings.forEach(h=>counts[h.status]=(counts[h.status]||0)+1);
  const period=(d.current&&d.current.reportDate)||'';
  let h=`<span class="back" onclick="setTab('search')">‹ back</span>`;
  if(demo||d.demo)h+=`<div class="banner">Sample data — run <b>python app.py</b> and search to pull live filings.</div>`;
  h+=`<div class="fhead"><div class="h1" style="cursor:pointer" onclick="openFundInfo('${d.cik}','${(d.name||'').replace(/'/g,'')}')">${d.name} <span style="color:var(--acc);font-size:14px;vertical-align:middle">ⓘ</span></div>
     <div class="muted" style="font-size:13px">13F-HR · period ending ${period} · filed ${(d.current&&d.current.filingDate)||''} · <span style="color:var(--acc);cursor:pointer" onclick="openFundInfo('${d.cik}','${(d.name||'').replace(/'/g,'')}')">about this fund</span></div></div>`;
  if(d.periods&&d.periods.length>1){
    h+=`<select onchange="openFund('${d.cik}','${(d.name||'').replace(/'/g,'')}',this.value)"
      style="width:100%;margin-bottom:12px;background:var(--bg2);color:var(--txt);border:1px solid var(--line);border-radius:12px;padding:12px;font-size:15px;font-weight:600">
      ${d.periods.map(p=>`<option value="${p.reportDate}" ${p.reportDate===d.selectedPeriod?'selected':''}>${qlabel(p.reportDate)} (${p.reportDate})</option>`).join('')}</select>`;
  }
  h+=`<div class="stats">
     <div class="stat"><div class="k">Portfolio value</div><div class="v">${fmt(d.totalValue)}</div></div>
     <div class="stat"><div class="k">Positions</div><div class="v">${d.positions}</div></div>
   </div>`;
  if(!demo&&!d.demo){const hs=histCache[d.cik];
    h+=`<div style="margin-bottom:12px"><div class="sec-title" style="margin:0 0 8px">Holdings value (13F) over time</div>`+
       `<div class="scrollx" style="padding:10px 12px">${hs?(hs.length?histChartSVG(hs):`<div class="muted" style="font-size:12px;padding:18px 0;text-align:center">No filing history available.</div>`):`<div class="muted" style="font-size:12px;padding:26px 0;text-align:center">Loading history from EDGAR…</div>`}</div>`+
       `<div class="muted" style="font-size:10.5px;text-align:center;margin:6px 2px 0">Total reported 13F holdings each quarter — long US-listed positions only, not total AUM.</div></div>`;}
  h+=`<button class="alertbtn ${al?'on':''}" onclick="toggleAlert('${d.cik}','${(d.name||'').replace(/'/g,'')}')">
     ${al?'🔔 Alerts on — you’ll be notified on new filings':'🔔 Alert me when this fund files'}</button>
   <button class="alertbtn ${on?'on':''}" style="margin-top:8px" onclick="star('${d.cik}','${(d.name||'').replace(/'/g,'')}');drawFund(${demo?true:false})">
     ${on?'★ In your watchlist':'☆ Add to watchlist'}</button>`;
  // filters
  const active=d.holdings.length-(counts.SOLD||0);
  const fl=['CURRENT','NEW','ADDED','TRIMMED','SOLD','HOLD'];
  h+=`<div class="filterbar">`+fl.map(f=>{const n=f==='CURRENT'?active:(counts[f]||0);
     return `<span class="chip ${filter===f?'on':''}" onclick="setFilter('${f}')">${f} ${n}</span>`;}).join('')+`</div>`;
  h+=`<div id="enrichTag" class="muted" style="font-size:11.5px;margin:0 2px 8px"></div>`;
  h+=`<div class="muted" style="font-size:11px;margin:0 2px 6px">Tap any holding to see which funds own it →</div>`;
  // columnar table (Wisdom Whale style) — CURRENT shows current holdings; SOLD in its own tab
  const rows=d.holdings.filter(x=>filter==='CURRENT'?x.status!=='SOLD':x.status===filter);
  if(!rows.length){h+=`<div class="card"><div class="empty" style="padding:30px">No ${filter.toLowerCase()} positions.</div></div>`;}
  else{
    h+=`<div class="scrollx"><table class="htbl"><thead><tr>
      <th class="l">Stock</th><th>Mkt Value</th><th>% Port</th><th>% Owned</th><th>12-mo</th><th>Δ Shares</th>
      </tr></thead><tbody>`;
    rows.forEach(x=>{
      let chg;
      if(x.status==='SOLD')chg=`<span class="badge b-SOLD">SOLD</span>`;
      else if(x.sharesChangePct===null)chg=`<span class="badge b-NEW">NEW</span>`;
      else if(x.sharesChangePct===0)chg=`<span class="badge b-HOLD">—</span>`;
      else chg=`<span class="${x.sharesChangePct>0?'up':'dn'}">${pct(x.sharesChangePct)}</span>`;
      const ret=(x.ret12m!=null)?`<span class="${x.ret12m>=0?'up':'dn'}">${pct(x.ret12m)}</span>`:`<span class="muted">—</span>`;
      const own=x.pctOwnership?`${(x.pctOwnership*100).toFixed(x.pctOwnership<0.01?2:1)}%`:`<span class="muted">—</span>`;
      const por=x.pctPortfolio?`${(x.pctPortfolio*100).toFixed(1)}%`:`<span class="muted">—</span>`;
      h+=`<tr onclick="openStock('${x.cusip}','${(x.name||'').replace(/'/g,'')}')">
        <td class="l"><div class="cellnm">${x.ticker?`<span style="color:var(--acc)">${x.ticker}</span> · `:''}${x.name}</div>
          <div class="cellsub">${x.sector?x.sector+' · ':''}${(x.shares||0).toLocaleString('en-US')} sh</div></td>
        <td>${fmt(x.value)}</td>
        <td>${por}</td>
        <td>${own}</td>
        <td>${ret}</td>
        <td>${chg}</td>
      </tr>`;});
    h+=`</tbody></table></div>`;
  }
  h+=`<div class="muted" style="font-size:11.5px;text-align:center;margin:14px 0 4px">
     Mkt Value in USD ($K/$M/$B); share counts under each name are exact. 13F from SEC EDGAR; sector & shares outstanding from SEC; 12-mo return from market prices (approx).</div>`;
  app.innerHTML=h;
  if(!demo&&!d.demo&&histCache[d.cik]===undefined)loadHistory(d.cik);
}
function setFilter(f){filter=f;drawFund(curFund.demo);}

function toggleAlert(cik,name){const a=store.alerts;if(a[cik])delete a[cik];else a[cik]={name,since:Date.now()};
  store.alerts=a;if(curFund&&curFund.cik===cik)drawFund(curFund.demo);else setTab(tab);}

function renderWatch(){const w=store.watch;window.scrollTo(0,0);
  if(!w.length){app.innerHTML=`<div class="empty"><div class="big">⭐</div>Your watchlist is empty.<br>Tap the star on any fund to track it here.</div>`;return;}
  app.innerHTML=`<div class="sec-title">Your funds (${w.length})</div>`+w.map(f=>fundCard(f)).join('');}

async function renderAlerts(){const a=store.alerts;const keys=Object.keys(a);window.scrollTo(0,0);
  if(!keys.length){app.innerHTML=`<div class="empty"><div class="big">🔔</div>No alerts yet.<br>Open a fund and tap “Alert me when this fund files”.<br><br>You'll see each fund's latest moves here.</div>`;return;}
  app.innerHTML=`<div class="sec-title">Following (${keys.length})</div><div class="spin">Loading latest moves…</div>`;
  let feed=[];
  try{const d=await api('/api/alerts?ciks='+keys.join(','));feed=d.feed||[];}
  catch(e){app.innerHTML=errBoxTab('alerts',e.message);return;}
  let h=`<div class="sec-title">Following (${keys.length})</div>`;
  feed.forEach(x=>{
    const c=x.cik;
    if(x.error){h+=`<div class="card"><div class="row">
      <div class="tkr">${tkr(x.name)}</div>
      <div class="grow"><div class="nm">${x.name}</div><div class="sub">${x.error}</div></div>
      <span class="star on" onclick="toggleAlert('${c}','${(x.name||'').replace(/'/g,'')}');renderAlerts()">🔔</span></div></div>`;return;}
    const buy=x.topBuy?`<span class="up">▲ ${x.topBuy.name}</span>`:'';
    const exit=x.topExit?`<span class="dn">▼ ${x.topExit.name} (${x.topExit.status.toLowerCase()})</span>`:'';
    const cnt=x.counts||{};
    h+=`<div class="card">
      <div class="row" onclick="openFund('${c}','${(x.name||'').replace(/'/g,'')}')">
        <div class="tkr">${tkr(x.name)}</div>
        <div class="grow"><div class="nm">${x.name}</div>
          <div class="sub">Latest 13F · ${x.reportDate} · ${fmt(x.totalValue)} · ${x.positions} positions</div></div>
        <span class="star on" onclick="evt(event);toggleAlert('${c}','${(x.name||'').replace(/'/g,'')}');renderAlerts()">🔔</span>
      </div>
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:10px;font-size:12px">
        ${cnt.NEW?`<span class="badge b-NEW">${cnt.NEW} new</span>`:''}
        ${cnt.ADDED?`<span class="badge b-ADDED">${cnt.ADDED} added</span>`:''}
        ${cnt.TRIMMED?`<span class="badge b-TRIMMED">${cnt.TRIMMED} trimmed</span>`:''}
        ${cnt.SOLD?`<span class="badge b-SOLD">${cnt.SOLD} sold</span>`:''}
      </div>
      ${(buy||exit)?`<div class="sub" style="margin-top:8px;font-size:12.5px">${buy}${buy&&exit?' · ':''}${exit}</div>`:''}
    </div>`;});
  h+=`<div class="muted" style="font-size:11.5px;text-align:center;margin:14px 0">Highlights from each fund's most recent 13F. New filings appear automatically when the daily refresh runs.</div>`;
  app.innerHTML=h;}
function errBoxTab(tab,msg){return `<div class="empty"><div class="big">⚠️</div>
  <div style="font-size:13px;max-width:340px;margin:0 auto">${msg}</div>
  <div style="margin-top:14px"><span class="chip on" onclick="runHealth()">Run a diagnostic</span></div></div>`;}

// ---- wiring ----
let t;$('#q').addEventListener('input',e=>{const v=e.target.value.trim();clearTimeout(t);
  const stockMode=(tab==='stocks');
  if(!v){stockMode?renderStocksHome():renderHome();return;}
  t=setTimeout(()=>stockMode?searchStocks(v):doSearch(v),380);});
document.querySelectorAll('.nav a').forEach(a=>a.addEventListener('click',()=>setTab(a.dataset.tab)));

// detect backend
fetch('/api/demo').then(r=>{if(r.ok)$('#modeTag').textContent='';}).catch(()=>{$('#modeTag').textContent='offline';});
renderHome();
</script>
</body>
</html>
"""

SAMPLE_JSON = r"""{
  "name": "BERKSHIRE HATHAWAY INC (sample data)",
  "cik": "0001067983",
  "current": { "form": "13F-HR", "filingDate": "2025-05-15", "reportDate": "2025-03-31" },
  "previous": { "form": "13F-HR", "filingDate": "2025-02-14", "reportDate": "2024-12-31" },
  "totalValue": 258740000000,
  "positions": 9,
  "demo": true,
  "holdings": [
    { "cusip": "037833100", "name": "APPLE INC", "class": "COM", "value": 66480000000, "shares": 300000000, "putCall": "", "status": "TRIMMED", "sharesChangePct": -0.1110 },
    { "cusip": "060505104", "name": "BANK OF AMERICA CORP", "class": "COM", "value": 28230000000, "shares": 631600000, "putCall": "", "status": "TRIMMED", "sharesChangePct": -0.0700 },
    { "cusip": "025816109", "name": "AMERICAN EXPRESS CO", "class": "COM", "value": 45990000000, "shares": 151610000, "putCall": "", "status": "HOLD", "sharesChangePct": 0.0 },
    { "cusip": "191216100", "name": "COCA COLA CO", "class": "COM", "value": 28680000000, "shares": 400000000, "putCall": "", "status": "HOLD", "sharesChangePct": 0.0 },
    { "cusip": "166764100", "name": "CHEVRON CORP", "class": "COM", "value": 21610000000, "shares": 118610000, "putCall": "", "status": "ADDED", "sharesChangePct": 0.0820 },
    { "cusip": "713448108", "name": "OCCIDENTAL PETROLEUM CORP", "class": "COM", "value": 13050000000, "shares": 264940000, "putCall": "", "status": "ADDED", "sharesChangePct": 0.0150 },
    { "cusip": "53807310", "name": "KRAFT HEINZ CO", "class": "COM", "value": 11320000000, "shares": 325630000, "putCall": "", "status": "HOLD", "sharesChangePct": 0.0 },
    { "cusip": "189054109", "name": "DOMINOS PIZZA INC", "class": "COM", "value": 1100000000, "shares": 2380000, "putCall": "", "status": "NEW", "sharesChangePct": null },
    { "cusip": "92826C839", "name": "VISA INC", "class": "COM", "value": 2280000000, "shares": 8290000, "putCall": "", "status": "HOLD", "sharesChangePct": 0.0 },
    { "cusip": "G5876H105", "name": "T-MOBILE US INC", "class": "COM", "value": 0, "shares": 0, "putCall": "", "status": "SOLD", "sharesChangePct": -1.0 }
  ]
}
"""

# ---------------------------------------------------------------------------
# Web app
# ---------------------------------------------------------------------------
app = Flask(__name__)
init_schema()  # ensure tables exist whether launched via dev server or gunicorn
try:
    migrate_fix_values()  # repair any value-unit errors in the stored data on boot
except Exception:
    pass

@app.after_request
def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

@app.route("/")
def _index():
    return Response(HTML, mimetype="text/html")

@app.route("/api/stats")
def _stats():
    total, ing = count_funds()
    return jsonify({"funds": total, "ingested": ing})

@app.route("/api/health")
def _health():
    ua = SEC_UA
    out = {"sec_ua": ua, "ua_ok": ("@" in ua and "example.com" not in ua and "your@email.com" not in ua)}
    total, ing = count_funds()
    out["funds"], out["ingested"] = total, ing
    out["directory_built"] = total > 0
    try:
        r = sec_get("https://data.sec.gov/submissions/CIK0001067983.json")
        out["edgar_reachable"], out["edgar_status"] = True, r.status_code
    except Exception as e:
        out["edgar_reachable"], out["edgar_error"] = False, str(e)
    return jsonify(out)

def _live_search(q):
    url = "https://efts.sec.gov/LATEST/search-index?q=%s&forms=13F-HR" % requests.utils.quote('"%s"' % q)
    out, seen = [], set()
    data = sec_get(url).json()
    for hit in data.get("hits", {}).get("hits", []):
        s = hit.get("_source", {})
        names = s.get("display_names", []) or []
        for i, c in enumerate(s.get("ciks", []) or []):
            c = str(c).zfill(10)
            if c in seen: continue
            seen.add(c)
            nm = names[i] if i < len(names) else (names[0] if names else c)
            out.append({"cik": c, "name": re.sub(r"\s*\(CIK.*\)$", "", nm)})
    return out[:15]

@app.route("/api/search")
def _search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    rows = search_funds(q)
    if not rows:
        try:
            for f in _live_search(q):
                upsert_fund(f["cik"], f["name"])
            rows = search_funds(q)
        except Exception:
            pass
    return jsonify([{"cik": r["cik"], "name": r["name"],
                     "ingested": r.get("holdings_status") == "ingested"} for r in rows])

@app.route("/api/holdings/<cik>")
def _holdings(cik):
    cik = str(cik).zfill(10)
    period = request.args.get("period", "").strip() or None
    try:
        return jsonify(build_fund_payload(cik, period))
    except Exception as e:
        return jsonify({"error": "Could not load from EDGAR: %s" % e, "cik": cik})

@app.route("/api/fundinfo/<cik>")
def _fundinfo(cik):
    try:
        return jsonify(fund_info(str(cik).zfill(10)))
    except Exception as e:
        return jsonify({"error": str(e), "cik": cik})

@app.route("/api/enrich/<cik>")
def _enrich(cik):
    """Live-fetch sector / shares-outstanding / 12m-return for a fund's holdings."""
    period = request.args.get("period", "").strip() or None
    try:
        return jsonify(enrich_fund(str(cik).zfill(10), period))
    except Exception as e:
        return jsonify({"error": str(e), "cik": cik})

@app.route("/api/history/<cik>")
def _history(cik):
    """Per-quarter total 13F holdings value, for the fund AUM-style chart."""
    try:
        return jsonify({"cik": str(cik).zfill(10), "series": fund_value_history(str(cik).zfill(10))})
    except Exception as e:
        return jsonify({"error": str(e), "cik": cik, "series": []})

@app.route("/api/stock")
def _stock():
    cusip = request.args.get("cusip", "").strip()
    q = request.args.get("q", "").strip()
    if cusip:
        base = holders_of(cusip=cusip)
        name = base[0]["security"] if base else cusip
        # enrich the security once to get shares outstanding (for % of shares out)
        try:
            enrich_security(cusip, name)
        except Exception:
            pass
        sec = _cached_security(cusip.upper()) or {}
        shares_out = sec.get("shares_out")
        holders = stock_holders(cusip, shares_out=shares_out)
        return jsonify({"security": name, "cusip": cusip,
                        "ticker": sec.get("ticker") or CUSIP_TICKER.get(cusip.upper()) or ticker_for(name, cusip),
                        "sector": sec.get("sector") or None, "sharesOutstanding": shares_out,
                        "ret12m": sec.get("ret_12m"),
                        "holders": holders, "count": len(holders)})
    if q:
        # If the query is a ticker (e.g. AAPL), search by its company name instead.
        t2title, _ = ticker_maps()
        key = q
        if q.upper() in t2title:
            key = _norm(t2title[q.upper()]) or q
        secs = distinct_securities(key)
        for s in secs:
            s["ticker"] = ticker_for(s["name"])
        return jsonify({"securities": secs})
    return jsonify({"securities": []})

@app.route("/api/alerts")
def _alerts():
    """Latest-filing highlights for the funds the user follows."""
    raw = request.args.get("ciks", "").strip()
    if not raw:
        return jsonify({"feed": []})
    feed = []
    for c in [x for x in raw.split(",") if x.strip()][:25]:
        c = c.strip().zfill(10)
        f = get_fund(c)
        if not f or f.get("holdings_status") != "ingested":
            try:
                ingest_fund(c, 2)
            except Exception as e:
                feed.append({"cik": c, "name": (f or {}).get("name", c), "error": str(e)})
                continue
        feed.append(fund_highlights(c))
    return jsonify({"feed": feed})

@app.route("/api/demo")
def _demo():
    return Response(SAMPLE_JSON, mimetype="application/json")

# ---------------------------------------------------------------------------
# Doctor
# ---------------------------------------------------------------------------
def doctor():
    G, R, Y, X = "\033[92m", "\033[91m", "\033[93m", "\033[0m"
    print("\nWhaleWatch doctor\n" + "-" * 42)
    problems = []
    ua = SEC_UA
    if "example.com" in ua or "your@email.com" in ua or "@" not in ua:
        print(" %s✗%s SEC_UA not set to a real email (now: %r)" % (R, X, ua))
        problems.append('Set your email, then restart:\n'
            '      export SEC_UA="WhaleWatch/1.0 (Your Name you@gmail.com)"   (mac/Linux)\n'
            '      $env:SEC_UA="WhaleWatch/1.0 (Your Name you@gmail.com)"     (Windows PowerShell)')
    else:
        print(" %s✓%s SEC_UA looks valid: %r" % (G, X, ua))
    reachable = False
    try:
        r = sec_get("https://data.sec.gov/submissions/CIK0001067983.json")
        print(" %s✓%s Reached data.sec.gov (HTTP %s)" % (G, X, r.status_code)); reachable = True
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        print(" %s✗%s data.sec.gov HTTP %s" % (R, X, code))
        problems.append("HTTP %s from SEC — usually a bad SEC_UA (see above)." % code if str(code) in ("401", "403")
                        else "HTTP %s from SEC; try again shortly." % code)
    except requests.exceptions.ConnectionError:
        print(" %s✗%s Could not connect to sec.gov" % (R, X))
        problems.append("No network route to sec.gov — check internet/VPN/firewall/proxy.")
    except Exception as e:
        print(" %s✗%s EDGAR error: %s" % (R, X, e)); problems.append(str(e))
    if reachable:
        try:
            info = list_13f_filings("0001067983")
            print(" %s✓%s Found %d Berkshire 13F filings" % (G, X, len(info["filings"])))
            if info["filings"]:
                rows = fetch_info_table("0001067983", info["filings"][0]["accession"])
                print(" %s✓%s Downloaded + parsed %d holdings rows" % (G, X, len(rows)))
        except Exception as e:
            print(" %s✗%s Holdings download failed: %s" % (R, X, e)); problems.append(str(e))
    try:
        init_schema(); total, ing = count_funds()
        if total == 0:
            print(" %s!%s Fund directory empty" % (Y, X))
            problems.append("Build the fund list:  python3 whalewatch.py build")
        else:
            print(" %s✓%s DB OK — %d funds, %d with holdings" % (G, X, total, ing))
    except Exception as e:
        print(" %s✗%s DB error: %s" % (R, X, e)); problems.append(str(e))
    print("-" * 42)
    if not problems:
        print("\n%s✓%s All good. Start it:  python3 whalewatch.py   then open http://localhost:8000\n" % (G, X))
    else:
        print("\nFix these:\n")
        for i, p in enumerate(problems, 1):
            print("  %d. %s\n" % (i, p))

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    init_schema()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "doctor":
        doctor(); return
    if cmd == "build":
        q = int(sys.argv[2]) if len(sys.argv) > 2 else 8
        print("Building fund directory (last %d quarters)..." % q)
        n = build_directory(q); print("Done — %d funds searchable." % n); return
    if cmd == "refresh":
        refresh(); return
    if cmd == "preload":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 150
        preload(n); return
    if cmd == "ingest":
        for c in sys.argv[2:]:
            n = ingest_fund(c, force=True); f = get_fund(c.zfill(10))
            print("%s (%s): %d filing(s)" % (c, f["name"] if f else "", n))
        return
    # default: run server
    total, ing = count_funds()
    if not ("@" in SEC_UA and "example.com" not in SEC_UA):
        print("\n  ⚠  SEC_UA is not set. Holdings will FAIL to load until you do:")
        print('      export SEC_UA="WhaleWatch/1.0 (Your Name you@gmail.com)"')
        print("      then restart this in the same terminal.\n")
    if total == 0:
        print("  Note: directory empty — run  python3 whalewatch.py build  for full search.")
    else:
        print("  Directory: %d funds (%d with holdings)." % (total, ing))
    print("  WhaleWatch -> http://localhost:8000   (Ctrl+C to stop)\n")
    start_prewarm()
    app.run(host="0.0.0.0", port=8000, debug=False)

# Under gunicorn/WSGI the module is imported (not __main__), and CLI commands like
# `refresh`/`build` run via main() below — so warm the cache only on the serving path.
if __name__ != "__main__":
    start_prewarm()

if __name__ == "__main__":
    main()
