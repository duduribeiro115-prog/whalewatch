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
def _throttle():
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

# ---------------------------------------------------------------------------
# Ticker <-> company name (so users can search "AAPL" or "Apple")
# 13F filings have NO ticker, only issuer name. SEC publishes a free
# ticker<->name list; we match it against holding names by normalization.
# ---------------------------------------------------------------------------
_SUFFIX = {"INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LTD",
           "LIMITED", "PLC", "LLC", "LP", "NV", "SA", "AG", "THE", "HOLDINGS",
           "HLDGS", "HLDG", "HOLDING", "GROUP", "GRP", "TRUST", "CLASS", "CL",
           "COM", "COMMON", "SHARES", "ADR", "ADS", "NEW", "SPONSORED", "PAR"}

def _norm(s):
    s = re.sub(r"[^A-Za-z0-9 ]", " ", (s or "").upper())
    toks = [t for t in s.split() if t and t not in _SUFFIX and len(t) > 1]
    return " ".join(toks)

_TMAPS = None
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

def ticker_maps():
    """Return (ticker->title, normalized_name->ticker). Cached; needs network once."""
    global _TMAPS
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
            n = _norm(title)
            if n and n not in norm2t:
                norm2t[n] = t
    except Exception:
        pass
    _TMAPS = (t2title, norm2t)
    return _TMAPS

def ticker_for(name):
    _, norm2t = ticker_maps()
    return norm2t.get(_norm(name))

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
    rows.sort(key=lambda r: r["value"], reverse=True)
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
    latest = """SELECT f.cik,f.accession FROM filings f
        JOIN (SELECT cik,MAX(report_date) mr FROM filings GROUP BY cik) m
        ON f.cik=m.cik AND f.report_date=m.mr"""
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

# ---------------------------------------------------------------------------
# Ingest + payload
# ---------------------------------------------------------------------------
def ingest_fund(cik, max_filings=2, force=False):
    cik = str(cik).zfill(10)
    info = list_13f_filings(cik)
    upsert_fund(cik, info["name"])
    new = 0
    for f in info["filings"][:max_filings]:
        if not force and has_filing(f["accession"]):
            continue
        try:
            raw = fetch_info_table(cik, f["accession"])
        except Exception as e:
            sys.stderr.write("  ! %s %s: %s\n" % (cik, f["accession"], e)); continue
        mult = value_multiplier(f["reportDate"])
        agg = aggregate([{**h, "value": h["value"] * mult} for h in raw])
        save_filing(cik, f["accession"], f["form"], f["filingDate"], f["reportDate"], list(agg.values()))
        new += 1
    return new

def build_fund_payload(cik):
    cik = str(cik).zfill(10)
    fund = get_fund(cik) or {"name": cik, "cik": cik}
    filings = get_fund_filings(cik)
    if not filings:
        return {"error": "No 13F-HR holdings found", "name": fund["name"], "cik": cik}
    curr = filings[0]
    curr_rows = aggregate(get_holdings(curr["accession"]))
    if len(filings) > 1:
        prev_rows = aggregate(get_holdings(filings[1]["accession"]))
        holdings = diff(curr_rows, prev_rows); prev = filings[1]
    else:
        holdings = sorted([{**c, "status": "NEW", "sharesChangePct": None} for c in curr_rows.values()],
                          key=lambda r: r["value"], reverse=True); prev = None
    active = [h for h in holdings if h["status"] != "SOLD"]
    return {"name": fund["name"], "cik": cik,
            "current": {"form": curr["form"], "filingDate": curr["filing_date"], "reportDate": curr["report_date"]},
            "previous": ({"reportDate": prev["report_date"]} if prev else None),
            "totalValue": sum(h["value"] for h in active), "positions": len(active), "holdings": holdings}

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

def refresh():
    init_schema()
    print("[%s] refresh start" % dt.datetime.now().isoformat(timespec="seconds"))
    seen = build_directory(2)
    funds = funds_needing_refresh(); updated = 0
    for f in funds:
        try:
            if ingest_fund(f["cik"], 2, force=False):
                updated += 1; print("  + %s (%s): new filing" % (f["name"], f["cik"]))
                # NOTE: fire push/email alert to watchers here.
        except Exception as e:
            sys.stderr.write("  ! %s: %s\n" % (f["cik"], e))
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
    else{e.innerHTML=`📒 <b>${hl.funds.toLocaleString()}</b> funds searchable · ${hl.ingested.toLocaleString()} with holdings loaded`;}
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
      ?`Searching across <b>${s.ingested.toLocaleString()}</b> funds with loaded holdings.`
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
async function openStock(cusip,name){window.scrollTo(0,0);
  app.innerHTML=`<span class="back" onclick="setTab('stocks')">‹ back</span><div class="spin">Loading holders…</div>`;
  try{const d=await api('/api/stock?cusip='+encodeURIComponent(cusip));
    const h=d.holders||[];
    let out=`<span class="back" onclick="setTab('stocks')">‹ back</span>
      <div class="fhead"><div class="h1">${d.security}${d.ticker?` <span style="color:var(--acc)">${d.ticker}</span>`:''}</div>
      <div class="muted" style="font-size:13px">CUSIP ${cusip} · held by ${d.count} loaded fund${d.count!==1?'s':''}</div></div>
      <div class="card" style="padding:4px 14px">`;
    h.forEach(r=>{out+=`<div class="hrow">
      <div class="tkr">${tkr(r.fund)}</div>
      <div class="grow"><div class="nm">${r.fund}</div>
        <div class="sub">${(r.shares||0).toLocaleString()} sh · as of ${r.report_date}</div></div>
      <div class="right"><div class="val">${fmt(r.value)}</div></div></div>`;});
    out+=`</div><div class="muted" style="font-size:11.5px;text-align:center;margin:14px 0">Across funds whose latest 13F is loaded in your DB.</div>`;
    app.innerHTML=out;
  }catch(e){app.innerHTML=`<span class="back" onclick="setTab('stocks')">‹ back</span><div class="empty">Couldn't load holders.</div>`;}
}

function evt(e){e.stopPropagation();}
function star(cik,name){toggleWatch({cik,name});setTab(tab);}

async function doSearch(q){
  app.innerHTML=`<div class="spin">Searching EDGAR…</div>`;
  try{const res=await api('/api/search?q='+encodeURIComponent(q));
    if(!res.length){app.innerHTML=`<div class="empty"><div class="big">🤷</div>No 13F filers found for “${q}”.<br>Try a manager's legal name or a CIK number.</div>`;return;}
    app.innerHTML=`<div class="sec-title">Results (${res.length})</div>`+res.map(f=>fundCard(f)).join('');
  }catch(e){app.innerHTML=errBox(false,e.message);}
}

async function openFund(cik,name){
  window.scrollTo(0,0);
  app.innerHTML=`<span class="back" onclick="setTab('search')">‹ back</span><div class="spin">Loading ${name||'fund'} holdings from EDGAR…<br><span style="font-size:12px">(first open downloads from SEC — can take 5–15s)</span></div>`;
  try{const d=await api('/api/holdings/'+cik);renderFund(d);}
  catch(e){app.innerHTML=errBox(true,e.message);}
}
async function loadDemo(){window.scrollTo(0,0);
  app.innerHTML=`<div class="spin">Loading sample…</div>`;
  try{renderFund(await api('/api/demo'),true);}catch(e){app.innerHTML=`<div class="empty">Sample unavailable.</div>`;}
}

let curFund=null, filter='ALL';
function renderFund(d,demo){
  if(d.error){app.innerHTML=`<span class="back" onclick="setTab('search')">‹ back</span><div class="empty"><div class="big">📭</div>${d.error} for ${d.name||''}.</div>`;return;}
  curFund=d; filter='ALL';
  drawFund(demo);
}
function drawFund(demo){
  const d=curFund, on=inWatch(d.cik), al=store.alerts[d.cik];
  const counts={NEW:0,ADDED:0,TRIMMED:0,SOLD:0,HOLD:0};
  d.holdings.forEach(h=>counts[h.status]=(counts[h.status]||0)+1);
  const period=(d.current&&d.current.reportDate)||'';
  let h=`<span class="back" onclick="setTab('search')">‹ back</span>`;
  if(demo||d.demo)h+=`<div class="banner">Sample data — run <b>python app.py</b> and search to pull live filings.</div>`;
  h+=`<div class="fhead"><div class="h1">${d.name}</div>
     <div class="muted" style="font-size:13px">13F-HR · period ending ${period} · filed ${(d.current&&d.current.filingDate)||''}</div></div>
   <div class="stats">
     <div class="stat"><div class="k">Portfolio value</div><div class="v">${fmt(d.totalValue)}</div></div>
     <div class="stat"><div class="k">Positions</div><div class="v">${d.positions}</div></div>
   </div>
   <button class="alertbtn ${al?'on':''}" onclick="toggleAlert('${d.cik}','${(d.name||'').replace(/'/g,'')}')">
     ${al?'🔔 Alerts on — you’ll be notified on new filings':'🔔 Alert me when this fund files'}</button>
   <button class="alertbtn ${on?'on':''}" style="margin-top:8px" onclick="star('${d.cik}','${(d.name||'').replace(/'/g,'')}');drawFund(${demo?true:false})">
     ${on?'★ In your watchlist':'☆ Add to watchlist'}</button>`;
  // filters
  const fl=['ALL','NEW','ADDED','TRIMMED','SOLD','HOLD'];
  h+=`<div class="filterbar">`+fl.map(f=>{const n=f==='ALL'?d.holdings.length:(counts[f]||0);
     return `<span class="chip ${filter===f?'on':''}" onclick="setFilter('${f}')">${f}${f!=='ALL'?' '+n:''}</span>`;}).join('')+`</div>`;
  // rows
  h+=`<div class="card" style="padding:4px 14px">`;
  const rows=d.holdings.filter(x=>filter==='ALL'||x.status===filter);
  if(!rows.length)h+=`<div class="empty" style="padding:30px">No ${filter} positions.</div>`;
  rows.forEach(x=>{
    let p='';
    if(x.status==='SOLD')p=`<div class="pct dn">sold out</div>`;
    else if(x.sharesChangePct===null)p=`<div class="pct up">new buy</div>`;
    else if(x.sharesChangePct!==0)p=`<div class="pct ${x.sharesChangePct>0?'up':'dn'}">${pct(x.sharesChangePct)} shares</div>`;
    h+=`<div class="hrow">
      <div class="tkr">${tkr(x.name)}</div>
      <div class="grow"><div class="nm">${x.name}</div>
        <div class="sub">${(x.shares||0).toLocaleString()} sh · ${x.class||''} ${x.putCall?'· '+x.putCall:''}</div></div>
      <div class="right"><div class="val">${fmt(x.value)}</div>
        <span class="badge b-${x.status}">${x.status}</span>${p}</div>
    </div>`;});
  h+=`</div><div class="muted" style="font-size:11.5px;text-align:center;margin:14px 0 4px">
     Source: SEC EDGAR 13F-HR. Values per filing. Changes vs prior quarter by shares held.</div>`;
  app.innerHTML=h;
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
    fund = get_fund(cik)
    if not fund or fund.get("holdings_status") != "ingested":
        try:
            ingest_fund(cik, 2)
        except Exception as e:
            return jsonify({"error": "Could not load from EDGAR: %s" % e, "cik": cik})
    return jsonify(build_fund_payload(cik))

@app.route("/api/stock")
def _stock():
    cusip = request.args.get("cusip", "").strip()
    q = request.args.get("q", "").strip()
    if cusip:
        holders = holders_of(cusip=cusip)
        name = holders[0]["security"] if holders else cusip
        return jsonify({"security": name, "cusip": cusip, "ticker": ticker_for(name),
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
    app.run(host="0.0.0.0", port=8000, debug=False)

if __name__ == "__main__":
    main()
