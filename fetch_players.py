#!/usr/bin/env python3
"""
THE SHIFT — NHL Player Data Fetcher
Merges NHL Stats API (counting stats) + Natural Stat Trick (advanced stats)
Fetches both 20242025 (previous) and 20252026 (current) seasons.
Writes: docs/players.json
"""

import json
import time
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installing requests...")
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "requests", "-q"])
    import requests

# ── CONFIG ───────────────────────────────────────────────────────────────────
SEASONS        = ["20252026", "20242025"]   # current first, previous second
CURRENT_SEASON = SEASONS[0]
GAME_TYPE      = 2                          # regular season
OUTPUT_PATH    = Path("docs/players.json")
NHL_PAGE_SIZE  = 100

# League-average SH% used as xG fallback when NST is unavailable
LEAGUE_AVG_SH_PCT = 0.105

NHL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TheShiftBot/1.0)",
    "Accept": "application/json",
}
NST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.naturalstattrick.com/",
}

# ── HELPERS ──────────────────────────────────────────────────────────────────
def normalize_name(name: str) -> str:
    name = name.lower().strip()
    for k, v in {
        "é":"e","è":"e","ê":"e","ë":"e","á":"a","à":"a","â":"a","ä":"a",
        "í":"i","ì":"i","î":"i","ï":"i","ó":"o","ò":"o","ô":"o","ö":"o",
        "ú":"u","ù":"u","û":"u","ü":"u","ý":"y","ñ":"n","ç":"c","ř":"r",
        "š":"s","ž":"z","č":"c","ě":"e","ů":"u","ď":"d","ť":"t","ň":"n",
    }.items():
        name = name.replace(k, v)
    return " ".join(re.sub(r"[^a-z ]", "", name).split())

def safe_get(url, headers, params=None, retries=3, delay=2.0):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None

def safe_float(v, default=None):
    try:    return float(v)
    except: return default

def safe_int(v, default=0):
    try:    return int(float(v))
    except: return default

def seconds_to_mmss(seconds: float) -> str:
    if not seconds: return "0:00"
    return f"{int(seconds // 60)}:{int(seconds % 60):02d}"


# ── NHL API FETCHERS ─────────────────────────────────────────────────────────
def fetch_nhl_endpoint(endpoint, sort, season_id, label):
    url = f"https://api.nhle.com/stats/rest/en/skater/{endpoint}"
    all_rows, start = [], 0
    print(f"  Fetching {label}...")
    while True:
        params = {
            "limit": NHL_PAGE_SIZE, "start": start, "sort": sort,
            "cayenneExp": f"seasonId={season_id} and gameTypeId={GAME_TYPE}",
        }
        r = safe_get(url, NHL_HEADERS, params)
        if not r: break
        rows = r.json().get("data", [])
        if not rows: break
        all_rows.extend(rows)
        if len(rows) < NHL_PAGE_SIZE: break
        start += NHL_PAGE_SIZE
        time.sleep(0.35)
    print(f"    → {len(all_rows)} rows")
    return all_rows

def fetch_season(season_id):
    return {
        "summary":     fetch_nhl_endpoint("summary",     "points",      season_id, "summary"),
        "realtime":    fetch_nhl_endpoint("realtime",    "hits",        season_id, "realtime"),
        "powerplay":   fetch_nhl_endpoint("powerplay",   "ppPoints",    season_id, "powerplay"),
        "penaltykill": fetch_nhl_endpoint("penaltykill", "shTimeOnIce", season_id, "penaltykill"),
        "shootout":    fetch_nhl_endpoint("shootout",    "shootoutGoals", season_id, "shootout"),
    }


# ── NST FETCHERS ─────────────────────────────────────────────────────────────
def fetch_nst_onice(season_id, sit="5v5") -> list[dict]:
    """On-ice stats (CF%, xGF%) from playerteams.php with stdoi=oi."""
    url = "https://www.naturalstattrick.com/playerteams.php"
    label = f"NST on-ice {sit} [{season_id}]"
    params = {
        "fromseason": season_id, "thruseason": season_id,
        "stype": 2, "sit": sit, "score": "all", "stdoi": "oi",
        "rate": "n", "team": "ALL", "pos": "S", "loc": "B",
        "toi": 0, "gpfilt": "none", "fd": "", "td": "",
        "tgp": 410, "lines": "single", "draftteam": "ALL",
    }
    print(f"  Fetching {label}...")
    r = safe_get(url, NST_HEADERS, params)
    if not r:
        print(f"    → failed, will use fallback")
        return []
    rows = parse_nst_html(r.text, label)
    time.sleep(1.0)
    return rows


# NST individual page (skatersindividual.php) returns 404 and playerteams.php?stdoi=ind
# renders via JavaScript — not scrapeable without a headless browser.
# ixG is instead estimated from NHL API shot data using positional shot quality weights.
# See compute_ixg() below.

# NST uses different table IDs depending on the view
NST_TABLE_IDS = {"players", "skaters", "skaterstats", "report"}

def parse_nst_html(html, label):
    import re as _re
    from html.parser import HTMLParser

    # Debug: report all table IDs found when we get 0 rows
    all_table_ids = _re.findall(r'<table[^>]+id="([^"]+)"', html)

    class P(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_t = self.in_th = self.in_td = False
            self.headers = []; self.rows = []; self.cur_row = []
            self.cell = ""; self.hdr_done = False
            self.active_id = None

        def handle_starttag(self, tag, attrs):
            d = dict(attrs)
            # Accept any known NST table ID
            if tag == "table" and d.get("id", "").lower() in NST_TABLE_IDS:
                self.in_t = True
                self.active_id = d.get("id")
            if not self.in_t: return
            if tag == "th":   self.in_th = True;  self.cell = ""
            elif tag == "td": self.in_td = True;  self.cell = ""
            elif tag == "tr" and self.hdr_done: self.cur_row = []

        def handle_endtag(self, tag):
            if not self.in_t: return
            if tag == "th":
                self.headers.append(self.cell.strip()); self.in_th = False
            elif tag == "td":
                self.cur_row.append(self.cell.strip()); self.in_td = False
            elif tag == "tr":
                if not self.hdr_done and self.headers: self.hdr_done = True
                elif self.cur_row and len(self.cur_row) > 3:
                    self.rows.append(dict(zip(self.headers, self.cur_row)))
            elif tag == "table": self.in_t = False

        def handle_data(self, data):
            if self.in_th or self.in_td: self.cell += data

    p = P(); p.feed(html)

    if not p.rows:
        print(f"    → 0 rows ({label})")
        print(f"    DEBUG: table IDs in page: {all_table_ids or 'none found'}")
        # Print first column headers we can find to help diagnose
        import re as re2
        ths = re2.findall(r'<th[^>]*>\s*(.*?)\s*</th>', html[:6000])
        if ths:
            print(f"    DEBUG: first headers found: {ths[:10]}")
        return []

    print(f"    → {len(p.rows)} rows ({label}) [table id={p.active_id!r}]")
    # Print header keys on first successful parse to confirm field names
    if p.rows:
        print(f"    DEBUG: columns: {list(p.rows[0].keys())[:15]}")
    return p.rows

def nst_index(rows):
    idx = {}
    for row in rows:
        name = row.get("Player", row.get("Name", ""))
        key  = normalize_name(name)
        if key: idx[key] = row
    return idx


# ── MERGE ONE SEASON ─────────────────────────────────────────────────────────
def merge_season(nhl, nst5_idx, _nsti_idx=None):
    rt_map = {r["playerId"]: r for r in nhl["realtime"]}
    pp_map = {r["playerId"]: r for r in nhl["powerplay"]}
    pk_map = {r["playerId"]: r for r in nhl["penaltykill"]}
    so_map = {r["playerId"]: r for r in nhl.get("shootout", [])}

    players = {}
    for s in nhl["summary"]:
        pid  = s.get("playerId")
        name = s.get("skaterFullName", "")
        key  = normalize_name(name)

        rt = rt_map.get(pid, {})
        pp = pp_map.get(pid, {})
        pk = pk_map.get(pid, {})
        so = so_map.get(pid, {})
        n5 = nst5_idx.get(key, {})

        gp = safe_int(s.get("gamesPlayed"))

        # ── TOI: summary gives per-game seconds ──
        toi_sec = safe_float(s.get("timeOnIcePerGame"), 0)

        # PP/PK endpoints give TOTAL seconds for the season
        pp_toi_total = safe_float(pp.get("ppTimeOnIce"), 0)
        pp_toi_sec   = (pp_toi_total / gp) if gp > 0 else 0

        pk_toi_total = safe_float(pk.get("shTimeOnIce"), 0)
        pk_toi_sec   = (pk_toi_total / gp) if gp > 0 else 0

        # ── PP stats ──
        pp_g  = safe_int(pp.get("ppGoals"))
        pp_a1 = safe_int(pp.get("ppPrimaryAssists"))
        pp_a2 = safe_int(pp.get("ppSecondaryAssists"))
        pp_pts = pp_g + pp_a1 + pp_a2

        # ── PK / shorthanded stats ──
        sh_g   = safe_int(pk.get("shGoals"))
        sh_a   = safe_int(pk.get("shAssists", 0))
        sh_pts = sh_g + sh_a

        # ── Shooting % fix: API returns decimal ──
        shots      = safe_int(s.get("shots"))
        sh_pct_raw = safe_float(s.get("shootingPctg"))
        if sh_pct_raw is not None:
            # API returns decimal (0.142) or sometimes percentage (14.2) — handle both
            sh_pct = round(sh_pct_raw * 100, 1) if sh_pct_raw <= 1.0 else round(sh_pct_raw, 1)
        elif shots > 0:
            # Fallback: compute directly from goals/shots
            sh_pct = round(safe_int(s.get("goals")) / shots * 100, 1)
        else:
            sh_pct = 0.0

        # ── Advanced (NST) ──
        cf_pct     = safe_float(n5.get("CF%"))
        xgf_pct    = safe_float(n5.get("xGF%"))
        xg_for     = safe_float(n5.get("xGF"))
        xg_against = safe_float(n5.get("xGA"))
        # ixG computed from NHL API shots using positional shot quality weights.
        # Weights derived from historical NHL finishing rates by position:
        #   C/LW/RW: ~10.5%  D: ~6.5%  (shots from distance skew lower)
        pos_code = s.get("positionCode", "")
        shot_quality_weight = 0.065 if pos_code == "D" else 0.105
        ixg = round(shots * shot_quality_weight, 2) if shots > 0 else 0.0
        ixg_source = "estimated"
        icf = None  # requires NST headless scrape

        # ── Derived ──
        g      = safe_int(s.get("goals"))
        ev_pts = safe_int(s.get("evPoints"))
        ev_toi_h = max(0, ((toi_sec - pp_toi_sec - pk_toi_sec) * gp)) / 3600
        pp_toi_h = pp_toi_total / 3600

        players[pid] = {
            "id": pid, "name": name,
            "team": s.get("teamAbbrevs", ""),
            "team_full": s.get("teamFullName", ""),
            "pos": s.get("positionCode", ""),
            "number": str(s.get("sweaterNumber", "")),
            "gp": gp,
            "g":  g,
            "a":  safe_int(s.get("assists")),
            "pts":safe_int(s.get("points")),
            "plus_minus": safe_int(s.get("plusMinus")),
            "pim": safe_int(s.get("penaltyMinutes")),
            "shots": shots,
            "sh_pct": sh_pct,
            "ev_g":   safe_int(s.get("evGoals")),
            "ev_pts": ev_pts,
            "pp_g": pp_g, "pp_a": pp_a1 + pp_a2,
            "pp_a1": pp_a1, "pp_a2": pp_a2, "pp_pts": pp_pts,
            "sh_g": sh_g, "sh_a": sh_a, "sh_pts": sh_pts,
            "toi": seconds_to_mmss(toi_sec),   "toi_sec": toi_sec,
            "pp_toi": seconds_to_mmss(pp_toi_sec), "pp_toi_sec": pp_toi_sec,
            "pk_toi": seconds_to_mmss(pk_toi_sec), "pk_toi_sec": pk_toi_sec,
            "hits": safe_int(rt.get("hits")),
            "blocks": safe_int(rt.get("blockedShots")),
            "takeaways": safe_int(rt.get("takeaways")),
            "giveaways": safe_int(rt.get("giveaways")),
            "cf_pct": cf_pct, "xgf_pct": xgf_pct,
            "xg_for": xg_for, "xg_against": xg_against,
            "ixg": ixg, "ixg_source": ixg_source, "icf": icf,
            "p60_5v5": round(ev_pts / ev_toi_h, 2) if ev_toi_h > 0 else None,
            "p60_pp":  round(pp_pts / pp_toi_h, 2) if pp_toi_h > 0 else None,
            "xg_diff": round(g - ixg, 2) if ixg is not None else None,
            "ozs_pct": safe_float(s.get("offensiveZoneFaceoffPct")),
            "so_g": safe_int(so.get("soGoals")),
            "so_att": safe_int(so.get("soShots", so.get("soAttempts", 0))),
        }
    return players


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"  THE SHIFT  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Seasons: {', '.join(SEASONS)}")
    print(f"{'='*60}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    season_data = {}

    for season_id in SEASONS:
        print(f"\n── {season_id} ──────────────────────────────────────────")
        nhl  = fetch_season(season_id)
        nst5 = fetch_nst_onice(season_id, "5v5")
        print(f"  Merging...")
        season_data[season_id] = merge_season(nhl, nst_index(nst5), {})
        print(f"  → {len(season_data[season_id])} players merged")

    # Build final list: current season primary, attach prev_ fields
    current  = season_data[SEASONS[0]]
    previous = season_data[SEASONS[1]] if len(SEASONS) > 1 else {}
    prev_by_name = {normalize_name(v["name"]): v for v in previous.values()}

    players = []
    for pid, p in current.items():
        prev = previous.get(pid) or prev_by_name.get(normalize_name(p["name"]), {})
        for k in ["gp","g","a","pts","sh_pct","cf_pct","xgf_pct","p60_5v5"]:
            p[f"prev_{k}"] = prev.get(k)
        players.append(p)

    # Include prev-season-only players (injured/AHL this season)
    current_names = {normalize_name(p["name"]) for p in players}
    for prev in previous.values():
        if normalize_name(prev["name"]) not in current_names:
            prev["inactive_current_season"] = True
            for k in ["gp","g","a","pts","sh_pct","cf_pct","xgf_pct","p60_5v5"]:
                prev[f"prev_{k}"] = None
            players.append(prev)

    players.sort(key=lambda x: (
        0 if x.get("inactive_current_season") else 1,
        x["pts"], x["g"]
    ), reverse=True)

    if not players:
        print("\n⚠ No players — aborting write.")
        sys.exit(1)

    output = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "current_season": CURRENT_SEASON,
        "seasons": SEASONS,
        "count": len(players),
        "players": players,
    }

    # Atomic write with JSON validation
    tmp = OUTPUT_PATH.with_suffix(".tmp.json")
    tmp.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    try:
        json.loads(tmp.read_text())
    except json.JSONDecodeError as e:
        print(f"\n⚠ Invalid JSON output: {e} — aborting.")
        tmp.unlink(); sys.exit(1)
    tmp.replace(OUTPUT_PATH)

    nst_n = sum(1 for p in players if p.get("ixg_source") == "nst")
    est_n = sum(1 for p in players if p.get("ixg_source") == "estimated")
    print(f"\n✓ {len(players)} players → {OUTPUT_PATH}  ({OUTPUT_PATH.stat().st_size/1024:.0f} KB)")
    print(f"  xG: {nst_n} from NST  |  {est_n} estimated\n")

if __name__ == "__main__":
    main()
