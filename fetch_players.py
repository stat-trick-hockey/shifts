#!/usr/bin/env python3
"""
THE SHIFT — NHL Player Data Fetcher
Merges NHL Stats API (counting stats) + Natural Stat Trick (advanced stats)
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
    subprocess.run([sys.executable, "-m", "pip", "install", "requests", "--break-system-packages", "-q"])
    import requests

# ── CONFIG ──────────────────────────────────────────────────────────────────
SEASON_ID   = "20242025"
GAME_TYPE   = 2          # regular season
OUTPUT_PATH = Path("docs/players.json")
NHL_PAGE_SIZE = 100      # NHL API max per request

NHL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TheShiftBot/1.0)",
    "Accept": "application/json",
}

NST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.naturalstattrick.com/",
}

# ── HELPERS ─────────────────────────────────────────────────────────────────
def normalize_name(name: str) -> str:
    """Lowercase, strip accents-ish, remove punctuation for matching."""
    name = name.lower().strip()
    replacements = {
        "é":"e","è":"e","ê":"e","ë":"e",
        "á":"a","à":"a","â":"a","ä":"a",
        "í":"i","ì":"i","î":"i","ï":"i",
        "ó":"o","ò":"o","ô":"o","ö":"o",
        "ú":"u","ù":"u","û":"u","ü":"u",
        "ý":"y","ñ":"n","ç":"c","ř":"r",
        "š":"s","ž":"z","č":"c","ě":"e",
        "ů":"u","ď":"d","ť":"t","ň":"n",
    }
    for k, v in replacements.items():
        name = name.replace(k, v)
    name = re.sub(r"[^a-z ]", "", name)
    return " ".join(name.split())


def safe_get(url: str, headers: dict, params: dict = None, retries: int = 3, delay: float = 2.0):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


# ── NHL STATS API ────────────────────────────────────────────────────────────
def fetch_nhl_summary() -> list[dict]:
    """
    Endpoint: https://api.nhle.com/stats/rest/en/skater/summary
    Returns all skaters with: GP, G, A, PTS, +/-, TOI, PP/PK TOI
    """
    url = "https://api.nhle.com/stats/rest/en/skater/summary"
    all_players = []
    start = 0

    print("Fetching NHL summary stats...")
    while True:
        params = {
            "limit": NHL_PAGE_SIZE,
            "start": start,
            "sort": "points",
            "cayenneExp": f"seasonId={SEASON_ID} and gameTypeId={GAME_TYPE}",
        }
        r = safe_get(url, NHL_HEADERS, params)
        if not r:
            print(f"  Failed at offset {start}, stopping.")
            break
        data = r.json()
        rows = data.get("data", [])
        if not rows:
            break
        all_players.extend(rows)
        print(f"  Fetched {len(all_players)} skaters so far...")
        if len(rows) < NHL_PAGE_SIZE:
            break
        start += NHL_PAGE_SIZE
        time.sleep(0.4)

    print(f"  Total NHL skaters: {len(all_players)}")
    return all_players


def fetch_nhl_realtime() -> list[dict]:
    """
    Endpoint: https://api.nhle.com/stats/rest/en/skater/realtime
    Returns: hits, blocks, takeaways, giveaways, missed shots
    """
    url = "https://api.nhle.com/stats/rest/en/skater/realtime"
    all_players = []
    start = 0

    print("Fetching NHL realtime stats...")
    while True:
        params = {
            "limit": NHL_PAGE_SIZE,
            "start": start,
            "sort": "hits",
            "cayenneExp": f"seasonId={SEASON_ID} and gameTypeId={GAME_TYPE}",
        }
        r = safe_get(url, NHL_HEADERS, params)
        if not r:
            break
        rows = r.json().get("data", [])
        if not rows:
            break
        all_players.extend(rows)
        if len(rows) < NHL_PAGE_SIZE:
            break
        start += NHL_PAGE_SIZE
        time.sleep(0.4)

    print(f"  Total realtime rows: {len(all_players)}")
    return all_players


def fetch_nhl_powerplay() -> list[dict]:
    """
    Endpoint: https://api.nhle.com/stats/rest/en/skater/powerplay
    Returns: PP goals, assists, TOI breakdown
    """
    url = "https://api.nhle.com/stats/rest/en/skater/powerplay"
    all_players = []
    start = 0

    print("Fetching NHL power play stats...")
    while True:
        params = {
            "limit": NHL_PAGE_SIZE,
            "start": start,
            "sort": "ppPoints",
            "cayenneExp": f"seasonId={SEASON_ID} and gameTypeId={GAME_TYPE}",
        }
        r = safe_get(url, NHL_HEADERS, params)
        if not r:
            break
        rows = r.json().get("data", [])
        if not rows:
            break
        all_players.extend(rows)
        if len(rows) < NHL_PAGE_SIZE:
            break
        start += NHL_PAGE_SIZE
        time.sleep(0.4)

    print(f"  Total PP rows: {len(all_players)}")
    return all_players


# ── NATURAL STAT TRICK ───────────────────────────────────────────────────────
def fetch_nst_5v5() -> list[dict]:
    """
    NST individual skater stats at 5v5, season-to-date.
    URL: https://www.naturalstattrick.com/playerteams.php
    Returns CSV-style table parsed from HTML.
    """
    url = "https://www.naturalstattrick.com/playerteams.php"
    season_str = f"{SEASON_ID[:4]}{SEASON_ID[4:]}"  # e.g. "20242025"
    params = {
        "fromseason": season_str,
        "thruseason": season_str,
        "stype": 2,       # regular season
        "sit": "5v5",
        "score": "all",
        "stdoi": "oi",    # on-ice
        "rate": "n",      # totals not rates
        "team": "ALL",
        "pos": "S",       # skaters
        "loc": "B",       # home + away
        "toi": 0,
        "gpfilt": "none",
        "fd": "",
        "td": "",
        "tgp": 410,
        "lines": "single",
        "draftteam": "ALL",
    }

    print("Fetching Natural Stat Trick 5v5 data...")
    r = safe_get(url, NST_HEADERS, params)
    if not r:
        print("  NST fetch failed — advanced stats will be null.")
        return []

    return parse_nst_html(r.text, "5v5")


def fetch_nst_individual() -> list[dict]:
    """
    NST individual (not on-ice) for xG scored, iCF, iSCF.
    """
    url = "https://www.naturalstattrick.com/playerteams.php"
    season_str = f"{SEASON_ID[:4]}{SEASON_ID[4:]}"
    params = {
        "fromseason": season_str,
        "thruseason": season_str,
        "stype": 2,
        "sit": "5v5",
        "score": "all",
        "stdoi": "ind",   # individual
        "rate": "n",
        "team": "ALL",
        "pos": "S",
        "loc": "B",
        "toi": 0,
        "gpfilt": "none",
        "fd": "",
        "td": "",
        "tgp": 410,
        "lines": "single",
        "draftteam": "ALL",
    }

    print("Fetching Natural Stat Trick individual data...")
    r = safe_get(url, NST_HEADERS, params)
    if not r:
        print("  NST individual fetch failed.")
        return []

    return parse_nst_html(r.text, "individual")


def parse_nst_html(html: str, table_type: str) -> list[dict]:
    """Parse NST player table from HTML. Returns list of dicts."""
    from html.parser import HTMLParser

    class TableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_table = False
            self.headers = []
            self.rows = []
            self.current_row = []
            self.in_th = False
            self.in_td = False
            self.current_cell = ""
            self.header_done = False

        def handle_starttag(self, tag, attrs):
            attrs_dict = dict(attrs)
            if tag == "table" and attrs_dict.get("id") == "players":
                self.in_table = True
            if self.in_table:
                if tag == "th":
                    self.in_th = True
                    self.current_cell = ""
                elif tag == "td":
                    self.in_td = True
                    self.current_cell = ""
                elif tag == "tr" and self.header_done:
                    self.current_row = []

        def handle_endtag(self, tag):
            if self.in_table:
                if tag == "th":
                    self.headers.append(self.current_cell.strip())
                    self.in_th = False
                elif tag == "td":
                    self.current_row.append(self.current_cell.strip())
                    self.in_td = False
                elif tag == "tr":
                    if not self.header_done and self.headers:
                        self.header_done = True
                    elif self.current_row and len(self.current_row) > 3:
                        self.rows.append(dict(zip(self.headers, self.current_row)))
                elif tag == "table" and self.in_table:
                    self.in_table = False

        def handle_data(self, data):
            if self.in_th or self.in_td:
                self.current_cell += data

    parser = TableParser()
    parser.feed(html)

    if not parser.rows:
        print(f"  No rows parsed from NST ({table_type})")
        return []

    print(f"  Parsed {len(parser.rows)} rows from NST ({table_type})")
    return parser.rows


# ── MERGE ────────────────────────────────────────────────────────────────────
def seconds_to_mmss(seconds: float) -> str:
    if not seconds:
        return "0:00"
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m}:{s:02d}"


def merge_players(
    nhl_summary: list[dict],
    nhl_realtime: list[dict],
    nhl_pp: list[dict],
    nst_5v5: list[dict],
    nst_ind: list[dict],
) -> list[dict]:

    # Index realtime and PP by playerId
    rt_map = {r["playerId"]: r for r in nhl_realtime}
    pp_map = {r["playerId"]: r for r in nhl_pp}

    # Index NST by normalized name
    def nst_index(rows: list[dict]) -> dict:
        idx = {}
        for row in rows:
            name = row.get("Player", row.get("Name", ""))
            key = normalize_name(name)
            if key:
                idx[key] = row
        return idx

    nst5_idx = nst_index(nst_5v5)
    nsti_idx = nst_index(nst_ind)

    def safe_float(v, default=None):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    def safe_int(v, default=0):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    merged = []
    for s in nhl_summary:
        pid    = s.get("playerId")
        name   = s.get("skaterFullName", "")
        key    = normalize_name(name)

        rt  = rt_map.get(pid, {})
        pp  = pp_map.get(pid, {})
        n5  = nst5_idx.get(key, {})
        ni  = nsti_idx.get(key, {})

        # TOI strings
        toi_sec    = safe_float(s.get("timeOnIcePerGame"), 0)
        pp_toi_sec = safe_float(s.get("ppTimeOnIcePerGame"), 0)
        pk_toi_sec = safe_float(s.get("shTimeOnIcePerGame"), 0)

        # Advanced stats from NST
        cf_pct  = safe_float(n5.get("CF%"))
        xgf_pct = safe_float(n5.get("xGF%"))
        xg_for  = safe_float(n5.get("xGF"))   # on-ice xG for
        xg_against = safe_float(n5.get("xGA"))

        # Individual xG from NST individual table
        ixg     = safe_float(ni.get("ixG"))    # individual xG
        icf     = safe_float(ni.get("iCF"))

        player = {
            # Identity
            "id":       pid,
            "name":     name,
            "team":     s.get("teamAbbrevs", ""),
            "team_full": s.get("teamFullName", ""),
            "pos":      s.get("positionCode", ""),
            "number":   str(s.get("sweaterNumber", "")),

            # Counting stats
            "gp":  safe_int(s.get("gamesPlayed")),
            "g":   safe_int(s.get("goals")),
            "a":   safe_int(s.get("assists")),
            "pts": safe_int(s.get("points")),
            "plus_minus": safe_int(s.get("plusMinus")),
            "pim": safe_int(s.get("penaltyMinutes")),
            "shots": safe_int(s.get("shots")),
            "shooting_pct": safe_float(s.get("shootingPctg")),

            # TOI
            "toi":    seconds_to_mmss(toi_sec),
            "toi_sec": toi_sec,
            "pp_toi": seconds_to_mmss(pp_toi_sec),
            "pp_toi_sec": pp_toi_sec,
            "pk_toi": seconds_to_mmss(pk_toi_sec),
            "pk_toi_sec": pk_toi_sec,

            # PP breakdown
            "pp_g":   safe_int(pp.get("ppGoals")),
            "pp_a":   safe_int(pp.get("ppAssists") or pp.get("ppPrimaryAssists", 0)),
            "pp_pts": safe_int(pp.get("ppPoints")),

            # Physical (realtime)
            "hits":       safe_int(rt.get("hits")),
            "blocks":     safe_int(rt.get("blockedShots")),
            "takeaways":  safe_int(rt.get("takeaways")),
            "giveaways":  safe_int(rt.get("giveaways")),

            # Advanced (NST 5v5)
            "cf_pct":   cf_pct,
            "xgf_pct":  xgf_pct,
            "xg_for":   xg_for,
            "xg_against": xg_against,

            # Individual advanced (NST)
            "ixg":  ixg,
            "icf":  icf,

            # Derived
            "ev_pts": safe_int(s.get("evPoints")),
            "ev_g":   safe_int(s.get("evGoals")),
            "ozs_pct": safe_float(s.get("offensiveZoneFaceoffPct")),
        }

        # Compute P/60 values
        ev_toi_h = ((toi_sec - pp_toi_sec - pk_toi_sec) * player["gp"]) / 3600
        pp_toi_h = (pp_toi_sec * player["gp"]) / 3600

        player["p60_5v5"] = round(player["ev_pts"] / ev_toi_h, 2) if ev_toi_h > 0 else None
        player["p60_pp"]  = round(player["pp_pts"] / pp_toi_h, 2) if pp_toi_h > 0 else None

        # xG over/under
        if player["ixg"] is not None:
            player["xg_diff"] = round(player["g"] - player["ixg"], 2)
        else:
            player["xg_diff"] = None

        merged.append(player)

    # Sort by points descending
    merged.sort(key=lambda x: (x["pts"], x["g"]), reverse=True)
    print(f"  Merged {len(merged)} players total.")
    return merged


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*56}")
    print(f"  THE SHIFT — Data Fetch  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Season: {SEASON_ID}")
    print(f"{'='*56}\n")

    # Create output directory
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Fetch from NHL API
    nhl_summary  = fetch_nhl_summary()
    nhl_realtime = fetch_nhl_realtime()
    nhl_pp       = fetch_nhl_powerplay()

    # Fetch from NST (with graceful fallback)
    nst_5v5 = fetch_nst_5v5()
    nst_ind = fetch_nst_individual()

    # Merge
    print("\nMerging datasets...")
    players = merge_players(nhl_summary, nhl_realtime, nhl_pp, nst_5v5, nst_ind)

    # Build output
    output = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "season": SEASON_ID,
        "count": len(players),
        "players": players,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\n✓ Written {len(players)} players → {OUTPUT_PATH}")
    print(f"  File size: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB\n")


if __name__ == "__main__":
    main()
