# KBO_crawl.py
# -----------------------------------------
# KBO 경기 리뷰 크롤러 (gameId 기반, 완료 경기만 저장, 구간 upsert)
# -----------------------------------------
import os, re, sys, time, argparse, csv
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SCHEDULE_URL = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
REVIEW_URL   = "https://www.koreabaseball.com/GameCenter/Main.aspx?gameId={gid}&section=REVIEW"

DEFAULT_SINCE = "20250322"
WAIT_SEC = 12
SKIP_KEYWORDS = ("예정", "취소", "우천", "노게임")
GAMEID_RE = re.compile(r'gameId[=:\'"]?(\d{8})')

def yyyymmdd(d: date) -> str: return d.strftime("%Y%m%d")
def to_iso(yyyymmdd_str: str) -> str: return datetime.strptime(yyyymmdd_str, "%Y%m%d").date().isoformat()
def norm(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()
def safe_int(s: str, default: int = 0) -> int:
    try: return int(re.sub(r"[^\d]", "", s))
    except: return default

# ---------------- Selenium ----------------
def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new"); opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage"); opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    drv = webdriver.Chrome(options=opts); drv.set_page_load_timeout(60)
    return drv

def goto_schedule(drv: webdriver.Chrome) -> None:
    drv.get(SCHEDULE_URL)
    WebDriverWait(drv, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    time.sleep(2); drv.refresh()
    WebDriverWait(drv, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    time.sleep(1)

def collect_gameids_from_schedule_html(html: str, target_yyyymmdd: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    gids = []
    for a in soup.find_all("a"):
        payload = (a.get("href", "") or "") + " " + (a.get("onclick", "") or "")
        m = GAMEID_RE.search(payload)
        if not m: continue
        row = a.find_parent("tr")
        row_text = norm(row.get_text(" ")) if row else ""
        if any(k in row_text for k in SKIP_KEYWORDS):  # 예정/취소 등 제외
            continue
        gid = m.group(1)
        if gid.startswith(target_yyyymmdd):
            gids.append(gid)
    return sorted(set(gids))

def collect_gameids_for_date(d: date) -> List[str]:
    drv = make_driver()
    try:
        goto_schedule(drv)
        html = drv.page_source
        return collect_gameids_from_schedule_html(html, yyyymmdd(d))
    finally:
        drv.quit()

# --------- 리뷰 파서(완료 경기만 반환) ----------
def fetch_review(gid: str) -> Optional[Dict]:
    drv = make_driver()
    try:
        drv.get(REVIEW_URL.format(gid=gid))
        WebDriverWait(drv, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
        time.sleep(1)
        soup = BeautifulSoup(drv.page_source, "html.parser")
        game_date = to_iso(gid)

        stadium_el = soup.select_one("#txtStadium") or soup.select_one("#lblStadium") or soup.select_one(".stadium")
        stadium = norm(stadium_el.get_text(strip=True)) if stadium_el else ""

        # 점수(완료 여부 판단)
        away_team = home_team = ""
        away_runs = home_runs = None
        sb3 = soup.select_one("#tblScoreboard3")
        if sb3:
            ths = sb3.select("thead th")
            r_idx = None
            for i, th in enumerate(ths):
                if norm(th.get_text()) in ("R", "득점", "점수"): r_idx = i; break
            rows = []
            for tr in sb3.select("tbody tr"):
                tds = tr.find_all(["th", "td"])
                rows.append([norm(td.get_text()) for td in tds])
            if r_idx is not None and len(rows) >= 2:
                away_team = rows[0][0]; home_team = rows[1][0]
                away_runs = safe_int(rows[0][r_idx], 0)
                home_runs = safe_int(rows[1][r_idx], 0)

        # 보정(팀명)
        if not away_team or not home_team:
            sb1 = soup.select_one("#tblScoreboard1")
            if sb1:
                tds = [norm(td.get_text()) for td in sb1.find_all("td")]
                if len(tds) >= 3:
                    home_team = home_team or tds[0]
                    away_team = away_team or tds[-1]

        # 완료 경기만 저장(점수가 둘 다 있어야)
        if away_runs is None or home_runs is None:
            return None

        def sum_h_hr_ab(sel: str):
            tbl = soup.select_one(sel)
            if not tbl: return 0, 0, 0
            heads = [norm(th.get_text()).upper() for th in tbl.select("thead th")]
            idx = {h:i for i,h in enumerate(heads)}
            h_idx = idx.get("H") or idx.get("안타") or idx.get("HIT") or idx.get("HITS")
            hr_idx = idx.get("HR") or idx.get("홈런") or idx.get("HOMERUN")
            ab_idx = idx.get("AB") or idx.get("타수")
            H = HR = AB = 0
            for tr in tbl.select("tbody tr"):
                cells = [norm(td.get_text()) for td in tr.find_all("td")]
                if not cells: continue
                if h_idx is not None and h_idx < len(cells):  H  += safe_int(cells[h_idx], 0)
                if hr_idx is not None and hr_idx < len(cells): HR += safe_int(cells[hr_idx], 0)
                if ab_idx is not None and ab_idx < len(cells): AB += safe_int(cells[ab_idx], 0)
            return H, HR, AB

        home_H, home_HR, home_AB = 0, 0, 0
        away_H, away_HR, away_AB = 0, 0, 0
        for sel in ["#tblHomeHitter2", "#tblHomeHitter", "#tblHitterHome"]:
            h, hr, ab = sum_h_hr_ab(sel)
            if h or hr or ab: home_H, home_HR, home_AB = h, hr, ab; break
        for sel in ["#tblAwayHitter2", "#tblAwayHitter", "#tblHitterAway"]:
            h, hr, ab = sum_h_hr_ab(sel)
            if h or hr or ab: away_H, away_HR, away_AB = h, hr, ab; break

        # 승/패/무
        if away_runs > home_runs:
            away_res, home_res = "승", "패"
        elif away_runs < home_runs:
            away_res, home_res = "패", "승"
        else:
            away_res = home_res = "무"

        def div(a,b): return round(a/b,4) if b else 0.0

        return {
            # 앱과 동일 스키마(16열)
            "date":    game_date,
            "stadium": stadium,
            "away_team": away_team, "home_team": home_team,
            "away_score": away_runs, "home_score": home_runs,
            "away_result": away_res, "home_result": home_res,
            "away_hit": away_H, "home_hit": home_H,
            "away_hr": away_HR, "home_hr": home_HR,
            "away_ab": away_AB, "home_ab": home_AB,
            "away_avg": div(away_H, away_AB),
            "home_avg": div(home_H, home_AB),
        }
    except Exception as e:
        print(f"[ERR] fetch_review({gid}): {e}", file=sys.stderr)
        return None
    finally:
        drv.quit()

# -------------- CSV upsert ----------------
SCHEMA = ["date","stadium","away_team","home_team","away_score","home_score",
          "away_result","home_result","away_hit","home_hit","away_hr","home_hr",
          "away_ab","home_ab","away_avg","home_avg"]

def upsert_range(out_csv: str, rows: List[Dict], since: date, until: date) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
    new_df = pd.DataFrame(rows, columns=SCHEMA)
    if os.path.exists(out_csv):
        old = pd.read_csv(out_csv, encoding="utf-8-sig")
        # 스키마 강제 정렬
        for c in SCHEMA:
            if c not in old.columns: old[c] = pd.Series([None]*len(old))
        old = old[SCHEMA]
        # 지정 구간 삭제 후 신규 합치기
        mask = (pd.to_datetime(old["date"])>=pd.to_datetime(since)) & (pd.to_datetime(old["date"])<=pd.to_datetime(until))
        kept = old.loc[~mask].copy()
        out = pd.concat([kept, new_df], ignore_index=True)
    else:
        out = new_df
    out = out.sort_values("date").reset_index(drop=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] upserted {len(new_df)} rows into {out_csv}")

# ------------------- main -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=DEFAULT_SINCE)
    ap.add_argument("--until", default=None)
    ap.add_argument("--out",   default="data/kbo_latest.csv")
    args = ap.parse_args()

    since = datetime.strptime(args.since, "%Y%m%d").date()
    until = datetime.strptime(args.until, "%Y%m%d").date() if args.until else date.today()

    all_rows: List[Dict] = []
    d = since
    while d <= until:
        print(f"[DAY] {d.isoformat()} 수집 시도…")
        gids = collect_gameids_for_date(d)
        if not gids:
            print(f"[DAY] {d.isoformat()} gameId 없음(경기 없거나 미노출).")
        for gid in gids:
            row = fetch_review(gid)
            if row:
                all_rows.append(row)
        time.sleep(1.0)
        d += timedelta(days=1)

    print(f"[DONE] collected rows: {len(all_rows)}")
    if all_rows:
        upsert_range(args.out, all_rows, since, until)

if __name__ == "__main__":
    main()
