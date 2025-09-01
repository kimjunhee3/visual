# KBO_crawl.py
# -----------------------------------------
# KBO 경기 리뷰 크롤러 (gameId 기반 날짜/증분 수집/예정 스킵)
# -----------------------------------------
import os
import re
import sys
import time
import csv
import argparse
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

DEFAULT_SINCE = "20250322"  # 첫 수집 시작일(YYYYMMDD)
WAIT_SEC = 12               # 페이지 로드 대기 (네트워크 상황 따라 조정)
SKIP_KEYWORDS = ("예정", "취소", "우천", "노게임")

GAMEID_RE = re.compile(r'gameId[=:\'"]?(\d{8})')

# -----------------------------
# 유틸
# -----------------------------
def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def to_iso(yyyymmdd_str: str) -> str:
    return datetime.strptime(yyyymmdd_str, "%Y%m%d").date().isoformat()

def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def safe_int(s: str, default: int = 0) -> int:
    try:
        return int(re.sub(r"[^\d]", "", s))
    except Exception:
        return default

def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)

# -----------------------------
# Selenium setup
# -----------------------------
def make_driver() -> webdriver.Chrome:
    opts = Options()
    # GitHub Actions / 서버 환경에서도 안정적으로 동작
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    # 안정성 옵션
    opts.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver

# -----------------------------
# 스케줄 페이지 로드 (날짜 직접 조작은 불안정 → 필터로 보정)
# -----------------------------
def goto_schedule_date(driver: webdriver.Chrome, d: date) -> None:
    driver.get(SCHEDULE_URL)
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body"))
    )
    # 초기 스크립트/위젯 부팅 여유
    time.sleep(2)
    driver.refresh()
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body"))
    )
    time.sleep(1)

def collect_gameids_from_schedule_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    gids = []
    for a in soup.find_all("a"):
        payload = (a.get("href", "") or "") + " " + (a.get("onclick", "") or "")
        m = GAMEID_RE.search(payload)
        if not m:
            continue
        # 행 텍스트에서 '예정/취소/우천/노게임' 등 상태 감지 시 스킵
        row = a.find_parent("tr")
        row_text = norm_text(row.get_text(" ")) if row else ""
        if any(key in row_text for key in SKIP_KEYWORDS):
            continue
        gids.append(m.group(1))
    return sorted(set(gids))

def collect_gameids_for_date(d: date) -> List[str]:
    driver = make_driver()
    try:
        goto_schedule_date(driver, d)
        html = driver.page_source
        gids = collect_gameids_from_schedule_html(html)

        # ✅ 날짜 필터: 현재 루프 날짜(YYYYMMDD)로 시작하는 gameId만 사용
        target = d.strftime("%Y%m%d")
        gids = [g for g in gids if g.startswith(target)]

        return gids
    finally:
        driver.quit()

# -----------------------------
# 리뷰 페이지 파서
# -----------------------------
def fetch_review(gid: str) -> Optional[Dict]:
    """gameId 기준 리뷰 페이지에서 핵심 정보 파싱"""
    drv = make_driver()
    try:
        url = REVIEW_URL.format(gid=gid)
        drv.get(url)
        WebDriverWait(drv, WAIT_SEC).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body"))
        )
        time.sleep(1.0)
        soup = BeautifulSoup(drv.page_source, "html.parser")

        # 날짜는 gameId로 확정
        game_date_iso = to_iso(gid)

        # 구장명
        stadium_el = (soup.select_one("#txtStadium")
                      or soup.select_one("#lblStadium")
                      or soup.select_one(".stadium"))
        stadium = norm_text(stadium_el.get_text(strip=True)) if stadium_el else ""

        # 스코어보드 (점수)
        home_team = away_team = ""
        home_runs = away_runs = None
        sb3 = soup.select_one("#tblScoreboard3")
        if sb3:
            r_col_idx = None
            ths = sb3.select("thead th")
            for i, th in enumerate(ths):
                if norm_text(th.get_text()) in ("R", "득점", "점수"):
                    r_col_idx = i
                    break
            trs = sb3.select("tbody tr")
            rows = []
            for tr in trs:
                tds = tr.find_all(["th", "td"])
                cells = [norm_text(td.get_text()) for td in tds]
                if cells:
                    rows.append(cells)
            if r_col_idx is not None and len(rows) >= 2:
                away_team = rows[0][0]
                home_team = rows[1][0]
                away_runs = safe_int(rows[0][r_col_idx], 0) if len(rows[0]) > r_col_idx else 0
                home_runs = safe_int(rows[1][r_col_idx], 0) if len(rows[1]) > r_col_idx else 0

        # 팀명 보정 실패 시 scoreboard1에서 백업
        if not home_team or not away_team:
            sb1 = soup.select_one("#tblScoreboard1")
            if sb1:
                tds = sb1.find_all("td")
                texts = [norm_text(td.get_text()) for td in tds]
                if len(texts) >= 3:
                    home_team = home_team or texts[0]
                    away_team = away_team or texts[-1]

        # 타자 기록 표에서 안타/홈런 합계
        home_hits = away_hits = 0
        home_hr   = away_hr   = 0

        def sum_hits_hr(table_sel: str) -> (int, int):
            tbl = soup.select_one(table_sel)
            if not tbl:
                return 0, 0
            hits_idx = hr_idx = None
            heads = tbl.select("thead th")
            for i, th in enumerate(heads):
                t = norm_text(th.get_text()).upper()
                if t in ("H", "안타", "HIT", "HITS"):
                    hits_idx = i
                if t in ("HR", "홈런", "HOMERUN"):
                    hr_idx = i
            total_h = 0
            total_hr = 0
            for tr in tbl.select("tbody tr"):
                tds = tr.find_all("td")
                cells = [norm_text(td.get_text()) for td in tds]
                if not cells:
                    continue
                if hits_idx is not None and hits_idx < len(cells):
                    total_h += safe_int(cells[hits_idx], 0)
                if hr_idx is not None and hr_idx < len(cells):
                    total_hr += safe_int(cells[hr_idx], 0)
            return total_h, total_hr

        for sel_home in ["#tblHomeHitter2", "#tblHomeHitter", "#tblHitterHome"]:
            h, hr = sum_hits_hr(sel_home)
            if h or hr:
                home_hits, home_hr = h, hr
                break
        for sel_away in ["#tblAwayHitter2", "#tblAwayHitter", "#tblHitterAway"]:
            h, hr = sum_hits_hr(sel_away)
            if h or hr:
                away_hits, away_hr = h, hr
                break

        if (home_hr and home_hits and home_hr > home_hits) or (away_hr and away_hits and away_hr > away_hits):
            print(f"[WARN] HR > H ? gid={gid} home({home_hr}/{home_hits}) away({away_hr}/{away_hits})", file=sys.stderr)

        result_row = {
            "날짜": game_date_iso,
            "gameId": gid,
            "홈팀": home_team,
            "원정팀": away_team,
            "홈점수": home_runs if home_runs is not None else "",
            "원정점수": away_runs if away_runs is not None else "",
            "홈안타": home_hits,
            "원정안타": away_hits,
            "홈런": (home_hr + away_hr),
            "구장": stadium or "",
            "상태": "완료" if (home_runs is not None and away_runs is not None) else "리뷰확인필요",
        }
        return result_row
    except Exception as e:
        print(f"[ERR] fetch_review({gid}): {e}", file=sys.stderr)
        return None
    finally:
        drv.quit()

# -----------------------------
# 기존 CSV 로드 및 증분 제어
# -----------------------------
def load_existing_ids(out_csv: str) -> set:
    if not os.path.exists(out_csv):
        return set()
    try:
        df = pd.read_csv(out_csv, encoding="utf-8")
        if "gameId" in df.columns:
            return set(str(x) for x in df["gameId"].dropna().astype(str).tolist())
        return set()
    except Exception:
        return set()

def append_rows(out_csv: str, rows: List[Dict]) -> None:
    if not rows:
        return
    ensure_parent_dir(out_csv)
    df = pd.DataFrame(rows)
    write_header = not os.path.exists(out_csv)
    df.to_csv(out_csv, mode="a", header=write_header, index=False, encoding="utf-8")
    print(f"[INFO] appended {len(rows)} rows -> {out_csv}")

# -----------------------------
# 메인
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=DEFAULT_SINCE, help="수집 시작일(YYYYMMDD)")
    ap.add_argument("--until", default=None, help="수집 종료일(YYYYMMDD, 미지정 시 오늘)")
    ap.add_argument("--out",   default="data/kbo_latest.csv", help="출력 CSV 파일명")
    args = ap.parse_args()

    since = datetime.strptime(args.since, "%Y%m%d").date()
    until = datetime.strptime(args.until, "%Y%m%d").date() if args.until else date.today()

    existing = load_existing_ids(args.out)
    print(f"[INFO] existing rows: {len(existing)}")

    all_new_rows: List[Dict] = []

    d = since
    while d <= until:
        print(f"[DAY] {d.isoformat()} 수집 시도…")
        gids = collect_gameids_for_date(d)
        if not gids:
            print(f"[DAY] {d.isoformat()} gameId 없음(스케줄 노출 X 가능).")
        else:
            print(f"[DAY] {d.isoformat()} 발견 gameId={gids}")

        day_rows: List[Dict] = []
        for gid in gids:
            if gid in existing:
                continue
            row = fetch_review(gid)
            if not row:
                continue
            day_rows.append(row)

        if day_rows:
            append_rows(args.out, day_rows)
            existing.update(r["gameId"] for r in day_rows)
            all_new_rows.extend(day_rows)

        time.sleep(1.0)
        d += timedelta(days=1)

    print(f"[DONE] new rows: {len(all_new_rows)}")

if __name__ == "__main__":
    main()
