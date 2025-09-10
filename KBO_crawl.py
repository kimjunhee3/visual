# KBO_crawl.py
# -*- coding: utf-8 -*-

import os, re, sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------------------------------------------------
# 상수/환경
# -------------------------------------------------
DEFAULT_SINCE = "20250322"
DEFAULT_OUT = "data/kbo_latest.csv"

# '예정' 재확인 범위(최근 N일)
RECHECK_DAYS = int(os.getenv("RECHECK_DAYS", "7"))
# CSV 최신 경기/날짜 강제 재크롤 개수
RECENT_RECRAWL_GAMES = int(os.getenv("RECENT_RECRAWL_GAMES", "3"))
RECENT_RECRAWL_DATES = int(os.getenv("RECENT_RECRAWL_DATES", "3"))

SCHEDULE_DAY_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={d}"
REVIEW_URL       = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={gdt}&section=REVIEW"

TEAM_NAMES = ["두산","LG","키움","KT","KIA","NC","SSG","삼성","롯데","한화"]
STADIUM_NAMES = ["잠실","수원","창원","대구","문학","인천","대전","광주","사직","고척",
                 "울산","포항","군산","제주","청주","목동","마산"]

# -------------------------------------------------
# 유틸
# -------------------------------------------------
def _today_kst() -> datetime.date:
    return pd.Timestamp.now(tz="Asia/Seoul").date()

def _ymd(dt: datetime.date) -> str:
    return dt.strftime("%Y%m%d")

def _text(el) -> str:
    if el is None: return ""
    return el.get_text(" ", strip=True)

def _clean_stadium(s: str) -> str:
    return re.sub(r"^구장\s*:\s*", "", (s or "").strip())

def _strip_num(s: str) -> Optional[int]:
    s = (s or "").strip()
    if s == "": return None
    m = re.search(r"-?\d+", s.replace(",", ""))
    return int(m.group()) if m else None

# -------------------------------------------------
# CSV 로드/저장 보조
# -------------------------------------------------
def load_existing(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    df = pd.read_csv(csv_path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def has_pending(row: pd.Series) -> bool:
    def txt(k):
        v = row.get(k, "")
        return (str(v) if pd.notna(v) else "").strip()

    keys = [k for k in row.index if any(s in k.lower() for s in ["result", "결과", "상태"])]
    if any("예정" in txt(k) for k in keys):
        return True
    if txt("home_score") == "" and txt("away_score") == "":
        return True
    if txt("section") != "REVIEW" and txt("review_url") == "":
        return True
    return False

def replace_by_gameid(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    if df_old is None or len(df_old) == 0:
        return df_new.copy()

    if "gameId" in df_new.columns and "gameId" in df_old.columns:
        old = df_old[~df_old["gameId"].isin(set(df_new["gameId"].dropna()))]
        return pd.concat([old, df_new], ignore_index=True)

    # 임시키(date|home|away) 폴백
    must = ["date","home","away"]
    if all(c in df_old.columns for c in must) and all(c in df_new.columns for c in must):
        old = df_old.copy(); new = df_new.copy()
        old["_k"] = pd.to_datetime(old["date"]).dt.strftime("%Y%m%d") + "|" + old["home"].astype(str) + "|" + old["away"].astype(str)
        new["_k"] = pd.to_datetime(new["date"]).dt.strftime("%Y%m%d") + "|" + new["home"].astype(str) + "|" + new["away"].astype(str)
        old = old[~old["_k"].isin(set(new["_k"]))]
        return pd.concat([old.drop(columns=["_k"], errors="ignore"),
                          new.drop(columns=["_k"], errors="ignore")], ignore_index=True)

    return pd.concat([df_old, df_new], ignore_index=True).drop_duplicates()

# -------------------------------------------------
# 대상 날짜 산정
# -------------------------------------------------
def decide_since_until(args_since: str, args_until: str, df_old: pd.DataFrame, force: bool) -> Tuple[str, str]:
    today = _today_kst()
    until_date = pd.to_datetime(args_until, format="%Y%m%d", errors='coerce').date() if args_until else (today - timedelta(days=1))
    if until_date > today:
        until_date = today - timedelta(days=1)

    if args_since:
        since_date = pd.to_datetime(args_since, format="%Y%m%d").date()
    else:
        if (df_old is not None) and len(df_old) and ("date" in df_old.columns) and not force:
            last = pd.to_datetime(df_old["date"]).dt.date.max()
            since_date = last + timedelta(days=1)
            if since_date > until_date:
                since_date = until_date
        else:
            since_date = until_date

    return _ymd(since_date), _ymd(until_date)

def build_target_dates(since_str: str, until_str: str, df_old: pd.DataFrame) -> List[str]:
    since = datetime.strptime(since_str, "%Y%m%d").date()
    until = datetime.strptime(until_str, "%Y%m%d").date()
    base = {_ymd(since + timedelta(days=i)) for i in range((until - since).days + 1)}

    if df_old is not None and len(df_old) and "date" in df_old.columns and RECHECK_DAYS > 0:
        today = _today_kst()
        cutoff = today - timedelta(days=RECHECK_DAYS)
        recent = df_old[df_old["date"] >= cutoff]
        if len(recent):
            pend_days = {_ymd(d) for d in recent[recent.apply(has_pending, axis=1)]["date"].unique()}
            base |= pend_days

    return sorted(base)

# -------------------------------------------------
# Selenium 드라이버
# -------------------------------------------------
def make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,1000")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(40)
    return driver

# -------------------------------------------------
# 일정/리뷰 파싱
# -------------------------------------------------
def extract_game_ids_from_schedule_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    gids = set()

    for tag in soup.select('a, button'):
        text = _text(tag)
        id_attr = (tag.get("id") or "").lower()
        cls = " ".join(tag.get("class") or []).lower()
        if "리뷰" in text or "btnreview" in id_attr or "btnreview" in cls:
            for attr in ["href", "onclick", "data-href", "data-url"]:
                v = tag.get(attr)
                if not v: continue
                m = re.search(r"gameId=([0-9A-Za-z\-]+)", v)
                if m: gids.add(m.group(1))

    for a in soup.find_all("a", href=True):
        m = re.search(r"gameId=([0-9A-Za-z\-]+)", a["href"])
        if m: gids.add(m.group(1))

    return sorted(gids)

def extract_schedule_rows(html: str, d: str, df_old: Optional[pd.DataFrame]=None) -> pd.DataFrame:
    """
    리뷰가 없을 때를 위해 일정표에서 홈/원정/구장/상태(예정/취소)를 추출.
    - 팀/구장 이름은 리스트 기반으로 로버스트하게 매칭
    - 홈/원정 방향은 기존 CSV에 같은 날짜의 기록이 있으면 그 방향을 따름
      (없으면 '원정 먼저, 홈 나중' 추정)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # 모든 tr을 훑어 간단히 팀명 2개 이상 포함된 줄을 수집
    for tr in soup.find_all("tr"):
        txt = tr.get_text(" ", strip=True)
        if not txt:
            continue

        teams_found = []
        for t in TEAM_NAMES:
            if t in txt and t not in teams_found:
                teams_found.append(t)
        if len(teams_found) < 2:
            continue

        # 홈/원정 방향 유추
        away_guess, home_guess = teams_found[0], teams_found[1]

        if df_old is not None and "date" in df_old.columns:
            subset = df_old[pd.to_datetime(df_old["date"]).dt.strftime("%Y%m%d") == d]
            if not subset.empty:
                for _, r in subset.iterrows():
                    h, a = str(r.get("home")), str(r.get("away"))
                    if {h, a} == set(teams_found):
                        home_guess, away_guess = h, a
                        break

        stadium = None
        for sname in STADIUM_NAMES:
            if sname in txt:
                stadium = sname
                break

        status = "취소" if ("취소" in txt or "우천" in txt) else "예정"

        rows.append({
            "date": pd.to_datetime(d).date(),
            "gameId": None,
            "stadium": stadium,
            "home": home_guess,
            "away": away_guess,
            "home_score": None,
            "away_score": None,
            "home_result": None,
            "away_result": None,
            "home_hits": None,
            "home_hr": None,
            "away_hits": None,
            "away_hr": None,
            "status": status,
            "section": "SCHEDULE",
            "review_url": "",
        })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["date","home","away"], keep="first")
    return df

def parse_review_page_html(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    stadium = None
    s_el = soup.select_one("#txtStadium")
    if s_el: stadium = _clean_stadium(_text(s_el))
    if not stadium:
        m = soup.find(string=re.compile(r"구장\s*:"))
        if m: stadium = _clean_stadium(str(m))

    home_score = away_score = None
    sc_tb = soup.select_one("#tblScoreboard3")

    home_team = away_team = None
    home_result = away_result = None
    sb_tb = soup.select_one("#tblScoreboard1")
    if sb_tb:
        body = sb_tb.find("tbody") or sb_tb
        rows = body.find_all("tr")
        team_rows = []
        for r in rows:
            cols = [c.get_text(strip=True) for c in r.find_all(["td","th"])]
            if not cols: continue
            txt = " ".join(cols)
            m_team = re.findall(r"[A-Za-z가-힣]+", txt)
            team = m_team[0] if m_team else None
            res = "예정"
            if   "승" in txt: res = "승"
            elif "패" in txt: res = "패"
            elif "무" in txt: res = "무"
            team_rows.append((team, res))
        if len(team_rows) >= 2:
            away_team, away_result = team_rows[0]
            home_team, home_result = team_rows[1]

    if sc_tb and (home_score is None or away_score is None):
        body = sc_tb.find("tbody") or sc_tb
        rows = body.find_all("tr")
        num_cells = []
        for r in rows:
            for c in r.find_all(["td","th"]):
                v = _strip_num(c.get_text())
                if v is not None: num_cells.append(v)
        if len(num_cells) >= 2:
            away_score, home_score = num_cells[0], num_cells[1]

    home_hits = home_hr = away_hits = away_hr = None
    home_hit_tb = soup.select_one("#tblHomeHitter2")
    away_hit_tb = soup.select_one("#tblAwayHitter2")
    if home_hit_tb:
        s = _sum_hitter_table(home_hit_tb); home_hits, home_hr = s["hits"], s["home_runs"]
    if away_hit_tb:
        s = _sum_hitter_table(away_hit_tb); away_hits, away_hr = s["hits"], s["home_runs"]

    status = "예정" if (home_score is None and away_score is None) or any(
        x in ["예정", None, ""] for x in [home_result, away_result]) else "종료"

    return {
        "stadium": stadium,
        "home": home_team,
        "away": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "home_result": home_result,
        "away_result": away_result,
        "home_hits": home_hits,
        "home_hr": home_hr,
        "away_hits": away_hits,
        "away_hr": away_hr,
        "status": status,
    }

def _sum_hitter_table(table: Optional[BeautifulSoup]) -> Dict[str, Optional[int]]:
    if table is None:
        return {"hits": None, "home_runs": None}
    thead = table.find("thead"); tbody = table.find("tbody") or table
    if not thead or not tbody:
        return {"hits": None, "home_runs": None}

    headers = [h.get_text(strip=True) for h in thead.select("th, td")]
    hit_idx = hr_idx = None
    for i, h in enumerate(headers):
        if hit_idx is None and any(k in h for k in ["안타","H","Hit","Hits"]): hit_idx = i
        if hr_idx  is None and any(k in h for k in ["홈런","HR","HomeRun"]):   hr_idx  = i

    total_hits = total_hr = 0; found = False
    for r in tbody.find_all("tr"):
        tds = r.find_all(["td","th"])
        if not tds: continue
        found = True
        if hit_idx is not None and hit_idx < len(tds):
            v = _strip_num(tds[hit_idx].get_text());  total_hits += v or 0
        if hr_idx  is not None and hr_idx  < len(tds):
            v = _strip_num(tds[hr_idx ].get_text());  total_hr   += v or 0

    if not found: return {"hits": None, "home_runs": None}
    return {"hits": total_hits, "home_runs": total_hr}

def crawl_one_game(driver: webdriver.Chrome, game_id: str, game_date: str) -> Dict[str, Optional[str]]:
    url = REVIEW_URL.format(gid=game_id, gdt=game_date)
    driver.get(url)
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#txtStadium, #tblScoreboard1, #tblScoreboard3"))
        )
    except Exception:
        pass
    html = driver.page_source
    data = parse_review_page_html(html)
    data["date"] = pd.to_datetime(game_date).date()
    data["gameId"] = game_id
    data["section"] = "REVIEW"
    data["review_url"] = url
    return data

def crawl_day(driver: webdriver.Chrome, d: str, df_old: Optional[pd.DataFrame]=None) -> pd.DataFrame:
    """
    1) 일정 페이지 HTML을 먼저 가져옴
    2) 리뷰 gameId가 있으면 리뷰 데이터 수집
    3) 동시에 일정표에서 '예정/취소' 플레이스홀더 생성(구장/팀 포함)
       → 리뷰가 있는 경기와 겹치면 일정행은 제거
    """
    url = SCHEDULE_DAY_URL.format(d=d)
    driver.get(url)
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )
    except Exception:
        pass
    html = driver.page_source

    # 리뷰
    gids = extract_game_ids_from_schedule_html(html)
    review_rows = []
    for gid in gids:
        try:
            review_rows.append(crawl_one_game(driver, gid, d))
        except Exception as e:
            review_rows.append({
                "date": pd.to_datetime(d).date(),
                "gameId": gid, "section": "REVIEW",
                "review_url": REVIEW_URL.format(gid=gid, gdt=d),
                "stadium": None, "home": None, "away": None,
                "home_score": None, "away_score": None,
                "home_result": None, "away_result": None,
                "home_hits": None, "home_hr": None,
                "away_hits": None, "away_hr": None,
                "status": "예정", "_error": str(e),
            })

    df_review = pd.DataFrame(review_rows) if review_rows else pd.DataFrame()

    # 일정(플레이스홀더)
    df_sched = extract_schedule_rows(html, d, df_old=df_old)

    # 리뷰가 있는 경기와 겹치는 일정행 삭제
    if not df_review.empty and not df_sched.empty:
        have = set(zip(df_review.get("home").astype(str), df_review.get("away").astype(str)))
        df_sched = df_sched[~df_sched.apply(lambda r: (str(r.get("home")), str(r.get("away"))) in have, axis=1)]

    # 합치기
    if df_review.empty and df_sched.empty:
        print(f"[INFO] {d} : 수집 결과 없음(리뷰/일정 모두 미노출)")
        return pd.DataFrame()
    out = pd.concat([x for x in [df_review, df_sched] if not x.empty], ignore_index=True)

    # 정렬/칼럼 순서
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"]).dt.date
    order = ["date","gameId","stadium","home","away",
             "home_score","away_score","home_result","away_result",
             "home_hits","home_hr","away_hits","away_hr",
             "status","section","review_url"]
    cols = [c for c in order if c in out.columns] + [c for c in out.columns if c not in order]
    return out[cols]

# -------------------------------------------------
# 최신 K경기/날짜 강제 재크롤
# -------------------------------------------------
def pick_recent_game_ids(df_old: pd.DataFrame, k: int) -> List[Tuple[str, str]]:
    if df_old is None or len(df_old) == 0: return []
    if "gameId" not in df_old.columns or "date" not in df_old.columns: return []
    df = df_old.dropna(subset=["gameId"]).copy()
    if len(df) == 0: return []
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date", ascending=False)
    df = df.drop_duplicates(subset=["gameId"], keep="first")
    top = df.head(k)
    return [(row["gameId"], _ymd(row["date"])) for _, row in top.iterrows()]

def recrawl_recent_games(driver: webdriver.Chrome, df_old: pd.DataFrame, k: int) -> pd.DataFrame:
    pairs = pick_recent_game_ids(df_old, k)
    if not pairs:
        return pd.DataFrame()
    rows = []
    for gid, d in pairs:
        try:
            rows.append(crawl_one_game(driver, gid, d))
        except Exception as e:
            rows.append({
                "date": pd.to_datetime(d).date(),
                "gameId": gid, "section": "REVIEW",
                "review_url": REVIEW_URL.format(gid=gid, gdt=d),
                "stadium": None, "home": None, "away": None,
                "home_score": None, "away_score": None,
                "home_result": None, "away_result": None,
                "home_hits": None, "home_hr": None,
                "away_hits": None, "away_hr": None,
                "status": "예정", "_error": str(e),
            })
    return pd.DataFrame(rows)

def recrawl_recent_dates(driver: webdriver.Chrome, df_old: pd.DataFrame, k: int) -> pd.DataFrame:
    if df_old is None or len(df_old) == 0 or "date" not in df_old.columns: 
        return pd.DataFrame()
    dates = list(pd.to_datetime(df_old["date"]).dt.strftime("%Y%m%d").unique())
    dates.sort(reverse=True)
    dates = dates[:k]
    frames = []
    for d in dates:
        frames.append(crawl_day(driver, d, df_old=df_old))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# -------------------------------------------------
# CLI
# -------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="KBO 리뷰/일정 크롤러")
    p.add_argument("--since", type=str, default="", help="YYYYMMDD 시작일(비우면 CSV 마지막+1)")
    p.add_argument("--until", type=str, default="", help="YYYYMMDD 종료일(비우면 어제)")
    p.add_argument("--out",   type=str, default=DEFAULT_OUT, help="출력 CSV 경로")
    p.add_argument("--force", type=str, default="false", help="체크포인트 무시 재수집(true/false)")
    return p.parse_args()

# -------------------------------------------------
# 메인
# -------------------------------------------------
def main():
    args = parse_args()
    out_csv = args.out
    force = str(args.force).lower() in ("1","true","yes","y")

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    df_old = load_existing(out_csv)
    since_str, until_str = decide_since_until(args.since, args.until, df_old, force)
    print(f"[INFO] 대상 범위: {since_str} ~ {until_str} (force={force})")

    targets = build_target_dates(since_str, until_str, df_old)

    driver = make_driver()
    all_new = []
    try:
        # 1) 날짜별 수집 (리뷰 + 일정 플레이스홀더)
        for d in targets:
            print(f"[INFO] 수집 시작: {d}")
            df_d = crawl_day(driver, d, df_old=df_old)
            if df_d is not None and not df_d.empty:
                all_new.append(df_d)

        # 2) 최신 gameId 재크롤 (있을 때만)
        if df_old is not None and len(df_old) and RECENT_RECRAWL_GAMES > 0:
            print(f"[INFO] 최신 {RECENT_RECRAWL_GAMES}경기 강제 재크롤링...")
            df_recent = recrawl_recent_games(driver, df_old, RECENT_RECRAWL_GAMES)
            if df_recent is not None and not df_recent.empty:
                all_new.append(df_recent)

        # 3) 최신 날짜 재크롤 (gameId 없어도)
        if df_old is not None and len(df_old) and RECENT_RECRAWL_DATES > 0:
            print(f"[INFO] 최신 {RECENT_RECRAWL_DATES}일 재크롤링(리뷰없어도 일정 반영)...")
            df_recent_dates = recrawl_recent_dates(driver, df_old, RECENT_RECRAWL_DATES)
            if df_recent_dates is not None and not df_recent_dates.empty:
                all_new.append(df_recent_dates)

    finally:
        driver.quit()

    if not all_new:
        print("[INFO] 신규/갱신 데이터가 없습니다.")
        return

    df_new = pd.concat(all_new, ignore_index=True)

    if df_old is None or len(df_old) == 0:
        df_out = df_new
    else:
        df_out = replace_by_gameid(df_old, df_new)

    if "date" in df_out.columns:
        df_out["date"] = pd.to_datetime(df_out["date"])
        sort_keys = [k for k in ["date","stadium","home","away"] if k in df_out.columns]
        df_out = df_out.sort_values(sort_keys, na_position="last")
        df_out["date"] = df_out["date"].dt.date

    df_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] 저장 완료 → {out_csv} (rows={len(df_out)})")

if __name__ == "__main__":
    main()

