# KBO_crawl.py
# -*- coding: utf-8 -*-

import os, re, time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------------------------------------------------
# 설정값
# -------------------------------------------------
DEFAULT_SINCE = "20250322"
OUT_CSV = "data/kbo_latest.csv"

# 최근 N일 동안 CSV에 '예정'이 남아있으면 해당 날짜 재수집
RECHECK_DAYS = int(os.getenv("RECHECK_DAYS", "7"))
# CSV에 이미 들어있는 가장 최근 경기 K개를 무조건 재크롤하여 교체
RECENT_RECRAWL_GAMES = int(os.getenv("RECENT_RECRAWL_GAMES", "3"))

SCHEDULE_DAY_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={d}"
REVIEW_URL       = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={gdt}&section=REVIEW"

_UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
    )
}

# -------------------------------------------------
# 유틸
# -------------------------------------------------
def _ymd(dt: datetime.date) -> str:
    return dt.strftime("%Y%m%d")

def _text(el) -> str:
    if el is None: return ""
    return el.get_text(" ", strip=True)

def _clean_stadium(s: str) -> str:
    return re.sub(r"^구장\s*:\s*", "", (s or "").strip())

def _strip_num(s: str) -> Optional[int]:
    s = (s or "").strip()
    if s == "":
        return None
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
    # '예정' 텍스트, 점수 미기입, REVIEW 정보 부재 등을 포괄
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
# 대상 날짜 산정 (속도 최적화)
# -------------------------------------------------
def build_target_dates(since_str: str, until_str: Optional[str], df_old: pd.DataFrame) -> List[str]:
    """
    기존: since~until 전체
    변경: CSV가 존재하면 → (CSV 최신일 - RECHECK_DAYS) ~ 오늘 로 '좁혀서' 수행
          + CSV 안의 '예정' 날짜는 무조건 포함
    CSV가 없을 때만 환경변수 구간 전체 수행
    """
    today = datetime.now().date()
    since_env = datetime.strptime(since_str, "%Y%m%d").date()
    until = today if not until_str else datetime.strptime(until_str, "%Y%m%d").date()

    if df_old is not None and len(df_old) and "date" in df_old.columns:
        latest = pd.to_datetime(df_old["date"]).dt.date.max()
        # 최소 버퍼 14일 확보
        start = max(latest - timedelta(days=RECHECK_DAYS),
                    today - timedelta(days=max(RECHECK_DAYS, 14)))
        base = {_ymd(start + timedelta(days=i)) for i in range((until - start).days + 1)}
        # '예정' 남아있는 날짜 추가
        cutoff = today - timedelta(days=RECHECK_DAYS)
        recent = df_old[df_old["date"] >= cutoff]
        pend_days = {_ymd(d) for d in recent[recent.apply(has_pending, axis=1)]["date"].unique()}
        base |= pend_days
    else:
        # 초기 적재
        base = {_ymd(since_env + timedelta(days=i)) for i in range((until - since_env).days + 1)}

    return sorted(base)

# -------------------------------------------------
# Selenium 드라이버 (빠른 로딩 전략)
# -------------------------------------------------
def make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,1000")
    options.page_load_strategy = "eager"  # DOMContentLoaded 후 바로 진행
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(40)
    return driver

# -------------------------------------------------
# 날짜별 gameId 수집 (원래 방식 + 빠른 경로 우선)
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

def get_game_ids_for_date_fast(d: str) -> List[str]:
    """Selenium 없이 requests로 빠르게"""
    url = SCHEDULE_DAY_URL.format(d=d)
    r = requests.get(url, headers=_UA_HEADERS, timeout=12)
    r.raise_for_status()
    return extract_game_ids_from_schedule_html(r.text)

def get_game_ids_for_date(driver: webdriver.Chrome, d: str) -> List[str]:
    # 1) 빠른 경로: requests
    try:
        gids = get_game_ids_for_date_fast(d)
        if gids:
            return gids
    except Exception:
        pass  # 실패 시 Selenium 백업

    # 2) 백업: Selenium (원래 흐름)
    url = SCHEDULE_DAY_URL.format(d=d)
    driver.get(url)
    try:
        WebDriverWait(driver, 4).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )
    except Exception:
        pass
    return extract_game_ids_from_schedule_html(driver.page_source)

# -------------------------------------------------
# 리뷰 페이지 파싱
# -------------------------------------------------
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

def crawl_day(driver: webdriver.Chrome, d: str) -> pd.DataFrame:
    gids = get_game_ids_for_date(driver, d)
    if not gids:
        print(f"[INFO] {d} : 수집 결과 없음(리뷰 버튼 미노출 등)")
        return pd.DataFrame()

    rows = []
    for gid in gids:
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

    df = pd.DataFrame(rows)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    order = ["date","gameId","stadium","home","away",
             "home_score","away_score","home_result","away_result",
             "home_hits","home_hr","away_hits","away_hr",
             "status","section","review_url"]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    return df[cols]

# -------------------------------------------------
# 최신 K경기 강제 재크롤
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

# -------------------------------------------------
# 메인
# -------------------------------------------------
def main(since: Optional[str] = None, until: Optional[str] = None, out_csv: str = OUT_CSV):
    since = since or os.getenv("SINCE", DEFAULT_SINCE)
    until = until or os.getenv("UNTIL", None)

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    df_old = load_existing(out_csv)
    targets = build_target_dates(since, until, df_old)

    driver = make_driver()
    all_new = []
    try:
        # 1) 날짜별 수집
        for d in targets:
            print(f"[INFO] 수집 시작: {d}")
            df_d = crawl_day(driver, d)
            if df_d is not None and not df_d.empty:
                all_new.append(df_d)

        # 2) CSV 최신 K경기 강제 재크롤 → 교체용
        if df_old is not None and len(df_old) and RECENT_RECRAWL_GAMES > 0:
            print(f"[INFO] 최신 {RECENT_RECRAWL_GAMES}경기 강제 재크롤링...")
            df_recent = recrawl_recent_games(driver, df_old, RECENT_RECRAWL_GAMES)
            if df_recent is not None and not df_recent.empty:
                all_new.append(df_recent)

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
    main(
        since=os.environ.get("SINCE"),
        until=os.environ.get("UNTIL"),
        out_csv=os.environ.get("OUT", OUT_CSV),
    )

