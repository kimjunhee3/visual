# KBO_crawl.py
# -*- coding: utf-8 -*-
"""
KBO 리뷰 크롤러 (append-mode aware + pending refresh)

핵심 기능
- 날짜 범위 수집 + 기존 CSV에서 '예정'이 남아있는 최근 N일(RECHECK_DAYS)을 재수집
- 같은 gameId는 새 수집분으로 교체(덮어쓰기)
- 리뷰 페이지에서 구장/팀/점수/승패/안타/홈런 추출
- '구장:' 접두어 제거하여 이름만 저장
- REVIEW가 없거나 점수가 비어 있으면 해당 경기 상태를 '예정'으로 간주

환경변수
- SINCE: 기본 수집 시작일(YYYYMMDD). 기본값 DEFAULT_SINCE
- UNTIL: 수집 종료일(YYYYMMDD). 기본값: 오늘
- OUT: 출력 CSV 경로. 기본값 data/kbo_latest.csv
- RECHECK_DAYS: 최근 N일 재검사(기본 7)

의존성
- selenium>=4.21, beautifulsoup4, pandas
(셀레니움 4.10+는 Selenium Manager로 드라이버/브라우저 자동 관리)
"""

import os
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -----------------------------
# 설정
# -----------------------------
DEFAULT_SINCE = "20250322"
OUT_CSV = "data/kbo_latest.csv"
RECHECK_DAYS = int(os.getenv("RECHECK_DAYS", "7"))

SCHEDULE_DAY_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={d}"
REVIEW_URL       = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={gdt}&section=REVIEW"

# -----------------------------
# 유틸
# -----------------------------
def _ymd(dt: datetime.date) -> str:
    return dt.strftime("%Y%m%d")

def _to_date(s) -> Optional[datetime.date]:
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None

def _text(el) -> str:
    if el is None:
        return ""
    return el.get_text(" ", strip=True)

def _clean_stadium(s: str) -> str:
    # "구장: 잠실" -> "잠실"
    s = re.sub(r"^구장\s*:\s*", "", s.strip())
    return s

def _strip_num(s: str) -> Optional[int]:
    s = (s or "").strip()
    if s == "": 
        return None
    m = re.search(r"-?\d+", s.replace(",", ""))
    return int(m.group()) if m else None

# -----------------------------
# CSV 로드/저장 보조
# -----------------------------
def load_existing(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    df = pd.read_csv(csv_path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def has_pending(row: pd.Series) -> bool:
    """
    '예정'으로 판단하는 조건:
    - 결과 칼럼(결과/result/상태) 중 하나라도 '예정'
    - 또는 점수 공란
    - 또는 REVIEW 섹션/URL 부재
    """
    def txt(k):
        v = row.get(k, "")
        return (str(v) if pd.notna(v) else "").strip()

    # 결과 텍스트 기반
    keys = [k for k in row.index if any(s in k.lower() for s in ["result", "결과", "상태"])]
    if any("예정" in txt(k) for k in keys):
        return True

    # 점수 공란
    hs, as_ = txt("home_score"), txt("away_score")
    if hs == "" and as_ == "":
        return True

    # 리뷰 플래그/URL
    if txt("section") != "REVIEW" and txt("review_url") == "":
        return True

    return False

def replace_by_gameid(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    같은 gameId는 신규로 교체(덮어쓰기).
    gameId가 없다면 (date,home,away)를 임시키로 사용.
    """
    if df_old is None or len(df_old) == 0:
        return df_new.copy()

    if "gameId" in df_new.columns and "gameId" in df_old.columns:
        old = df_old[~df_old["gameId"].isin(set(df_new["gameId"].dropna()))]
        out = pd.concat([old, df_new], ignore_index=True)
        return out

    # 임시키 (date|home|away)
    must = ["date", "home", "away"]
    if all(c in df_old.columns for c in must) and all(c in df_new.columns for c in must):
        old = df_old.copy()
        new = df_new.copy()
        old["_k"] = pd.to_datetime(old["date"]).dt.strftime("%Y%m%d") + "|" + old["home"].astype(str) + "|" + old["away"].astype(str)
        new["_k"] = pd.to_datetime(new["date"]).dt.strftime("%Y%m%d") + "|" + new["home"].astype(str) + "|" + new["away"].astype(str)
        old = old[~old["_k"].isin(set(new["_k"]))]
        out = pd.concat([old.drop(columns=["_k"], errors="ignore"), new.drop(columns=["_k"], errors="ignore")], ignore_index=True)
        return out

    # 마지막 수단
    return pd.concat([df_old, df_new], ignore_index=True).drop_duplicates()

def build_target_dates(since_str: str, until_str: Optional[str], df_old: pd.DataFrame) -> List[str]:
    today = datetime.now().date()
    since = datetime.strptime(since_str, "%Y%m%d").date()
    until = today if not until_str else datetime.strptime(until_str, "%Y%m%d").date()

    base = {_ymd(since + timedelta(days=i)) for i in range((until - since).days + 1)}

    # 최근 N일 중 CSV에 '예정'이 남아있으면 재수집
    if df_old is not None and len(df_old) and "date" in df_old.columns:
        cutoff = today - timedelta(days=RECHECK_DAYS)
        recent = df_old[df_old["date"] >= cutoff]
        if len(recent):
            pend_days = {_ymd(d) for d in recent[recent.apply(has_pending, axis=1)]["date"].unique()}
            base |= pend_days

    return sorted(base)

# -----------------------------
# Selenium 드라이버
# -----------------------------
def make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,1000")
    driver = webdriver.Chrome(options=options)  # Selenium Manager가 브라우저/드라이버 관리
    driver.set_page_load_timeout(60)
    return driver

# -----------------------------
# 날짜별 gameId 수집
# -----------------------------
def extract_game_ids_from_schedule_html(html: str) -> List[str]:
    """
    일정 페이지의 '리뷰' 버튼/링크에서 gameId를 뽑아낸다.
    - onclick 또는 href 파라미터로 노출되는 케이스를 모두 처리
    """
    soup = BeautifulSoup(html, "html.parser")
    gids = set()

    # 1) id가 btnReview* 이거나 텍스트가 '리뷰' 인 버튼/링크
    for tag in soup.select('a, button'):
        text = _text(tag)
        id_attr = (tag.get("id") or "").lower()
        cls = " ".join(tag.get("class") or []).lower()
        if "리뷰" in text or "btnreview" in id_attr or "btnreview" in cls:
            # href / onclick에서 gameId 추출
            for attr in ["href", "onclick", "data-href", "data-url"]:
                v = tag.get(attr)
                if not v:
                    continue
                m = re.search(r"gameId=([0-9A-Za-z\-]+)", v)
                if m:
                    gids.add(m.group(1))

    # 2) 백업: URL 텍스트 자체에 gameId가 있는 경우
    for a in soup.find_all("a", href=True):
        m = re.search(r"gameId=([0-9A-Za-z\-]+)", a["href"])
        if m:
            gids.add(m.group(1))

    return sorted(gids)

def get_game_ids_for_date(driver: webdriver.Chrome, d: str) -> List[str]:
    url = SCHEDULE_DAY_URL.format(d=d)
    driver.get(url)
    # 테이블 로드 대기(없어도 2~3초는 기다린다)
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )
    except Exception:
        pass
    html = driver.page_source
    gids = extract_game_ids_from_schedule_html(html)
    return gids

# -----------------------------
# 리뷰 페이지 파싱
# -----------------------------
def _sum_hitter_table(table: Optional[BeautifulSoup]) -> Dict[str, Optional[int]]:
    """
    홈/원정 타자 테이블에서 '안타', '홈런' 합계를 계산한다.
    컬럼명이 약간씩 달라질 수 있으므로 헤더 텍스트 기반으로 인덱스를 찾는다.
    """
    if table is None:
        return {"hits": None, "home_runs": None}

    thead = table.find("thead")
    tbody = table.find("tbody")
    if not thead or not tbody:
        return {"hits": None, "home_runs": None}

    headers = [h.get_text(strip=True) for h in thead.select("th, td")]
    # 안타/홈런 컬럼 위치 탐색
    hit_idx = None
    hr_idx = None
    for i, h in enumerate(headers):
        if any(k in h for k in ["안타", "H", "Hit", "Hits"]):
            if hit_idx is None:
                hit_idx = i
        if any(k in h for k in ["홈런", "HR", "HomeRun"]):
            if hr_idx is None:
                hr_idx = i

    total_hits = 0
    total_hr = 0
    rows = tbody.find_all("tr")
    found_row = False
    for r in rows:
        tds = r.find_all(["td", "th"])
        if not tds:
            continue
        found_row = True
        if hit_idx is not None and hit_idx < len(tds):
            v = _strip_num(tds[hit_idx].get_text())
            if v is not None:
                total_hits += v
        if hr_idx is not None and hr_idx < len(tds):
            v = _strip_num(tds[hr_idx].get_text())
            if v is not None:
                total_hr += v

    if not found_row:
        return {"hits": None, "home_runs": None}

    return {"hits": total_hits, "home_runs": total_hr}

def parse_review_page_html(html: str) -> Dict[str, Optional[str]]:
    """
    리뷰 페이지에서 필요한 필드 추출.
    우선순위 셀렉터:
      - 구장명: #txtStadium (텍스트에서 "구장:" 제거)
      - 승패/팀: #tblScoreboard1
      - 점수:   #tblScoreboard3
      - 타자합: #tblHomeHitter2, #tblAwayHitter2
    """
    soup = BeautifulSoup(html, "html.parser")

    # 구장
    stadium = None
    s_el = soup.select_one("#txtStadium")
    if s_el:
        stadium = _clean_stadium(_text(s_el))
    if not stadium:
        # 백업: 텍스트 어딘가 "구장:" 패턴
        m = soup.find(string=re.compile(r"구장\s*:"))
        if m:
            stadium = _clean_stadium(str(m))

    # 점수 (tblScoreboard3: 보통 홈/원정 점수 표)
    home_score = away_score = None
    sc_tb = soup.select_one("#tblScoreboard3")
    if sc_tb:
        # 관례상 thead에 열 제목, tbody 첫 행에 점수
        try:
            body = sc_tb.find("tbody")
            if not body:
                body = sc_tb
            rows = body.find_all("tr")
            # 점수 두 칸을 찾는다(숫자 가장 많은 두 칸)
            nums = []
            for r in rows:
                for c in r.find_all(["td", "th"]):
                    v = _strip_num(c.get_text())
                    if v is not None:
                        nums.append(v)
            if len(nums) >= 2:
                # 표 구조가 일정하지 않으므로, 첫 2개를 away, home로 가정하지 않고
                # 아래에서 팀명 매핑 후 재정렬
                # 일단 최대/두번째 같은 방식 쓰지 말고 keep
                pass
        except Exception:
            pass

    # 승패/팀 (tblScoreboard1): 팀명/승패가 같이 있는 표
    home_team = away_team = None
    home_result = away_result = None
    sb_tb = soup.select_one("#tblScoreboard1")
    if sb_tb:
        try:
            body = sb_tb.find("tbody") or sb_tb
            rows = body.find_all("tr")
            # 보통 2행(홈/원정 또는 반대). 텍스트에서 '승','패','무','예정' 감지
            # 첫 행을 보통 원정, 두 번째를 홈으로 보는 경우 많지만 안전하게 W/L 텍스트와 점수 표 순서로 다시 매칭
            team_rows = []
            for r in rows:
                cols = [c.get_text(strip=True) for c in r.find_all(["td","th"])]
                if not cols:
                    continue
                txt = " ".join(cols)
                # 팀명: 한글/영문 혼합 허용
                m_team = re.findall(r"[A-Za-z가-힣]+", txt)
                team = None
                if m_team:
                    team = m_team[0]  # 가장 앞에 오는 토큰을 팀명으로
                res = None
                if "승" in txt: res = "승"
                elif "패" in txt: res = "패"
                elif "무" in txt: res = "무"
                elif "예정" in txt: res = "예정"
                team_rows.append((team, res, txt))
            if len(team_rows) >= 2:
                # 관례적으로 [away, home] 순서를 가정
                away_team, away_result, _ = team_rows[0]
                home_team, home_result, _ = team_rows[1]
        except Exception:
            pass

    # 점수 재획득(팀 순서를 알았으니 다시 시도)
    if sc_tb and (home_score is None or away_score is None):
        try:
            # 많은 페이지에서 score 표는 좌=원정, 우=홈
            body = sc_tb.find("tbody") or sc_tb
            rows = body.find_all("tr")
            # 숫자 셀들을 왼→오 순서대로 모아서 2개만 뽑는다
            num_cells = []
            for r in rows:
                for c in r.find_all(["td","th"]):
                    v = _strip_num(c.get_text())
                    if v is not None:
                        num_cells.append(v)
            if len(num_cells) >= 2:
                away_score = num_cells[0]
                home_score = num_cells[1]
        except Exception:
            pass

    # 타자 표 합계
    home_hits = home_hr = away_hits = away_hr = None
    home_hit_tb = soup.select_one("#tblHomeHitter2")
    away_hit_tb = soup.select_one("#tblAwayHitter2")
    if home_hit_tb:
        s = _sum_hitter_table(home_hit_tb)
        home_hits, home_hr = s["hits"], s["home_runs"]
    if away_hit_tb:
        s = _sum_hitter_table(away_hit_tb)
        away_hits, away_hr = s["hits"], s["home_runs"]

    # 상태 결정
    status = None
    if any(x in ["예정", None, ""] for x in [home_result, away_result]) or (home_score is None and away_score is None):
        status = "예정"
    else:
        status = "종료"

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
    # 리뷰 섹션이 로드되기를 잠깐 대기
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
    """
    1) 해당 날짜 일정 페이지에서 리뷰가 있는 경기의 gameId를 수집
    2) 각 gameId의 REVIEW 페이지를 파싱
    3) 결과 DataFrame 반환
    """
    game_ids = get_game_ids_for_date(driver, d)
    rows = []
    if not game_ids:
        # 리뷰 버튼이 안보이는 날(비/취소), 또는 사이트 구조 변경 등
        # 최소한 일정 페이지에서 팀/예정 여부라도 만들어 둘 수 있지만,
        # 여기서는 empty 반환
        return pd.DataFrame()

    for gid in game_ids:
        try:
            row = crawl_one_game(driver, gid, d)
            rows.append(row)
        except Exception as e:
            # 실패한 건 '예정'에 준하는 레코드로 최소 보존
            rows.append({
                "date": pd.to_datetime(d).date(),
                "gameId": gid,
                "section": "REVIEW",
                "review_url": REVIEW_URL.format(gid=gid, gdt=d),
                "stadium": None, "home": None, "away": None,
                "home_score": None, "away_score": None,
                "home_result": None, "away_result": None,
                "home_hits": None, "home_hr": None,
                "away_hits": None, "away_hr": None,
                "status": "예정",
                "_error": str(e),
            })

    df = pd.DataFrame(rows)
    # 정렬/형 변환
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    order = [
        "date","gameId","stadium","home","away",
        "home_score","away_score","home_result","away_result",
        "home_hits","home_hr","away_hits","away_hr",
        "status","section","review_url"
    ]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    df = df[cols]
    return df

# -----------------------------
# 메인
# -----------------------------
def main(since: Optional[str] = None, until: Optional[str] = None, out_csv: str = OUT_CSV):
    since = since or os.getenv("SINCE", DEFAULT_SINCE)
    until = until or os.getenv("UNTIL", None)

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    df_old = load_existing(out_csv)
    targets = build_target_dates(since, until, df_old)

    if not targets:
        print("[INFO] 수집 대상 날짜가 없습니다.")
        return

    driver = make_driver()
    all_daily = []
    try:
        for d in targets:
            print(f"[INFO] {d} 수집 시작...")
            df_d = crawl_day(driver, d)
            if df_d is not None and not df_d.empty:
                all_daily.append(df_d)
            else:
                print(f"[INFO] {d} : 수집 결과 없음(리뷰 버튼 미노출 등)")
    finally:
        driver.quit()

    if not all_daily:
        # 정말로 아무것도 갱신할 게 없을 수 있음 (예정도 없고 전부 최신)
        print("[INFO] 신규/갱신 데이터가 없습니다.")
        return

    df_new = pd.concat(all_daily, ignore_index=True)

    if df_old is None or len(df_old) == 0:
        df_out = df_new
    else:
        df_out = replace_by_gameid(df_old, df_new)

    # 정렬
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

