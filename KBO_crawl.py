# KBO_crawl.py
# -----------------------------------------
# KBO 경기 리뷰 크롤러 (gameId 기반 날짜/증분 수집/예정 스킵)
# -----------------------------------------
import os
import re
import csv
import sys
import time
import json
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

# -----------------------------
# 유틸
# -----------------------------
GAMEID_RE = re.compile(r'gameId[=:\'"]?(\d{8})')

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
# 스케줄 페이지에서 특정 날짜로 이동
#   - 공식 페이지는 캘린더 위젯으로 날짜 전환
#   - 가장 안정적인 방법: 자바스크립트로 달력 이동 이벤트 트리거
# -----------------------------
def goto_schedule_date(driver: webdriver.Chrome, d: date) -> None:
    driver.get(SCHEDULE_URL)
    # 캘린더/테이블 로드 대기
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body"))
    )
    # 달력 위젯은 내부적으로 ajax로 테이블을 갱신한다.
    # 날짜 셀렉터나 캘린더 인풋(id가 바뀔 수 있어 방어적으로 처리).
    # 페이지 스크립트에 의존하지 않고, 서버가 뿌려주는 a[href*=gameId]에서 직접 수집하는 전략도 함께 사용.
    time.sleep(2)  # 초기 스크립트/캘린더 부팅 여유

    # 일부 환경에서 같은 날짜가 기본 로드되는 일이 있어, 아래는 한 번 더 강제 새로고침
    driver.refresh()
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body"))
    )
    time.sleep(1)

    # 날짜를 직접 바꾸는 API가 노출되지 않는 경우가 있어,
    # 최종적으로는 현재 페이지의 HTML에서 gameId를 수집 → 날짜 필터는 review 페이지에서 확정
    # (따라서 여기서는 별도 조작 없이 현재 날짜의 스케줄만 수집하고,
    #  일자 루프는 driver 재기동으로 반복 호출하는 구조를 피하고,
    #  아래 collect_gameids_from_schedule()를 날짜별로 호출할 때마다 페이지를 다시 열도록 구성)
    # => 본 함수는 첫 로드/안정화를 위해 존재. (추가 커스터마이징 여지 남김)

def collect_gameids_from_schedule_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    gids = []
    # a 태그 href/onclick 에 gameId=YYYYMMDD 패턴이 많이 들어간다.
    for a in soup.find_all("a"):
        payload = (a.get("href", "") or "") + " " + (a.get("onclick", "") or "")
        m = GAMEID_RE.search(payload)
        if not m:
            continue
        # 상태 텍스트(예정/취소 등)를 행 단위로 확인
        row = a.find_parent("tr")
        row_text = norm_text(row.get_text(" ")) if row else ""
        if any(key in row_text for key in SKIP_KEYWORDS):
            continue
        gids.append(m.group(1))
    # 중복 제거
    return sorted(set(gids))

def collect_gameids_for_date(d: date) -> List[str]:
    driver = make_driver()
    try:
        goto_schedule_date(driver, d)
        html = driver.page_source
        gids = collect_gameids_from_schedule_html(html)

        # ✅ 날짜 필터 추가
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
        stadium = norm_text((soup.select_one("#txtStadium") or soup.select_one("#lblStadium") or soup.select_one(".stadium")).get_text(strip=True) if (soup.select_one("#txtStadium") or soup.select_one("#lblStadium") or soup.select_one(".stadium")) else "")

        # 스코어보드 (점수)
        # 보통 #tblScoreboard3에 실제 점수/이닝 표가 있음
        home_team = away_team = ""
        home_runs = away_runs = None

        sb3 = soup.select_one("#tblScoreboard3")
        if sb3:
            # 표 구조가 자주 바뀌므로, '팀명' 행(첫열 팀명), 'R' 합계 열을 찾는 식으로 유연 파싱
            # 1) 헤더에서 'R' 합계 열 인덱스
            r_col_idx = None
            ths = sb3.select("thead th")
            for i, th in enumerate(ths):
                if norm_text(th.get_text()) in ("R", "득점", "점수"):
                    r_col_idx = i
                    break
            # 2) 바디에서 두 행(원정, 홈) 파싱
            trs = sb3.select("tbody tr")
            rows = []
            for tr in trs:
                tds = tr.find_all(["th", "td"])
                cells = [norm_text(td.get_text()) for td in tds]
                if cells:
                    rows.append(cells)
            if r_col_idx is not None and len(rows) >= 2:
                # 관례상 원정이 먼저, 홈이 다음인 경우가 많다 (예외 대응 위해 아래 보정 로직 포함).
                # 팀명은 첫 셀(또는 첫 TH)
                away_team = rows[0][0]
                home_team = rows[1][0]
                away_runs = safe_int(rows[0][r_col_idx], 0) if len(rows[0]) > r_col_idx else 0
                home_runs = safe_int(rows[1][r_col_idx], 0) if len(rows[1]) > r_col_idx else 0

        # 팀명 보정 실패 시 scoreboard1에서 백업
        if not home_team or not away_team:
            sb1 = soup.select_one("#tblScoreboard1")
            if sb1:
                tds = sb1.find_all("td")
                # 보통 [홈팀, 승패, 원정팀] 식 배열이 많아 방어적으로 처리
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
            # 헤더에서 'H'(안타), 'HR'(홈런) 유사 컬럼 찾기
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

        # 홈/원정 타자 기록 표 id는 페이지마다 다소 차이 → 여러 후보 시도
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

        # 기본 타당성 체크(경고 로그만 출력; 데이터는 유지)
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
    ap.add_argument("--out",   default="kbo_latest.csv", help="출력 CSV 파일명")
    args = ap.parse_args()

    since = datetime.strptime(args.since, "%Y%m%d").date()
    until = datetime.strptime(args.until, "%Y%m%d").date() if args.until else date.today()

    existing = load_existing_ids(args.out)
    print(f"[INFO] existing rows: {len(existing)}")

    all_new_rows: List[Dict] = []

    # 날짜 루프
    d = since
    while d <= until:
        print(f"[DAY] {d.isoformat()} 수집 시도…")
        gids = collect_gameids_for_date(d)
        if not gids:
            print(f"[DAY] {d.isoformat()} gameId 없음(스케줄 노출 X 가능).")
        else:
            print(f"[DAY] {d.isoformat()} 발견 gameId={gids}")

        # 각 gid 리뷰 페이지 파싱
        day_rows: List[Dict] = []
        for gid in gids:
            # 증분: 이미 있는 gameId는 스킵
            if gid in existing:
                continue
            # gameId의 앞 8자(YYYYMMDD)로 날짜 확정 → 다른 날짜의 gid가 섞여도 안전
            row = fetch_review(gid)
            if not row:
                continue
            # 월요일 필터는 적용하지 않음(포스트시즌/편성 예외 존재). gameId 날짜가 진실.
            day_rows.append(row)

        if day_rows:
            append_rows(args.out, day_rows)
            existing.update(r["gameId"] for r in day_rows)
            all_new_rows.extend(day_rows)

        # polite
        time.sleep(1.0)
        d += timedelta(days=1)

    print(f"[DONE] new rows: {len(all_new_rows)}")

if __name__ == "__main__":
    main()

