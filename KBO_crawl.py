# KBO_crawl.py
import os
import re
import csv
import json
import time
import shutil
import tempfile
import argparse
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ---------------------------
# 설정
# ---------------------------
DEFAULT_SINCE = "20250322"
WAIT_SEC = 12

SCHEDULE_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={yyyymmdd}"
REVIEW_URL   = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={gdt}&section=REVIEW"

SCHEMA = [
    "date", "stadium", "away_team", "home_team",
    "away_score", "home_score",
    "away_result", "home_result",
    "away_hit", "home_hit",
    "away_hr", "home_hr",
    "away_ab", "home_ab",
    "away_avg", "home_avg"
]


# ---------------------------
# 유틸
# ---------------------------
def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def to_int(s: str, default: int = 0) -> int:
    try:
        m = re.search(r"-?\d+", s or "")
        return int(m.group()) if m else default
    except Exception:
        return default


# ---------------------------
# 크롤러
# ---------------------------
class UltraPreciseKBOCrawler:
    def __init__(self, headless: bool = True):
        self.driver = None
        self.tmp_profile_dir = None
        self.setup_driver(headless=headless)

    # ---- 브라우저 ----
    def setup_driver(self, headless: bool = True):
        """
        GitHub Actions 등 다중 세션/컨테이너에서 'session not created' 방지:
        - headless=new
        - 고유한 user-data-dir (임시 디렉토리)
        """
        self.tmp_profile_dir = tempfile.mkdtemp(prefix="kbo_chrome_")

        opts = Options()
        if headless:
            # 최신 크롬 헤드리스
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(f"--user-data-dir={self.tmp_profile_dir}")
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        self.driver = webdriver.Chrome(options=opts)
        self.driver.set_page_load_timeout(60)
        print("[INFO] ChromeDriver started (headless=%s)" % headless)

    def cleanup(self):
        try:
            if self.driver:
                self.driver.quit()
        finally:
            if self.tmp_profile_dir and os.path.exists(self.tmp_profile_dir):
                shutil.rmtree(self.tmp_profile_dir, ignore_errors=True)

    # ---- 상단 흐름 ----
    def crawl_kbo_games(self, date_list: List[str], checkpoint_dir="checkpoints", force_refresh=False) -> pd.DataFrame:
        """
        날짜 리스트를 돌며 GameCenter 페이지에서 <li class="game-cont">를 읽고
        각 경기 정보를 추출한다.
        - 경기 없는 날은 CSV에 기록하지 않는다.
        - 날짜는 g_dt(실제 경기일) 우선, 없으면 URL 날짜 fallback.
        """
        os.makedirs(checkpoint_dir, exist_ok=True)
        result: List[Dict] = []

        for dt in date_list:
            ckpt = os.path.join(checkpoint_dir, f"{dt}.csv")

            if (not force_refresh) and os.path.exists(ckpt):
                print(f"[SKIP] {dt} (checkpoint exists)")
                try:
                    df = pd.read_csv(ckpt, encoding="utf-8-sig")
                    result.extend(df.to_dict(orient="records"))
                except Exception as e:
                    print(f"[WARN] checkpoint load failed for {dt}: {e}")
                continue

            # 페이지 진입
            url = SCHEDULE_URL.format(yyyymmdd=dt)
            self.driver.get(url)
            time.sleep(4)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            # g_id가 있는 li만 유효 경기로 취급 (예정이라도 g_id는 붙는 편)
            games = soup.select("li.game-cont[g_id]")

            day_rows = []
            for li in games:
                row = self.extract_game_row(li, dt)
                if row:
                    day_rows.append(row)

            # 경기 없으면 스킵
            if not day_rows:
                print(f"[INFO] {dt} → 경기 없음 (no rows)")
                continue

            # 저장 및 합치기
            pd.DataFrame(day_rows).to_csv(ckpt, index=False, encoding="utf-8-sig")
            print(f"[INFO] {dt} 저장 완료 ({len(day_rows)}경기)")
            result.extend(day_rows)

            time.sleep(1.0)

        return pd.DataFrame(result)

    # ---- li 에서 한 경기 파싱 ----
    def extract_game_row(self, li, fallback_date: str) -> Optional[Dict]:
        """
        <li class="game-cont">에서 경기 요약을 파싱하고, 종료 경기면 리뷰 탭에서 상세 스탯을 추가
        """
        try:
            gid = li.get("g_id")
            gdt = (li.get("g_dt") or fallback_date).strip()
            stadium = li.get("s_nm") or ""
            away_team = li.get("away_nm") or ""
            home_team = li.get("home_nm") or ""

            # 상태
            cls = set((li.get("class") or "").split())
            if "end" in cls:
                status = "종료"
            elif "play" in cls:
                status = "진행중"
            else:
                status = "예정"

            # 점수
            away_score, home_score = self._extract_score_from_li(li)

            # 종료 경기면 상세 스탯
            details = {
                "away_hit": 0, "home_hit": 0,
                "away_hr": 0, "home_hr": 0,
                "away_ab": 0, "home_ab": 0
            }
            if status == "종료" and gid:
                deets = self._fetch_review_details(gid, gdt, away_team, home_team)
                details.update(deets)

            # 결과
            away_result = self._result_str(away_score, home_score, True)
            home_result = self._result_str(away_score, home_score, False)

            # 날짜 YYYY-MM-DD
            d = f"{gdt[:4]}-{gdt[4:6]}-{gdt[6:8]}"

            row = {
                "date": d,
                "stadium": stadium,
                "away_team": away_team,
                "home_team": home_team,
                "away_score": away_score,
                "home_score": home_score,
                "away_result": away_result,
                "home_result": home_result,
                "away_hit": details["away_hit"],
                "home_hit": details["home_hit"],
                "away_hr": details["away_hr"],
                "home_hr": details["home_hr"],
                "away_ab": details["away_ab"],
                "home_ab": details["home_ab"],
            }
            # 파생(타율)
            row["away_avg"] = round(row["away_hit"] / row["away_ab"], 4) if row["away_ab"] else 0.0
            row["home_avg"] = round(row["home_hit"] / row["home_ab"], 4) if row["home_ab"] else 0.0

            return row

        except Exception as e:
            print(f"[ERR] extract_game_row: {e}")
            return None

    # ---- li 내부에서 점수 읽기 ----
    def _extract_score_from_li(self, li) -> Tuple[int, int]:
        """
        li.game-cont 내부의 .team.away/home .score 텍스트에서 점수 추출
        """
        try:
            a_score = 0
            h_score = 0
            a = li.select_one(".team.away .score")
            h = li.select_one(".team.home .score")
            if a:
                a_score = to_int(a.get_text(strip=True), 0)
            if h:
                h_score = to_int(h.get_text(strip=True), 0)
            return a_score, h_score
        except Exception:
            return 0, 0

    # ---- 리뷰 페이지에서 상세 수집 ----
    def _fetch_review_details(self, gid: str, gdt: str, away_team: str, home_team: str) -> Dict:
        """
        리뷰 탭에서 안타/홈런/AB 수집
        홈런: #tblEtc '홈런' 행 파싱 → 투수 이름으로 소속 식별 (투수 ∈ awayPitchers => 타자는 home 팀)
        """
        url = REVIEW_URL.format(gid=gid, gdt=gdt)
        self.driver.get(url)
        # 리뷰 탭이 아닐 수 있으므로 시도 클릭
        self._ensure_review_tab()
        time.sleep(2)
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        away_hit, home_hit = self._extract_hits(soup)
        away_ab  , home_ab  = self._extract_ab(soup)
        away_hr  , home_hr  = self._extract_homeruns_by_pitcher_map(soup)

        return {
            "away_hit": away_hit, "home_hit": home_hit,
            "away_hr": away_hr,   "home_hr": home_hr,
            "away_ab": away_ab,   "home_ab": home_ab
        }

    def _ensure_review_tab(self):
        try:
            # 이미 REVIEW 활성?
            li_on = self.driver.find_elements(By.XPATH, "//li[@section='REVIEW' and contains(@class, 'on')]")
            if li_on:
                return
            tab_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//li[@section='REVIEW']/a"))
            )
            tab_link.click()
            time.sleep(1.5)
        except Exception:
            pass

    # ---- 안타 ----
    def _extract_hits(self, soup) -> Tuple[int, int]:
        """
        1순위: #tblScoreboard3 의 'H' 컬럼
        2순위: #tblAwayHitter3 / #tblHomeHitter3 tfoot 합계(두번째 셀이 H인 케이스가 많음)
        """
        # Scoreboard3
        try:
            sb = soup.select_one("#tblScoreboard3")
            if sb:
                headers = [norm_space(th.get_text()) for th in sb.select("thead th")]
                if "H" in headers:
                    idx = headers.index("H")
                    rows = sb.select("tbody tr")
                    if len(rows) >= 2:
                        def cell_int(tr):
                            tds = tr.select("td")
                            if idx < len(tds):
                                return to_int(tds[idx].get_text(strip=True), 0)
                            return 0
                        away_hit = cell_int(rows[0])
                        home_hit = cell_int(rows[1])
                        return away_hit, home_hit
        except Exception:
            pass

        # Hitter3 tfoot
        def hitter3_hits(sel_id: str) -> int:
            tb = soup.select_one(sel_id)
            if not tb:
                return 0
            tf = tb.select_one("tfoot tr")
            if not tf:
                return 0
            tds = tf.select("td")
            # 일반적으로 tfoot: [AB, H, ...] 구조
            if len(tds) >= 2:
                return to_int(tds[1].get_text(strip=True), 0)
            return 0

        a = hitter3_hits("#tblAwayHitter3")
        h = hitter3_hits("#tblHomeHitter3")
        return a, h

    # ---- 타수(AB) ----
    def _extract_ab(self, soup) -> Tuple[int, int]:
        """
        1순위: #tbl(Away|Home)Hitter3 tfoot 의 1번째 셀(AB)
        2순위: #tbl(Away|Home)Hitter2 각 선수별 AB 합산 (H+OUT으로 계산은 페이지별 편차가 있어 Hitter2의 AB 열 합산이 안전)
        """
        def hitter3_ab(sel_id: str) -> int:
            tb = soup.select_one(sel_id)
            if not tb:
                return 0
            tf = tb.select_one("tfoot tr")
            if not tf:
                return 0
            tds = tf.select("td")
            if tds:
                return to_int(tds[0].get_text(strip=True), 0)
            return 0

        away_ab = hitter3_ab("#tblAwayHitter3")
        home_ab = hitter3_ab("#tblHomeHitter3")

        if away_ab == 0:
            away_ab = self._sum_ab_from_hitter2(soup, which="away")
        if home_ab == 0:
            home_ab = self._sum_ab_from_hitter2(soup, which="home")

        return away_ab, home_ab

    def _sum_ab_from_hitter2(self, soup, which="away") -> int:
        """
        #tbl(Away|Home)Hitter2 의 각 행에서 AB 열을 찾아 합산
        """
        sel = "#tblAwayHitter2" if which == "away" else "#tblHomeHitter2"
        tb = soup.select_one(sel)
        if not tb:
            return 0

        # header에서 AB 위치 찾기
        ab_idx = None
        try:
            heads = [norm_space(th.get_text()) for th in tb.select("thead th")]
            for i, h in enumerate(heads):
                if h in ("AB", "타수"):
                    ab_idx = i
                    break
        except Exception:
            pass

        s = 0
        for tr in tb.select("tbody tr"):
            tds = tr.select("td")
            if ab_idx is not None and ab_idx < len(tds):
                s += to_int(tds[ab_idx].get_text(strip=True), 0)
        return s

    # ---- 홈런 ----
    def _extract_homeruns_by_pitcher_map(self, soup) -> Tuple[int, int]:
        """
        #tblEtc 의 '홈런' 행을 파싱.
        텍스트 예: "장성우(1점 6회2아) ... 장준원(1점 7회1아 윤성빈)"
        → 괄호 안 마지막에 투수 이름이 들어간 형식이 다수.
        awayPitchers = #tblAwayPitcher tbody의 투수명 set
        homePitchers = #tblHomePitcher tbody의 투수명 set

        - 투수가 awayPitchers에 있으면 타자는 홈팀
        - 투수가 homePitchers에 있으면 타자는 원정팀
        """
        def pitcher_names(sel_id: str) -> set:
            names = set()
            tb = soup.select_one(sel_id)
            if not tb:
                return names
            for tr in tb.select("tbody tr"):
                tds = tr.select("td")
                if tds:
                    name = norm_space(tds[0].get_text())
                    if name:
                        names.add(name)
            return names

        away_pitchers = pitcher_names("#tblAwayPitcher")
        home_pitchers = pitcher_names("#tblHomePitcher")

        away_hr = 0
        home_hr = 0

        etc = soup.select_one("#tblEtc")
        if not etc:
            return away_hr, home_hr

        for tr in etc.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            if "홈런" not in th.get_text():
                continue

            text = norm_space(td.get_text())
            if not text:
                continue

            # 괄호 단락 기준으로 분해: batter ( ... pitcher )
            # 라인에 여러 개가 연달아 있을 수 있어 모두 잡는다.
            # 패턴:  홍길동( ... 투수명) ...
            # '투수명'은 괄호 내부 마지막 토큰인 경우가 많음 → 괄호 내용에서 마지막 '단어' 추출
            chunks = re.findall(r"([가-힣A-Za-z0-9]+)\(([^)]*)\)", text)
            for batter, inside in chunks:
                inside_norm = norm_space(inside)
                # 괄호 내 마지막 단어를 투수명 후보로
                cand = inside_norm.split()[-1] if inside_norm else ""
                pitcher = cand

                # 투수 소속으로 타자 팀 결정
                if pitcher in away_pitchers:
                    # 투수가 원정이면, 타자는 홈팀 → home_hr++
                    home_hr += 1
                elif pitcher in home_pitchers:
                    # 투수가 홈이면, 타자는 원정팀 → away_hr++
                    away_hr += 1
                else:
                    # pitcher 식별 실패: 위치 정보(회/말)로 추론
                    # '말' 이 있으면 홈타자, '초'면 원정타자 → 너무 공격적이면 오탐 가능, 보수적으로 skip 가능
                    if "말" in inside_norm:  # home batting
                        home_hr += 1
                    elif "초" in inside_norm:  # away batting
                        away_hr += 1
                    else:
                        # 추론 불가 → 패스
                        pass

        return away_hr, home_hr

    # ---- 결과 문자열 ----
    @staticmethod
    def _result_str(away_score: int, home_score: int, is_away: bool) -> str:
        if away_score == 0 and home_score == 0:
            return "예정"
        if away_score == home_score:
            return "무"
        if is_away:
            return "승" if away_score > home_score else "패"
        return "승" if home_score > away_score else "패"


# ---------------------------
# CSV upsert
# ---------------------------
def upsert_range(out_csv: str, rows: List[Dict], since: date, until: date):
    """
    기존 CSV에서 [since..until] 구간을 제거하고, 새 rows를 합쳐서 저장.
    SCHEMA 순서로 정렬해서 저장.
    """
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
    new_df = pd.DataFrame(rows, columns=SCHEMA)

    if os.path.exists(out_csv):
        old = pd.read_csv(out_csv, encoding="utf-8-sig")
        # 스키마 보정
        for c in SCHEMA:
            if c not in old.columns:
                old[c] = pd.NA
        old = old[SCHEMA].copy()

        # 날짜 비교 위해 datetime 변환
        def to_d(s):
            try:
                return datetime.strptime(str(s), "%Y-%m-%d").date()
            except Exception:
                return None

        old["__d"] = old["date"].map(to_d)
        mask = old["__d"].notna() & (old["__d"] >= since) & (old["__d"] <= until)
        kept = old.loc[~mask, SCHEMA].copy()
        out = pd.concat([kept, new_df], ignore_index=True)
    else:
        out = new_df

    out = out.sort_values(["date", "stadium", "away_team", "home_team"]).reset_index(drop=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] upserted {len(new_df)} rows -> {out_csv}")


# ---------------------------
# 날짜 범위 유틸
# ---------------------------
def build_date_list(since_yyyymmdd: str, until_yyyymmdd: Optional[str] = None) -> List[str]:
    since_d = datetime.strptime(since_yyyymmdd, "%Y%m%d").date()
    until_d = datetime.strptime(until_yyyymmdd, "%Y%m%d").date() if until_yyyymmdd else date.today()
    lst = []
    d = since_d
    while d <= until_d:
        lst.append(yyyymmdd(d))
        d += timedelta(days=1)
    return lst


# ---------------------------
# main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=DEFAULT_SINCE, help="YYYYMMDD")
    ap.add_argument("--until", default=None, help="YYYYMMDD (default=today)")
    ap.add_argument("--out",   default="data/kbo_latest.csv")
    ap.add_argument("--force", action="store_true", help="ignore checkpoints and recrawl")
    args = ap.parse_args()

    dates = build_date_list(args.since, args.until)
    print(f"[INFO] Running crawler for {dates[0]}..{dates[-1]} (force_refresh={args.force})")

    crawler = UltraPreciseKBOCrawler(headless=True)
    try:
        df = crawler.crawl_kbo_games(dates, checkpoint_dir="checkpoints", force_refresh=args.force)
    finally:
        crawler.cleanup()

    rows = df.to_dict(orient="records")
    if not rows:
        print("[INFO] No rows collected. Nothing to upsert.")
        return

    since_d = datetime.strptime(args.since, "%Y%m%d").date()
    until_d = datetime.strptime(args.until, "%Y%m%d").date() if args.until else date.today()
    upsert_range(args.out, rows, since_d, until_d)


if __name__ == "__main__":
    main()



