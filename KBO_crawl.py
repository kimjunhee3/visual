# KBO_crawl.py
import os
import re
import time
import json
import shutil
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DEFAULT_SINCE = "20250322"

SCHEDULE_DAY_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={d}"
REVIEW_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={gdt}&section=REVIEW"

# ---------------------- util ----------------------
def _norm(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def _yyyymmdd_to_iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

def _to_int(s: str) -> int:
    m = re.search(r"\d+", s or "")
    return int(m.group()) if m else 0

# ---------------------- crawler ----------------------
class UltraPreciseKBOCrawler:
    def __init__(self, headless: bool = True):
        self.driver = None
        self._tmp_profile = None
        self._setup_driver(headless=headless)

    def _setup_driver(self, headless: bool):
        opts = Options()
        if headless:
            # GitHub Actions에서 오류 적은 최신 headless
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        # 세션 충돌 방지: 매 실행마다 임시 프로필
        self._tmp_profile = tempfile.mkdtemp(prefix="chrome-profile-")
        opts.add_argument(f"--user-data-dir={self._tmp_profile}")

        # 몇몇 환경에서 sandbox 문제 회피
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        try:
            self.driver = webdriver.Chrome(options=opts)
            self.driver.set_page_load_timeout(60)
            print("ChromeDriver 시작 성공")
        except SessionNotCreatedException as e:
            print(f"[FATAL] Chrome 세션 생성 실패: {e}")
            raise
        except Exception as e:
            print(f"[FATAL] Chrome 시작 실패: {e}")
            raise

    # --------------- public API ---------------
    def crawl_kbo_games(self, date_list, checkpoint_dir="checkpoints", force_refresh=False) -> pd.DataFrame:
        os.makedirs(checkpoint_dir, exist_ok=True)
        results = []

        for d in date_list:
            print(f"[DAY] {d} 수집 시도…")
            ck = os.path.join(checkpoint_dir, f"{d}.csv")
            if (not force_refresh) and os.path.exists(ck):
                print(f"  └ checkpoint 존재 → 건너뜀")
                try:
                    results.extend(pd.read_csv(ck).to_dict("records"))
                except Exception as e:
                    print("  └ checkpoint 읽기 실패:", e)
                continue

            try:
                self.driver.get(SCHEDULE_DAY_URL.format(d=d))
                time.sleep(2)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                games = soup.select("li.game-cont")
                # 경기 없는 날 스킵
                if not games:
                    print(f"  └ {d}: 경기 없음 → 스킵")
                    continue

                day_rows = []
                for li in games:
                    gi = self._extract_one_game_from_list(li, d)
                    if not gi:
                        continue
                    # 종료 경기만 리뷰 파싱
                    if gi["away_result"] in ("승", "패", "무") and gi.get("game_id"):
                        detail = self._fetch_review_detail(gi["game_id"], gi["raw_date"], gi["away_team"], gi["home_team"])
                        gi.update(detail)
                    day_rows.append(gi)

                if day_rows:
                    pd.DataFrame(day_rows).to_csv(ck, index=False, encoding="utf-8-sig")
                    results.extend(day_rows)
                    print(f"  └ 저장 완료: {len(day_rows)}경기")
                else:
                    print(f"  └ {d}: 종료 경기 없음")
            except Exception as e:
                print(f"[WARN] {d} 처리 실패:", e)
                continue

            time.sleep(1.5)

        return pd.DataFrame(results)

    def cleanup(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
        if self._tmp_profile and os.path.isdir(self._tmp_profile):
            shutil.rmtree(self._tmp_profile, ignore_errors=True)

    # --------------- list page helpers ---------------
        def _extract_one_game_from_list(self, li, url_date_yyyymmdd: str) -> dict | None:
        """리스트에 있는 한 경기 블록에서 기본 정보 추출"""
        try:
            game_id = li.get("g_id") or li.get("gid") or ""
            game_date = li.get("g_dt") or url_date_yyyymmdd
            date_iso = _yyyymmdd_to_iso(game_date)

            # 점수
            away_score, home_score = 0, 0
            sc_away_el = li.select_one(".team.away .score")
            sc_home_el = li.select_one(".team.home .score")
            if sc_away_el:
                txt = _norm(sc_away_el.get_text())
                if txt.isdigit():
                    away_score = int(txt)
            if sc_home_el:
                txt = _norm(sc_home_el.get_text())
                if txt.isdigit():
                    home_score = int(txt)

            # 종료 여부
            end_flag = "end" in (li.get("class") or [])

            # 팀 이름
            away_nm_el = li.select_one(".team.away .name")
            home_nm_el = li.select_one(".team.home .name")
            away_name = li.get("away_nm") or (_norm(away_nm_el.get_text()) if away_nm_el else "")
            home_name = li.get("home_nm") or (_norm(home_nm_el.get_text()) if home_nm_el else "")

            def result_for(is_away: bool) -> str:
                if not end_flag:
                    return "예정"
                if away_score == home_score:
                    return "무"
                if is_away:
                    return "승" if away_score > home_score else "패"
                else:
                    return "승" if home_score > away_score else "패"

            stadium_el = li.select_one(".place")
            stadium = _norm(stadium_el.get_text()) if stadium_el else ""

            return {
                "raw_date": game_date,
                "date": date_iso,
                "stadium": stadium,
                "away_team": away_name,
                "home_team": home_name,
                "away_score": away_score,
                "home_score": home_score,
                "away_result": result_for(True),
                "home_result": result_for(False),
                "game_id": game_id,
            }
        except Exception as e:
            print("  └ list 파싱 실패:", e)
            return None

    # --------------- review page helpers ---------------
    def _ensure_review_tab(self) -> bool:
        """리뷰 탭이 활성화되도록 시도"""
        try:
            # 이미 REVIEW면 on 클래스가 있음
            on = self.driver.find_elements(By.XPATH, "//li[@section='REVIEW' and contains(@class,'on')]")
            if on:
                return True
            tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//li[@section='REVIEW']//a"))
            )
            tab.click()
            time.sleep(1.5)
            return True
        except Exception:
            return False

    def _fetch_review_detail(self, game_id: str, gdt_yyyymmdd: str, away_team: str, home_team: str) -> dict:
        """리뷰 페이지에서 H, HR, AB 계산"""
        try:
            self.driver.get(REVIEW_URL.format(gid=game_id, gdt=gdt_yyyymmdd))
            time.sleep(2)
            self._ensure_review_tab()
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            away_hit, home_hit = self._extract_hits(soup)
            away_hr, home_hr = self._extract_hrs_with_fallback(soup)
            away_ab, home_ab = self._extract_ab_with_fallback(soup)

            # 타율
            away_avg = round(away_hit / away_ab, 4) if away_ab else 0.0
            home_avg = round(home_hit / home_ab, 4) if home_ab else 0.0

            return {
                "away_hit": away_hit,
                "home_hit": home_hit,
                "away_hr": away_hr,
                "home_hr": home_hr,
                "away_ab": away_ab,
                "home_ab": home_ab,
                "away_avg": away_avg,
                "home_avg": home_avg,
            }
        except Exception as e:
            print("  └ 리뷰 상세 파싱 실패:", e)
            return {
                "away_hit": 0, "home_hit": 0,
                "away_hr": 0, "home_hr": 0,
                "away_ab": 0, "home_ab": 0,
                "away_avg": 0.0, "home_avg": 0.0
            }

    # ----- H, HR, AB 추출 -----
    def _extract_hits(self, soup: BeautifulSoup) -> tuple[int, int]:
        """Scoreboard3의 H열 → 실패 시 Hitter3 tfoot 2열"""
        away_hit = home_hit = 0

        sb3 = soup.select_one("#tblScoreboard3")
        if sb3:
            heads = [ _norm(th.get_text()) for th in sb3.select("thead th") ]
            if "H" in heads:
                h_idx = heads.index("H")
                rows = sb3.select("tbody tr")
                if len(rows) >= 2:
                    a_cells = rows[0].find_all("td")
                    h_cells = rows[1].find_all("td")
                    if len(a_cells) > h_idx and _norm(a_cells[h_idx].get_text()).isdigit():
                        away_hit = int(_norm(a_cells[h_idx].get_text()))
                    if len(h_cells) > h_idx and _norm(h_cells[h_idx].get_text()).isdigit():
                        home_hit = int(_norm(h_cells[h_idx].get_text()))
        if away_hit == 0 and home_hit == 0:
            # fallback: Hitter3 tfoot → 두 번째 td가 H
            a = soup.select_one("#tblAwayHitter3 tfoot tr")
            h = soup.select_one("#tblHomeHitter3 tfoot tr")
            if a:
                tds = a.find_all("td")
                if len(tds) >= 2: away_hit = _to_int(tds[1].get_text())
            if h:
                tds = h.find_all("td")
                if len(tds) >= 2: home_hit = _to_int(tds[1].get_text())
        return away_hit, home_hit

    def _count_hr_in_hitter2(self, soup: BeautifulSoup, home: bool) -> int:
        """
        히터2 테이블에서 '홈'이 들어간 셀(좌홈, 좌중홈, 우홈 등)을 모두 홈런으로 집계.
        (한 셀에 홈런이 2개 들어갈 일은 없으므로 단순히 '홈' 포함 여부로 카운트)
        """
        table_id = "#tblHomeHitter2" if home else "#tblAwayHitter2"
        tbody = soup.select_one(f"{table_id} tbody")
        if not tbody:
            return -1  # 실패
        cnt = 0
        for tr in tbody.select("tr"):
            for td in tr.find_all("td"):
                t = _norm(td.get_text())
                # 홈런 표기: '좌중홈', '좌홈', '우중홈' 등 → '홈' 포함
                # '홈인' 등의 수비/주루 이벤트는 히터2에 거의 나타나지 않지만 방지 차원으로 예외 처리
                if "홈" in t and "홈인" not in t:
                    cnt += t.count("홈") if "홈런" not in t else 1
        return cnt

    def _extract_hrs_with_fallback(self, soup: BeautifulSoup) -> tuple[int, int]:
        """히터2 우선, 실패 시 요약(#tblEtc)+투수표로 보정"""
        home_hr = self._count_hr_in_hitter2(soup, home=True)
        away_hr = self._count_hr_in_hitter2(soup, home=False)
        if home_hr >= 0 and away_hr >= 0:
            return away_hr, home_hr

        # fallback: 요약표에 '홈런' 문구 + 투수 소속으로 팀 판별
        away_pitchers = { _norm(tds[0].get_text())
                          for tr in soup.select("#tblAwayPitcher tbody tr")
                          if (tds := tr.find_all("td")) }
        home_pitchers = { _norm(tds[0].get_text())
                          for tr in soup.select("#tblHomePitcher tbody tr")
                          if (tds := tr.find_all("td")) }

        a_hr = h_hr = 0
        etc = soup.select_one("#tblEtc")
        if etc:
            for tr in etc.select("tr"):
                th = tr.find("th"); td = tr.find("td")
                if not th or not td: 
                    continue
                if "홈런" not in _norm(th.get_text()):
                    continue
                text = _norm(td.get_text())
                # 괄호 안 마지막 토큰을 투수명으로 가정
                # 예: "장성우 11호(2점 5회1,2루 강보라 윤영빈) 장준원 1호(7회1점 윤영빈)"
                for p in re.findall(r"\([^)]* ([가-힣A-Za-z0-9]+)\)", text):
                    if p in away_pitchers:
                        h_hr += 1
                    elif p in home_pitchers:
                        a_hr += 1
        if away_hr < 0: away_hr = a_hr
        if home_hr < 0: home_hr = h_hr
        return away_hr, home_hr

    def _extract_ab_with_fallback(self, soup: BeautifulSoup) -> tuple[int, int]:
        """
        우선 Hitter3 tfoot 첫 번째 칸(타수) 사용.
        없으면 Hitter2의 각 타자의 이벤트를 이용해 AB 추정:
        - 안타/홈/아웃 등 타수로 잡히는 이벤트 개수 합
        """
        def from_hitter3(home: bool) -> int:
            t = soup.select_one("#tblHomeHitter3 tfoot tr" if home else "#tblAwayHitter3 tfoot tr")
            if not t:
                return -1
            tds = t.find_all("td")
            return _to_int(tds[0].get_text()) if tds else -1

        home_ab = from_hitter3(True)
        away_ab = from_hitter3(False)
        if home_ab >= 0 and away_ab >= 0:
            return away_ab, home_ab

        def estimate_from_hitter2(home: bool) -> int:
            table_id = "#tblHomeHitter2" if home else "#tblAwayHitter2"
            tbody = soup.select_one(f"{table_id} tbody")
            if not tbody:
                return 0
            ab = 0
            for tr in tbody.select("tr"):
                # 선수명 이후의 셀들이 타석 이벤트
                tds = tr.find_all("td")
                if not tds:
                    continue
                # 첫 번째 혹은 두 번째 셀에 선수명이 있어 구조가 가변 → 뒤쪽만 이벤트로 간주
                # 실제 AB 판정: 히트/아웃/홈(=HR)만 카운트 (볼넷/사구/희비/희플 제외)
                for td in tds:
                    t = _norm(td.get_text())
                    if not t or t == "&nbsp;":
                        continue
                    if ("4구" in t) or ("사구" in t) or ("희비" in t) or ("희플" in t):
                        continue
                    # 홈 포함(홈런) 또는 '안타'(안타), 혹은 일반 '땅볼/뜬공/삼진' 등 타수 소모
                    if ("홈" in t) or ("안타" in t) or re.search(r"(땅|뜬|삼진|중안|좌안|우안)", t):
                        ab += 1
            return ab

        if away_ab < 0: away_ab = estimate_from_hitter2(False)
        if home_ab < 0: home_ab = estimate_from_hitter2(True)
        return away_ab, home_ab

# ---------------------- CLI main ----------------------
def days(since: str | None, until: str | None) -> list[str]:
    """YYYYMMDD 범위를 리스트로 반환"""
    if not since:
        since = DEFAULT_SINCE
    if not until:
        until = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    s = datetime.strptime(since, "%Y%m%d").date()
    e = datetime.strptime(until, "%Y%m%d").date()
    d = s
    out = []
    while d <= e:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out

def upsert_csv(out_csv: str, new_rows: list[dict], since: str, until: str):
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
    new_df = pd.DataFrame(new_rows)
    if os.path.exists(out_csv):
        old = pd.read_csv(out_csv, encoding="utf-8-sig")
        # 동일 스키마 강제
        cols = [
            "date","stadium","away_team","home_team",
            "away_score","home_score","away_result","home_result",
            "away_hit","home_hit","away_hr","home_hr",
            "away_ab","home_ab","away_avg","home_avg"
        ]
        for c in cols:
            if c not in old.columns:
                old[c] = pd.Series([None]*len(old))
        old = old[cols]
        mask = (pd.to_datetime(old["date"]) >= pd.to_datetime(_yyyymmdd_to_iso(since))) & \
               (pd.to_datetime(old["date"]) <= pd.to_datetime(_yyyymmdd_to_iso(until)))
        kept = old.loc[~mask].copy()
        out = pd.concat([kept, new_df], ignore_index=True)
    else:
        out = new_df
    out = out.sort_values("date").reset_index(drop=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] upserted {len(new_df)} rows -> {out_csv}")

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None)
    ap.add_argument("--until", default=None)
    ap.add_argument("--out",   default="data/kbo_latest.csv")
    ap.add_argument("--force", default="false")
    args = ap.parse_args()

    force_refresh = str(args.force).lower() in ("1","true","yes")

    rng = days(args.since, args.until)
    print(f"[INFO] Running crawler for {rng[0]}..{rng[-1]} (force_refresh={force_refresh})")

    crawler = UltraPreciseKBOCrawler(headless=True)
    try:
        df = crawler.crawl_kbo_games(rng, force_refresh=force_refresh)
        rows = df.to_dict("records")
        if rows:
            upsert_csv(args.out, rows, rng[0], rng[-1])
        else:
            print("[INFO] 신규 행 없음")
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()

