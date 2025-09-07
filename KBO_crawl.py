# KBO_crawl.py
import os, re, time, json, shutil, tempfile
from datetime import datetime, timedelta

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DEFAULT_SINCE = "20250322"

SCHEDULE_DAY_URL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={d}"
REVIEW_URL       = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={gdt}&section=REVIEW"

# ---------------------- util ----------------------
def _norm(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def _yyyymmdd_to_iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

def _iso_to_yyyymmdd(iso: str) -> str:
    return iso.replace("-", "")

def _to_int(s: str) -> int:
    m = re.search(r"\d+", s or "")
    return int(m.group()) if m else 0

def _retry(n: int, delay: float, fn, *args, **kwargs):
    last = None
    for _ in range(n):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last = e
            time.sleep(delay)
    raise last if last else RuntimeError("retry failed")

def _clean_stadium_name(s: str) -> str:
    """
    '구장:', '장소:', '경기장:' 같은 접두어/콜론 제거 +
    끝의 괄호 부가정보 제거 → 깔끔한 구장명만 남김
    """
    t = _norm(s)
    if not t:
        return ""
    t = re.sub(r"^(구장|장소|경기장)\s*[:：]\s*", "", t)
    t = re.sub(r"\s*\([^()]*\)\s*$", "", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t

# ---------------------- crawler ----------------------
class UltraPreciseKBOCrawler:
    def __init__(self, headless: bool = True):
        self.driver = None
        self._tmp_profile = None
        self._setup_driver(headless=headless)

    def _setup_driver(self, headless: bool):
        opts = Options()
        if headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        self._tmp_profile = tempfile.mkdtemp(prefix="chrome-profile-")
        opts.add_argument(f"--user-data-dir={self._tmp_profile}")
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
                _retry(3, 1.0, self._wait_list_loaded)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                games = soup.select("li.game-cont")
                if not games:
                    print(f"  └ {d}: 경기 없음 → 스킵")
                    continue

                day_rows = []
                for li in games:
                    gi = self._extract_one_game_from_list(li, d)
                    if not gi:
                        continue

                    # 종료 경기만 리뷰 파싱(리뷰 버튼이 보이면 종료로 간주)
                    if gi["away_result"] in ("승", "패", "무") and gi.get("game_id"):
                        detail = _retry(3, 1.0, self._fetch_review_detail,
                                        gi["game_id"], gi["raw_date"], gi["away_team"], gi["home_team"])
                        # 목록에서 못 구한 구장명은 리뷰에서 보강
                        if (not gi.get("stadium")) and detail.get("stadium_review"):
                            gi["stadium"] = detail["stadium_review"]
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

            time.sleep(1.2)

        return pd.DataFrame(results)

    def _wait_list_loaded(self):
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.game-cont"))
            )
        except TimeoutException:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.game-cont"))
            )
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)

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
        try:
            # gameId / gameDate 우선 확보
            game_id  = li.get("g_id") or li.get("gid") or ""
            game_date = li.get("g_dt") or url_date_yyyymmdd

            # 리뷰 링크가 있으면 href에서 보정
            a_review = li.select_one("a#btnReview, a[href*='section=REVIEW']")
            if a_review and a_review.has_attr("href"):
                href = a_review["href"]
                gid  = re.search(r"gameId=([0-9A-Z]+)", href)
                gdt  = re.search(r"gameDate=(\d{8})", href)
                if gid: game_id = gid.group(1)
                if gdt: game_date = gdt.group(1)

            date_iso = _yyyymmdd_to_iso(game_date)

            # 점수
            def _score(sel):
                el = li.select_one(sel)
                t  = _norm(el.get_text()) if el else ""
                return int(t) if t.isdigit() else 0
            away_score = _score(".team.away .score")
            home_score = _score(".team.home .score")

            # 종료 판정: class end 또는 리뷰 버튼 유무
            end_flag = ("end" in (li.get("class") or [])) or (a_review is not None)

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

            # 구장명: 속성/여러 후보 셀렉터 순차 시도
            stadium = ""
            for attr in ("stadium", "place", "data-stadium", "data-place"):
                v = li.get(attr)
                if v:
                    stadium = _norm(v); break
            if not stadium:
                for sel in [".place", ".info .place", ".stadium", ".ballpark", "span.place", "p.place"]:
                    el = li.select_one(sel)
                    if el:
                        stadium = _norm(el.get_text()); break

            # 이름만 남기기
            stadium = _clean_stadium_name(stadium)

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
        try:
            on = self.driver.find_elements(By.XPATH, "//li[@section='REVIEW' and contains(@class,'on')]")
            if on:
                return True
            tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//li[@section='REVIEW']//a"))
            )
            tab.click()
            time.sleep(1.0)
            return True
        except Exception:
            return False

    def _fetch_review_detail(self, game_id: str, gdt_yyyymmdd: str, away_team: str, home_team: str) -> dict:
        try:
            self.driver.get(REVIEW_URL.format(gid=game_id, gdt=gdt_yyyymmdd))
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#tblScoreboard3, #tblAwayHitter3"))
            )
            self._ensure_review_tab()
            time.sleep(0.8)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # 구장명 fallback
            stadium_review = ""
            for sel in ["#txtStadium", "#txtStadium1", "#txtStadium2",
                        ".st_name", ".stadium", ".ballpark", ".park"]:
                el = soup.select_one(sel)
                if el:
                    stadium_review = _norm(el.get_text()); break

            stadium_review = _clean_stadium_name(stadium_review)

            away_hit, home_hit = self._extract_hits(soup)
            away_hr,  home_hr  = self._extract_hrs_with_fallback(soup)
            away_ab,  home_ab  = self._extract_ab_with_fallback(soup)

            away_avg = round(away_hit / away_ab, 4) if away_ab else 0.0
            home_avg = round(home_hit / home_ab, 4) if home_ab else 0.0

            return {
                "stadium_review": stadium_review,
                "away_hit": away_hit, "home_hit": home_hit,
                "away_hr": away_hr,   "home_hr": home_hr,
                "away_ab": away_ab,   "home_ab": home_ab,
                "away_avg": away_avg, "home_avg": home_avg,
            }
        except Exception as e:
            print("  └ 리뷰 상세 파싱 실패:", e)
            return {
                "stadium_review": "",
                "away_hit": 0, "home_hit": 0,
                "away_hr": 0, "home_hr": 0,
                "away_ab": 0, "home_ab": 0,
                "away_avg": 0.0, "home_avg": 0.0
            }

    # ----- H, HR, AB 추출 -----
    def _extract_hits(self, soup: BeautifulSoup) -> tuple[int, int]:
        away_hit = home_hit = 0
        sb3 = soup.select_one("#tblScoreboard3")
        if sb3:
            heads = [_norm(th.get_text()) for th in sb3.select("thead th")]
            if "H" in heads:
                h_idx = heads.index("H")
                rows = sb3.select("tbody tr")
                if len(rows) >= 2:
                    a_cells = rows[0].find_all("td")
                    h_cells = rows[1].find_all("td")
                    if len(a_cells) > h_idx: away_hit = _to_int(a_cells[h_idx].get_text())
                    if len(h_cells) > h_idx: home_hit = _to_int(h_cells[h_idx].get_text())
        if away_hit == 0 and home_hit == 0:
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
        table_id = "#tblHomeHitter2" if home else "#tblAwayHitter2"
        tbody = soup.select_one(f"{table_id} tbody")
        if not tbody:
            return -1
        cnt = 0
        for tr in tbody.select("tr"):
            for td in tr.find_all("td"):
                t = _norm(td.get_text())
                if not t:
                    continue
                if "홈" in t and "홈인" not in t:
                    n = _to_int(t)
                    cnt += n if n > 0 else 1
        return cnt

    def _extract_hrs_with_fallback(self, soup: BeautifulSoup) -> tuple[int, int]:
        home_hr = self._count_hr_in_hitter2(soup, home=True)
        away_hr = self._count_hr_in_hitter2(soup, home=False)
        if home_hr >= 0 and away_hr >= 0:
            return away_hr, home_hr

        away_pitchers = {_norm(tds[0].get_text())
                         for tr in soup.select("#tblAwayPitcher tbody tr")
                         if (tds := tr.find_all("td"))}
        home_pitchers = {_norm(tds[0].get_text())
                         for tr in soup.select("#tblHomePitcher tbody tr")
                         if (tds := tr.find_all("td"))}

        a_hr = h_hr = 0
        etc = soup.select_one("#tblEtc")
        if etc:
            for tr in etc.select("tr"):
                th = tr.find("th"); td = tr.find("td")
                if not th or not td or "홈런" not in _norm(th.get_text()):
                    continue
                text = _norm(td.get_text())
                for p in re.findall(r"\([^)]* ([가-힣A-Za-z0-9]+)\)", text):
                    if p in away_pitchers:
                        h_hr += 1
                    elif p in home_pitchers:
                        a_hr += 1
        if away_hr < 0: away_hr = a_hr
        if home_hr < 0: home_hr = h_hr
        return away_hr, home_hr

    def _extract_ab_with_fallback(self, soup: BeautifulSoup) -> tuple[int, int]:
        def from_hitter3(home: bool) -> int:
            t = soup.select_one("#tblHomeHitter3 tfoot tr" if home else "#tblAwayHitter3 tfoot tr")
            if not t: return -1
            tds = t.find_all("td")
            return _to_int(tds[0].get_text()) if tds else -1

        home_ab = from_hitter3(True)
        away_ab = from_hitter3(False)
        if home_ab >= 0 and away_ab >= 0:
            return away_ab, home_ab

        EXCLUDE = ("4구", "볼넷", "사구", "고의4구", "고의사구", "희비", "희플", "희번트", "희타")
        HIT_PAT = re.compile(r"(안타|좌안|중안|우안)")
        OUT_PAT = re.compile(r"(땅|뜬|삼진|파|직|수비|병살)")

        def estimate_from_hitter2(home: bool) -> int:
            table_id = "#tblHomeHitter2" if home else "#tblAwayHitter2"
            tbody = soup.select_one(f"{table_id} tbody")
            if not tbody:
                return 0
            ab = 0
            for tr in tbody.select("tr"):
                for td in tr.find_all("td"):
                    t = _norm(td.get_text())
                    if not t: continue
                    if any(x in t for x in EXCLUDE):
                        continue
                    if "홈" in t and "홈인" not in t:
                        ab += 1; continue
                    if HIT_PAT.search(t) or OUT_PAT.search(t):
                        ab += 1
            return ab

        if away_ab < 0: away_ab = estimate_from_hitter2(False)
        if home_ab < 0: home_ab = estimate_from_hitter2(True)
        return away_ab, home_ab

# ---------------------- date helpers ----------------------
def _yesterday_yyyymmdd() -> str:
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

def _next_day(yyyymmdd: str) -> str:
    d = datetime.strptime(yyyymmdd, "%Y%m%d").date() + timedelta(days=1)
    return d.strftime("%Y%m%d")

def _max_date_in_csv(out_csv: str) -> str | None:
    if not os.path.exists(out_csv):
        return None
    try:
        df = pd.read_csv(out_csv, encoding="utf-8-sig")
        if "date" not in df.columns or df.empty:
            return None
        mx = pd.to_datetime(df["date"]).max()
        if pd.isna(mx):
            return None
        return mx.strftime("%Y%m%d")
    except Exception:
        return None

def days_append_mode(out_csv: str, since_arg: str | None, until_arg: str | None) -> list[str]:
    """
    뒤에 부분만 붙이는 모드:
    - since가 주어지면 그대로 사용
    - 아니면 out_csv의 마지막 날짜+1일부터
    - 그래도 없으면 DEFAULT_SINCE
    - until이 없으면 어제
    """
    if until_arg and not re.fullmatch(r"\d{8}", until_arg):
        raise ValueError("until은 YYYYMMDD 8자리여야 합니다.")
    if since_arg and not re.fullmatch(r"\d{8}", since_arg):
        raise ValueError("since는 YYYYMMDD 8자리여야 합니다.")

    until = until_arg or _yesterday_yyyymmdd()

    if since_arg:
        since = since_arg
    else:
        last = _max_date_in_csv(out_csv)
        if last:
            since = _next_day(last)
        else:
            since = DEFAULT_SINCE

    s = datetime.strptime(since, "%Y%m%d").date()
    e = datetime.strptime(until, "%Y%m%d").date()
    if s > e:
        print(f"[INFO] 최신 CSV({out_csv})가 이미 {until}까지 포함 → 신규 수집 없음")
        return []
    out = []
    d = s
    while d <= e:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    print(f"[INFO] append-mode 수집 범위: {since}..{until}")
    return out

# ---------------------- upsert writer ----------------------
def append_csv(out_csv: str, new_rows: list[dict], since: str | None = None, until: str | None = None):
    """
    기존 CSV에 덧붙이되, since..until 구간은 먼저 제거하고 새 데이터로 교체(업서트).
    """
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
    new_df = pd.DataFrame(new_rows)
    cols = [
        "date","stadium","away_team","home_team",
        "away_score","home_score","away_result","home_result",
        "away_hit","home_hit","away_hr","home_hr",
        "away_ab","home_ab","away_avg","home_avg"
    ]
    for c in cols:
        if c not in new_df.columns:
            new_df[c] = pd.Series([None]*len(new_df))
    new_df = new_df[cols]

    if os.path.exists(out_csv):
        old = pd.read_csv(out_csv, encoding="utf-8-sig")
        for c in cols:
            if c not in old.columns:
                old[c] = pd.Series([None]*len(old))
        old = old[cols]

        # 겹치는 날짜 범위 제거 후 새 데이터 삽입
        if since and until and not old.empty:
            s_iso = _yyyymmdd_to_iso(since)
            u_iso = _yyyymmdd_to_iso(until)
            mask = (pd.to_datetime(old["date"]) < pd.to_datetime(s_iso)) | (pd.to_datetime(old["date"]) > pd.to_datetime(u_iso))
            old = old.loc[mask]
        out = pd.concat([old, new_df], ignore_index=True)
    else:
        out = new_df

    out = out.sort_values(["date","stadium","away_team","home_team"]).reset_index(drop=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] upserted {len(new_df)} rows -> {out_csv}")

# ---------------------- CLI main ----------------------
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="YYYYMMDD (생략 시 append-mode: 기존 CSV 마지막+1)")
    ap.add_argument("--until", default=None, help="YYYYMMDD (생략 시 어제)")
    ap.add_argument("--out",   default="data/kbo_latest.csv")
    ap.add_argument("--force", default="false", help="checkpoints 무시 여부(true/false)")
    args = ap.parse_args()

    force_refresh = str(args.force).lower() in ("1","true","yes")

    rng = days_append_mode(args.out, args.since, args.until)
    if not rng:
        return

    crawler = UltraPreciseKBOCrawler(headless=True)
    try:
        df = crawler.crawl_kbo_games(rng, force_refresh=force_refresh)
        rows = df.to_dict("records")
        if rows:
            # since..until 범위를 넘겨서 해당 구간을 교체(업서트)
            append_csv(args.out, rows, since=rng[0], until=rng[-1])
        else:
            print("[INFO] 신규 행 없음")
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()
