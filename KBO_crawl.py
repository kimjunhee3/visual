#KBO_crawl.py

import time
import pandas as pd
import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import argparse

DEFAULT_SINCE = "20250322"

def _norm(s: str) -> str:
    return re.sub(r'\s+', '', s.replace('\xa0', '')).strip()

def _to_int(txt: str) -> int:
    m = re.search(r'\d+', txt or '')
    return int(m.group()) if m else 0

# ========= OPS 계산 유틸 =========

EVENT_PATTERNS = {
    "1B": [r"안(?!타율)"],
    "2B": [r"(?:^|[^\d])2(?:$|[^\d])", r"2루타"],
    "3B": [r"(?:^|[^\d])3(?:$|[^\d])", r"3루타"],
    "HR": [r"본", r"홈런"],
    "BB": [r"4구", r"고4"],
    "HBP": [r"사구"],
    "SF": [r"희플"],
    "SH": [r"희번"],
}

def text_in_patterns(txt, patterns):
    for p in patterns:
        if re.search(p, txt):
            return True
    return False

def classify_cell(txt):
    if txt is None:
        return None
    t = txt.replace("\xa0", "").strip()
    if not t or t == "&nbsp;":
        return None
    if text_in_patterns(t, EVENT_PATTERNS["HR"]):  return "HR"
    if text_in_patterns(t, EVENT_PATTERNS["3B"]):  return "3B"
    if text_in_patterns(t, EVENT_PATTERNS["2B"]):  return "2B"
    if text_in_patterns(t, EVENT_PATTERNS["1B"]):  return "1B"
    if text_in_patterns(t, EVENT_PATTERNS["BB"]):  return "BB"
    if text_in_patterns(t, EVENT_PATTERNS["HBP"]): return "HBP"
    if text_in_patterns(t, EVENT_PATTERNS["SF"]):  return "SF"
    if text_in_patterns(t, EVENT_PATTERNS["SH"]):  return "SH"
    return "OUT"

def parse_hitter_table(soup, which="away"):
    tbl_id = "#tblAwayHitter2" if which == "away" else "#tblHomeHitter2"
    table = soup.select_one(tbl_id)
    if not table:
        return pd.DataFrame()
    rows = table.select("tbody tr")
    records = []
    for tr in rows:
        tds = tr.select("td")
        if not tds:
            continue
        texts = [td.get_text(strip=True) for td in tds]
        if not texts:
            continue
        if len(texts) >= 2:
            c0, c1 = texts[0], texts[1]
            player_idx = 1 if re.search(r"[가-힣A-Za-z]", c1) else 0
        else:
            player_idx = 0
        player = texts[player_idx] if player_idx < len(texts) else ""
        if not player:
            continue
        pa_cells = texts[player_idx+1:]
        cnt = dict(H=0, _1B=0, _2B=0, _3B=0, HR=0, BB=0, HBP=0, SF=0, SH=0, OUT=0)
        for cell in pa_cells:
            ev = classify_cell(cell)
            if not ev:
                continue
            if ev == "1B":
                cnt["_1B"] += 1; cnt["H"] += 1
            elif ev == "2B":
                cnt["_2B"] += 1; cnt["H"] += 1
            elif ev == "3B":
                cnt["_3B"] += 1; cnt["H"] += 1
            elif ev == "HR":
                cnt["HR"]  += 1; cnt["H"] += 1
            else:
                cnt[ev] += 1
        singles = cnt["_1B"]; doubles = cnt["_2B"]; triples = cnt["_3B"]; hr = cnt["HR"]
        bb = cnt["BB"]; hbp = cnt["HBP"]; sf = cnt["SF"]; sh = cnt["SH"]; outs = cnt["OUT"]
        AB = cnt["H"] + outs
        TB = singles + 2*doubles + 3*triples + 4*hr
        records.append({
            "팀": which.upper(),
            "선수": player,
            "H": cnt["H"], "AB": AB
        })
    return pd.DataFrame(records)

class UltraPreciseKBOCrawler:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            print("ChromeDriver 시작 성공")
        except Exception as e:
            print(f"ChromeDriver 시작 실패: {e}")
            raise

    def crawl_kbo_games(self, date_list, checkpoint_dir="checkpoints", force_refresh=False):
        os.makedirs(checkpoint_dir, exist_ok=True)
        result = []
        for date in date_list:
            checkpoint_file = os.path.join(checkpoint_dir, f"{date}.csv")
            if (not force_refresh) and os.path.exists(checkpoint_file):
                print(f"{date} 이미 저장됨, 건너뜀")
                try:
                    df = pd.read_csv(checkpoint_file)
                    result.extend(df.to_dict(orient="records"))
                except Exception as e:
                    print(f"{date} 파일 읽기 오류: {e}")
                continue
            url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={date}"
            try:
                self.driver.get(url)
                time.sleep(4)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                games = soup.select("li.game-cont")
                day_result = []
                for game in games:
                    game_info = self.extract_ultra_precise_game_info(game, date)
                    if game_info:
                        day_result.append(game_info)
                if day_result:
                    pd.DataFrame(day_result).to_csv(checkpoint_file, index=False, encoding="utf-8-sig")
                    print(f"{date} 저장 완료 ({len(day_result)}경기)")
                result.extend(day_result)
            except Exception as e:
                print(f"{date} 오류: {e}")
                continue
            time.sleep(2)
        return pd.DataFrame(result)

    def cleanup(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

    def extract_ultra_precise_game_info(self, game, date):
        try:
            game_id = game.get("g_id")
            game_date = game.get("g_dt")
            stadium = game.get("s_nm")
            away_team = game.get("away_nm")
            home_team = game.get("home_nm")
            away_score, home_score = self.extract_precise_scores(game)
            game_classes = game.get("class", [])
            if "end" in game_classes:
                status = "종료"
            elif "play" in game_classes:
                status = "진행중"
            else:
                status = "예정"
            detailed_info = {}
            if status == "종료" and game_id:
                detailed_info = self.get_ultra_detailed_info(
                    game_id, game_date, away_team, home_team
                )
            game_info = {
                "date": f"{date[:4]}-{date[4:6]}-{date[6:8]}",
                "stadium": stadium,
                "away_team": away_team,
                "home_team": home_team,
                "away_score": away_score,
                "home_score": home_score,
                "away_result": self.get_result(away_score, home_score, True),
                "home_result": self.get_result(away_score, home_score, False),
                "away_hit": detailed_info.get("away_hit", 0),
                "home_hit": detailed_info.get("home_hit", 0),
                "away_hr": detailed_info.get("away_hr", 0),
                "home_hr": detailed_info.get("home_hr", 0),
                "away_ab": detailed_info.get("away_ab", 0),
                "home_ab": detailed_info.get("home_ab", 0)
            }
            return game_info
        except Exception as e:
            print(f"게임 정보 추출 오류: {e}")
            return None

    def extract_precise_scores(self, game):
        away_score = home_score = 0
        try:
            away_team_div = game.select(".team.away .score")
            home_team_div = game.select(".team.home .score")
            if away_team_div:
                score_text = away_team_div[0].get_text(strip=True)
                if score_text.isdigit():
                    away_score = int(score_text)
            if home_team_div:
                score_text = home_team_div[0].get_text(strip=True)
                if score_text.isdigit():
                    home_score = int(score_text)
            return away_score, home_score
        except Exception as e:
            print(f"점수 추출 오류: {e}")
            return 0, 0

    def get_ultra_detailed_info(self, game_id, game_date, away_team, home_team):
        try:
            detail_url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}&gameDate={game_date}&section=REVIEW"
            self.driver.get(detail_url)
            time.sleep(6)

            if not self.ensure_review_tab_active():
                return {}

            time.sleep(3)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            away_hit, home_hit = self.extract_precise_hits(soup)
            away_hr, home_hr = self.extract_precise_homeruns_fixed(soup, away_team, home_team)

            # 1차: Hitter3 TOTAL/tfoot에서 AB 계산
            away_ab = self.extract_team_ab_from_total(soup, which="away")
            home_ab = self.extract_team_ab_from_total(soup, which="home")

            # 2. 만약 Hitter3에서 못 구하면 Hitter2에서 선수별 합계로 보완
            if away_ab == 0:
                away_ab_df = parse_hitter_table(soup, which="away")
                away_ab = int(pd.to_numeric(away_ab_df.get("AB", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not away_ab_df.empty else 0
            if home_ab == 0:
                home_ab_df = parse_hitter_table(soup, which="home")
                home_ab = int(pd.to_numeric(home_ab_df.get("AB", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not home_ab_df.empty else 0

            print(f"AB 최종: away={away_ab}, home={home_ab}")

            return {
                "away_hit": away_hit, "home_hit": home_hit,
                "away_hr": away_hr,   "home_hr": home_hr,
                "away_ab": away_ab,   "home_ab": home_ab
            }
        except Exception as e:
            print(f"상세 정보 추출 오류: {e}")
            return {}

    def extract_team_ab_from_total(self, soup, which="away"):
        tbl_id = "#tblAwayHitter3" if which == "away" else "#tblHomeHitter3"
        table = soup.select_one(tbl_id)
        if not table:
            return 0

        tfoot_tr = table.select_one("tfoot tr")
        if tfoot_tr:
            cells = tfoot_tr.find_all("td")
            if cells:
                return _to_int(cells[0].get_text(strip=True))  # 첫 번째 셀이 타수(AB)
        return 0

    def ensure_review_tab_active(self):
        try:
            active_review = self.driver.find_elements(By.XPATH, "//li[@section='REVIEW' and contains(@class, 'on')]")
            if active_review:
                return True
            review_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//li[@section='REVIEW']//a"))
            )
            review_tab.click()
            time.sleep(3)
            return True
        except Exception as e:
            print(f"리뷰 탭 처리 실패: {e}")
            return False

    def extract_precise_hits(self, soup):
        try:
            away_hit = home_hit = 0
            scoreboard3 = soup.select("#tblScoreboard3")
            if scoreboard3:
                headers = scoreboard3[0].select("thead th")
                header_texts = [h.get_text(strip=True) for h in headers]
                if 'H' in header_texts:
                    h_index = header_texts.index('H')
                    rows = scoreboard3[0].select("tbody tr")
                    if len(rows) >= 2:
                        away_cells = rows[0].select("td")
                        if len(away_cells) > h_index:
                            hit_text = away_cells[h_index].get_text(strip=True)
                            if hit_text.isdigit():
                                away_hit = int(hit_text)
                        home_cells = rows[1].select("td")
                        if len(home_cells) > h_index:
                            hit_text = home_cells[h_index].get_text(strip=True)
                            if hit_text.isdigit():
                                home_hit = int(hit_text)
                return away_hit, home_hit
            away_hitter = soup.select("#tblAwayHitter3 tfoot tr td")
            home_hitter = soup.select("#tblHomeHitter3 tfoot tr td")
            if len(away_hitter) >= 2:
                hit_text = away_hitter[1].get_text(strip=True)
                if hit_text.isdigit():
                    away_hit = int(hit_text)
            if len(home_hitter) >= 2:
                hit_text = home_hitter[1].get_text(strip=True)
                if hit_text.isdigit():
                    home_hit = int(hit_text)
            return away_hit, home_hit
        except Exception as e:
            print(f"안타 추출 오류: {e}")
            return 0, 0

    def extract_precise_homeruns_fixed(self, soup, away_team, home_team):
        try:
            away_hr = 0
            home_hr = 0
            away_pitchers = set()
            home_pitchers = set()
            away_pitcher_table = soup.select("#tblAwayPitcher tbody tr")
            for row in away_pitcher_table:
                tds = row.select("td")
                if tds:
                    name = tds[0].get_text(strip=True)
                    if name:
                        away_pitchers.add(name)
            home_pitcher_table = soup.select("#tblHomePitcher tbody tr")
            for row in home_pitcher_table:
                tds = row.select("td")
                if tds:
                    name = tds[0].get_text(strip=True)
                    if name:
                        home_pitchers.add(name)
            etc_table = soup.select("#tblEtc")
            if etc_table:
                rows = etc_table[0].select("tr")
                for row in rows:
                    th = row.select("th")
                    td = row.select("td")
                    if th and td and "홈런" in th[0].get_text():
                        homerun_text = td[0].get_text().strip()
                        hr_items = re.findall(r'([가-힣A-Za-z0-9]+)\d+호\([^)]+ ([가-힣A-Za-zA-Z0-9]+)\)', homerun_text)
                        for batter, pitcher in hr_items:
                            if pitcher in away_pitchers:
                                home_hr += 1
                            elif pitcher in home_pitchers:
                                away_hr += 1
            return away_hr, home_hr
        except Exception as e:
            print(f"홈런 추출 오류: {e}")
            return 0, 0

    def get_result(self, away_score, home_score, is_away):
        if away_score == 0 and home_score == 0:
            return "예정"
        elif away_score == home_score:
            return "무"
        elif is_away:
            return "승" if away_score > home_score else "패"
        else:
            return "승" if home_score > away_score else "패"

    def save_results(self, results):
        if results is None or results.empty:
            print("수집된 경기가 없습니다.")
            return
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"kbo_games_ultra_precise_{timestamp}.csv"
        df = pd.DataFrame(results)

        # 숫자 보정
        for c in ["away_hit","home_hit","away_ab","home_ab"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
            else:
                df[c] = 0

        # 타율은 항상 계산해서 포함
        df["away_avg"] = df.apply(lambda r: (r["away_hit"]/r["away_ab"]) if r["away_ab"] else 0, axis=1)
        df["home_avg"] = df.apply(lambda r: (r["home_hit"]/r["home_ab"]) if r["home_ab"] else 0, axis=1)

        

        keep_cols = [
            "date","stadium","away_team","home_team",
            "away_score","home_score","away_result","home_result",
            "away_hit","home_hit","away_hr","home_hr","away_ab","home_ab",
            "away_avg","home_avg"
        ]
        df = df[[c for c in keep_cols if c in df.columns]]

        # 콘솔 출력
        for i, g in enumerate(df.to_dict(orient="records"), 1):
            print(f"[{i}] {g['date']} {g['away_team']} {g['away_score']} : {g['home_score']} {g['home_team']} ({g['away_result']}/{g['home_result']})")
            print(f"    안타: {g['away_team']} {g['away_hit']} vs {g['home_team']} {g['home_hit']}")
            print(f"    홈런: {g['away_team']} {g['away_hr']} vs {g['home_team']} {g['home_hr']}")
            print(f"    AB: {g['away_team']} {g['away_ab']} vs {g['home_team']} {g['home_ab']}")
            print(f"    타율: {g['away_team']} {g['away_avg']:.3f} vs {g['home_team']} {g['home_avg']:.3f}")

        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"결과 저장: {filename} (총 {len(df)}경기)")

        # --- 마지막 갱신 시간 업데이트 (추가, 스크립트 기준 절대경로 사용) ---
        try:
            base = os.path.dirname(os.path.abspath(__file__))
            ts_path = os.path.join(base, "static", "cache", "last_update.json")
            os.makedirs(os.path.dirname(ts_path), exist_ok=True)
            with open(ts_path, "w", encoding="utf-8") as f:
                json.dump({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f, ensure_ascii=False)
            print(f"last_update.json 업데이트: {ts_path}")
        except Exception as e:
            print("last_update 업데이트 실패:", e)

# ========= 신규 경기 날짜 추출 =========

def get_dates_to_crawl_from_last_update(last_update_path=None, since=None, force_default=False):
    """
    last_update_path: 기본값은 <repo>/static/cache/last_update.json
    since: 'YYYYMMDD'로 주면 그 날짜부터 강제 시작
    force_default: True면 last_update/CSV 무시하고 DEFAULT_SINCE부터 시작
    """
    # 절대경로 보장
    base = os.path.dirname(os.path.abspath(__file__))
    if last_update_path is None:
        last_update_path = os.path.join(base, "static", "cache", "last_update.json")
    else:
        if not os.path.isabs(last_update_path):
            last_update_path = os.path.join(base, last_update_path)

    # 1) since가 주어지면 그 날짜부터
    if since:
        try:
            start_dt = datetime.strptime(since, "%Y%m%d").date()
        except Exception:
            raise ValueError("since는 YYYYMMDD 형식이어야 합니다.")
    else:
        if force_default:
            # 2) 강제로 기본 시작일
            start_dt = datetime.strptime(DEFAULT_SINCE, "%Y%m%d").date()
        else:
            # 3) last_update.json이 있으면 그 다음 날부터
            if os.path.exists(last_update_path):
                try:
                    with open(last_update_path, encoding="utf-8") as f:
                        last_update = json.load(f)
                    # 형식: "YYYY-MM-DD HH:MM:SS"
                    last_dt = datetime.strptime(last_update.get("ts", ""), "%Y-%m-%d %H:%M:%S").date()
                    start_dt = last_dt + timedelta(days=1)
                except Exception:
                    start_dt = None
            else:
                start_dt = None

            # 4) last_update가 없거나 파싱 실패 시, data/kbo_latest.csv에서 마지막 날짜+1
            if start_dt is None:
                data_csv = os.path.join(base, "data", "kbo_latest.csv")
                if os.path.exists(data_csv):
                    try:
                        df_prev = pd.read_csv(data_csv, encoding="utf-8-sig")
                        if "date" in df_prev.columns and not df_prev.empty:
                            s = pd.to_datetime(df_prev["date"], errors="coerce")
                            last = s.max()
                            if pd.notna(last):
                                start_dt = (last + timedelta(days=1)).date()
                    except Exception:
                        start_dt = None

            # 5) 그래도 못 정하면 DEFAULT_SINCE
            if start_dt is None:
                start_dt = datetime.strptime(DEFAULT_SINCE, "%Y%m%d").date()

    # 6) start_dt ~ 오늘까지 날짜 리스트(YYYYMMDD) 생성
    today = datetime.today().date()
    dates = []
    d = start_dt
    while d <= today:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return dates

def get_backfill_dates(days_back: int) -> list[str]:
    """오늘 포함 최근 N일(=days_back) 날짜 리스트(YYYYMMDD)."""
    today = datetime.today().date()
    start = today - timedelta(days=max(0, days_back-1))
    d, out = start, []
    while d <= today:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out

def main():
    print("초정밀 KBO 크롤링 시작")

    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=7, help="오늘 포함 최근 N일을 backfill")
    args = ap.parse_args()

    # ✅ 항상 최근 N일을 재수집 (예정→종료 갱신 보장)
    dates_to_crawl = get_backfill_dates(args.days_back)
    print("백필 대상 날짜:", dates_to_crawl)

    crawler = UltraPreciseKBOCrawler()
    try:
        results = crawler.crawl_kbo_games(dates_to_crawl, force_refresh=True)
        crawler.save_results(results)
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()
