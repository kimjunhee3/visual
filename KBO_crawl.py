# -----------------------------------------
# KBO 경기 리뷰 크롤러 (리스트→리뷰, 종료 경기만 수집)
# - H/AB: Hitter3 헤더 인덱스 기반 추출, 실패 시 Hitter2 백업
# - HR: etc 테이블 파싱(투수 매칭)
# - CSV 업서트: data/kbo_latest.csv (스키마 고정)
# -----------------------------------------
import os
import re
import json
import time
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

DEFAULT_SINCE = "20250322"
LIST_URL_TMPL   = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={yyyymmdd}"
REVIEW_URL_TMPL = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={gid}&gameDate={yyyymmdd}&section=REVIEW"

SCHEMA = [
    "date","stadium","away_team","home_team",
    "away_score","home_score","away_result","home_result",
    "away_hit","home_hit","away_hr","home_hr","away_ab","home_ab",
    "away_avg","home_avg"
]

# ========= 유틸 =========
def _norm(s: Optional[str]) -> str:
    if s is None: return ""
    return re.sub(r"\s+", "", s.replace("\xa0", "")).strip()

def _norm_sp(s: Optional[str]) -> str:
    if s is None: return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def _to_int(txt: Optional[str], default: int = 0) -> int:
    if txt is None: return default
    m = re.search(r"-?\d+", txt)
    return int(m.group()) if m else default

def _pct(a: int, b: int) -> float:
    return round(a / b, 4) if b else 0.0

# ========= Hitter2(타석별) 백업 파서 =========
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
def _match_any(txt: str, patterns) -> bool:
    for p in patterns:
        if re.search(p, txt): return True
    return False
def _classify_pa_cell(txt: Optional[str]) -> Optional[str]:
    if not txt: return None
    t = txt.replace("\xa0", "").strip()
    if not t or t == "&nbsp;": return None
    if _match_any(t, EVENT_PATTERNS["HR"]):  return "HR"
    if _match_any(t, EVENT_PATTERNS["3B"]):  return "3B"
    if _match_any(t, EVENT_PATTERNS["2B"]):  return "2B"
    if _match_any(t, EVENT_PATTERNS["1B"]):  return "1B"
    if _match_any(t, EVENT_PATTERNS["BB"]):  return "BB"
    if _match_any(t, EVENT_PATTERNS["HBP"]): return "HBP"
    if _match_any(t, EVENT_PATTERNS["SF"]):  return "SF"
    if _match_any(t, EVENT_PATTERNS["SH"]):  return "SH"
    return "OUT"
def _sum_from_hitter2(soup: BeautifulSoup, which: str) -> tuple[int,int,int,int]:
    """
    Hitter2 표에서 팀 합계를 추정: (H, AB, HR, PA)
    """
    tbl_id = "#tblAwayHitter2" if which == "away" else "#tblHomeHitter2"
    table = soup.select_one(tbl_id)
    if not table:
        return 0,0,0,0

    H=AB=HR=PA=0
    for tr in table.select("tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.select("td")]
        if not tds: continue

        # 선수명 칼럼 보정
        start = 1 if (len(tds)>=2 and re.search(r"[가-힣A-Za-z]", tds[1])) else 0
        pas = tds[start+1:]

        h=ab=hr=0
        for cell in pas:
            ev = _classify_pa_cell(cell)
            if not ev: continue
            if ev == "HR":
                hr += 1; h += 1
            elif ev == "3B":
                h += 1
            elif ev == "2B":
                h += 1
            elif ev == "1B":
                h += 1
            elif ev == "OUT":
                ab += 1
            PA += 1
        AB += ab; H += h; HR += hr
    return H, AB, HR, PA

# ========= Selenium =========
def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(40)
    drv.implicitly_wait(3)
    return drv

# ========= 리스트 페이지(일자별) =========
def fetch_day_games(drv: webdriver.Chrome, yyyymmdd: str) -> List[Dict]:
    url = LIST_URL_TMPL.format(yyyymmdd=yyyymmdd)
    drv.get(url)
    WebDriverWait(drv, 12).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    time.sleep(2)

    soup = BeautifulSoup(drv.page_source, "html.parser")
    games = soup.select("li.game-cont")
    rows: List[Dict] = []

    for li in games:
        classes = li.get("class", [])
        # 종료 경기만 저장
        if not ("end" in classes or "game-end" in classes):
            continue

        gid   = li.get("g_id")   or li.get("gid") or ""
        gdt   = li.get("g_dt")   or yyyymmdd
        st    = li.get("s_nm")   or ""
        away  = li.get("away_nm") or ""
        home  = li.get("home_nm") or ""

        away_score = _to_int(_norm_sp("".join([t.get_text(strip=True) for t in li.select(".team.away .score")])))
        home_score = _to_int(_norm_sp("".join([t.get_text(strip=True) for t in li.select(".team.home .score")])))

        if gid == "" or (away_score is None) or (home_score is None):
            continue

        det = get_ultra_detailed_info(drv, gid, gdt, away, home)
        away_hit = det.get("away_hit", 0);  home_hit = det.get("home_hit", 0)
        away_hr  = det.get("away_hr", 0);   home_hr  = det.get("home_hr", 0)
        away_ab  = det.get("away_ab", 0);   home_ab  = det.get("home_ab", 0)

        row = {
            "date": f"{gdt[:4]}-{gdt[4:6]}-{gdt[6:8]}",
            "stadium": st,
            "away_team": away, "home_team": home,
            "away_score": away_score, "home_score": home_score,
            "away_result": "승" if away_score > home_score else ("패" if away_score < home_score else "무"),
            "home_result": "승" if home_score > away_score else ("패" if home_score < away_score else "무"),
            "away_hit": int(away_hit), "home_hit": int(home_hit),
            "away_hr":  int(away_hr),  "home_hr":  int(home_hr),
            "away_ab":  int(away_ab),  "home_ab":  int(home_ab),
            "away_avg": _pct(int(away_hit), int(away_ab)),
            "home_avg": _pct(int(home_hit), int(home_ab)),
        }

        # 구장 오염 방지(8자리 숫자 등)
        if re.fullmatch(r"\d{8}", _norm(row["stadium"])):
            continue

        rows.append(row)

    return rows

# ========= 리뷰 상세 =========
def ensure_review_tab_active(drv: webdriver.Chrome) -> bool:
    try:
        if drv.find_elements(By.XPATH, "//li[@section='REVIEW' and contains(@class, 'on')]"):
            return True
        tab = WebDriverWait(drv, 6).until(EC.element_to_be_clickable((By.XPATH, "//li[@section='REVIEW']//a")))
        tab.click()
        time.sleep(1.5)
        return True
    except Exception:
        return False

def _table_index_by_header(table, want: str) -> Optional[int]:
    if not table: return None
    ths = [ _norm_sp(th.get_text()) for th in table.select("thead th") ]
    for i, t in enumerate(ths):
        if t.upper() == want.upper():
            return i
    return None

def _extract_hits(soup: BeautifulSoup) -> (int, int):
    # 1) Scoreboard3 H
    away_hit = home_hit = 0
    sb3 = soup.select_one("#tblScoreboard3")
    if sb3:
        h_idx = _table_index_by_header(sb3, "H")
        if h_idx is not None:
            trs = sb3.select("tbody tr")
            if len(trs) >= 2:
                away_tds = trs[0].find_all(["td","th"])
                home_tds = trs[1].find_all(["td","th"])
                if h_idx < len(away_tds): away_hit = _to_int(away_tds[h_idx].get_text())
                if h_idx < len(home_tds): home_hit = _to_int(home_tds[h_idx].get_text())
            if away_hit or home_hit:
                return away_hit, home_hit

    # 2) Hitter3 tfoot 헤더 인덱스로 H 추출
    for which, sel in (("away", "#tblAwayHitter3"), ("home", "#tblHomeHitter3")):
        tbl = soup.select_one(sel)
        if not tbl: continue
        h_idx = _table_index_by_header(tbl, "H")
        if h_idx is None: continue
        tfoot = tbl.select_one("tfoot tr")
        if not tfoot: continue
        tds = tfoot.find_all("td")
        val = _to_int(tds[h_idx].get_text()) if h_idx < len(tds) else 0
        if which == "away": away_hit = val
        else: home_hit = val

    # 3) 최종 백업: Hitter2 이벤트로 직접 합산
    if away_hit == 0 or home_hit == 0:
        aH, _, _, _ = _sum_from_hitter2(soup, "away")
        hH, _, _, _ = _sum_from_hitter2(soup, "home")
        away_hit = away_hit or aH
        home_hit = home_hit or hH

    return away_hit, home_hit

def _extract_ab(soup: BeautifulSoup) -> (int, int):
    # Hitter3 tfoot에서 헤더 인덱스로 AB 정확 추출
    away_ab = home_ab = 0
    for which, sel in (("away", "#tblAwayHitter3"), ("home", "#tblHomeHitter3")):
        tbl = soup.select_one(sel)
        if not tbl: continue
        ab_idx = _table_index_by_header(tbl, "AB")
        if ab_idx is None: continue
        tfoot = tbl.select_one("tfoot tr")
        if not tfoot: continue
        tds = tfoot.find_all("td")
        val = _to_int(tds[ab_idx].get_text()) if ab_idx < len(tds) else 0
        if which == "away": away_ab = val
        else: home_ab = val

    # 백업: Hitter2 이벤트 합에서 AB 보완
    if away_ab == 0 or home_ab == 0:
        _, aAB, _, _ = _sum_from_hitter2(soup, "away")
        _, hAB, _, _ = _sum_from_hitter2(soup, "home")
        away_ab = away_ab or aAB
        home_ab = home_ab or hAB

    return away_ab, home_ab

def _extract_homeruns(soup: BeautifulSoup) -> (int, int):
    away_hr = home_hr = 0
    away_pitchers = { _norm_sp(td.get_text()) for td in soup.select("#tblAwayPitcher tbody tr td:first-child") }
    home_pitchers = { _norm_sp(td.get_text()) for td in soup.select("#tblHomePitcher tbody tr td:first-child") }
    etc = soup.select_one("#tblEtc")
    if etc:
        for tr in etc.select("tr"):
            th = _norm_sp(tr.find("th").get_text()) if tr.find("th") else ""
            if "홈런" not in th: continue
            td = _norm_sp(tr.find("td").get_text()) if tr.find("td") else ""
            for p in home_pitchers:
                if p and p in td: away_hr += 1
            for p in away_pitchers:
                if p and p in td: home_hr += 1
    return away_hr, home_hr

def get_ultra_detailed_info(drv: webdriver.Chrome, game_id: str, game_date: str,
                            away_team: str, home_team: str) -> Dict:
    try:
        detail_url = REVIEW_URL_TMPL.format(gid=game_id, yyyymmdd=game_date)
        drv.get(detail_url)
        time.sleep(2.5)

        ensure_review_tab_active(drv)
        time.sleep(1.2)

        soup = BeautifulSoup(drv.page_source, "html.parser")

        away_hit, home_hit = _extract_hits(soup)
        away_ab,  home_ab  = _extract_ab(soup)
        away_hr,  home_hr  = _extract_homeruns(soup)

        return {
            "away_hit": away_hit, "home_hit": home_hit,
            "away_hr": away_hr,   "home_hr": home_hr,
            "away_ab": away_ab,   "home_ab": home_ab
        }
    except Exception as e:
        print(f"[detail err] {game_id}: {e}")
        return {}

# ========= 파이프라인 =========
def crawl_dates(date_list: List[str], checkpoint_dir="checkpoints", force_refresh=False) -> pd.DataFrame:
    os.makedirs(checkpoint_dir, exist_ok=True)
    rows: List[Dict] = []
    drv = make_driver()
    try:
        for ymd in date_list:
            cp = os.path.join(checkpoint_dir, f"{ymd}.csv")
            if os.path.exists(cp) and not force_refresh:
                try:
                    rows.extend(pd.read_csv(cp, encoding="utf-8-sig").to_dict("records"))
                    print(f"{ymd} (checkpoint) 읽음")
                    continue
                except Exception:
                    pass
            try:
                day_rows = fetch_day_games(drv, ymd)
                if day_rows:
                    pd.DataFrame(day_rows, columns=SCHEMA).to_csv(cp, index=False, encoding="utf-8-sig")
                    print(f"{ymd} 저장 완료 ({len(day_rows)}경기)")
                else:
                    print(f"{ymd} 종료 경기 없음")
                rows.extend(day_rows)
            except Exception as e:
                print(f"{ymd} 수집 오류: {e}")
            time.sleep(0.8)
    finally:
        try: drv.quit()
        except: pass

    return pd.DataFrame(rows, columns=SCHEMA)

# ========= CSV 업서트 =========
def _drop_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    # 숫자 보정
    for c in ["away_score","home_score","away_hit","home_hit","away_hr","home_hr","away_ab","home_ab"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)

    # 오염행 제거
    bad = (
        (df["away_result"] == "예정") | (df["home_result"] == "예정") |
        df["stadium"].astype(str).str.fullmatch(r"\d{8}", na=False)
    )
    df = df.loc[~bad].copy()

    # 타율 재계산
    df["away_avg"] = (df["away_hit"] / df["away_ab"]).replace([float("inf")], 0).fillna(0).round(4)
    df["home_avg"] = (df["home_hit"] / df["home_ab"]).replace([float("inf")], 0).fillna(0).round(4)

    # 날짜 포맷
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df

def upsert_csv(out_path: str, new_df: pd.DataFrame, since: date, until: date) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    new_df = _drop_bad_rows(new_df)
    if new_df is None or new_df.empty:
        print("[INFO] 신규 유효 행 없음.")
        return

    if os.path.exists(out_path):
        old = pd.read_csv(out_path, encoding="utf-8-sig")
        old = _drop_bad_rows(old)

        mask_range = (
            (pd.to_datetime(old["date"], errors="coerce") >= pd.to_datetime(since)) &
            (pd.to_datetime(old["date"], errors="coerce") <= pd.to_datetime(until))
        )
        kept = old.loc[~mask_range].copy()
        out = pd.concat([kept, new_df], ignore_index=True)
    else:
        out = new_df

    out = out.sort_values(["date","stadium","home_team"]).drop_duplicates(
        subset=["date","away_team","home_team"], keep="last"
    ).reset_index(drop=True)
    out = out[SCHEMA]
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[INFO] upserted {len(new_df)} rows → {out_path}")

# ========= 날짜 계산 / last_update =========
def get_dates_from_last_update(last_update_path: Optional[str], since: Optional[str]) -> List[str]:
    base = os.path.dirname(os.path.abspath(__file__))
    if not last_update_path:
        last_update_path = os.path.join(base, "static", "cache", "last_update.json")
    else:
        if not os.path.isabs(last_update_path):
            last_update_path = os.path.join(base, last_update_path)

    if since:
        start_dt = datetime.strptime(since, "%Y%m%d").date()
    else:
        if os.path.exists(last_update_path):
            try:
                with open(last_update_path, encoding="utf-8") as f:
                    j = json.load(f)
                last_dt = datetime.strptime(j.get("ts",""), "%Y-%m-%d %H:%M:%S").date()
                start_dt = last_dt + timedelta(days=1)
            except Exception:
                start_dt = datetime.strptime(DEFAULT_SINCE, "%Y%m%d").date()
        else:
            start_dt = datetime.strptime(DEFAULT_SINCE, "%Y%m%d").date()

    today = datetime.today().date()
    dates = []
    d = start_dt
    while d <= today:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return dates

def write_last_update_ts():
    try:
        base = os.path.dirname(os.path.abspath(__file__))
        ts_path = os.path.join(base, "static", "cache", "last_update.json")
        os.makedirs(os.path.dirname(ts_path), exist_ok=True)
        with open(ts_path, "w", encoding="utf-8") as f:
            json.dump({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f, ensure_ascii=False)
        print(f"last_update.json 업데이트: {ts_path}")
    except Exception as e:
        print("last_update 업데이트 실패:", e)

# ========= 메인 =========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="", help="YYYYMMDD (없으면 last_update 기준)")
    ap.add_argument("--until", default="", help="YYYYMMDD (없으면 오늘)")
    ap.add_argument("--out",   default="data/kbo_latest.csv")
    ap.add_argument("--force_refresh", action="store_true")
    args = ap.parse_args()

    if args.since:
        since = datetime.strptime(args.since, "%Y%m%d").date()
    else:
        since = None
    if args.until:
        until = datetime.strptime(args.until, "%Y%m%d").date()
    else:
        until = datetime.today().date()

    if since is None:
        dates = get_dates_from_last_update("static/cache/last_update.json", since=None)
        if not dates:
            print("새로 크롤링할 날짜 없음.")
            return
        since = datetime.strptime(dates[0], "%Y%m%d").date()
        until = datetime.strptime(dates[-1], "%Y%m%d").date()
    else:
        dates = []
        d = since
        while d <= until:
            dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)

    print(f"[RANGE] {dates[0]} ~ {dates[-1]}  ({len(dates)} days)")
    df_new = crawl_dates(dates, checkpoint_dir="checkpoints", force_refresh=args.force_refresh)
    if not df_new.empty:
        upsert_csv(args.out, df_new, since, until)
        write_last_update_ts()
    else:
        print("[DONE] 신규 수집 0건 (업서트 생략)")

if __name__ == "__main__":
    main()


