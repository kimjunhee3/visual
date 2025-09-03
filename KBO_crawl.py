# -----------------------------------------
# KBO 경기 리뷰 크롤러 (리뷰 버튼 있는 '완료 경기'만 저장, 구간 upsert)
# -----------------------------------------
import os, re, sys, time, argparse
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

DEFAULT_SINCE = "20250322"
WAIT_SEC = 12
SKIP_KEYWORDS = ("예정", "취소", "우천", "노게임")
GAMEID_RE = re.compile(r'gameId[=:\'"]?(\d{8})')

SCHEMA = [
    "date","stadium","away_team","home_team","away_score","home_score",
    "away_result","home_result","away_hit","home_hit","away_hr","home_hr",
    "away_ab","home_ab","away_avg","home_avg"
]

def yyyymmdd(d: date) -> str: return d.strftime("%Y%m%d")
def to_iso(yyyymmdd_str: str) -> str: return datetime.strptime(yyyymmdd_str, "%Y%m%d").date().isoformat()
def norm(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()
def safe_int(s: str, default: int = 0) -> int:
    try: return int(re.sub(r"[^\d]", "", s))
    except: return default

# ---------------- Selenium ----------------
def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(60)
    return drv

def goto_schedule(drv: webdriver.Chrome) -> None:
    drv.get(SCHEDULE_URL)
    WebDriverWait(drv, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    time.sleep(2)
    drv.refresh()
    WebDriverWait(drv, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    time.sleep(1)

# ----- 리뷰 버튼 있는 경기만 추출 -----
def collect_gameids_from_schedule_html(html: str, target_yyyymmdd: str) -> List[str]:
    """
    일정 테이블에서 '리뷰' 버튼이 있는 행만 골라 해당 날짜의 gameId를 수집.
    예정/취소/노게임인 행은 제외.
    """
    soup = BeautifulSoup(html, "html.parser")
    gids: List[str] = []

    for tr in soup.select("table tbody tr"):
        row_text = norm(tr.get_text(" "))
        if any(k in row_text for k in SKIP_KEYWORDS):
            continue

        links = [a for a in tr.find_all("a") if "리뷰" in (a.get_text(strip=True) or "")]
        if not links:
            continue

        for a in links:
            payload = f"{a.get('href','')} {a.get('onclick','')}"
            m = GAMEID_RE.search(payload)
            if not m:
                parent_payload = " ".join([
                    a.find_parent("td").get("onclick","") if a.find_parent("td") else "",
                    a.find_parent("tr").get("onclick","") if a.find_parent("tr") else ""
                ])
                m = GAMEID_RE.search(parent_payload)
            if not m:
                continue
            gid = m.group(1)
            if gid.startswith(target_yyyymmdd):
                gids.append(gid)

    return sorted(set(gids))

def collect_gameids_for_date(d: date) -> List[str]:
    drv = make_driver()
    try:
        goto_schedule(drv)
        html = drv.page_source
        return collect_gameids_from_schedule_html(html, yyyymmdd(d))
    finally:
        drv.quit()

# --------- 리뷰 파싱(완료 경기만 반환), 드라이버 재사용 ----------
def fetch_reviews_bulk(gids: List[str]) -> List[Dict]:
    rows: List[Dict] = []
    if not gids:
        return rows

    drv = make_driver()
    try:
        for gid in gids:
            try:
                drv.get(REVIEW_URL.format(gid=gid))
                WebDriverWait(drv, WAIT_SEC).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
                time.sleep(1)

                soup = BeautifulSoup(drv.page_source, "html.parser")
                game_date = to_iso(gid)

                stadium_el = soup.select_one("#txtStadium, #lblStadium, .stadium")
                stadium = norm(stadium_el.get_text(strip=True)) if stadium_el else ""

                away_team = home_team = ""
                away_runs = home_runs = None

                sb3 = soup.select_one("#tblScoreboard3")
                if sb3:
                    ths = sb3.select("thead th")
                    r_idx = None
                    for i, th in enumerate(ths):
                        if norm(th.get_text()) in ("R", "득점", "점수"):
                            r_idx = i; break
                    rows_ = []
                    for tr in sb3.select("tbody tr"):
                        tds = tr.find_all(["th","td"])
                        rows_.append([norm(td.get_text()) for td in tds])
                    if r_idx is not None and len(rows_) >= 2:
                        away_team = rows_[0][0]; home_team = rows_[1][0]
                        away_runs = safe_int(rows_[0][r_idx], 0)
                        home_runs = safe_int(rows_[1][r_idx], 0)

                if away_runs is None or home_runs is None:
                    # 완료되지 않은 경기(리뷰 열려도 경우에 따라 점수 미노출일 수 있음)
                    continue

                def sum_h_hr_ab(sel: str):
                    tbl = soup.select_one(sel)
                    if not tbl: return 0,0,0
                    heads = [norm(th.get_text()).upper() for th in tbl.select("thead th")]
                    idx = {h:i for i,h in enumerate(heads)}
                    h_idx  = idx.get("H") or idx.get("안타") or idx.get("HIT") or idx.get("HITS")
                    hr_idx = idx.get("HR") or idx.get("홈런") or idx.get("HOMERUN")
                    ab_idx = idx.get("AB") or idx.get("타수")
                    H=HR=AB=0
                    for tr in tbl.select("tbody tr"):
                        cells = [norm(td.get_text()) for td in tr.find_all("td")]
                        if not cells: continue
                        if h_idx  is not None and h_idx  < len(cells): H  += safe_int(cells[h_idx])
                        if hr_idx is not None and hr_idx < len(cells): HR += safe_int(cells[hr_idx])
                        if ab_idx is not None and ab_idx < len(cells): AB += safe_int(cells[ab_idx])
                    return H,HR,AB

                home_H=home_HR=home_AB=0
                away_H=away_HR=away_AB=0
                for sel in ("#tblHomeHitter2", "#tblHomeHitter", "#tblHitterHome"):
                    h,hr,ab = sum_h_hr_ab(sel)
                    if h or hr or ab: home_H,home_HR,home_AB = h,hr,ab; break
                for sel in ("#tblAwayHitter2", "#tblAwayHitter", "#tblHitterAway"):
                    h,hr,ab = sum_h_hr_ab(sel)
                    if h or hr or ab: away_H,away_HR,away_AB = h,hr,ab; break

                if away_runs > home_runs:
                    away_res, home_res = "승", "패"
                elif away_runs < home_runs:
                    away_res, home_res = "패", "승"
                else:
                    away_res = home_res = "무"

                def div(a,b): return round(a/b,4) if b else 0.0

                row = {
                    "date": game_date,
                    "stadium": stadium,
                    "away_team": away_team, "home_team": home_team,
                    "away_score": away_runs, "home_score": home_runs,
                    "away_result": away_res, "home_result": home_res,
                    "away_hit": away_H, "home_hit": home_H,
                    "away_hr": away_HR, "home_hr": home_HR,
                    "away_ab": away_AB, "home_ab": home_AB,
                    "away_avg": div(away_H, away_AB),
                    "home_avg": div(home_H, home_AB),
                }

                # 최소 유효성
                if not(row["away_team"] and row["home_team"] and row["stadium"]):
                    continue

                rows.append(row)

            except Exception as e:
                print(f"[ERR] review gid={gid}: {e}", file=sys.stderr)
                continue
        return rows
    finally:
        drv.quit()

# -------------- CSV upsert ----------------
def _valid_row_dict(r: Dict) -> bool:
    try:
        if str(r.get("away_result","")) == "예정" or str(r.get("home_result","")) == "예정":
            return False
        int(r.get("away_score", 0)); int(r.get("home_score", 0))
        # stadium이 8자리 숫자(잘못 파싱된 날짜)인 경우 제거
        if re.fullmatch(r"\d{8}", str(r.get("stadium","")).strip()):
            return False
        return True
    except Exception:
        return False

def upsert_range(out_csv: str, rows: List[Dict], since: date, until: date) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)

    clean_new = [r for r in rows if _valid_row_dict(r)]
    if not clean_new:
        print("[INFO] no valid rows to upsert.")
        return

    new_df = pd.DataFrame(clean_new, columns=SCHEMA)
    # 중복 방지 (동일 일자 동일 매치)
    new_df = new_df.drop_duplicates(subset=["date","away_team","home_team"], keep="last")

    if os.path.exists(out_csv):
        old = pd.read_csv(out_csv, encoding="utf-8-sig")

        # 기존 파일에서도 불량행 제거
        def _drop_bad(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            for c in ("away_score","home_score"):
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce")
            mask_bad = (
                df["away_score"].isna() | df["home_score"].isna() |
                (df.get("away_result","") == "예정") | (df.get("home_result","") == "예정") |
                df.get("stadium","").astype(str).str.fullmatch(r"\d{8}", na=False)
            )
            return df.loc[~mask_bad].copy()

        old = _drop_bad(old)

        # 지정 구간만 제거 후 신규 삽입
        mask_range = (
            (pd.to_datetime(old["date"], errors="coerce") >= pd.to_datetime(since)) &
            (pd.to_datetime(old["date"], errors="coerce") <= pd.to_datetime(until))
        )
        kept = old.loc[~mask_range].copy()
        out = pd.concat([kept, new_df], ignore_index=True)
    else:
        out = new_df

    out = out.sort_values(["date","stadium","home_team"]).reset_index(drop=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] upserted {len(new_df)} rows into {out_csv}")

# ------------------- main -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=DEFAULT_SINCE)
    ap.add_argument("--until", default=None)
    ap.add_argument("--out",   default="data/kbo_latest.csv")
    args = ap.parse_args()

    since = datetime.strptime(args.since, "%Y%m%d").date()
    until = datetime.strptime(args.until, "%Y%m%d").date() if args.until else date.today()

    all_rows: List[Dict] = []
    d = since
    while d <= until:
        print(f"[DAY] {d.isoformat()} 수집 시도…")
        gids = collect_gameids_for_date(d)
        if not gids:
            print(f"[DAY] {d.isoformat()} 리뷰 버튼 달린 경기 없음.")
        day_rows = fetch_reviews_bulk(gids)
        print(f"[DAY] {d.isoformat()} 완료경기 {len(day_rows)}건")
        all_rows.extend(day_rows)
        time.sleep(0.8)
        d += timedelta(days=1)

    print(f"[DONE] collected rows: {len(all_rows)}")
    if all_rows:
        upsert_range(args.out, all_rows, since, until)

if __name__ == "__main__":
    main()

