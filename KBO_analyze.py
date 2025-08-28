# KBO_analyze.py
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_cors import CORS
import pandas as pd
import numpy as np
import os
import shutil
import re

app = Flask(__name__)
CORS(app)

# =========================
# 파일 관리 / 캐시
# =========================
def keep_latest_kbo_csv(backup_dir="csv_backup"):
    os.makedirs(backup_dir, exist_ok=True)
    csv_files = [f for f in os.listdir('.') if f.startswith('kbo_games_') and f.endswith('.csv')]
    if not csv_files:
        return
    csv_files.sort(reverse=True)
    latest = csv_files[0]
    for f in csv_files[1:]:
        shutil.move(f, os.path.join(backup_dir, f))
    print(f"최신 파일만 남기고 {len(csv_files)-1}개 백업 완료: {latest}")

# 서버 시작 전에 한 번만 호출
keep_latest_kbo_csv()
kbo_data_cache = None

# =========================
# 로드 후 정규화 유틸
# =========================
TEAM_MAP = {
    "LG트윈스": "LG", "두산베어스": "두산", "키움히어로즈": "키움",
    "SSG랜더스": "SSG", "KT위즈": "KT", "한화이글스": "한화",
    "삼성라이온즈": "삼성", "KIA타이거즈": "KIA", "NC다이노스": "NC",
    "롯데자이언츠": "롯데"
}
STADIUM_MAP = {
    "잠실야구장": "잠실",
    "인천SSG랜더스필드": "문학", "인천 SSG 랜더스필드": "문학",
    "광주-기아 챔피언스 필드": "광주", "광주기아챔피언스필드": "광주",
    "대구삼성라이온즈파크": "대구",
    "대전한화생명이글스파크": "대전",
    "창원NC파크": "창원",
    "수원KT위즈파크": "수원",
    "사직야구장": "사직",
    "포항야구장": "포항",
    "울산문수야구장": "울산",
}

def _canonicalize_stadium_input(stadium: str) -> str:
    if not stadium:
        return stadium
    s = re.sub(r'\s+', '', stadium)
    return STADIUM_MAP.get(s, s)

NUM_COLS = [
    'away_hit','home_hit','away_hr','home_hr','away_ab','home_ab',
    'away_score','home_score'
]

def _post_load_normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return None

    col_map = {
        'away_hits': 'away_hit', 'home_hits': 'home_hit',
        'away_homerun': 'away_hr', 'home_homerun': 'home_hr',
        'away_atbat': 'away_ab', 'home_atbat': 'home_ab'
    }
    for old, new in col_map.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    required = [
        'date','stadium','away_team','home_team',
        'away_score','home_score','away_result','home_result',
        'away_hit','home_hit','away_hr','home_hr','away_ab','home_ab'
    ]
    for c in required:
        if c not in df.columns:
            df[c] = 0

    for col in ['stadium', 'away_team', 'home_team', 'away_result', 'home_result']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\s+', '', regex=True)

    if 'stadium' in df.columns:
        df['stadium'] = df['stadium'].replace(STADIUM_MAP)
    for col in ['away_team','home_team']:
        if col in df.columns:
            df[col] = df[col].replace(TEAM_MAP)

    for c in NUM_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
    return df

# =========================
# 데이터 로드
# =========================
def load_latest_kbo_data():
    global kbo_data_cache
    if kbo_data_cache is not None:
        return kbo_data_cache
    try:
        preferred_file = "kbo_games_ultra_precise_20250828_020405.csv"
        preferred_file = r"C:\Users\82102\Desktop\캡디\커서\.vscode\csv_backup\kbo_games_ultra_precise_20250828_003137.csv"
        if os.path.exists(preferred_file):
             kbo_data_cache = pd.read_csv(preferred_file, encoding='utf-8-sig')
             kbo_data_cache = _post_load_normalize(kbo_data_cache)
             return kbo_data_cache

        csv_files = [f for f in os.listdir('.') if f.startswith('kbo_games_') and f.endswith('.csv')]
        if not csv_files:
            return None
        csv_files.sort(reverse=True)
        latest = csv_files[0]
        kbo_data_cache = pd.read_csv(latest, encoding='utf-8-sig')
        kbo_data_cache = _post_load_normalize(kbo_data_cache)
        return kbo_data_cache
    except Exception as e:
        print("데이터 로드 오류:", e)
        return None

def clear_kbo_data_cache():
    global kbo_data_cache
    kbo_data_cache = None

# =========================
# 라우트
# =========================
@app.route("/")
@app.route("/map")
def map_view():
    selected_team = request.args.get('team', 'LG')
    return render_template("Vis_map.html", selected_team=selected_team)

@app.route("/stadium/<stadium>")
def stadium_entrance(stadium):
    # 기존: return render_template("Vis_ent.html", ...)
    selected_team = request.args.get('team', None)
    # 구장 약칭 통일
    stadium = _canonicalize_stadium_input(stadium)
    # 바로 차트 페이지로 리다이렉트
    return redirect(url_for('stadium_chart', stadium=stadium, team=selected_team))

@app.route("/stadium/<stadium>/data")
def stadium_data_overview(stadium):
    # 기존: Vis_st.html 렌더 → JSON으로 교체
    selected_team = re.sub(r'\s+', '', (request.args.get('team') or ''))
    stadium = _canonicalize_stadium_input(stadium)

    df = load_latest_kbo_data()
    if df is None:
        return jsonify({"error": "데이터가 없습니다.", "summary": None, "games": []}), 200

    mask_st = (df['stadium'] == stadium)
    team_games = df[((df['away_team'] == selected_team) | (df['home_team'] == selected_team)) & mask_st].copy()
    is_away = (team_games['away_team'] == selected_team)
    is_home = (team_games['home_team'] == selected_team)

    team_result = np.where(is_home, team_games['home_result'], team_games['away_result'])
    finished_mask = (team_result != '예정')

    res_list = (
        team_games.loc[is_away & finished_mask, 'away_result'].tolist()
      + team_games.loc[is_home  & finished_mask, 'home_result'].tolist()
    )
    wins = res_list.count('승')
    losses = res_list.count('패')
    draws = res_list.count('무')

    summary_card = {
        '경기수': int(finished_mask.sum()),
        '승': wins,
        '패': losses,
        '무': draws,
        '득점': int(team_games['away_score'][is_away].sum() + team_games['home_score'][is_home].sum()),
        '실점': int(team_games['home_score'][is_away].sum() + team_games['away_score'][is_home].sum()),
        '안타': int(team_games['away_hit'][is_away].sum() + team_games['home_hit'][is_home].sum()),
        '홈런': int(team_games['away_hr'][is_away].sum() + team_games['home_hr'][is_home].sum()),
    }

    return jsonify({
        "stadium": stadium,
        "team": selected_team,
        "summary": summary_card,
        "games": team_games.sort_values("date", ascending=False).to_dict("records")
    }), 200
    

@app.route("/api/teams")
def get_teams():
    teams = ["LG", "두산", "키움", "SSG", "KT", "한화", "삼성", "KIA", "NC", "롯데"]
    return jsonify(teams)

@app.route("/api/team-summary")
def team_summary_api():
    team = re.sub(r'\s+', '', (request.args.get('team') or ''))
    df = load_latest_kbo_data()
    if df is None or not team:
        return jsonify({"error": "데이터가 없습니다."}), 400

    team_games = df[(df['away_team'] == team) | (df['home_team'] == team)].copy()
    is_away = (team_games['away_team'] == team)
    is_home = (team_games['home_team'] == team)

    team_result = np.where(is_home, team_games['home_result'], team_games['away_result'])
    finished_mask = (team_result != '예정')

    res_list = (
        team_games.loc[is_away & finished_mask, 'away_result'].tolist()
      + team_games.loc[is_home  & finished_mask, 'home_result'].tolist()
    )
    wins = res_list.count('승')
    losses = res_list.count('패')
    draws = res_list.count('무')

    summary_card = {
        '경기수': int(finished_mask.sum()),
        '승': wins,
        '패': losses,
        '무': draws,
        '득점': int(team_games['away_score'][is_away].sum() + team_games['home_score'][is_home].sum()),
        '실점': int(team_games['home_score'][is_away].sum() + team_games['away_score'][is_home].sum()),
        '안타': int(team_games['away_hit'][is_away].sum() + team_games['home_hit'][is_home].sum()),
        '홈런': int(team_games['away_hr'][is_away].sum() + team_games['home_hr'][is_home].sum()),
    }
    recent_games = team_games.sort_values("date", ascending=False).head(10).to_dict("records")
    return jsonify({"summary": summary_card, "games": recent_games})

@app.route("/api/stadium-summary")
def stadium_summary_api():
    team = re.sub(r'\s+', '', (request.args.get('team') or ''))
    stadium_raw = request.args.get('stadium') or ''
    stadium = _canonicalize_stadium_input(stadium_raw)
    df = load_latest_kbo_data()
    if df is None or not team or not stadium:
        return jsonify({"summary": None, "games": []})

    mask_st = (df['stadium'] == stadium)
    team_games = df[((df['away_team'] == team) | (df['home_team'] == team)) & mask_st].copy()
    is_away = (team_games['away_team'] == team)
    is_home = (team_games['home_team'] == team)

    team_result = np.where(is_home, team_games['home_result'], team_games['away_result'])
    finished_mask = (team_result != '예정')

    res_list = (
       team_games.loc[is_away & finished_mask, 'away_result'].tolist()
     + team_games.loc[is_home  & finished_mask, 'home_result'].tolist()
    )
    wins = res_list.count('승')
    losses = res_list.count('패')
    draws = res_list.count('무')



    summary_card = {
        '경기수': int(finished_mask.sum()),
        '승': wins,
        '패': losses,
        '무': draws,
        '득점': int(team_games['away_score'][is_away].sum() + team_games['home_score'][is_home].sum()),
        '실점': int(team_games['home_score'][is_away].sum() + team_games['away_score'][is_home].sum()),
        '안타': int(team_games['away_hit'][is_away].sum() + team_games['home_hit'][is_home].sum()),
        '홈런': int(team_games['away_hr'][is_away].sum() + team_games['home_hr'][is_home].sum()),
    }
    recent_games = team_games.sort_values("date", ascending=False).head(10).to_dict("records")
    return jsonify({"summary": summary_card, "games": recent_games})

# =========================
# 구장 차트 페이지
# =========================
@app.route("/stadium/<stadium>/chart")
def stadium_chart(stadium):
    # 안전 초기값
    league_arr = [0.0, 0.0, 0.0]
    stadium_arr = [0.0, 0.0, 0.0]
    stadium_others_arr = [0.0, 0.0, 0.0]
    games = []
    summary_card = {'경기수':0,'승':0,'패':0,'무':0,'득점':0,'실점':0,'안타':0,'홈런':0}

    # 입력 정리
    try:
        stadium = _canonicalize_stadium_input(stadium)
    except Exception:
        stadium = re.sub(r'\s+', '', stadium)

    selected_team = re.sub(r'\s+', '', (request.args.get("team") or ""))
    df = load_latest_kbo_data()
    if df is None or not selected_team:
        return render_template(
            "KBO_analyze_de.html",
            league_data=league_arr,
            stadium_data=stadium_arr,
            stadium_others=stadium_others_arr,
            stadium=stadium,
            selected_team=selected_team,
            games=games,
            summary_card=summary_card,
            error="데이터가 없거나 팀이 지정되지 않았습니다."
        )

    # (1) 선택팀 @ 구장 경기들
    mask_st = (df['stadium'] == stadium)
    team_games = df[
        ((df['away_team'] == selected_team) | (df['home_team'] == selected_team)) & mask_st
    ].copy()

    # 홈/원정 플래그
    is_away = (team_games['away_team'] == selected_team)
    is_home = (team_games['home_team'] == selected_team)

    # '예정' 경기 제외 마스크 (우리 팀 입장에서의 결과 기준)
    team_result = np.where(is_home, team_games['home_result'], team_games['away_result']).astype(str)
    finished_mask = (team_result != '예정')

    # 유효 경기수(G): 예정 제외
    G = int(finished_mask.sum())

    # 합계(예정 제외) 계산
    team_hit = int(
        team_games.loc[is_away & finished_mask, 'away_hit'].sum()
      + team_games.loc[is_home  & finished_mask, 'home_hit'].sum()
    ) if G else 0

    team_hr = int(
        team_games.loc[is_away & finished_mask, 'away_hr'].sum()
      + team_games.loc[is_home  & finished_mask, 'home_hr'].sum()
    ) if G else 0

    team_ab = int(
        team_games.loc[is_away & finished_mask, 'away_ab'].sum()
      + team_games.loc[is_home  & finished_mask, 'home_ab'].sum()
    ) if G else 0

    team_avg = round(team_hit / team_ab, 4) if team_ab else 0.0

    # 구장에서의 우리 팀 per-game 지표 (예정 제외)
    stadium_arr = [
        round(team_hit / G, 4) if G else 0.0,   # 안타/G
        round(team_hr  / G, 4) if G else 0.0,   # HR/G
        team_avg                                 # 타율
    ]

    # (2) 리그 전체 평균(우리 팀 제외, '예정' 제외) — 필요 시 사용
    rows = []
    for _, r in df.iterrows():
        rows.append({"team": r.get('away_team',''), "H": int(r.get('away_hit',0)),
                     "HR": int(r.get('away_hr',0)), "AB": int(r.get('away_ab',0)),
                     "result": str(r.get('away_result',''))})
        rows.append({"team": r.get('home_team',''), "H": int(r.get('home_hit',0)),
                     "HR": int(r.get('home_hr',0)), "AB": int(r.get('home_ab',0)),
                     "result": str(r.get('home_result',''))})
    long_df = pd.DataFrame(rows)
    if not long_df.empty:
        others = long_df[(long_df["team"] != selected_team) & (long_df["result"] != '예정')].copy()
        apps = int(len(others))
        H_sum  = int(others["H"].sum())  if apps else 0
        HR_sum = int(others["HR"].sum()) if apps else 0
        AB_sum = int(others["AB"].sum()) if apps else 0
        league_arr = [
            round(H_sum / apps, 4) if apps else 0.0,
            round(HR_sum / apps, 4) if apps else 0.0,
            round(H_sum / AB_sum, 4) if AB_sum else 0.0
        ]

    # (3) 같은 구장에서의 다른 팀 평균 (우리 팀 제외, '예정' 제외)
    rows_st = []
    for _, r in df[mask_st].iterrows():
        rows_st.append({"team": r.get('away_team',''), "H": int(r.get('away_hit',0)),
                        "HR": int(r.get('away_hr',0)), "AB": int(r.get('away_ab',0)),
                        "result": str(r.get('away_result',''))})
        rows_st.append({"team": r.get('home_team',''), "H": int(r.get('home_hit',0)),
                        "HR": int(r.get('home_hr',0)), "AB": int(r.get('home_ab',0)),
                        "result": str(r.get('home_result',''))})
    long_st = pd.DataFrame(rows_st)
    if not long_st.empty:
        others_at_st = long_st[(long_st["team"] != selected_team) & (long_st["result"] != '예정')].copy()
        apps_st = int(len(others_at_st))
        H_sum_st  = int(others_at_st["H"].sum())  if apps_st else 0
        HR_sum_st = int(others_at_st["HR"].sum()) if apps_st else 0
        AB_sum_st = int(others_at_st["AB"].sum()) if apps_st else 0

        stadium_others_arr = [
            round(H_sum_st / apps_st, 4) if apps_st else 0.0,
            round(HR_sum_st / apps_st, 4) if apps_st else 0.0,
            round(H_sum_st / AB_sum_st, 4) if AB_sum_st else 0.0
        ]
        others_team_count = int(others_at_st["team"].nunique()) if apps_st else 0
    else:
        apps_st = 0
        others_team_count = 0

    # (4) summary + games
    games = team_games.sort_values("date", ascending=False).to_dict(orient="records") if not team_games.empty else []

    # 승/패/무(예정 제외)
    res_list = []
    res_list.extend(team_games.loc[is_home & finished_mask, 'home_result'].tolist())
    res_list.extend(team_games.loc[is_away & finished_mask, 'away_result'].tolist())
    wins = res_list.count('승'); losses = res_list.count('패'); draws = res_list.count('무')

    runs_for = int(team_games.loc[is_home & finished_mask, 'home_score'].sum()
                   + team_games.loc[is_away & finished_mask, 'away_score'].sum())
    runs_against = int(team_games.loc[is_away & finished_mask, 'home_score'].sum()
                       + team_games.loc[is_home & finished_mask, 'away_score'].sum())

    summary_card = {
        '경기수': G,
        '승': wins, '패': losses, '무': draws,
        '득점': runs_for, '실점': runs_against,
        '안타': team_hit, '홈런': team_hr
    }

    return render_template(
        "KBO_analyze_de.html",
        league_data=league_arr,
        stadium_data=stadium_arr,
        stadium_others=stadium_others_arr,
        stadium=stadium,
        selected_team=selected_team,
        games=games,
        summary_card=summary_card,
        error=None,
        # 타팀 메타(hover 계산용)
        others_apps_count=apps_st,
        others_team_count=others_team_count
    )

# =========================
# 디버그
# =========================
@app.route("/_debug/count")
def _debug_count():
    t = re.sub(r'\s+', '', (request.args.get("team") or ""))
    s = _canonicalize_stadium_input(request.args.get("stadium") or "")
    df = load_latest_kbo_data()
    if df is None:
        return jsonify({"msg":"no data"})

    # 팀 + 구장 필터
    sub = df[(((df['away_team']==t) | (df['home_team']==t)) & (df['stadium']==s))].copy()
    if sub.empty:
        return jsonify({"team": t, "stadium": s, "rows": 0})

    # 우리 팀 입장에서 '예정' 제외
    is_home = (sub['home_team'] == t)
    team_result = np.where(is_home, sub['home_result'], sub['away_result']).astype(str)
    finished_mask = (team_result != '예정')

    cnt = int(finished_mask.sum())
    return jsonify({"team": t, "stadium": s, "rows": cnt})

@app.route("/_debug/team_stadiums")
def _debug_team_stadiums():
    t = re.sub(r'\s+', '', (request.args.get("team") or ""))
    df = load_latest_kbo_data()
    if df is None:
        return jsonify({"error":"no data"})
    # 팀이 등장한 행(홈 또는 원정)
    mask = (df['away_team'] == t) | (df['home_team'] == t)
    sub = df[mask].copy()
    
    counts = sub['stadium'].value_counts().to_dict()
    sample = sub.head(20).to_dict(orient='records')
    return jsonify({"team": t, "total_rows": int(sub.shape[0]), "by_stadium":counts,"sample": sample})

@app.route("/_debug/raw_stadium_search")
def _debug_raw_stadium_search():
    q = (request.args.get("query") or "").strip()
    # which file was loaded
    pref = "kbo_games_ultra_precise_20250828_020405.csv"
    csv_files = [f for f in os.listdir('.') if f.startswith('kbo_games_') and f.endswith('.csv')]
    csv_files.sort(reverse=True)
    used = pref if os.path.exists(pref) else (csv_files[0] if csv_files else None)
    info = {"used_file": used, "query": q, "matches": []}
    if not used:
        return jsonify({"error":"no csv found", **info})
    try:
        import csv
        with open(used, 'r', encoding='utf-8-sig', errors='replace') as fh:
            reader = csv.DictReader(fh)
            i = 0
            for row in reader:
                i += 1
                # raw stadium cell
                stad = row.get('stadium') or row.get('Stadium') or ''
                # also check full concatenated row text
                full = "|".join([str(v) for v in row.values()])
                if q and (q in stad or q in full):
                    info["matches"].append({"row": i, "stadium_raw": stad, "away_team": row.get('away_team'), "home_team": row.get('home_team'), "date": row.get('date'), "full": full})
                if len(info["matches"]) >= 50:
                    break
        info["checked_rows"] = i
    except Exception as e:
        return jsonify({"error":"read failed", "exc": str(e), **info})
    return jsonify(info)
    
@app.get("/healthz")
def _health():
    return "ok", 200

if __name__ == "__main__":
    app.run(debug=True, port=5004)
