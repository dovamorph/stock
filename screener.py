"""
StockPilot KR — screener.py
KRX 로그인 기반 실제 데이터 수집
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import pandas as pd
    from pykrx import stock
    import requests
except ImportError:
    print("pip install -r requirements.txt 먼저 실행하세요")
    exit(1)

# ── 설정 ──────────────────────────────────────────────────────────
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "")
KRX_ID          = os.environ.get("KRX_ID", "")
KRX_PW          = os.environ.get("KRX_PW", "")
TOP_N           = 20
FILTER_ROE      = 15.0
FILTER_PER      = 15.0
FILTER_PBR      = 1.5
FILTER_DIV      = 3.0
MOMENTUM_THRESH = 20.0

# ── KRX 로그인 ────────────────────────────────────────────────────
def krx_login():
    if not KRX_ID or not KRX_PW:
        print("⚠️  KRX_ID / KRX_PW 환경변수 미설정")
        return False
    try:
        stock.set_proxy(id=KRX_ID, pw=KRX_PW)
        print(f"  ✅ KRX 로그인 성공 ({KRX_ID})")
        return True
    except Exception as e:
        print(f"  ❌ KRX 로그인 실패: {e}")
        return False

# ── 날짜 유틸 ─────────────────────────────────────────────────────
def last_trading_day() -> str:
    now = datetime.now()
    for i in range(1, 10):
        d = now - timedelta(days=i)
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    return (now - timedelta(days=1)).strftime("%Y%m%d")

def n_days_ago(base: str, n: int = 30) -> str:
    return (datetime.strptime(base, "%Y%m%d") - timedelta(days=n)).strftime("%Y%m%d")

# ── 1단계: 거래대금 상위 20 ──────────────────────────────────────
def fetch_top20(date: str) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중... (기준일 {date})")
    rows = []
    for market in ["KOSPI", "KOSDAQ"]:
        try:
            df = stock.get_market_trading_value_by_ticker(date, market=market)
            if df is not None and not df.empty:
                df["market"] = market
                rows.append(df.reset_index())
                print(f"  {market}: {len(df)}종목")
        except Exception as e:
            print(f"  {market} 오류: {e}")
        time.sleep(0.5)

    if not rows:
        return []

    combined = pd.concat(rows, ignore_index=True)

    # 거래대금 컬럼 찾기
    tvol_col = next((c for c in combined.columns if '거래대금' in c), None)
    if not tvol_col:
        tvol_col = combined.select_dtypes(include='number').columns[0]

    ticker_col = combined.columns[0]
    combined[tvol_col] = pd.to_numeric(combined[tvol_col], errors='coerce').fillna(0)
    combined = combined[combined[tvol_col] > 0]
    combined = combined.sort_values(tvol_col, ascending=False).head(TOP_N).reset_index(drop=True)

    result = []
    for i, row in combined.iterrows():
        ticker = str(row[ticker_col]).zfill(6)
        try:
            name = stock.get_market_ticker_name(ticker)
        except:
            name = ticker
        result.append({
            "rank":   i + 1,
            "ticker": ticker,
            "name":   name,
            "market": str(row.get("market", "KOSPI")),
            "tvol":   int(row[tvol_col]) // 100000000,
        })

    print(f"  상위 {len(result)}종목 선정:")
    for r in result[:5]:
        print(f"    {r['rank']}. {r['name']} — {r['tvol']:,}억")
    return result

# ── 2단계: 종목별 재무 + 가격 데이터 ─────────────────────────────
def fetch_fundamentals(ticker: str, date: str) -> dict:
    try:
        df = stock.get_market_fundamental(date, date, ticker)
        if df is not None and not df.empty:
            r = df.iloc[0]
            per = float(r.get("PER") or 0)
            pbr = float(r.get("PBR") or 0)
            eps = float(r.get("EPS") or 0)
            bps = float(r.get("BPS") or 0)
            div = float(r.get("DIV") or 0)
            roe = round(eps / bps * 100, 1) if bps > 0 else 0.0
            return {"per": per, "pbr": pbr, "roe": roe, "div": div}
    except Exception as e:
        print(f"    재무 오류: {e}")
    return {"per": 0.0, "pbr": 0.0, "roe": 0.0, "div": 0.0}

def fetch_foreign(ticker: str, start: str, end: str) -> int:
    try:
        df = stock.get_market_trading_value_by_investor(start, end, ticker)
        if df is not None and not df.empty:
            for lbl in ["외국인합계", "외국인 합계", "외국인"]:
                if lbl in df.index:
                    return int(float(df.loc[lbl, "순매수"]) / 1e8)
    except Exception as e:
        print(f"    외국인 오류: {e}")
    return 0

def fetch_price_data(ticker: str, start: str, end: str) -> dict:
    try:
        df = stock.get_market_ohlcv(start, end, ticker)
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]["종가"])
            p1 = float(df.iloc[-1]["종가"])
            ch20 = round((p1 - p0) / p0 * 100, 1) if p0 > 0 else 0.0

            vols = df["거래대금"].astype(float) if "거래대금" in df.columns else df["거래량"].astype(float)
            avg5 = vols.iloc[-5:].mean()
            avgA = vols.mean()
            vol_trend = round((avg5 - avgA) / avgA * 100, 1) if avgA > 0 else 0.0
            return {"ch20": ch20, "vol_trend": vol_trend}
    except Exception as e:
        print(f"    가격 오류: {e}")
    return {"ch20": 0.0, "vol_trend": 0.0}

# ── 점수 / 등급 / 필터 ────────────────────────────────────────────
def calc_score(d: dict) -> int:
    s = 0
    per=d.get('per',0) or 0; pbr=d.get('pbr',0) or 0
    roe=d.get('roe',0) or 0; div=d.get('div',0) or 0

    if 0<per<5: s+=5
    elif per<10: s+=4
    elif per<15: s+=3
    elif per<20: s+=1

    if pbr<0.3: s+=5
    elif pbr<0.6: s+=4
    elif pbr<1.0: s+=3
    elif pbr<1.5: s+=2

    if roe>=15: s+=5
    elif roe>=8: s+=3
    elif roe>0: s+=1

    s += 5  # 단독상장 기본값

    if div>7: s+=10
    elif div>5: s+=7
    elif div>3: s+=5
    elif div>0: s+=2

    s += 12  # 성장/경영 기본값

    if roe>=20: s+=5
    elif roe>=15: s+=4
    elif roe>=10: s+=2

    return min(int(s), 100)

def get_grade(score: int) -> str:
    return "A" if score>80 else "B" if score>=70 else "C" if score>=50 else "D"

def apply_filters(d: dict) -> dict:
    return {
        "vol_ok":     (d.get("vol_trend") or 0) > -10,
        "foreign_ok": (d.get("foreign_net") or 0) > 0,
        "roe_ok":     (d.get("roe") or 0) >= FILTER_ROE,
        "per_ok":     0 < (d.get("per") or 0) <= FILTER_PER,
        "pbr_ok":     0 < (d.get("pbr") or 0) <= FILTER_PBR,
        "div_ok":     (d.get("div") or 0) >= FILTER_DIV,
        "momentum":   (d.get("ch20") or 0) >= MOMENTUM_THRESH,
    }

# ── Discord 전송 ──────────────────────────────────────────────────
def send_discord(results: list, date: str, recommended: list):
    if not DISCORD_WEBHOOK:
        print("  ℹ️  DISCORD_WEBHOOK 미설정"); return
    dt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ge = {"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    fields = []
    for r in recommended[:6]:
        g = r.get("grade","D"); f = r.get("filters",{})
        flags = ("  🔥급등" if f.get("momentum") else "") + ("  💰배당" if f.get("div_ok") else "")
        fields.append({
            "name": f"{ge.get(g,'⚪')} {r['name']} ({r['market']})  {g}등급 {r['score']}점{flags}",
            "value": (
                f"```ROE {r.get('roe',0):.1f}%  |  PER {r.get('per',0):.1f}배  |  PBR {r.get('pbr',0):.2f}```"
                f"외국인 {r.get('foreign_net',0):+,}억  ·  20일 {r.get('ch20',0):+.1f}%  ·  배당 {r.get('div',0):.1f}%"
            ),
            "inline": False
        })
    if not fields:
        fields.append({
            "name": "⚠️ 추천 종목 없음",
            "value": "오늘 모든 필터를 통과한 종목이 없습니다.",
            "inline": False
        })
    try:
        res = requests.post(DISCORD_WEBHOOK, json={"embeds":[{
            "title": f"📊 StockPilot 스크리닝 — {dt}",
            "description": f"거래대금 상위{TOP_N} → 추천 **{len(recommended)}종목**\nROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PBR≤{FILTER_PBR}배 · 외국인 순매수",
            "color": 0x00d97e if recommended else 0xff4560,
            "fields": fields,
            "footer": {"text":"⚠️ 투자 손실 책임은 본인에게 있습니다."},
            "timestamp": datetime.utcnow().isoformat()+"Z"
        }]}, timeout=10)
        print(f"  {'✅ Discord 전송 완료' if res.status_code==204 else f'⚠️ {res.status_code}'}")
    except Exception as e:
        print(f"  ❌ Discord 실패: {e}")

# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print("╔══════════════════════════════════╗")
    print("║   StockPilot KR  자동 스크리닝   ║")
    print("╚══════════════════════════════════╝")

    # KRX 로그인
    logged_in = krx_login()
    if not logged_in:
        print("❌ KRX 로그인 실패. GitHub Secret KRX_ID/KRX_PW 확인하세요.")
        json.dump({"date":"","generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"KRX 로그인 실패"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    date  = last_trading_day()
    start = n_days_ago(date, 30)
    print(f"  기준일: {date}  |  조회범위: {start} ~ {date}\n")

    tickers = fetch_top20(date)
    if not tickers:
        print("❌ 거래대금 데이터 없음.")
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"데이터 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    print(f"\n[2/4] {len(tickers)}종목 세부 데이터 조회 중...\n")
    results = []
    for t in tickers:
        tk = t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:12s} ({tk})", end=" ... ", flush=True)
        try:
            fund  = fetch_fundamentals(tk, date)
            price = fetch_price_data(tk, start, date)
            fgn   = fetch_foreign(tk, start, date)

            data = {**t, **fund, **price, "foreign_net": fgn}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (filters["vol_ok"] and filters["foreign_ok"] and
                   filters["roe_ok"] and filters["per_ok"] and
                   filters["pbr_ok"] and score >= 50)
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(f"{grade}등급 {score}점  ROE:{fund.get('roe',0):.1f}%  PER:{fund.get('per',0):.1f}  PBR:{fund.get('pbr',0):.2f}")
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.5)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*55}")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(f"  ★ {r['name']} [{r['grade']}등급 {r['score']}점]"
              f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  배당 {r.get('div',0):.1f}%")

    json.dump({"date":date,"generated_at":datetime.now().isoformat(),
               "total":len(results),"results":results,"recommended":recommended},
              open("results.json","w",encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results, date, recommended)
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
