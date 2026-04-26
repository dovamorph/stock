"""
StockPilot KR — screener.py
- 종목 리스트: FinanceDataReader (시가총액 상위 20)
- 재무 데이터: yfinance (PER/PBR/ROE/배당, IP 차단 없음)
- 가격 데이터: FinanceDataReader
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import pandas as pd
    import requests
    import yfinance as yf
    import FinanceDataReader as fdr
except ImportError:
    print("pip install -r requirements.txt 먼저 실행하세요")
    exit(1)

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "")
TOP_N           = 20
FILTER_ROE      = 15.0
FILTER_PER      = 15.0
FILTER_PBR      = 1.5
FILTER_DIV      = 3.0
MOMENTUM_THRESH = 20.0

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

def fmt(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"

def safe_float(v, default=0.0) -> float:
    try:
        val = float(v or 0)
        return val if val == val else default  # NaN 체크
    except:
        return default

# ── 1단계: 시가총액 상위 20 ──────────────────────────────────────
def fetch_top20() -> list[dict]:
    print(f"\n[1/4] 시가총액 상위 {TOP_N} 조회 중...")
    try:
        kospi  = fdr.StockListing('KOSPI');  kospi['market']  = 'KOSPI'
        kosdaq = fdr.StockListing('KOSDAQ'); kosdaq['market'] = 'KOSDAQ'
        df = pd.concat([kospi, kosdaq], ignore_index=True)

        # 컬럼 정규화
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if cl in ('symbol','code','ticker'): col_map[c] = 'Code'
            elif cl == 'name':                   col_map[c] = 'Name'
            elif 'marcap' in cl:                 col_map[c] = 'Marcap'
        df = df.rename(columns=col_map)

        if 'Marcap' not in df.columns:
            num = df.select_dtypes(include='number').columns
            if len(num): df['Marcap'] = df[num[0]]

        df['Marcap'] = pd.to_numeric(df['Marcap'], errors='coerce').fillna(0)
        df = df[df['Marcap'] > 0].sort_values('Marcap', ascending=False).head(TOP_N).reset_index(drop=True)

        result = []
        for i, row in df.iterrows():
            ticker = str(row.get('Code', row.iloc[0])).zfill(6)
            market = str(row.get('market', 'KOSPI'))
            result.append({
                "rank":   i + 1,
                "ticker": ticker,
                "name":   str(row.get('Name', ticker)),
                "market": market,
                "tvol":   int(row.get('Marcap', 0)) // 100000000,
                "suffix": ".KS" if market == "KOSPI" else ".KQ",
            })

        print(f"  {len(result)}개 선정:")
        for r in result[:5]:
            print(f"    {r['rank']}. {r['name']} ({r['ticker']}) — 시총 {r['tvol']:,}억")
        return result
    except Exception as e:
        print(f"  오류: {e}"); traceback.print_exc(); return []

# ── 2단계: yfinance 재무 데이터 ───────────────────────────────────
def fetch_yfinance(ticker: str, suffix: str) -> dict:
    """Yahoo Finance로 PER/PBR/ROE/배당 가져오기"""
    result = {"per": 0.0, "pbr": 0.0, "roe": 0.0, "div": 0.0}
    try:
        yf_ticker = f"{ticker}{suffix}"
        info = yf.Ticker(yf_ticker).info

        per = safe_float(info.get("trailingPE") or info.get("forwardPE") or 0)
        pbr = safe_float(info.get("priceToBook", 0))
        roe_raw = info.get("returnOnEquity", 0)
        roe = safe_float(roe_raw * 100 if roe_raw else 0)
        div_raw = info.get("dividendYield", 0)
        div = safe_float(div_raw * 100 if div_raw else 0)

        result = {"per": per, "pbr": pbr, "roe": roe, "div": div}
    except Exception as e:
        print(f"    yfinance 오류: {e}")
    return result

# ── 3단계: 가격/등락률 ────────────────────────────────────────────
def fetch_price(ticker: str, start: str, end: str) -> dict:
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]['Close'])
            p1 = float(df.iloc[-1]['Close'])
            ch20 = round((p1 - p0) / p0 * 100, 1) if p0 > 0 else 0.0
            if 'Volume' in df.columns:
                v = df['Volume'].astype(float)
                avg5 = v.iloc[-5:].mean()
                avgA = v.mean()
                vol_trend = round((avg5 - avgA) / avgA * 100, 1) if avgA > 0 else 0.0
            else:
                vol_trend = 0.0
            return {"ch20": ch20, "vol_trend": vol_trend}
    except:
        pass
    return {"ch20": 0.0, "vol_trend": 0.0}

# ── 점수 / 등급 / 필터 ────────────────────────────────────────────
def calc_score(d: dict) -> int:
    s = 0
    per=d.get("per",0) or 0; pbr=d.get("pbr",0) or 0
    roe=d.get("roe",0) or 0; div=d.get("div",0) or 0
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
    s += 5
    if div>7: s+=10
    elif div>5: s+=7
    elif div>3: s+=5
    elif div>0: s+=2
    s += 12
    if roe>=20: s+=5
    elif roe>=15: s+=4
    elif roe>=10: s+=2
    return min(int(s), 100)

def get_grade(score: int) -> str:
    return "A" if score>80 else "B" if score>=70 else "C" if score>=50 else "D"

def apply_filters(d: dict) -> dict:
    return {
        "vol_ok":   (d.get("vol_trend") or 0) > -10,
        "roe_ok":   (d.get("roe") or 0) >= FILTER_ROE,
        "per_ok":   0 < (d.get("per") or 0) <= FILTER_PER,
        "pbr_ok":   0 < (d.get("pbr") or 0) <= FILTER_PBR,
        "div_ok":   (d.get("div") or 0) >= FILTER_DIV,
        "momentum": (d.get("ch20") or 0) >= MOMENTUM_THRESH,
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
                f"20일 {r.get('ch20',0):+.1f}%  ·  배당 {r.get('div',0):.1f}%  ·  시총 {r.get('tvol',0):,}억"
            ),
            "inline": False
        })
    if not fields:
        fields.append({
            "name": "⚠️ 추천 종목 없음",
            "value": "오늘 필터(ROE≥15% · PER≤15배 · PBR≤1.5배)를 통과한 종목이 없습니다.",
            "inline": False
        })
    try:
        res = requests.post(DISCORD_WEBHOOK, json={"embeds":[{
            "title": f"📊 StockPilot 스크리닝 — {dt}",
            "description": f"시가총액 상위{TOP_N} → 추천 **{len(recommended)}종목**",
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
    date  = last_trading_day()
    start = n_days_ago(date, 30)
    print(f"  기준일: {date}  |  조회범위: {start} ~ {date}")

    tickers = fetch_top20()
    if not tickers:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"종목 데이터 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    print(f"\n[2/4] {len(tickers)}종목 재무 + 가격 조회 중...\n")
    results = []
    for t in tickers:
        tk     = t["ticker"]
        suffix = t.get("suffix", ".KS")
        print(f"  [{t['rank']:2d}] {t['name']:12s} ({tk}{suffix})", end=" ... ", flush=True)
        try:
            fund  = fetch_yfinance(tk, suffix)
            price = fetch_price(tk, start, date)
            data  = {**t, **fund, **price}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (filters["vol_ok"] and filters["roe_ok"] and
                   filters["per_ok"] and filters["pbr_ok"] and score >= 50)
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(f"{grade}등급 {score}점  ROE:{fund.get('roe',0):.1f}%  PER:{fund.get('per',0):.1f}  PBR:{fund.get('pbr',0):.2f}  20일:{price['ch20']:+.1f}%")
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.5)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*60}")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(f"  ★ {r['name']} [{r['grade']}등급 {r['score']}점]"
              f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  PBR {r.get('pbr',0):.2f}")

    json.dump({
        "date":        date,
        "generated_at": datetime.now().isoformat(),
        "total":       len(results),
        "results":     results,
        "recommended": recommended,
    }, open("results.json","w",encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results, date, recommended)
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
