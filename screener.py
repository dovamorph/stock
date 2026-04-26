"""
StockPilot KR — screener.py
KRX 직접 HTTP 요청 방식 (pykrx 미사용)
- KRX 공개 API로 거래대금, PER, PBR 직접 수집
- FinanceDataReader로 가격/등락률 수집
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import pandas as pd
    import requests
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

KRX_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
KRX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.krx.co.kr/",
    "Content-Type": "application/x-www-form-urlencoded",
}

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

def to_float(v) -> float:
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except:
        return 0.0

def to_int(v) -> int:
    try:
        return int(str(v).replace(",", "").strip() or 0)
    except:
        return 0

# ── 1단계: KRX 거래대금 상위 20 ──────────────────────────────────
def fetch_top20(date: str) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중... (기준일 {date})")
    all_stocks = []

    for mkt_id, mkt_name in [("STK", "KOSPI"), ("KSQ", "KOSDAQ")]:
        try:
            payload = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
                "mktId": mkt_id,
                "trdDd": date,
                "share": "1",
                "money": "1",
                "csvxls_isNo": "false",
            }
            res = requests.post(KRX_URL, data=payload, headers=KRX_HEADERS, timeout=15)
            data = res.json()
            items = data.get("output", [])
            print(f"  {mkt_name}: {len(items)}종목")
            for item in items:
                ticker = str(item.get("ISU_SRT_CD", "")).zfill(6)
                name   = item.get("ISU_ABBRV", ticker)
                tvol   = to_int(item.get("ACC_TRDVAL", 0))
                close  = to_float(item.get("TDD_CLSPRC", 0))
                if ticker and tvol > 0:
                    all_stocks.append({
                        "ticker": ticker,
                        "name":   name,
                        "market": mkt_name,
                        "tvol":   tvol // 100000000,
                        "close":  close,
                    })
        except Exception as e:
            print(f"  {mkt_name} 오류: {e}")
        time.sleep(0.5)

    if not all_stocks:
        print("  ❌ 거래대금 데이터 없음")
        return []

    # 거래대금 상위 TOP_N
    df = pd.DataFrame(all_stocks)
    df = df.sort_values("tvol", ascending=False).head(TOP_N).reset_index(drop=True)
    result = []
    for i, row in df.iterrows():
        result.append({
            "rank":   i + 1,
            "ticker": row["ticker"],
            "name":   row["name"],
            "market": row["market"],
            "tvol":   int(row["tvol"]),
            "close":  row["close"],
        })

    print(f"  상위 {len(result)}종목:")
    for r in result[:5]:
        print(f"    {r['rank']}. {r['name']} ({r['ticker']}) — {r['tvol']:,}억")
    return result

# ── 2단계: KRX PER/PBR/배당 전종목 한번에 ───────────────────────
def fetch_all_fundamentals(date: str) -> dict:
    print(f"\n[2/4] PER/PBR/배당 전종목 조회 중...")
    result = {}
    for mkt_id, mkt_name in [("STK", "KOSPI"), ("KSQ", "KOSDAQ")]:
        try:
            payload = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT03501",
                "mktId": mkt_id,
                "trdDd": date,
                "share": "1",
                "money": "1",
                "csvxls_isNo": "false",
            }
            res = requests.post(KRX_URL, data=payload, headers=KRX_HEADERS, timeout=15)
            data = res.json()
            items = data.get("output", [])
            print(f"  {mkt_name}: {len(items)}종목 재무 데이터")
            for item in items:
                ticker = str(item.get("ISU_SRT_CD", "")).zfill(6)
                if not ticker:
                    continue
                per = to_float(item.get("PER", 0))
                pbr = to_float(item.get("PBR", 0))
                eps = to_float(item.get("EPS", 0))
                bps = to_float(item.get("BPS", 0))
                div = to_float(item.get("DVD_YLD", 0))
                roe = round(eps / bps * 100, 1) if bps > 0 else 0.0
                result[ticker] = {"per": per, "pbr": pbr, "roe": roe, "div": div}
        except Exception as e:
            print(f"  {mkt_name} 재무 오류: {e}")
        time.sleep(0.5)

    print(f"  총 {len(result)}종목 재무 데이터 수집")
    return result

# ── 3단계: 가격/등락률 (FinanceDataReader) ────────────────────────
def fetch_price(ticker: str, start: str, end: str) -> dict:
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]["Close"])
            p1 = float(df.iloc[-1]["Close"])
            ch20 = round((p1 - p0) / p0 * 100, 1) if p0 > 0 else 0.0
            if "Volume" in df.columns:
                v    = df["Volume"].astype(float)
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
                f"20일 {r.get('ch20',0):+.1f}%  ·  배당 {r.get('div',0):.1f}%  ·  거래대금 {r.get('tvol',0):,}억"
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
            "description": f"거래대금 상위{TOP_N} → 추천 **{len(recommended)}종목**",
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

    # 1) 거래대금 상위 20
    tickers = fetch_top20(date)
    if not tickers:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"거래대금 데이터 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    # 2) 전종목 재무 한번에
    fund_map = fetch_all_fundamentals(date)

    # 3) 종목별 가격 + 병합
    print(f"\n[3/4] {len(tickers)}종목 가격 데이터 조회 중...\n")
    results = []
    for t in tickers:
        tk = t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:12s} ({tk})", end=" ... ", flush=True)
        try:
            fund  = fund_map.get(tk, {"per":0.0,"pbr":0.0,"roe":0.0,"div":0.0})
            price = fetch_price(tk, start, date)
            data  = {**t, **fund, **price}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (filters["vol_ok"] and filters["roe_ok"] and
                   filters["per_ok"] and filters["pbr_ok"] and score >= 50)
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(f"{grade}등급 {score}점  ROE:{fund.get('roe',0):.1f}%  PER:{fund.get('per',0):.1f}  20일:{price['ch20']:+.1f}%")
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.3)

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
