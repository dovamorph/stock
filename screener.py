"""
StockPilot KR — screener.py
pykrx 로그인 불필요 함수만 사용
- get_market_ohlcv_by_ticker : 거래대금 상위 20
- get_market_fundamental_by_ticker : 전종목 재무 한방에
- FinanceDataReader : 가격/등락률
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import pandas as pd
    from pykrx import stock
    import FinanceDataReader as fdr
    import requests
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

# ── 1단계: 거래대금 상위 20 (로그인 불필요) ──────────────────────
def fetch_top20(date: str) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중... (기준일 {date})")
    try:
        # 전종목 OHLCV — 거래대금 포함, 로그인 불필요
        df = stock.get_market_ohlcv_by_ticker(date, market="ALL")
        if df is None or df.empty:
            print("  KOSPI+KOSDAQ 통합 실패, 분리 시도...")
            rows = []
            for m in ["KOSPI", "KOSDAQ"]:
                try:
                    tmp = stock.get_market_ohlcv_by_ticker(date, market=m)
                    if tmp is not None and not tmp.empty:
                        rows.append(tmp)
                except:
                    pass
                time.sleep(0.5)
            if not rows:
                print("  ❌ 데이터 없음")
                return []
            df = pd.concat(rows)

        print(f"  전체 {len(df)}종목 수신")

        # 거래대금 컬럼
        tvol_col = next((c for c in df.columns if '거래대금' in str(c)), None)
        if not tvol_col:
            print(f"  컬럼 목록: {list(df.columns)}")
            tvol_col = df.select_dtypes(include='number').columns[-1]

        df[tvol_col] = pd.to_numeric(df[tvol_col], errors='coerce').fillna(0)
        df = df[df[tvol_col] > 0].sort_values(tvol_col, ascending=False).head(TOP_N)

        result = []
        for i, (ticker, row) in enumerate(df.iterrows()):
            try:
                name = stock.get_market_ticker_name(str(ticker))
            except:
                name = str(ticker)
            # 시장 구분
            market = "KOSPI"
            try:
                tickers_kosdaq = stock.get_market_ticker_list(date, market="KOSDAQ")
                if str(ticker) in tickers_kosdaq:
                    market = "KOSDAQ"
            except:
                pass

            result.append({
                "rank":   i + 1,
                "ticker": str(ticker).zfill(6),
                "name":   name,
                "market": market,
                "tvol":   int(row[tvol_col]) // 100000000,
            })

        print(f"  상위 {len(result)}종목 선정:")
        for r in result[:5]:
            print(f"    {r['rank']}. {r['name']} ({r['ticker']}) — {r['tvol']:,}억")
        return result

    except Exception as e:
        print(f"  오류: {e}")
        traceback.print_exc()
        return []

# ── 2단계: 전종목 재무 한번에 가져오기 (로그인 불필요) ────────────
def fetch_all_fundamentals(date: str) -> pd.DataFrame:
    print("\n[2/4] 전종목 재무 데이터 조회 중...")
    try:
        df = stock.get_market_fundamental_by_ticker(date)
        if df is not None and not df.empty:
            print(f"  {len(df)}종목 재무 데이터 수신")
            return df
    except Exception as e:
        print(f"  재무 오류: {e}")
    return pd.DataFrame()

# ── 3단계: 개별 종목 가격/등락률 (FinanceDataReader) ─────────────
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

# ── 점수 계산 ─────────────────────────────────────────────────────
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
        "vol_ok":     (d.get("vol_trend") or 0) > -10,
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
                f"20일 {r.get('ch20',0):+.1f}%  ·  배당 {r.get('div',0):.1f}%  ·  거래대금 {r.get('tvol',0):,}억"
            ),
            "inline": False
        })
    if not fields:
        fields.append({
            "name": "⚠️ 추천 종목 없음",
            "value": "오늘 모든 필터를 통과한 종목이 없습니다.\n필터 기준: ROE≥15% · PER≤15배 · PBR≤1.5배",
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
    fund_df = fetch_all_fundamentals(date)

    # 3) 종목별 가격 + 재무 병합
    print(f"\n[3/4] {len(tickers)}종목 가격 데이터 조회 중...\n")
    results = []
    for t in tickers:
        tk = t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:12s} ({tk})", end=" ... ", flush=True)
        try:
            # 재무 (전종목 DataFrame에서 가져오기)
            per = pbr = roe = div = 0.0
            if not fund_df.empty and tk in fund_df.index:
                row = fund_df.loc[tk]
                per = float(row.get("PER") or 0)
                pbr = float(row.get("PBR") or 0)
                eps = float(row.get("EPS") or 0)
                bps = float(row.get("BPS") or 0)
                div = float(row.get("DIV") or 0)
                roe = round(eps / bps * 100, 1) if bps > 0 else 0.0

            # 가격 데이터
            price = fetch_price(tk, start, date)

            data = {
                **t,
                "per": per, "pbr": pbr, "roe": roe, "div": div,
                **price,
            }
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (filters["vol_ok"] and filters["roe_ok"] and
                   filters["per_ok"] and filters["pbr_ok"] and score >= 50)
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(f"{grade}등급 {score}점  ROE:{roe:.1f}%  PER:{per:.1f}  PBR:{pbr:.2f}  20일:{price['ch20']:+.1f}%")
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.3)

    recommended = [r for r in results if r.get("recommended")]

    print(f"\n{'─'*60}")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(f"  ★ {r['name']} [{r['grade']}등급 {r['score']}점]"
              f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  PBR {r.get('pbr',0):.2f}")

    # 저장
    json.dump({
        "date":        date,
        "generated_at": datetime.now().isoformat(),
        "total":       len(results),
        "results":     results,
        "recommended": recommended,
    }, open("results.json","w",encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("\n  💾 results.json 저장 완료")

    # Discord
    send_discord(results, date, recommended)
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
