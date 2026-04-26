"""
StockPilot KR — screener.py
FinanceDataReader 전용 (pykrx 로그인 불필요)
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import pandas as pd
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

def last_trading_day() -> str:
    now = datetime.now()
    for i in range(1, 10):
        d = now - timedelta(days=i)
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    return (now - timedelta(days=1)).strftime("%Y%m%d")

def fmt(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"

def n_days_ago(base: str, n: int = 30) -> str:
    return (datetime.strptime(base, "%Y%m%d") - timedelta(days=n)).strftime("%Y%m%d")

# ── 1단계: 시가총액 상위 20 (거래대금 대용) ───────────────────────
def fetch_top20() -> list[dict]:
    print(f"\n[1/4] 시가총액 상위 {TOP_N} 조회 중...")
    try:
        kospi  = fdr.StockListing('KOSPI');  kospi['market']  = 'KOSPI'
        kosdaq = fdr.StockListing('KOSDAQ'); kosdaq['market'] = 'KOSDAQ'
        df = pd.concat([kospi, kosdaq], ignore_index=True)

        # Marcap(시가총액) 기준 정렬
        marcap_col = next((c for c in df.columns if 'marcap' in c.lower() or 'cap' in c.lower()), None)
        code_col   = next((c for c in df.columns if c.lower() in ('symbol','code','ticker')), df.columns[0])
        name_col   = next((c for c in df.columns if 'name' in c.lower()), df.columns[1])

        if not marcap_col:
            print("  시가총액 컬럼 없음, 첫 번째 숫자 컬럼 사용")
            marcap_col = df.select_dtypes(include='number').columns[0]

        df[marcap_col] = pd.to_numeric(df[marcap_col], errors='coerce').fillna(0)
        top = df[df[marcap_col] > 0].sort_values(marcap_col, ascending=False).head(TOP_N).reset_index(drop=True)

        result = []
        for i, row in top.iterrows():
            result.append({
                "rank":   i + 1,
                "ticker": str(row[code_col]).zfill(6),
                "name":   str(row[name_col]),
                "market": str(row.get('market', 'KOSPI')),
                "tvol":   int(row[marcap_col]) // 100000000,
            })
        print(f"  {len(result)}개 선정 완료")
        for r in result[:5]:
            print(f"    {r['rank']}. {r['name']} ({r['ticker']}) — 시총 {r['tvol']:,}억")
        return result
    except Exception as e:
        print(f"  오류: {e}"); traceback.print_exc(); return []

# ── 2단계: 종목별 가격 + 재무 데이터 ─────────────────────────────
def fetch_stock_data(ticker: str, start: str, end: str) -> dict:
    r = {"ch20": 0.0, "vol_trend": 0.0, "per": 0.0,
         "pbr": 0.0, "roe": 0.0, "div": 0.0, "foreign_net": 0}
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]['Close'])
            p1 = float(df.iloc[-1]['Close'])
            r['ch20'] = round((p1 - p0) / p0 * 100, 1) if p0 > 0 else 0.0
            if 'Volume' in df.columns:
                v = df['Volume'].astype(float)
                avg5 = v.iloc[-5:].mean()
                avgA = v.mean()
                r['vol_trend'] = round((avg5 - avgA) / avgA * 100, 1) if avgA > 0 else 0.0
    except Exception as e:
        print(f"    가격 오류: {e}")

    # KRX 재무 데이터 (FinanceDataReader)
    try:
        df_krx = fdr.DataReader(f'KRX:{ticker}')
        if df_krx is not None and not df_krx.empty:
            latest = df_krx.iloc[-1]
            for key, candidates in {
                'per': ['PER','per','P/E'],
                'pbr': ['PBR','pbr','P/B'],
                'div': ['DIV','div','배당수익률','DividendYield'],
            }.items():
                for c in candidates:
                    if c in latest.index and pd.notna(latest[c]):
                        r[key] = float(latest[c] or 0)
                        break
    except:
        pass

    # 재무제표로 ROE 계산 시도
    try:
        fs = fdr.DataReader(ticker, kind='fundamental')
        if fs is not None and not fs.empty and 'ROE' in fs.columns:
            r['roe'] = float(fs['ROE'].iloc[-1] or 0)
    except:
        pass

    # ROE 없으면 PBR/PER로 추정 (ROE ≈ PBR/PER * 100)
    if r['roe'] == 0 and r['per'] > 0 and r['pbr'] > 0:
        r['roe'] = round(r['pbr'] / r['per'] * 100, 1)

    return r

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
    s+=5  # 단독상장
    if div>7: s+=10
    elif div>5: s+=7
    elif div>3: s+=5
    elif div>0: s+=2
    s+=12  # 성장/경영 기본
    if roe>=20: s+=5
    elif roe>=15: s+=4
    elif roe>=10: s+=2
    return min(int(s), 100)

def get_grade(score: int) -> str:
    return "A" if score>80 else "B" if score>=70 else "C" if score>=50 else "D"

def apply_filters(d: dict) -> dict:
    return {
        "vol_ok":     (d.get("vol_trend") or 0) > -10,
        "foreign_ok": True,  # 외국인 데이터 없으므로 통과 처리
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
                f"20일 {r.get('ch20',0):+.1f}%  ·  배당 {r.get('div',0):.1f}%"
            ),
            "inline": False
        })
    if not fields:
        fields.append({"name":"⚠️ 추천 종목 없음","value":"오늘 필터 통과 종목 없음","inline":False})
    try:
        res = requests.post(DISCORD_WEBHOOK, json={"embeds":[{
            "title": f"📊 StockPilot — {dt}",
            "description": f"시가총액 상위{TOP_N} → 추천 **{len(recommended)}종목**\nROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PBR≤{FILTER_PBR}배",
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
    print(f"  기준일: {date}  |  조회범위: {start} ~ {date}\n")

    tickers = fetch_top20()
    if not tickers:
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
            data = {**t, **fetch_stock_data(tk, start, date)}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (filters["roe_ok"] and filters["per_ok"] and
                   filters["pbr_ok"] and score >= 50)
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(f"{grade}등급 {score}점  ROE:{data.get('roe',0):.1f}%  PER:{data.get('per',0):.1f}  PBR:{data.get('pbr',0):.2f}")
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.5)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*55}")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(f"  ★ {r['name']} [{r['grade']}등급 {r['score']}점]"
              f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  PBR {r.get('pbr',0):.2f}")

    json.dump({"date":date,"generated_at":datetime.now().isoformat(),
               "total":len(results),"results":results,"recommended":recommended},
              open("results.json","w",encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results, date, recommended)
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
