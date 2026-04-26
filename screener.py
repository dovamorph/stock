"""
StockPilot KR — screener.py
FinanceDataReader 기반 (로그인 불필요)
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

def fmt_date(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"

def n_days_ago(base: str, n: int = 30) -> str:
    return (datetime.strptime(base, "%Y%m%d") - timedelta(days=n)).strftime("%Y%m%d")

def fetch_top20(date: str) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중...")
    try:
        kospi  = fdr.StockListing('KOSPI');  kospi['market']  = 'KOSPI'
        kosdaq = fdr.StockListing('KOSDAQ'); kosdaq['market'] = 'KOSDAQ'
        df = pd.concat([kospi, kosdaq], ignore_index=True)

        # 컬럼명 정규화
        rename = {}
        for c in df.columns:
            cl = c.lower()
            if cl in ('symbol','code','ticker'): rename[c] = 'Code'
            elif cl == 'name':                   rename[c] = 'Name'
            elif 'volume' in cl:                 rename[c] = 'Volume'
            elif 'marcap' in cl:                 rename[c] = 'Marcap'
        df = df.rename(columns=rename)

        sort_col = next((c for c in ('Volume','Marcap') if c in df.columns), df.columns[2])
        df[sort_col] = pd.to_numeric(df[sort_col], errors='coerce').fillna(0)
        top = df.sort_values(sort_col, ascending=False).head(TOP_N).reset_index(drop=True)

        result = []
        for i, row in top.iterrows():
            ticker = str(row.get('Code', row.iloc[0])).zfill(6)
            result.append({
                "rank":   i + 1,
                "ticker": ticker,
                "name":   str(row.get('Name', ticker)),
                "market": str(row.get('market', 'KOSPI')),
                "tvol":   int(row.get('Volume', row.get('Marcap', 0))) // 100000000,
            })
        print(f"  {len(result)}개 선정 완료")
        return result
    except Exception as e:
        print(f"  오류: {e}"); traceback.print_exc(); return []

def fetch_stock_data(ticker: str, date: str, start: str) -> dict:
    r = {"ch20": 0.0, "vol_trend": 0.0, "per": 0.0, "pbr": 0.0, "roe": 0.0, "div": 0.0, "foreign_net": 0}
    try:
        df = fdr.DataReader(ticker, fmt_date(start), fmt_date(date))
        if df is not None and len(df) >= 2:
            p0, p1 = float(df.iloc[0]['Close']), float(df.iloc[-1]['Close'])
            r['ch20'] = round((p1 - p0) / p0 * 100, 1) if p0 > 0 else 0.0
            if 'Volume' in df.columns:
                v = df['Volume'].astype(float)
                avg5 = v.iloc[-5:].mean(); avgA = v.mean()
                r['vol_trend'] = round((avg5 - avgA) / avgA * 100, 1) if avgA > 0 else 0.0
    except: pass

    try:
        from pykrx import stock as px
        df_f = px.get_market_fundamental(date, date, ticker)
        if df_f is not None and not df_f.empty:
            row = df_f.iloc[0]
            per = float(row.get('PER') or 0); pbr = float(row.get('PBR') or 0)
            eps = float(row.get('EPS') or 0); bps = float(row.get('BPS') or 0)
            div = float(row.get('DIV') or 0)
            r.update({'per': per, 'pbr': pbr, 'roe': round(eps/bps*100,1) if bps>0 else 0.0, 'div': div})
    except: pass

    try:
        from pykrx import stock as px
        df_inv = px.get_market_trading_value_by_investor(start, date, ticker)
        if df_inv is not None and not df_inv.empty:
            for lbl in ["외국인합계","외국인 합계","외국인"]:
                if lbl in df_inv.index:
                    r['foreign_net'] = int(float(df_inv.loc[lbl,"순매수"]) / 1e8); break
    except: pass

    return r

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
    s+=5
    if div>7: s+=10
    elif div>5: s+=7
    elif div>3: s+=5
    elif div>0: s+=2
    s+=12
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

def send_discord(results: list, date: str, recommended: list):
    if not DISCORD_WEBHOOK:
        print("  ℹ️  DISCORD_WEBHOOK 미설정"); return
    dt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ge = {"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    fields = []
    for r in recommended[:6]:
        g=r.get("grade","D"); f=r.get("filters",{})
        flags=("  🔥급등" if f.get("momentum") else "")+("  💰배당" if f.get("div_ok") else "")
        fields.append({
            "name": f"{ge.get(g,'⚪')} {r['name']} ({r['market']})  {g}등급 {r['score']}점{flags}",
            "value": f"```ROE {r.get('roe',0):.1f}%  |  PER {r.get('per',0):.1f}배  |  PBR {r.get('pbr',0):.2f}```외국인 {r.get('foreign_net',0):+,}억  ·  20일 {r.get('ch20',0):+.1f}%",
            "inline": False
        })
    if not fields:
        fields.append({"name":"⚠️ 추천 종목 없음","value":"오늘 필터 통과 종목 없음","inline":False})
    try:
        res = requests.post(DISCORD_WEBHOOK, json={"embeds":[{
            "title": f"📊 StockPilot — {dt}",
            "description": f"거래대금 상위{TOP_N} → 추천 **{len(recommended)}종목**",
            "color": 0x00d97e if recommended else 0xff4560,
            "fields": fields,
            "footer": {"text":"⚠️ 투자 손실 책임은 본인에게 있습니다."},
            "timestamp": datetime.utcnow().isoformat()+"Z"
        }]}, timeout=10)
        print(f"  {'✅ Discord 전송 완료' if res.status_code==204 else f'⚠️ {res.status_code}'}")
    except Exception as e:
        print(f"  ❌ Discord 실패: {e}")

def main():
    print("╔══════════════════════════════════╗")
    print("║   StockPilot KR  자동 스크리닝   ║")
    print("╚══════════════════════════════════╝")
    date  = last_trading_day()
    start = n_days_ago(date, 30)
    print(f"  기준일: {date}  |  조회범위: {start} ~ {date}\n")

    tickers = fetch_top20(date)
    if not tickers:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),"results":[],"recommended":[],"error":"데이터 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    print(f"\n[2/4] {len(tickers)}종목 세부 데이터 조회 중...\n")
    results = []
    for t in tickers:
        tk = t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:10s} ({tk})", end=" ... ", flush=True)
        try:
            data = {**t, **fetch_stock_data(tk, date, start)}
            score=calc_score(data); grade=get_grade(score)
            filters=apply_filters(data)
            rec=(filters["vol_ok"] and filters["foreign_ok"] and
                 filters["roe_ok"] and filters["per_ok"] and filters["pbr_ok"] and score>=50)
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(f"{grade}등급 {score}점")
        except Exception:
            print("오류 (건너뜀)"); traceback.print_exc()
        time.sleep(0.3)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*50}\n  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(f"  ★ {r['name']} [{r['grade']}등급 {r['score']}점]  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배")

    json.dump({"date":date,"generated_at":datetime.now().isoformat(),"total":len(results),
               "results":results,"recommended":recommended},
              open("results.json","w",encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results, date, recommended)
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
