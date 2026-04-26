"""
StockPilot KR — screener.py
기준:
  1. 거래대금 상위 30 (Naver)
  2. 외국인 순매수 (Naver 모바일 API)
  3. ROE ≥ 15%
  4. PER ≤ 15배
  5. PSR ≤ 3배 (1.5배 이하 선호)
  6. EPS ≥ 1 + 상승 추세
  7. 배당주 선호
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
TOP_N           = 30
FILTER_ROE      = 15.0
FILTER_PER      = 15.0
FILTER_PSR_MAX  = 3.0
FILTER_PSR_GOOD = 1.5
FILTER_EPS      = 1.0
MOMENTUM_THRESH = 20.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
MOBILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
    "Referer": "https://m.stock.naver.com/",
}

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
        return default if (val != val) else val
    except:
        return default

# ── 1단계: 거래대금 상위 30 ──────────────────────────────────────
def fetch_top_stocks() -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중...")

    # 종목코드 매핑
    ticker_map = {}
    try:
        for m in ["KOSPI", "KOSDAQ"]:
            lst = fdr.StockListing(m)
            nc = next((c for c in lst.columns if c.lower() == "name"), lst.columns[1])
            cc = next((c for c in lst.columns if c.lower() in ("symbol","code","ticker")), lst.columns[0])
            for _, r in lst.iterrows():
                ticker_map[str(r[nc]).strip()] = (str(r[cc]).zfill(6), m)
    except Exception as e:
        print(f"  종목코드 매핑 오류: {e}")

    all_stocks = []
    for sosok, market_name in [("0", "KOSPI"), ("1", "KOSDAQ")]:
        try:
            url = f"https://finance.naver.com/sise/sise_trans.naver?sosok={sosok}"
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.encoding = "euc-kr"
            tables = pd.read_html(res.text)
            for tbl in tables:
                cols = [str(c) for c in tbl.columns]
                tbl.columns = cols
                name_col = next((c for c in cols if "종목" in c), None)
                vol_col  = next((c for c in cols if "거래대금" in c), None)
                if not name_col and len(tbl) > 0:
                    first_row = [str(v) for v in tbl.iloc[0].values]
                    if "종목명" in first_row:
                        tbl.columns = first_row
                        tbl = tbl[1:].reset_index(drop=True)
                        name_col = next((c for c in tbl.columns if "종목" in c), None)
                        vol_col  = next((c for c in tbl.columns if "거래대금" in c), None)
                if name_col:
                    for _, row in tbl.iterrows():
                        name = str(row[name_col]).strip()
                        if not name or name in ("nan","종목명",""):
                            continue
                        tvol = safe_float(str(row.get(vol_col,"0")).replace(",","") if vol_col else "0")
                        ticker, mkt = ticker_map.get(name, ("", market_name))
                        all_stocks.append({"name":name,"market":mkt or market_name,"tvol":int(tvol),"ticker":ticker})
                    print(f"  {market_name}: {len([s for s in all_stocks if s['market']==market_name])}종목")
                    break
        except Exception as e:
            print(f"  {market_name} 오류: {e}")
        time.sleep(0.8)

    if not all_stocks:
        print("  Naver 실패 → 시가총액 기준 대체")
        return fetch_fallback(ticker_map)

    df = pd.DataFrame(all_stocks)
    df = df[df["name"].str.len() > 0]
    df = df.sort_values("tvol", ascending=False).drop_duplicates("name").head(TOP_N).reset_index(drop=True)

    result = []
    for i, row in df.iterrows():
        name = row["name"]; market = row["market"]
        ticker = row.get("ticker","")
        if not ticker:
            ticker, market = ticker_map.get(name, ("", market))
        suffix = ".KS" if market == "KOSPI" else ".KQ"
        result.append({"rank":i+1,"ticker":ticker,"name":name,"market":market,
                        "tvol":int(row["tvol"]),"suffix":suffix})

    print(f"  거래대금 상위 {len(result)}종목:")
    for r in result[:5]:
        print(f"    {r['rank']}. {r['name']} ({r['ticker']}) — {r['tvol']:,}억")
    return result

def fetch_fallback(ticker_map: dict) -> list[dict]:
    try:
        kospi  = fdr.StockListing('KOSPI');  kospi['market'] = 'KOSPI'
        kosdaq = fdr.StockListing('KOSDAQ'); kosdaq['market'] = 'KOSDAQ'
        df = pd.concat([kospi, kosdaq], ignore_index=True)
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
            result.append({"rank":i+1,"ticker":ticker,"name":str(row.get('Name',ticker)),
                           "market":market,"tvol":int(row.get('Marcap',0))//100000000,
                           "suffix":".KS" if market=="KOSPI" else ".KQ"})
        return result
    except:
        return []

# ── 2단계: 외국인 순매수 (Naver 모바일) ──────────────────────────
def fetch_foreign_net(ticker: str) -> dict:
    """
    Naver 모바일 API로 외국인 순매수 조회
    반환: {"foreign_net": 억원, "foreign_ok": bool}
    """
    result = {"foreign_net": 0, "foreign_ok": False}
    if not ticker:
        return result
    try:
        # Naver 모바일 투자자별 매매동향
        url = f"https://m.stock.naver.com/api/stock/{ticker}/investorTradeTrend"
        res = requests.get(url, headers=MOBILE_HEADERS, timeout=5)
        if res.status_code == 200:
            data = res.json()
            # 최근 데이터에서 외국인 순매수 확인
            items = data if isinstance(data, list) else data.get("list", data.get("data", []))
            if items and len(items) > 0:
                latest = items[0] if isinstance(items, list) else items
                for key in ["foreigner", "foreign", "외국인"]:
                    if key in latest:
                        net = safe_float(latest[key].get("netBuy", latest[key].get("net", 0)))
                        result["foreign_net"] = int(net // 100000000)
                        result["foreign_ok"] = net > 0
                        return result
    except:
        pass

    # fallback: Naver PC API
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.encoding = "euc-kr"
        tables = pd.read_html(res.text)
        for tbl in tables:
            cols = [str(c) for c in tbl.columns]
            net_col = next((c for c in cols if "순매수" in c or "외국인" in c), None)
            if net_col and len(tbl) > 0:
                net_val = safe_float(str(tbl.iloc[0][net_col]).replace(",","").replace("+",""))
                result["foreign_net"] = int(net_val)
                result["foreign_ok"] = net_val > 0
                return result
    except:
        pass

    return result

# ── 3단계: yfinance 재무 (PER/ROE/PSR/배당/EPS) ──────────────────
def fetch_yfinance(ticker: str, suffix: str) -> dict:
    result = {
        "per": 0.0, "pbr": 0.0, "roe": 0.0,
        "div": 0.0, "psr": 0.0,
        "eps": 0.0, "eps_growth": 0.0, "eps_trend": "데이터없음"
    }
    if not ticker:
        return result
    try:
        yticker = yf.Ticker(f"{ticker}{suffix}")
        info = yticker.info

        result["per"] = safe_float(info.get("trailingPE") or info.get("forwardPE"))
        result["pbr"] = safe_float(info.get("priceToBook"))
        result["psr"] = safe_float(info.get("priceToSalesTrailing12Months"))
        roe_raw = info.get("returnOnEquity")
        result["roe"] = safe_float(roe_raw * 100 if roe_raw else 0)
        div_raw = info.get("dividendYield")
        result["div"] = safe_float(div_raw * 100 if div_raw else 0)
        result["eps"] = safe_float(info.get("trailingEps"))

        # EPS 성장 추세 (연간 재무제표)
        try:
            stmt = yticker.income_stmt
            if stmt is not None and not stmt.empty:
                ni_row = None
                for idx in stmt.index:
                    if "Net Income" in str(idx):
                        ni_row = stmt.loc[idx]
                        break
                if ni_row is not None and len(ni_row) >= 2:
                    ni_sorted = ni_row.sort_index(ascending=False).dropna()
                    shares = safe_float(info.get("sharesOutstanding", 0))
                    eps_vals = []
                    if shares > 0:
                        for val in ni_sorted:
                            eps_vals.append(round(safe_float(val) / shares, 0))
                    if len(eps_vals) >= 2:
                        growing = all(eps_vals[i] >= eps_vals[i+1] for i in range(min(len(eps_vals)-1, 2)))
                        latest = eps_vals[0]
                        if latest != 0:
                            result["eps"] = latest
                        if growing and latest >= FILTER_EPS:
                            result["eps_trend"] = "상승"
                            gr = ((eps_vals[0]-eps_vals[1])/abs(eps_vals[1])*100) if eps_vals[1] != 0 else 0
                            result["eps_growth"] = round(gr, 1)
                        elif latest >= FILTER_EPS:
                            result["eps_trend"] = "유지"
                        else:
                            result["eps_trend"] = "부진"
                        return result
        except:
            pass

        # fallback: earningsGrowth
        eps = result["eps"]
        eg = safe_float(info.get("earningsGrowth", 0) * 100 if info.get("earningsGrowth") else 0)
        result["eps_growth"] = eg
        result["eps_trend"] = "상승" if eps >= FILTER_EPS and eg > 0 else "유지" if eps >= FILTER_EPS else "부진"

    except Exception as e:
        print(f"    yfinance 오류: {e}")
    return result

# ── 4단계: 가격/등락률 ────────────────────────────────────────────
def fetch_price(ticker: str, start: str, end: str) -> dict:
    if not ticker:
        return {"ch20": 0.0, "vol_trend": 0.0}
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]['Close'])
            p1 = float(df.iloc[-1]['Close'])
            ch20 = round((p1 - p0) / p0 * 100, 1) if p0 > 0 else 0.0
            if 'Volume' in df.columns:
                v = df['Volume'].astype(float)
                avg5 = v.iloc[-5:].mean(); avgA = v.mean()
                vol_trend = round((avg5 - avgA) / avgA * 100, 1) if avgA > 0 else 0.0
            else:
                vol_trend = 0.0
            return {"ch20": ch20, "vol_trend": vol_trend}
    except:
        pass
    return {"ch20": 0.0, "vol_trend": 0.0}

# ── 점수 계산 (책 기반 100점) ─────────────────────────────────────
def calc_score(d: dict) -> int:
    s = 0
    per=d.get("per",0) or 0; pbr=d.get("pbr",0) or 0
    roe=d.get("roe",0) or 0; div=d.get("div",0) or 0
    psr=d.get("psr",0) or 0; eps=d.get("eps",0) or 0
    eps_trend=d.get("eps_trend","")

    # PER (최대 5점)
    if 0<per<5: s+=5
    elif per<10: s+=4
    elif per<15: s+=3
    elif per<20: s+=1

    # PBR (최대 5점)
    if pbr<0.3: s+=5
    elif pbr<0.6: s+=4
    elif pbr<1.0: s+=3
    elif pbr<1.5: s+=2

    # ROE 이익지속성 (최대 5점)
    if roe>=15: s+=5
    elif roe>=8: s+=3
    elif roe>0: s+=1

    # 단독상장 기본값 (5점)
    s += 5

    # 배당 (최대 10점)
    if div>7: s+=10
    elif div>5: s+=7
    elif div>3: s+=5
    elif div>0: s+=2

    # 성장/경영 기본값 (10점)
    s += 10

    # ROE 보너스 (최대 5점)
    if roe>=20: s+=5
    elif roe>=15: s+=4
    elif roe>=10: s+=2

    # PSR (최대 8점)
    if 0<psr<=1.5: s+=8
    elif psr<=3:   s+=4

    # EPS 보너스 (최대 10점)
    if eps>=FILTER_EPS and eps_trend=="상승": s+=10
    elif eps>=FILTER_EPS and eps_trend=="유지": s+=5
    elif eps>=FILTER_EPS: s+=3

    # 외국인 순매수 보너스 (3점)
    if d.get("foreign_ok"): s+=3

    return min(int(s), 100)

def get_grade(score: int) -> str:
    return "A" if score>80 else "B" if score>=70 else "C" if score>=50 else "D"

def apply_filters(d: dict) -> dict:
    per = d.get("per",0) or 0
    roe = d.get("roe",0) or 0
    psr = d.get("psr",0) or 0
    eps = d.get("eps",0) or 0
    return {
        "vol_ok":      (d.get("vol_trend") or 0) > -10,
        "foreign_ok":  d.get("foreign_ok", False),
        "roe_ok":      roe >= FILTER_ROE,
        "per_ok":      0 < per <= FILTER_PER,
        "psr_ok":      psr == 0 or psr <= FILTER_PSR_MAX,   # 데이터 없으면 통과
        "psr_good":    0 < psr <= FILTER_PSR_GOOD,
        "eps_ok":      eps >= FILTER_EPS,
        "eps_growing": d.get("eps_trend","") == "상승",
        "div_ok":      (d.get("div") or 0) >= 3.0,
        "momentum":    (d.get("ch20") or 0) >= MOMENTUM_THRESH,
    }

# ── Discord 전송 ──────────────────────────────────────────────────
def send_discord(results: list, date: str, recommended: list):
    if not DISCORD_WEBHOOK:
        print("  ℹ️  DISCORD_WEBHOOK 미설정"); return
    dt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ge = {"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    eps_icon = {"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}

    display = recommended[:5] if recommended else sorted(
        results, key=lambda x: x.get("score",0), reverse=True
    )[:5]

    fields = []
    for r in display:
        g = r.get("grade","D"); f = r.get("filters",{})
        is_rec = r.get("recommended",False)
        per_str = f"PER {r.get('per',0):.1f}배" if r.get('per',0)>0 else "PER -"
        roe_str = f"ROE {r.get('roe',0):.1f}%" if r.get('roe',0)>0 else "ROE -"
        psr_str = f"PSR {r.get('psr',0):.1f}배" if r.get('psr',0)>0 else "PSR -"
        div_str = f"배당 {r.get('div',0):.1f}%" if r.get('div',0)>0 else ""
        eps_t   = r.get("eps_trend","데이터없음")
        eps_g   = r.get("eps_growth",0)
        eps_str = f"EPS {r.get('eps',0):.0f}원" if r.get('eps',0)!=0 else "EPS -"
        eps_growth_str = f"({eps_g:+.1f}%)" if eps_g!=0 else ""
        ch20    = r.get("ch20",0)
        fgn_str = f"외국인 {'✅순매수' if f.get('foreign_ok') else '❌순매도'}" if r.get('ticker') else ""
        star    = "⭐ " if is_rec else ""

        fields.append({
            "name": f"{ge.get(g,'⚪')} {star}{r['name']} ({r['market']}) — {g}등급 {r.get('score',0)}점",
            "value": (
                f"{roe_str}  {per_str}  {psr_str}  {div_str}\n"
                f"{eps_icon.get(eps_t,'❓')} {eps_str} {eps_growth_str} ({eps_t})  {fgn_str}\n"
                f"{'📈' if ch20>0 else '📉'} 20일 {ch20:+.1f}%  거래대금 {r.get('tvol',0):,}억"
            ),
            "inline": False
        })

    label = "⭐ 추천 종목" if recommended else "📊 점수 상위 종목 (추천 조건 미충족)"
    try:
        res = requests.post(DISCORD_WEBHOOK, json={"embeds":[{
            "title": f"📊 StockPilot 스크리닝 — {dt}",
            "description": (
                f"거래대금 상위 {TOP_N}개 분석\n"
                f"✅ 추천: **{len(recommended)}종목**\n"
                f"기준: ROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PSR≤{FILTER_PSR_MAX}배 · EPS≥{FILTER_EPS}·상승 · 외국인순매수\n\n"
                f"**{label}**"
            ),
            "color": 0x00d97e if recommended else 0x3399ff,
            "fields": fields,
            "footer": {"text": "⚠️ 투자 손실 책임은 본인에게 있습니다."},
            "timestamp": datetime.utcnow().isoformat() + "Z"
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
    print(f"  필터: ROE≥{FILTER_ROE}% | PER≤{FILTER_PER}배 | PSR≤{FILTER_PSR_MAX}배 | EPS≥{FILTER_EPS}·상승 | 외국인순매수")

    tickers = fetch_top_stocks()
    if not tickers:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"종목 데이터 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    print(f"\n[2/4] {len(tickers)}종목 외국인+재무+가격 조회 중...\n")
    results = []
    for t in tickers:
        tk     = t["ticker"]
        suffix = t.get("suffix", ".KS")
        print(f"  [{t['rank']:2d}] {t['name']:12s} ({tk}{suffix})", end=" ... ", flush=True)
        try:
            foreign = fetch_foreign_net(tk)
            fund    = fetch_yfinance(tk, suffix)
            price   = fetch_price(tk, start, date)
            data    = {**t, **foreign, **fund, **price}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)

            # 추천 기준: ROE + PER + PSR + EPS상승 + 외국인순매수
            rec = (
                filters["roe_ok"] and
                filters["per_ok"] and
                filters["psr_ok"] and
                filters["eps_ok"] and
                filters["eps_growing"] and
                filters["foreign_ok"]
            )
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)

            fgn_mark = "외국인✅" if foreign.get("foreign_ok") else "외국인❌"
            psr_mark = f"PSR:{fund.get('psr',0):.1f}" if fund.get('psr',0)>0 else "PSR:-"
            print(
                f"{grade}등급 {score}점  "
                f"ROE:{fund.get('roe',0):.1f}%  "
                f"PER:{fund.get('per',0):.1f}  "
                f"{psr_mark}  "
                f"EPS:{fund.get('eps',0):.0f}({fund.get('eps_trend','?')})  "
                f"{fgn_mark}"
                f"{'  ⭐' if rec else ''}"
            )
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.5)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*65}")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(
            f"  ⭐ {r['name']} [{r['grade']}등급 {r['score']}점]  "
            f"ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  "
            f"PSR {r.get('psr',0):.1f}배  "
            f"EPS {r.get('eps',0):.0f}({r.get('eps_trend','?')})  "
            f"외국인 {'✅' if r.get('foreign_ok') else '❌'}"
        )

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
