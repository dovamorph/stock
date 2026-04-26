"""
StockPilot KR — screener.py
- 거래대금 상위 30: KOSPI 15 + KOSDAQ 15 (ETF/ETN 제외)
- 외국인 순매수: Naver 모바일 API
- 재무: yfinance (PER/ROE/PSR/EPS)
- 추천: ROE≥15% + PER≤15배 + PSR≤3배 + EPS≥1·상승 + 외국인순매수
"""
import os, io, json, time, traceback
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
TOP_EACH        = 15
TOP_N           = TOP_EACH * 2
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
    "Accept": "application/json",
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

# ── ETF/ETN 제외 키워드 ───────────────────────────────────────────
ETF_KEYWORDS = [
    "KODEX","TIGER","KBSTAR","ARIRANG","HANARO","KOSEF","SMART","FOCUS",
    "TREX","ACE","SOL","PLUS","TIMEFOLIO","PACER","RISE",
    "ETN","선물","인버스","레버리지","ETF","인덱스","INDEX"
]

def is_etf(name: str) -> bool:
    name_upper = name.upper()
    return any(kw.upper() in name_upper for kw in ETF_KEYWORDS)

# ── 종목코드 매핑 + ETF 목록 로드 ────────────────────────────────
def load_ticker_map() -> dict:
    ticker_map = {}
    try:
        for m in ["KOSPI", "KOSDAQ"]:
            lst = fdr.StockListing(m)
            nc = next((c for c in lst.columns if c.lower() == "name"), lst.columns[1])
            cc = next((c for c in lst.columns if c.lower() in ("symbol","code","ticker")), lst.columns[0])
            for _, r in lst.iterrows():
                name = str(r[nc]).strip()
                code = str(r[cc]).zfill(6)
                ticker_map[name] = (code, m)
    except Exception as e:
        print(f"  종목코드 매핑 오류: {e}")
    return ticker_map

# ── 1단계: 거래대금 상위 (ETF 제외) ──────────────────────────────
def fetch_top_stocks() -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중... (ETF/ETN 제외)")
    ticker_map = load_ticker_map()

    result = []
    rank = 1
    for sosok, market_name in [("0", "KOSPI"), ("1", "KOSDAQ")]:
        stocks_this_market = []
        try:
            url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}"
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.encoding = "euc-kr"
            tables = pd.read_html(io.StringIO(res.text))

            for tbl in tables:
                tbl = tbl.dropna(how="all").reset_index(drop=True)
                cols = [str(c) for c in tbl.columns]
                tbl.columns = cols
                name_col = next((c for c in cols if "종목" in c), None)
                if not name_col and len(tbl) > 0:
                    first = [str(v) for v in tbl.iloc[0].values]
                    if "종목명" in first:
                        tbl.columns = first
                        tbl = tbl[1:].reset_index(drop=True)
                        name_col = next((c for c in tbl.columns if "종목" in c), None)
                if name_col:
                    for _, row in tbl.iterrows():
                        name = str(row[name_col]).strip()
                        if not name or name in ("nan","종목명",""):
                            continue
                        # ── ETF/ETN 제외 ──
                        if is_etf(name):
                            continue
                        ticker, mkt = ticker_map.get(name, ("", market_name))
                        suffix = ".KS" if (mkt or market_name) == "KOSPI" else ".KQ"
                        stocks_this_market.append({
                            "name":   name,
                            "market": mkt or market_name,
                            "tvol":   0,
                            "ticker": ticker,
                            "suffix": suffix,
                        })
                        if len(stocks_this_market) >= TOP_EACH:
                            break
                    break
            print(f"  {market_name}: {len(stocks_this_market)}종목 (ETF 제외)")
        except Exception as e:
            print(f"  {market_name} Naver 오류: {e}")

        if len(stocks_this_market) < TOP_EACH:
            # FDR fallback (시장별)
            print(f"  {market_name} Naver 부족 → FDR 시가총액 대체")
            stocks_this_market = fetch_fdr_market(market_name, ticker_map, TOP_EACH)

        for s in stocks_this_market:
            s["rank"] = rank
            rank += 1
        result.extend(stocks_this_market)
        time.sleep(0.5)

    print(f"\n  선정 {len(result)}종목:")
    kospi_n  = len([r for r in result if r["market"] == "KOSPI"])
    kosdaq_n = len([r for r in result if r["market"] == "KOSDAQ"])
    print(f"  KOSPI {kospi_n}개 + KOSDAQ {kosdaq_n}개")
    for r in result[:5]:
        print(f"    {r['rank']}. {r['name']} ({r['ticker']}) [{r['market']}]")
    return result

def fetch_fdr_market(market: str, ticker_map: dict, n: int) -> list[dict]:
    try:
        lst = fdr.StockListing(market)
        lst["market"] = market
        col_map = {}
        for c in lst.columns:
            cl = c.lower()
            if cl in ("symbol","code","ticker"): col_map[c] = "Code"
            elif cl == "name":                   col_map[c] = "Name"
            elif "marcap" in cl:                 col_map[c] = "Marcap"
        lst = lst.rename(columns=col_map)
        if "Marcap" not in lst.columns:
            num = lst.select_dtypes(include="number").columns
            if len(num): lst["Marcap"] = lst[num[0]]
        lst["Marcap"] = pd.to_numeric(lst["Marcap"], errors="coerce").fillna(0)
        # ETF 제외
        lst = lst[lst.apply(lambda r: not is_etf(str(r.get("Name",""))), axis=1)]
        top = lst[lst["Marcap"] > 0].sort_values("Marcap", ascending=False).head(n)
        result = []
        for _, row in top.iterrows():
            ticker = str(row.get("Code", row.iloc[0])).zfill(6)
            name   = str(row.get("Name", ticker))
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            result.append({"name":name,"market":market,"tvol":int(row.get("Marcap",0))//100000000,
                           "ticker":ticker,"suffix":suffix})
        return result
    except:
        return []

# ── 2단계: 외국인 순매수 ──────────────────────────────────────────
def fetch_foreign_net(ticker: str) -> dict:
    result = {"foreign_net": 0, "foreign_ok": False}
    if not ticker:
        return result
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/investorTradeTrend"
        res = requests.get(url, headers=MOBILE_HEADERS, timeout=5)
        if res.status_code == 200:
            data = res.json()
            items = data if isinstance(data, list) else data.get("list", data.get("data", []))
            if isinstance(items, list) and items:
                latest = items[0]
                for fkey in ["foreigner","foreign","외국인"]:
                    if fkey in latest:
                        fd = latest[fkey]
                        net = safe_float(fd.get("netBuy", fd.get("net", fd.get("netBuying", 0))))
                        result["foreign_net"] = int(net)
                        result["foreign_ok"]  = net > 0
                        return result
                if "foreignerNetBuy" in latest:
                    net = safe_float(latest["foreignerNetBuy"])
                    result["foreign_net"] = int(net)
                    result["foreign_ok"]  = net > 0
                    return result
    except:
        pass
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        res.encoding = "euc-kr"
        tables = pd.read_html(io.StringIO(res.text))
        for tbl in tables:
            cols = [str(c) for c in tbl.columns]
            nc = next((c for c in cols if "순매수" in c), None)
            if nc and len(tbl) > 0:
                val = str(tbl.iloc[0][nc]).replace(",","").replace("+","").strip()
                net = safe_float(val)
                result["foreign_net"] = int(net)
                result["foreign_ok"]  = net > 0
                return result
    except:
        pass
    return result

# ── 3단계: yfinance 재무 데이터 ───────────────────────────────────
def fetch_yfinance(ticker: str, suffix: str) -> dict:
    result = {"per":0.0,"pbr":0.0,"roe":0.0,"div":0.0,"psr":0.0,
              "eps":0.0,"eps_growth":0.0,"eps_trend":"데이터없음"}
    if not ticker:
        return result
    try:
        yt   = yf.Ticker(f"{ticker}{suffix}")
        info = yt.info
        result["per"] = safe_float(info.get("trailingPE") or info.get("forwardPE"))
        result["pbr"] = safe_float(info.get("priceToBook"))
        result["psr"] = safe_float(info.get("priceToSalesTrailing12Months"))
        roe_raw = info.get("returnOnEquity")
        result["roe"] = safe_float(roe_raw * 100 if roe_raw else 0)
        div_raw = info.get("dividendYield")
        result["div"] = safe_float(div_raw * 100 if div_raw else 0)
        result["eps"] = safe_float(info.get("trailingEps"))

        # EPS 성장 추세
        try:
            stmt = yt.income_stmt
            if stmt is not None and not stmt.empty:
                ni_row = None
                for idx in stmt.index:
                    if "Net Income" in str(idx):
                        ni_row = stmt.loc[idx]; break
                if ni_row is not None and len(ni_row) >= 2:
                    ni_sorted = ni_row.sort_index(ascending=False).dropna()
                    shares = safe_float(info.get("sharesOutstanding", 0))
                    eps_vals = [round(safe_float(v)/shares, 0) for v in ni_sorted if shares>0]
                    if len(eps_vals) >= 2:
                        growing = all(eps_vals[i] >= eps_vals[i+1] for i in range(min(len(eps_vals)-1, 2)))
                        latest  = eps_vals[0]
                        if latest != 0: result["eps"] = latest
                        if growing and latest >= FILTER_EPS:
                            result["eps_trend"]  = "상승"
                            gr = ((eps_vals[0]-eps_vals[1])/abs(eps_vals[1])*100) if eps_vals[1]!=0 else 0
                            result["eps_growth"] = round(gr, 1)
                        elif latest >= FILTER_EPS:
                            result["eps_trend"] = "유지"
                        else:
                            result["eps_trend"] = "부진"
                        return result
        except:
            pass

        eps = result["eps"]
        eg  = safe_float(info.get("earningsGrowth",0)*100 if info.get("earningsGrowth") else 0)
        result["eps_growth"] = eg
        result["eps_trend"]  = "상승" if eps>=FILTER_EPS and eg>0 else "유지" if eps>=FILTER_EPS else "부진"
    except Exception as e:
        print(f"    yfinance 오류: {e}")
    return result

# ── 4단계: 가격/등락률 ────────────────────────────────────────────
def fetch_price(ticker: str, start: str, end: str) -> dict:
    if not ticker:
        return {"ch20":0.0,"vol_trend":0.0}
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]["Close"]); p1 = float(df.iloc[-1]["Close"])
            ch20 = round((p1-p0)/p0*100, 1) if p0>0 else 0.0
            if "Volume" in df.columns:
                v    = df["Volume"].astype(float)
                avg5 = v.iloc[-5:].mean(); avgA = v.mean()
                vol_trend = round((avg5-avgA)/avgA*100, 1) if avgA>0 else 0.0
            else:
                vol_trend = 0.0
            return {"ch20":ch20,"vol_trend":vol_trend}
    except:
        pass
    return {"ch20":0.0,"vol_trend":0.0}

# ── 점수 계산 ─────────────────────────────────────────────────────
def calc_score(d: dict) -> int:
    s = 0
    per=d.get("per",0) or 0; pbr=d.get("pbr",0) or 0
    roe=d.get("roe",0) or 0; div=d.get("div",0) or 0
    psr=d.get("psr",0) or 0; eps=d.get("eps",0) or 0
    eps_trend=d.get("eps_trend","")

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
    elif roe>0:  s+=1

    s += 5  # 단독상장

    if div>7:  s+=10
    elif div>5: s+=7
    elif div>3: s+=5
    elif div>0: s+=2

    s += 10  # 성장/경영 기본

    if roe>=20: s+=5
    elif roe>=15: s+=4
    elif roe>=10: s+=2

    if 0<psr<=1.5: s+=8
    elif psr<=3:   s+=4

    if eps>=FILTER_EPS and eps_trend=="상승": s+=10
    elif eps>=FILTER_EPS and eps_trend=="유지": s+=5
    elif eps>=FILTER_EPS: s+=3

    if d.get("foreign_ok"): s+=3

    return min(int(s), 100)

def get_grade(score: int) -> str:
    return "A" if score>80 else "B" if score>=70 else "C" if score>=50 else "D"

def apply_filters(d: dict) -> dict:
    per=d.get("per",0) or 0; roe=d.get("roe",0) or 0
    psr=d.get("psr",0) or 0; eps=d.get("eps",0) or 0
    return {
        "vol_ok":      (d.get("vol_trend") or 0) > -10,
        "foreign_ok":  d.get("foreign_ok", False),
        "roe_ok":      roe >= FILTER_ROE,
        "per_ok":      0 < per <= FILTER_PER,
        "psr_ok":      psr == 0 or psr <= FILTER_PSR_MAX,
        "psr_good":    0 < psr <= FILTER_PSR_GOOD,
        "eps_ok":      eps >= FILTER_EPS,
        "eps_growing": d.get("eps_trend","") == "상승",
        "div_ok":      (d.get("div") or 0) >= 3.0,
        "momentum":    (d.get("ch20") or 0) >= MOMENTUM_THRESH,
    }

# ── Discord 전송 (텍스트 메시지 방식) ────────────────────────────
def send_discord(results: list, date: str, recommended: list):
    if not DISCORD_WEBHOOK:
        print("  ℹ️  DISCORD_WEBHOOK 미설정"); return
    dt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ge = {"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    eps_icon = {"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}

    display = recommended[:5] if recommended else sorted(
        results, key=lambda x: x.get("score",0), reverse=True
    )[:5]

    # 텍스트 메시지로 전송 (embed 대신)
    lines = []
    lines.append(f"📊 **StockPilot 스크리닝 — {dt}**")
    lines.append(f"KOSPI {len([r for r in results if r.get('market')=='KOSPI'])} + KOSDAQ {len([r for r in results if r.get('market')=='KOSDAQ'])} = {len(results)}종목 분석")
    lines.append(f"✅ 추천: **{len(recommended)}종목** (ROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PSR≤{FILTER_PSR_MAX}배 · EPS상승 · 외국인순매수)")
    lines.append("")
    label = "⭐ **추천 종목**" if recommended else "📊 **점수 상위 종목** (추천 조건 미충족)"
    lines.append(label)
    lines.append("─" * 30)

    for r in display:
        g       = r.get("grade","D")
        is_rec  = r.get("recommended", False)
        f       = r.get("filters", {})
        star    = "⭐ " if is_rec else ""
        per_s   = f"PER {r.get('per',0):.1f}배" if r.get("per",0)>0 else "PER -"
        roe_s   = f"ROE {r.get('roe',0):.1f}%" if r.get("roe",0)>0 else "ROE -"
        psr_s   = f"PSR {r.get('psr',0):.1f}배" if r.get("psr",0)>0 else "PSR -"
        div_s   = f"배당 {r.get('div',0):.1f}%" if r.get("div",0)>0 else "배당 -"
        eps_t   = r.get("eps_trend","데이터없음")
        eps_g   = r.get("eps_growth",0)
        eps_s   = f"EPS {r.get('eps',0):.0f}원" if r.get("eps",0)!=0 else "EPS -"
        eps_gs  = f"({eps_g:+.1f}%)" if eps_g!=0 else ""
        ch20    = r.get("ch20",0)
        fgn_s   = "외국인✅" if f.get("foreign_ok") else "외국인❌"

        lines.append(f"{ge.get(g,'⚪')} {star}**{r['name']}** ({r['market']}) — {g}등급 {r.get('score',0)}점")
        lines.append(f"  {roe_s}  {per_s}  {psr_s}  {div_s}")
        lines.append(f"  {eps_icon.get(eps_t,'❓')} {eps_s} {eps_gs} ({eps_t})  {fgn_s}")
        lines.append(f"  {'📈' if ch20>0 else '📉'} 20일 {ch20:+.1f}%  거래대금 {r.get('tvol',0):,}억")
        lines.append("")

    lines.append("⚠️ 투자 손실 책임은 본인에게 있습니다.")
    msg = "\n".join(lines)

    # 2000자 초과 시 분할 전송
    chunks = []
    while len(msg) > 1900:
        split_at = msg[:1900].rfind("\n")
        chunks.append(msg[:split_at])
        msg = msg[split_at:]
    chunks.append(msg)

    try:
        for chunk in chunks:
            res = requests.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=10)
            if res.status_code not in (200, 204):
                print(f"  ⚠️ Discord {res.status_code}")
            time.sleep(0.3)
        print(f"  ✅ Discord 전송 완료 ({len(chunks)}개 메시지)")
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
        suffix = t.get("suffix",".KS")
        mkt    = t.get("market","KOSPI")
        print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk}{suffix}) [{mkt}]", end=" ... ", flush=True)
        try:
            foreign = fetch_foreign_net(tk)
            fund    = fetch_yfinance(tk, suffix)
            price   = fetch_price(tk, start, date)
            data    = {**t, **foreign, **fund, **price}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
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
            print(
                f"{grade}등급 {score}점  "
                f"ROE:{fund.get('roe',0):.1f}%  "
                f"PER:{fund.get('per',0):.1f}  "
                f"PSR:{fund.get('psr',0):.1f}  "
                f"EPS:{fund.get('eps',0):.0f}({fund.get('eps_trend','?')})  "
                f"외국인{'✅' if foreign.get('foreign_ok') else '❌'}"
                f"{'  ⭐' if rec else ''}"
            )
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.5)

    recommended = [r for r in results if r.get("recommended")]
    kospi_n  = len([r for r in results if r.get("market")=="KOSPI"])
    kosdaq_n = len([r for r in results if r.get("market")=="KOSDAQ"])
    print(f"\n{'─'*65}")
    print(f"  분석: KOSPI {kospi_n} + KOSDAQ {kosdaq_n} = {len(results)}종목")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(
            f"  ⭐ {r['name']} ({r['market']}) [{r['grade']}등급 {r['score']}점]  "
            f"ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  "
            f"PSR {r.get('psr',0):.1f}배  EPS {r.get('eps',0):.0f}({r.get('eps_trend','?')})  "
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
