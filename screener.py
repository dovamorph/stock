"""
StockPilot KR — screener.py
- 거래대금 상위 30: Naver Finance 거래대금 순위 (KOSPI+KOSDAQ 합산)
- 재무: yfinance (PER/ROE/PSR/EPS)
- 추천: ROE≥15% + PER≤15배 + PSR≤3배 + EPS≥1·상승 (ETF 자동 제외)
"""
import os, io, json, time, traceback, re
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

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://finance.naver.com/",
})

ETF_KEYWORDS = [
    "KODEX","TIGER","KBSTAR","ARIRANG","HANARO","KOSEF","SMART","FOCUS",
    "TREX","ACE","SOL","PLUS","TIMEFOLIO","RISE","ETN","ETF",
    "선물인버스","레버리지","인버스","인덱스"
]

def is_etf(name: str) -> bool:
    n = name.upper()
    return any(k.upper() in n for k in ETF_KEYWORDS)

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
        s = re.sub(r"[^\d.\-]", "", str(v or "0"))
        val = float(s) if s and s not in ("","-") else default
        return default if val != val else val
    except:
        return default

# ── 티커 매핑 로드 ─────────────────────────────────────────────────
def load_ticker_map() -> dict:
    tmap = {}
    try:
        for m in ["KOSPI", "KOSDAQ"]:
            lst = fdr.StockListing(m)
            nc = next((c for c in lst.columns if c.lower() == "name"), lst.columns[1])
            cc = next((c for c in lst.columns if c.lower() in ("symbol","code","ticker")), lst.columns[0])
            for _, r in lst.iterrows():
                tmap[str(r[nc]).strip()] = (str(r[cc]).zfill(6), m)
    except Exception as e:
        print(f"  티커 매핑 오류: {e}")
    return tmap

# ── 1단계: Naver 거래대금 상위 ────────────────────────────────────
def fetch_naver_trans(sosok: str, market: str) -> list[dict]:
    """
    Naver Finance 거래대금 순위 페이지
    sosok=0: KOSPI, sosok=1: KOSDAQ
    """
    stocks = []
    urls = [
        f"https://finance.naver.com/sise/sise_trans.naver?sosok={sosok}",
        f"https://finance.naver.com/sise/sise_trans_analysis.naver?sosok={sosok}",
    ]
    for url in urls:
        try:
            res = SESSION.get(url, timeout=12)
            if res.status_code != 200:
                continue
            res.encoding = "euc-kr"
            html = res.text
            # HTML 파싱
            tables = pd.read_html(io.StringIO(html), encoding="euc-kr")
            for tbl in tables:
                tbl = tbl.dropna(how="all").reset_index(drop=True)
                if len(tbl) < 3:
                    continue
                cols = [str(c) for c in tbl.columns]
                tbl.columns = cols

                # 헤더 행 처리
                nc = next((c for c in cols if "종목" in c), None)
                vc = next((c for c in cols if "거래대금" in c), None)
                if not nc:
                    row0 = [str(v) for v in tbl.iloc[0]]
                    if any("종목" in v for v in row0):
                        tbl.columns = row0
                        tbl = tbl[1:].reset_index(drop=True)
                        nc = next((c for c in tbl.columns if "종목" in c), None)
                        vc = next((c for c in tbl.columns if "거래대금" in c), None)

                if nc and len(tbl) > 0:
                    for _, row in tbl.iterrows():
                        name = str(row[nc]).strip()
                        if not name or name in ("nan","종목명",""):
                            continue
                        tvol = safe_float(str(row.get(vc, "0")).replace(",","")) if vc else 0
                        stocks.append({"name": name, "market": market, "tvol": int(tvol)})
                    if stocks:
                        return stocks
        except Exception as e:
            continue
    return stocks

def fetch_top30(tmap: dict) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중... (KOSPI+KOSDAQ 합산)")

    all_stocks = []
    for sosok, mkt in [("0","KOSPI"), ("1","KOSDAQ")]:
        stocks = fetch_naver_trans(sosok, mkt)
        print(f"  {mkt}: {len(stocks)}종목 수집")
        all_stocks.extend(stocks)
        time.sleep(0.5)

    if len(all_stocks) < 5:
        # Naver 완전 실패 → 에러 반환
        print("  ❌ Naver 거래대금 데이터 없음")
        return []

    # 합산 후 거래대금 기준 상위 TOP_N
    df = pd.DataFrame(all_stocks)
    df = df[df["name"].str.len() > 0].drop_duplicates("name")
    df = df.sort_values("tvol", ascending=False).head(TOP_N).reset_index(drop=True)

    result = []
    for i, row in df.iterrows():
        name   = row["name"]
        market = row["market"]
        tvol   = int(row["tvol"])
        ticker, mkt2 = tmap.get(name, ("", market))
        suffix = ".KS" if (mkt2 or market) == "KOSPI" else ".KQ"
        result.append({
            "rank":   i + 1,
            "ticker": ticker,
            "name":   name,
            "market": mkt2 or market,
            "tvol":   tvol,
            "suffix": suffix,
            "is_etf": is_etf(name),
        })

    print(f"\n  거래대금 상위 {len(result)}종목 (ETF 포함):")
    for r in result[:10]:
        etf_mark = " [ETF]" if r["is_etf"] else ""
        print(f"    {r['rank']:2d}. {r['name']}{etf_mark} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 2단계: yfinance 재무 데이터 ───────────────────────────────────
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
                    ni_s = ni_row.sort_index(ascending=False).dropna()
                    shares = safe_float(info.get("sharesOutstanding", 0))
                    ev = [round(safe_float(v)/shares, 0) for v in ni_s if shares > 0]
                    if len(ev) >= 2:
                        growing = all(ev[i] >= ev[i+1] for i in range(min(len(ev)-1, 2)))
                        latest  = ev[0]
                        if latest != 0: result["eps"] = latest
                        if growing and latest >= FILTER_EPS:
                            result["eps_trend"] = "상승"
                            gr = ((ev[0]-ev[1])/abs(ev[1])*100) if ev[1] != 0 else 0
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

# ── 3단계: 가격/등락률 ────────────────────────────────────────────
def fetch_price(ticker: str, start: str, end: str) -> dict:
    if not ticker:
        return {"ch20":0.0,"vol_trend":0.0}
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]["Close"]); p1 = float(df.iloc[-1]["Close"])
            ch20 = round((p1-p0)/p0*100, 1) if p0 > 0 else 0.0
            if "Volume" in df.columns:
                v = df["Volume"].astype(float)
                avg5 = v.iloc[-5:].mean(); avgA = v.mean()
                vol_trend = round((avg5-avgA)/avgA*100, 1) if avgA > 0 else 0.0
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

    if div>7:   s+=10
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

    return min(int(s), 100)

def get_grade(score: int) -> str:
    return "A" if score>80 else "B" if score>=70 else "C" if score>=50 else "D"

def apply_filters(d: dict) -> dict:
    per=d.get("per",0) or 0; roe=d.get("roe",0) or 0
    psr=d.get("psr",0) or 0; eps=d.get("eps",0) or 0
    return {
        "vol_ok":      (d.get("vol_trend") or 0) > -10,
        "roe_ok":      roe >= FILTER_ROE,
        "per_ok":      0 < per <= FILTER_PER,
        "psr_ok":      psr == 0 or psr <= FILTER_PSR_MAX,
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
    ei = {"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}

    display = recommended[:5] if recommended else sorted(
        [r for r in results if not r.get("is_etf")],
        key=lambda x: x.get("score",0), reverse=True
    )[:5]

    lines = [
        f"📊 **StockPilot 스크리닝 — {dt}**",
        f"거래대금 상위 {TOP_N}개 분석 (ETF 포함)",
        f"✅ 추천: **{len(recommended)}종목** (ROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PSR≤{FILTER_PSR_MAX}배 · EPS상승)",
        "",
        "⭐ **추천 종목**" if recommended else "📊 **점수 상위 종목** (추천 조건 미충족)",
        "─" * 28,
    ]
    for r in display:
        g      = r.get("grade","D")
        f      = r.get("filters",{})
        is_rec = r.get("recommended",False)
        star   = "⭐ " if is_rec else ""
        per_s  = f"PER {r.get('per',0):.1f}배" if r.get("per",0) > 0 else "PER -"
        roe_s  = f"ROE {r.get('roe',0):.1f}%" if r.get("roe",0) > 0 else "ROE -"
        psr_s  = f"PSR {r.get('psr',0):.1f}배" if r.get("psr",0) > 0 else "PSR -"
        div_s  = f"배당 {r.get('div',0):.1f}%" if r.get("div",0) > 0 else "배당 -"
        eps_t  = r.get("eps_trend","데이터없음")
        eps_g  = r.get("eps_growth",0)
        eps_s  = f"EPS {r.get('eps',0):.0f}원" if r.get("eps",0) != 0 else "EPS -"
        eps_gs = f"({eps_g:+.1f}%)" if eps_g != 0 else ""
        ch20   = r.get("ch20", 0)
        tvol   = r.get("tvol", 0)

        lines.append(f"{ge.get(g,'⚪')} {star}**{r['name']}** ({r['market']}) — {g}등급 {r.get('score',0)}점")
        lines.append(f"  {roe_s}  {per_s}  {psr_s}  {div_s}")
        lines.append(f"  {ei.get(eps_t,'❓')} {eps_s} {eps_gs} ({eps_t})")
        lines.append(f"  {'📈' if ch20>0 else '📉'} 20일 {ch20:+.1f}%  거래대금 {tvol:,}억")
        lines.append("")

    lines.append("⚠️ 투자 손실 책임은 본인에게 있습니다.")
    msg = "\n".join(lines)

    # 2000자 분할
    chunks = []
    while len(msg) > 1900:
        si = msg[:1900].rfind("\n")
        chunks.append(msg[:si]); msg = msg[si:]
    chunks.append(msg)

    try:
        for chunk in chunks:
            r = requests.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=10)
            if r.status_code not in (200, 204):
                print(f"  ⚠️ Discord {r.status_code}: {r.text[:100]}")
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
    print(f"  필터: ROE≥{FILTER_ROE}% | PER≤{FILTER_PER}배 | PSR≤{FILTER_PSR_MAX}배 | EPS≥{FILTER_EPS}·상승")

    tmap    = load_ticker_map()
    tickers = fetch_top30(tmap)

    if not tickers:
        print("❌ 거래대금 데이터를 가져오지 못했습니다.")
        print("   → Naver Finance URL이 변경됐거나 접근이 차단됐습니다.")
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"거래대금 데이터 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    print(f"\n[2/4] {len(tickers)}종목 재무+가격 조회 중...\n")
    results = []
    for t in tickers:
        tk     = t["ticker"]
        suffix = t.get("suffix",".KS")
        etf_mark = "[ETF] " if t.get("is_etf") else ""
        print(f"  [{t['rank']:2d}] {etf_mark}{t['name']:14s} ({tk}{suffix})", end=" ... ", flush=True)

        if t.get("is_etf") or not tk:
            # ETF는 재무 분석 스킵
            data = {**t, "per":0,"pbr":0,"roe":0,"div":0,"psr":0,
                    "eps":0,"eps_growth":0,"eps_trend":"ETF",
                    "ch20":0,"vol_trend":0,
                    "score":0,"grade":"D",
                    "filters":{},"recommended":False}
            results.append(data)
            print("ETF — 스킵")
            continue

        try:
            fund  = fetch_yfinance(tk, suffix)
            price = fetch_price(tk, start, date)
            data  = {**t, **fund, **price}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (
                filters["roe_ok"] and
                filters["per_ok"] and
                filters["psr_ok"] and
                filters["eps_ok"] and
                filters["eps_growing"]
            )
            data.update({"score":score,"grade":grade,"filters":filters,"recommended":rec})
            results.append(data)
            print(
                f"{grade}등급 {score}점  "
                f"ROE:{fund.get('roe',0):.1f}%  "
                f"PER:{fund.get('per',0):.1f}  "
                f"PSR:{fund.get('psr',0):.1f}  "
                f"EPS:{fund.get('eps',0):.0f}({fund.get('eps_trend','?')})"
                f"{'  ⭐' if rec else ''}"
            )
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.5)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*65}")
    print(f"  거래대금 상위 {len(results)}종목 분석 완료")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(
            f"  ⭐ {r['name']} ({r['market']}) [{r['grade']}등급 {r['score']}점]  "
            f"ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  "
            f"PSR {r.get('psr',0):.1f}배  EPS {r.get('eps',0):.0f}({r.get('eps_trend','?')})"
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
