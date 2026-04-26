"""
StockPilot KR — screener.py
방법: FDR 시총 상위 100개 → yfinance 일별 OHLCV → 종가×거래량=거래대금 계산 → 상위 30 정렬
재무: yfinance (PER/ROE/PSR/EPS)
추천: ROE≥15% + PER≤15배 + PSR≤3배 + EPS≥1·상승 (ETF 제외)
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import pandas as pd
    import yfinance as yf
    import FinanceDataReader as fdr
    import requests
except ImportError:
    print("pip install -r requirements.txt 먼저 실행하세요")
    exit(1)

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "")
CANDIDATE_N     = 100   # 시총 상위 N개에서 거래대금 계산
TOP_N           = 30    # 최종 거래대금 상위 N개
FILTER_ROE      = 15.0
FILTER_PER      = 15.0
FILTER_PSR_MAX  = 3.0
FILTER_PSR_GOOD = 1.5
FILTER_EPS      = 1.0
MOMENTUM_THRESH = 20.0

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
        val = float(v or 0)
        return default if (val != val) else val
    except:
        return default

# ── 1단계: 후보 종목 (FDR 시총 상위 100) ──────────────────────────
def load_candidates() -> list[dict]:
    print(f"\n[1/4] 후보 {CANDIDATE_N}개 로드 중 (시총 기준)...")
    all_rows = []
    for m in ["KOSPI", "KOSDAQ"]:
        try:
            lst = fdr.StockListing(m)
            lst["market"] = m
            cm = {}
            for c in lst.columns:
                cl = c.lower()
                if cl in ("symbol","code","ticker"): cm[c] = "Code"
                elif cl == "name":                   cm[c] = "Name"
                elif "marcap" in cl:                 cm[c] = "Marcap"
            lst = lst.rename(columns=cm)
            if "Marcap" not in lst.columns:
                num = lst.select_dtypes(include="number").columns
                if len(num): lst["Marcap"] = lst[num[0]]
            lst["Marcap"] = pd.to_numeric(lst["Marcap"], errors="coerce").fillna(0)
            lst = lst[lst["Marcap"] > 0]
            all_rows.append(lst)
            print(f"  {m}: {len(lst)}종목")
        except Exception as e:
            print(f"  {m} 오류: {e}")

    if not all_rows:
        return []

    combined = pd.concat(all_rows, ignore_index=True)
    combined = combined.sort_values("Marcap", ascending=False).reset_index(drop=True)

    result = []
    seen = set()
    for _, row in combined.iterrows():
        name   = str(row.get("Name","")).strip()
        ticker = str(row.get("Code","")).zfill(6)
        market = str(row.get("market","KOSPI"))
        if not name or name in seen or not ticker:
            continue
        seen.add(name)
        suffix = ".KS" if market == "KOSPI" else ".KQ"
        result.append({
            "ticker": ticker,
            "name":   name,
            "market": market,
            "suffix": suffix,
            "is_etf": is_etf(name),
            "marcap": int(row.get("Marcap", 0)),
        })
        if len(result) >= CANDIDATE_N:
            break

    print(f"  → {len(result)}개 후보 확정")
    return result

# ── 2단계: yfinance 배치로 OHLCV → 거래대금 계산 ────────────────
def calc_trading_values(candidates: list[dict], date: str) -> list[dict]:
    print(f"\n[2/4] 거래대금 계산 중 (종가 × 거래량)...")

    # yfinance 배치 다운로드 (2일치로 안정성 확보)
    start_str = fmt(n_days_ago(date, 3))
    end_str   = fmt(date)

    # 50개씩 나눠서 다운로드
    all_data = {}
    batch_size = 50
    ticker_list = [f"{c['ticker']}{c['suffix']}" for c in candidates]

    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        try:
            raw = yf.download(
                batch,
                start=start_str,
                end=end_str,
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if raw.empty:
                continue

            # 단일 종목인 경우 멀티인덱스 아님
            if isinstance(raw.columns, pd.MultiIndex):
                close_df  = raw["Close"]
                volume_df = raw["Volume"]
            else:
                # 단일 종목
                tk = batch[0]
                close_df  = pd.DataFrame({tk: raw["Close"]})
                volume_df = pd.DataFrame({tk: raw["Volume"]})

            # 가장 최근 데이터
            for tk in batch:
                try:
                    closes  = close_df[tk].dropna()
                    volumes = volume_df[tk].dropna()
                    if len(closes) > 0 and len(volumes) > 0:
                        c = float(closes.iloc[-1])
                        v = float(volumes.iloc[-1])
                        all_data[tk] = {
                            "close": c,
                            "volume": v,
                            "tvol": int(c * v),  # 원화 기준
                            "tvol_ok": int(c * v) // 100000000,  # 억원
                        }
                except:
                    pass
        except Exception as e:
            print(f"  배치 오류 ({i}~{i+batch_size}): {e}")
        time.sleep(0.5)

    print(f"  → {len(all_data)}종목 거래대금 계산 완료")

    # 후보에 거래대금 추가
    for c in candidates:
        tk = f"{c['ticker']}{c['suffix']}"
        if tk in all_data:
            c["tvol"]     = all_data[tk]["tvol_ok"]   # 억원
            c["close"]    = all_data[tk]["close"]
        else:
            c["tvol"]  = 0
            c["close"] = 0

    return candidates

# ── 3단계: 거래대금 상위 TOP_N 확정 ──────────────────────────────
def select_top30(candidates: list[dict]) -> list[dict]:
    df = pd.DataFrame(candidates)
    df = df.sort_values("tvol", ascending=False).reset_index(drop=True)
    result = []
    for i, (_, row) in enumerate(df.iterrows()):
        result.append({
            "rank":   i + 1,
            "ticker": row["ticker"],
            "name":   row["name"],
            "market": row["market"],
            "suffix": row["suffix"],
            "is_etf": row["is_etf"],
            "tvol":   int(row["tvol"]),
            "close":  row.get("close", 0),
        })
        if len(result) >= TOP_N:
            break

    print(f"\n  거래대금 상위 {len(result)}종목 확정:")
    for r in result[:10]:
        etf_mark = " [ETF]" if r["is_etf"] else ""
        print(f"    {r['rank']:2d}. {r['name']}{etf_mark} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 4단계: yfinance 재무 데이터 ───────────────────────────────────
def fetch_fundamentals(ticker: str, suffix: str) -> dict:
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
                    ni_s   = ni_row.sort_index(ascending=False).dropna()
                    shares = safe_float(info.get("sharesOutstanding", 0))
                    ev = [round(safe_float(v)/shares, 0) for v in ni_s if shares > 0]
                    if len(ev) >= 2:
                        growing = all(ev[i] >= ev[i+1] for i in range(min(len(ev)-1, 2)))
                        latest  = ev[0]
                        if latest != 0: result["eps"] = latest
                        if growing and latest >= FILTER_EPS:
                            result["eps_trend"]  = "상승"
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

# ── 5단계: 가격/등락률 ────────────────────────────────────────────
def fetch_price_trend(ticker: str, start: str, end: str) -> dict:
    if not ticker:
        return {"ch20":0.0,"vol_trend":0.0}
    try:
        df = fdr.DataReader(ticker, fmt(start), fmt(end))
        if df is not None and len(df) >= 2:
            p0 = float(df.iloc[0]["Close"]); p1 = float(df.iloc[-1]["Close"])
            ch20 = round((p1-p0)/p0*100, 1) if p0 > 0 else 0.0
            if "Volume" in df.columns:
                v    = df["Volume"].astype(float)
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

    s += 5  # 단독상장 기본

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
    ei = {"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓","ETF":"🔷"}

    non_etf = [r for r in results if not r.get("is_etf")]
    display  = recommended[:5] if recommended else sorted(
        non_etf, key=lambda x: x.get("score",0), reverse=True
    )[:5]

    lines = [
        f"📊 **StockPilot 스크리닝 — {dt}**",
        f"거래대금 상위 {TOP_N}개 (종가×거래량 직접 계산)",
        f"✅ 추천: **{len(recommended)}종목** (ROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PSR≤{FILTER_PSR_MAX}배 · EPS상승)",
        "",
        "⭐ **추천 종목**" if recommended else "📊 **점수 상위 종목** (추천 조건 미충족)",
        "─"*28,
    ]
    for r in display:
        g      = r.get("grade","D")
        f      = r.get("filters",{})
        is_rec = r.get("recommended",False)
        star   = "⭐ " if is_rec else ""
        per_s  = f"PER {r.get('per',0):.1f}배" if r.get("per",0)>0 else "PER -"
        roe_s  = f"ROE {r.get('roe',0):.1f}%" if r.get("roe",0)>0 else "ROE -"
        psr_s  = f"PSR {r.get('psr',0):.1f}배" if r.get("psr",0)>0 else "PSR -"
        div_s  = f"배당 {r.get('div',0):.1f}%" if r.get("div",0)>0 else "배당 -"
        eps_t  = r.get("eps_trend","데이터없음")
        eps_g  = r.get("eps_growth",0)
        eps_s  = f"EPS {r.get('eps',0):.0f}원" if r.get("eps",0)!=0 else "EPS -"
        eps_gs = f"({eps_g:+.1f}%)" if eps_g!=0 else ""
        ch20   = r.get("ch20",0)
        tvol   = r.get("tvol",0)
        lines.append(f"{ge.get(g,'⚪')} {star}**{r['name']}** ({r['market']}) — {g}등급 {r.get('score',0)}점")
        lines.append(f"  {roe_s}  {per_s}  {psr_s}  {div_s}")
        lines.append(f"  {ei.get(eps_t,'❓')} {eps_s} {eps_gs} ({eps_t})")
        lines.append(f"  {'📈' if ch20>0 else '📉'} 20일 {ch20:+.1f}%  거래대금 {tvol:,}억")
        lines.append("")

    lines.append("⚠️ 투자 손실 책임은 본인에게 있습니다.")
    msg = "\n".join(lines)

    chunks = []
    while len(msg) > 1900:
        si = msg[:1900].rfind("\n")
        chunks.append(msg[:si]); msg = msg[si:]
    chunks.append(msg)

    try:
        for chunk in chunks:
            r = requests.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=10)
            if r.status_code not in (200, 204):
                print(f"  ⚠️ {r.status_code}: {r.text[:80]}")
            time.sleep(0.3)
        print(f"  ✅ Discord 전송 완료 ({len(chunks)}개)")
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

    # 1) 시총 상위 100개 후보 로드
    candidates = load_candidates()
    if not candidates:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"후보 로드 실패"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    # 2) 거래대금 계산 (종가 × 거래량)
    candidates = calc_trading_values(candidates, date)

    # 3) 거래대금 상위 30 확정
    top30 = select_top30(candidates)
    if not top30:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"거래대금 계산 실패"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    # 4) 재무 + 가격 분석
    print(f"\n[3/4] {len(top30)}종목 재무+가격 분석 중...\n")
    results = []
    for t in top30:
        tk     = t["ticker"]
        suffix = t.get("suffix",".KS")
        etf_m  = "[ETF] " if t.get("is_etf") else ""
        print(f"  [{t['rank']:2d}] {etf_m}{t['name']:14s} ({tk}{suffix})", end=" ... ", flush=True)

        if t.get("is_etf") or not tk:
            data = {**t, "per":0,"pbr":0,"roe":0,"div":0,"psr":0,
                    "eps":0,"eps_growth":0,"eps_trend":"ETF",
                    "ch20":0,"vol_trend":0,"score":0,"grade":"D",
                    "filters":{},"recommended":False}
            results.append(data)
            print("ETF — 스킵")
            continue

        try:
            fund  = fetch_fundamentals(tk, suffix)
            price = fetch_price_trend(tk, start, date)
            data  = {**t, **fund, **price}
            score   = calc_score(data)
            grade   = get_grade(score)
            filters = apply_filters(data)
            rec = (
                not t.get("is_etf") and
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
        time.sleep(0.3)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*65}")
    print(f"  거래대금 상위 {len(results)}종목 분석 완료")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(
            f"  ⭐ {r['name']} ({r['market']}) [{r['grade']}등급 {r['score']}점]  "
            f"ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  "
            f"PSR {r.get('psr',0):.1f}배  EPS {r.get('eps',0):.0f}({r.get('eps_trend','?')})  "
            f"거래대금 {r.get('tvol',0):,}억"
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
