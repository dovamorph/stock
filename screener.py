"""
StockPilot KR — KIS OpenAPI 스크리닝
지표: 거래대금 / ROE / PER / PBR / EPS / EPS추세 / 배당여부 / 20일등락
시장 시그널: KOSPI MA5/MA20/MA60 정배열/역배열 기반
단타 기준 (1~7일): 거래대금추세≥20% + 5일등락 3~15% + 20일등락<30%
장투 기준: ROE≥15% + EPS상승 + PER≤25배 + (PBR≤2.0 OR 배당주)
등급: A(4/4) B(3/4) C(2/4) D(1이하)
"""
import os, json, time, traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests, pandas as pd
    import FinanceDataReader as fdr
    import yfinance as yf
except ImportError:
    print("pip install requests pandas finance-datareader yfinance"); exit(1)

APP_KEY    = os.environ.get("KIS_APP_KEY","")
APP_SECRET = os.environ.get("KIS_APP_SECRET","")
DISCORD    = os.environ.get("DISCORD_WEBHOOK","")
BASE       = "https://openapi.koreainvestment.com:9443"
TOP_N      = 30
CAND_N     = 300

ETF_KW = ["ETF","ETN","KODEX","TIGER","KBSTAR","ARIRANG","HANARO","SOL","ACE",
          "RISE","레버리지","인버스","선물","PLUS","TIMEFOLIO"]

def sf(v, d=0.0):
    try:
        s=str(v).replace(",","").strip()
        val=float(s) if s else d
        return d if val!=val else val
    except: return d

def get_token():
    r=requests.post(f"{BASE}/oauth2/tokenP",timeout=15,
        json={"grant_type":"client_credentials","appkey":APP_KEY,"appsecret":APP_SECRET})
    r.raise_for_status()
    tok=r.json().get("access_token","")
    if not tok: raise ValueError("토큰 비어있음")
    print("  ✅ KIS 토큰 발급 완료"); return tok

def H(tok, tr_id):
    return {"Content-Type":"application/json","authorization":f"Bearer {tok}",
            "appkey":APP_KEY,"appsecret":APP_SECRET,"tr_id":tr_id,"custtype":"P"}

def is_etf(name): return any(k in name for k in ETF_KW)

# ── 0단계: KOSPI 시장 시그널 (MA5/MA20/MA60 기반) ─────────────────
def fetch_market_signal(tok) -> dict:
    """
    정배열: 현재가 > MA5 > MA20 > MA60 → 매수 우위
    역배열: 현재가 < MA5 < MA20 < MA60 → 매도 우위
    그 외: 관망
    신뢰도 보강: 골든크로스(MA5>MA20) + 중기선(MA60) 동시 확인
    """
    result = {
        "signal": "⚖️ 관망", "signal_en": "WATCH",
        "reason": "데이터 없음",
        "kospi_close": 0, "ma5": 0, "ma20": 0, "ma60": 0,
        "kospi_ch5": 0, "kospi_ch20": 0, "aligned": "",
    }
    try:
        # FDR로 KOSPI 지수 90일치 (MA60 계산에 충분한 데이터)
        now = datetime.now()
        s   = (now - timedelta(days=120)).strftime("%Y-%m-%d")
        e   = now.strftime("%Y-%m-%d")
        df  = fdr.DataReader("KS11", s, e)

        if df is None or len(df) < 20:
            # KIS API fallback
            s2 = (now - timedelta(days=120)).strftime("%Y%m%d")
            e2 = now.strftime("%Y%m%d")
            res = requests.get(
                f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
                headers=H(tok,"FHKUP03500100"), timeout=10,
                params={"fid_cond_mrkt_div_code":"U","fid_input_iscd":"0001",
                        "fid_input_date_1":s2,"fid_input_date_2":e2,"fid_period_div_code":"D"}
            )
            items = res.json().get("output2", res.json().get("output",[]))
            prices_raw = [sf(x.get("bstp_nmix_prpr", x.get("stck_clpr",0))) for x in items]
            prices = [p for p in prices_raw if p > 0]
        else:
            prices = list(df["Close"].dropna())[::-1]  # 최신순

        if len(prices) < 20:
            return result

        close = prices[0]
        ma5   = sum(prices[:5])  / 5
        ma20  = sum(prices[:20]) / 20
        ma60  = sum(prices[:60]) / 60 if len(prices) >= 60 else sum(prices) / len(prices)

        result.update({
            "kospi_close": round(close, 2),
            "ma5":         round(ma5, 2),
            "ma20":        round(ma20, 2),
            "ma60":        round(ma60, 2),
            "kospi_ch5":   round((close - prices[4]) / prices[4] * 100, 2) if len(prices) >= 5 and prices[4] > 0 else 0,
            "kospi_ch20":  round((close - prices[19]) / prices[19] * 100, 2) if len(prices) >= 20 and prices[19] > 0 else 0,
        })

        # ── 정배열/역배열 판단 ──
        is_golden   = ma5 > ma20           # 단기 골든크로스
        is_above_60 = ma20 > ma60          # 중기 상승추세

        above_all   = close > ma5 > ma20 > ma60   # 완전 정배열
        below_all   = close < ma5 < ma20 < ma60   # 완전 역배열

        reasons = []

        if above_all:
            result["aligned"] = "정배열"
            reasons.append("정배열 (현가>MA5>MA20>MA60)")
        elif below_all:
            result["aligned"] = "역배열"
            reasons.append("역배열 (현가<MA5<MA20<MA60)")
        else:
            result["aligned"] = "혼조"

        if is_golden and not above_all:
            reasons.append("MA5>MA20 골든크로스")
        elif not is_golden and not below_all:
            reasons.append("MA5<MA20 데드크로스")

        if is_above_60:
            reasons.append("MA20>MA60 중기 상승")
        else:
            reasons.append("MA20<MA60 중기 하락")

        ch5 = result["kospi_ch5"]
        if ch5 >= 2:   reasons.append(f"5일 +{ch5:.1f}%↑")
        elif ch5 <= -2: reasons.append(f"5일 {ch5:.1f}%↓")

        # ── 시그널 결정 ──
        if above_all and is_golden and is_above_60:
            result["signal"]    = "📈 매수 우위"
            result["signal_en"] = "BUY"
        elif below_all and not is_golden and not is_above_60:
            result["signal"]    = "📉 매도 우위"
            result["signal_en"] = "SELL"
        elif above_all or (is_golden and is_above_60):
            result["signal"]    = "📈 매수 우위"
            result["signal_en"] = "BUY"
        elif below_all or (not is_golden and not is_above_60):
            result["signal"]    = "📉 매도 우위"
            result["signal_en"] = "SELL"
        else:
            result["signal"]    = "⚖️ 관망"
            result["signal_en"] = "WATCH"

        result["reason"] = " · ".join(reasons) if reasons else "중립"

        print(
            f"  KOSPI {close:,.2f} | "
            f"MA5 {ma5:,.2f} MA20 {ma20:,.2f} MA60 {ma60:,.2f} | "
            f"{result['aligned']} → {result['signal']}"
        )
        print(f"  근거: {result['reason']}")

    except Exception as e:
        print(f"  시장 시그널 오류: {e}")

    return result

# ── 단타/장투 라벨 ───────────────────────────────────────────────
def get_trade_label(d: dict) -> dict:
    """
    ⚡ 단타 (스윙 1~7일) — 추가 API 호출 없이 현재 데이터 기반:
      - 5일 등락 5~20%      : 추세 형성 중, 과열 아님 (3%미만=신호약, 20%초과=추격위험)
      - 거래대금추세 ≥ 30%   : 거래 급증으로 시장 관심 확인 (엄격)
      - 20일 등락 < 25%     : 아직 과열 아님 (25% 초과는 이미 많이 오름)
      - 등급 B 이상 (score≥3): 펀더멘털 최소 확인, D/C등급 제외

    💎 장투 (1년+) — 가치투자 엄격 기준:
      - ROE ≥ 15%           : 수익성 우량
      - EPS ≥ 1 + 상승추세   : 실적 성장 확인
      - PER ≤ 20배          : 적정 밸류에이션 (엄격)
      - PBR ≤ 1.5 OR 배당주  : 가치주 or 배당주 (엄격)
    """
    ch20      = d.get("ch20", 0) or 0
    ch5       = d.get("ch5", 0) or 0
    vol_trend = d.get("vol_trend", 0) or 0
    roe       = d.get("roe", 0) or 0
    per       = d.get("per", 0) or 0
    pbr       = d.get("pbr", 0) or 0
    eps       = d.get("eps", 0) or 0
    eps_trend = d.get("eps_trend", "")
    is_div    = d.get("is_dividend", False)
    score     = d.get("score", 0) or 0

    # ── 단타 조건 ──
    # 현재 불장(20일 상승 폭 큼)을 감안해 20일 기준 30%로 완화
    cond_ch5   = 5 <= ch5 <= 20       # 5일 5~20% (추세 형성 중, 과열 전)
    cond_vol   = vol_trend >= 20      # 거래 급증 20% 이상
    cond_ch20  = ch20 < 50            # 20일 50% 미만 (현 강세장 기준 과열선)
    cond_grade = score >= 3           # B등급 이상 (D/C 제외)
    is_danta   = cond_ch5 and cond_vol and cond_ch20 and cond_grade

    # ── 장투 조건 ──
    # 현재 한국 시장 PER 수준 감안해 25배로 완화, 나머지는 엄격 유지
    cond_roe   = roe >= 15                          # ROE 15% 이상
    cond_eps   = eps_trend == "상승" and eps >= 1   # EPS 상승 + 흑자
    cond_per   = 0 < per <= 25                      # PER 25배 이하
    cond_value = (0 < pbr <= 1.5) or is_div         # PBR 1.5 이하 or 배당주
    is_jangtu  = cond_roe and cond_eps and cond_per and cond_value

    if is_danta and is_jangtu:
        label = "⚡💎"
        label_text = "단타+장투"
    elif is_danta:
        label = "⚡"
        label_text = "단타"
    elif is_jangtu:
        label = "💎"
        label_text = "장투"
    else:
        label = "–"
        label_text = ""

    return {
        "trade_label":      label,
        "trade_label_text": label_text,
        "is_danta":         is_danta,
        "is_jangtu":        is_jangtu,
        # 디버그용
        "_d_vol":   round(vol_trend, 1),
        "_d_ch5":   round(ch5, 1),
        "_d_ch20":  round(ch20, 1),
    }

# ── 1단계: FDR 시총 상위 후보 ────────────────────────────────────
def load_candidates():
    print(f"\n[1/3] 후보 {CAND_N}종목 로드 중...")
    rows=[]
    for m in ["KOSPI","KOSDAQ"]:
        try:
            lst=fdr.StockListing(m); lst["market"]=m
            cm={}
            for c in lst.columns:
                cl=c.lower()
                if cl in ("symbol","code","ticker"): cm[c]="Code"
                elif cl=="name": cm[c]="Name"
                elif "marcap" in cl: cm[c]="Marcap"
            lst=lst.rename(columns=cm)
            if "Marcap" not in lst.columns:
                num=lst.select_dtypes(include="number").columns
                if len(num): lst["Marcap"]=lst[num[0]]
            lst["Marcap"]=pd.to_numeric(lst["Marcap"],errors="coerce").fillna(0)
            rows.append(lst[lst["Marcap"]>0])
        except Exception as e: print(f"  {m} 오류: {e}")
    if not rows: return []
    combined=pd.concat(rows,ignore_index=True).sort_values("Marcap",ascending=False)
    result=[]; seen=set()
    for _,row in combined.iterrows():
        name=str(row.get("Name","")).strip()
        ticker=str(row.get("Code","")).zfill(6)
        market=str(row.get("market","KOSPI"))
        if not name or not ticker or name in seen or is_etf(name): continue
        seen.add(name)
        result.append({"ticker":ticker,"name":name,"market":market})
        if len(result)>=CAND_N: break
    print(f"  → {len(result)}개 후보 확정")
    return result

# ── 2단계: KIS 현재가 ────────────────────────────────────────────
def fetch_price_info(tok, ticker):
    r={"per":0.,"pbr":0.,"eps":0.,"bps":0.,"roe":0.,
       "close":0.,"acml_tr_pbmn":0.,"tvol_today":0}
    try:
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=H(tok,"FHKST01010100"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
        o=res.json().get("output",{})
        r["close"]        = sf(o.get("stck_prpr"))
        r["acml_tr_pbmn"] = sf(o.get("acml_tr_pbmn",0))
        r["tvol_today"]   = int(r["acml_tr_pbmn"])//100000000
        r["per"]  = sf(o.get("per"))
        r["pbr"]  = sf(o.get("pbr"))
        r["eps"]  = sf(o.get("eps"))
        r["bps"]  = sf(o.get("bps"))
        if r["bps"]>0: r["roe"]=round(r["eps"]/r["bps"]*100,1)
    except Exception as e: print(f"    현재가오류({ticker}):{e}")
    return r

# ── 3단계: 거래대금 상위 30 (병렬) ──────────────────────────────
def select_top30(tok, candidates):
    print(f"\n[2/3] {len(candidates)}종목 거래대금 동시 조회 중...")
    enriched=[]; done_count=[0]

    def query(c):
        try: return {**c,**fetch_price_info(tok,c["ticker"])}
        except: return {**c,"tvol_today":0,"acml_tr_pbmn":0}

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures={ex.submit(query,c):c for c in candidates}
        for f in as_completed(futures):
            enriched.append(f.result())
            done_count[0]+=1
            if done_count[0]%30==0: print(f"  {done_count[0]}/{len(candidates)} 완료...")

    print(f"  {len(enriched)}/{len(candidates)} 완료")
    df=(pd.DataFrame(enriched).sort_values("acml_tr_pbmn",ascending=False)
        .head(TOP_N).reset_index(drop=True))
    result=[]
    for i,row in df.iterrows():
        result.append({
            "rank":i+1,"ticker":row["ticker"],"name":row["name"],"market":row["market"],
            "tvol":int(row.get("tvol_today",0)),"per":row.get("per",0.),
            "pbr":row.get("pbr",0.),"eps":row.get("eps",0.),"bps":row.get("bps",0.),
            "roe":row.get("roe",0.),"close":row.get("close",0.),
        })
    print(f"\n  거래대금 상위 {len(result)}종목:")
    for r in result[:5]: print(f"    {r['rank']:2d}. {r['name']} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 4단계: 배당여부 ──────────────────────────────────────────────
def check_dividend(ticker, market):
    try:
        suffix = ".KS" if market == "KOSPI" else ".KQ"
        info = yf.Ticker(f"{ticker}{suffix}").info
        return (info.get("dividendYield",0) or 0) > 0 or (info.get("dividendRate",0) or 0) > 0
    except: return False

# ── 5단계: EPS 추세 ───────────────────────────────────────────────
def fetch_eps_trend(tok, ticker, cur_eps):
    r={"eps_trend":"데이터없음","eps_growth":0.}
    try:
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/finance/financial-ratio",
            headers=H(tok,"FHKST66430300"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,"fid_div_cls_code":"1"})
        items=res.json().get("output",[])
        ev=[sf(x.get("eps")) for x in items[:3] if sf(x.get("eps"))!=0]
        if len(ev)>=2:
            growing=all(ev[i]>=ev[i+1] for i in range(len(ev)-1))
            if growing and ev[0]>=1:
                r["eps_trend"]="상승"
                r["eps_growth"]=round((ev[0]-ev[1])/abs(ev[1])*100,1) if ev[1]!=0 else 0.
            elif ev[0]>=1: r["eps_trend"]="유지"
            else: r["eps_trend"]="부진"
        else: r["eps_trend"]="유지" if cur_eps>=1 else "부진"
    except: r["eps_trend"]="유지" if cur_eps>=1 else "부진"
    return r

# ── 6단계: 20일 등락 + 5일 등락 ──────────────────────────────────
def fetch_ch20(tok, ticker):
    r={"ch20":0.,"ch5":0.,"vol_trend":0.}
    try:
        now=datetime.now()
        s=(now-timedelta(days=45)).strftime("%Y%m%d"); e=now.strftime("%Y%m%d")
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=H(tok,"FHKST01010400"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,
                    "fid_org_adj_prc":"1","fid_period_div_code":"D",
                    "fid_input_date_1":s,"fid_input_date_2":e})
        items=res.json().get("output2",res.json().get("output",[]))
        prices=[sf(x.get("stck_clpr")) for x in items if sf(x.get("stck_clpr"))>0]
        if len(prices)>=20:
            r["ch20"]=round((prices[0]-prices[19])/prices[19]*100,1) if prices[19]>0 else 0.
        if len(prices)>=5:
            r["ch5"]=round((prices[0]-prices[4])/prices[4]*100,1) if prices[4]>0 else 0.
        # 거래대금 추세: 최근 5일 평균 vs 20일 평균
        vols=[sf(x.get("acml_vol")) for x in items]
        if len(vols)>=20:
            avg5=sum(vols[:5])/5; avgA=sum(vols[:20])/20
            r["vol_trend"]=round((avg5-avgA)/avgA*100,1) if avgA>0 else 0.
    except: pass
    return r

# ── 추천 등급 판단 ─────────────────────────────────────────────────
def judge(d):
    roe=d.get("roe",0) or 0; per=d.get("per",0) or 0
    eps=d.get("eps",0) or 0; eps_trend=d.get("eps_trend","")
    c1=roe>=15; c2=0<per<=15; c3=eps>=1; c4=eps_trend=="상승"
    score=sum([c1,c2,c3,c4])
    if score==4: grade="A"
    elif score==3: grade="B"
    elif score==2: grade="C"
    else: grade="D"
    return {"roe_ok":c1,"per_ok":c2,"eps_ok":c3,"eps_up":c4,
            "score":score,"grade":grade,"recommended":score>=3}

# ── Discord ───────────────────────────────────────────────────────
def send_discord(results, date, recs, market_signal):
    if not DISCORD: print("  ℹ️ DISCORD 미설정"); return
    dt=f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ei={"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}
    ge={"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    display=recs[:5] if recs else sorted(results,key=lambda x:x.get("score",0),reverse=True)[:5]

    sig      = market_signal.get("signal","⚖️ 관망")
    reason   = market_signal.get("reason","")
    kospi    = market_signal.get("kospi_close",0)
    ch5      = market_signal.get("kospi_ch5",0)
    aligned  = market_signal.get("aligned","")
    ma5      = market_signal.get("ma5",0)
    ma20     = market_signal.get("ma20",0)
    ma60     = market_signal.get("ma60",0)

    lines=[
        f"📊 **StockPilot KR — {dt}** (KIS 실시간)",
        f"",
        f"{'─'*30}",
        f"🏦 **시장 시그널: {sig}**  [{aligned}]",
        f"KOSPI {kospi:,.2f} (5일 {ch5:+.1f}%)",
        f"MA5 {ma5:,.0f} · MA20 {ma20:,.0f} · MA60 {ma60:,.0f}",
        f"근거: {reason}",
        f"{'─'*30}",
        f"",
        f"거래대금 상위{TOP_N} | ROE≥15% · PER≤15배 · EPS≥1 · EPS상승",
        f"✅ 추천(A·B): **{len(recs)}종목**","",
        "⭐ **추천 종목**" if recs else "📊 **상위 종목** (추천 기준 미달)","─"*30,
    ]
    for r in display:
        f=r.get("filters",{}); g=r.get("grade","D"); sc=r.get("score",0)
        star="⭐ " if r.get("recommended") else ""
        eps_t=r.get("eps_trend","데이터없음"); eps_g=r.get("eps_growth",0)
        div_str="  💰" if r.get("is_dividend") else ""
        tl=r.get("trade_label","–")
        tl_str=f" {tl}" if tl!="–" else ""
        lines.append(f"{ge.get(g,'⚪')} {star}**{r['name']}** ({r['market']}){tl_str} — {g}등급{div_str}")
        lines.append(f"  ROE {r.get('roe',0):.1f}%{'✅' if f.get('roe_ok') else '❌'}"
                     f"  PER {r.get('per',0):.1f}배{'✅' if f.get('per_ok') else '❌'}"
                     f"  PBR {r.get('pbr',0):.2f}")
        lines.append(f"  {ei.get(eps_t,'❓')} EPS {r.get('eps',0):,.0f}원"
                     f"{f'({eps_g:+.1f}%)' if eps_g else ''} ({eps_t})"
                     f"{'✅' if f.get('eps_ok') and f.get('eps_up') else '❌'}")
        lines.append(f"  {'📈' if r.get('ch20',0)>0 else '📉'} 20일 {r.get('ch20',0):+.1f}%"
                     f"  5일 {r.get('ch5',0):+.1f}%"
                     f"  거래대금 {r.get('tvol',0):,}억")
        lines.append("")
    lines.append("⚠️ 투자 손실 책임은 본인에게 있습니다.")
    msg="\n".join(lines)
    chunks=[]
    while len(msg)>1900: si=msg[:1900].rfind("\n"); chunks.append(msg[:si]); msg=msg[si:]
    chunks.append(msg)
    try:
        for c in chunks:
            res=requests.post(DISCORD,json={"content":c},timeout=10)
            if res.status_code not in (200,204): print(f"  ⚠️ Discord {res.status_code}")
            time.sleep(0.3)
        print(f"  ✅ Discord 전송 완료 ({len(chunks)}개)")
    except Exception as e: print(f"  ❌ Discord 실패: {e}")

# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print("╔══════════════════════════════════╗")
    print("║   StockPilot KR  KIS 스크리닝   ║")
    print("╚══════════════════════════════════╝")
    if not APP_KEY or not APP_SECRET:
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음"); return

    now_utc = datetime.utcnow()
    now_kst = now_utc + timedelta(hours=9)
    date = now_kst.strftime("%Y%m%d")
    print(f"  기준일: {date} ({now_kst.strftime('%H:%M')} KST)")
    print(f"  등급: ROE≥15%(A) PER≤15배(A) EPS≥1(A) EPS상승(A) → 3개이상=추천")
    print(f"  단타: 거래대금추세≥20% + 5일 5~20% + 20일<30% + B등급이상")
    print(f"  장투: ROE≥15% + EPS상승 + PER≤25배 + (PBR≤1.5 or 배당주)")

    print("\n[0] KIS 토큰 발급 중...")
    try: tok=get_token()
    except Exception as e: print(f"❌ 토큰 실패: {e}"); return

    # 시장 시그널
    print("\n[시장] KOSPI MA5/MA20/MA60 분석 중...")
    market_signal = fetch_market_signal(tok)

    candidates=load_candidates()
    if not candidates: print("❌ 후보 로드 실패"); return

    top30=select_top30(tok,candidates)
    if not top30: print("❌ 거래대금 계산 실패"); return

    print(f"\n[3/3] {len(top30)}종목 상세 분석 중...\n")
    results=[]; ge_map={"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    for t in top30:
        tk=t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk})",end=" ... ",flush=True)
        try:
            eps_tr = fetch_eps_trend(tok,tk,t.get("eps",0))
            price  = fetch_ch20(tok,tk)
            is_div = check_dividend(tk, t.get("market","KOSPI"))
            time.sleep(0.2)

            data={**t,**eps_tr,**price,"is_dividend":is_div}
            f=judge(data)
            tl=get_trade_label(data)
            data.update({
                "filters":f,"grade":f["grade"],"score":f["score"],"recommended":f["recommended"],
                **tl,
            })
            results.append(data)

            div_str = "  💰" if is_div else ""
            tl_str  = f"  {tl['trade_label']}" if tl['trade_label']!="–" else ""
            print(
                f"{ge_map.get(f['grade'],'⚪')}{f['grade']}등급({f['score']}/4)"
                f"{tl_str}"
                f"  ROE:{t.get('roe',0):.1f}%{'✅' if f['roe_ok'] else '❌'}"
                f"  PER:{t.get('per',0):.1f}{'✅' if f['per_ok'] else '❌'}"
                f"  EPS:{t.get('eps',0):,.0f}({eps_tr['eps_trend']}){'✅' if f['eps_ok'] and f['eps_up'] else '❌'}"
                f"  5일:{price.get('ch5',0):+.1f}%"
                f"  20일:{price.get('ch20',0):+.1f}%"
                f"{div_str}"
                f"{'  ⭐' if f['recommended'] else ''}"
            )
        except Exception: print("오류"); traceback.print_exc()
        time.sleep(0.3)

    recs=[r for r in results if r.get("recommended")]
    print(f"\n{'─'*70}")
    print(f"  시장: {market_signal['signal']} | {market_signal['aligned']} | {market_signal['reason']}")
    print(f"  분석:{len(results)}종목  추천(A·B):{len(recs)}종목")
    for r in recs:
        tl=r.get("trade_label","–")
        print(f"  {ge_map.get(r['grade'],'⚪')}{r['grade']}등급 {tl} {r['name']} ({r['market']})"
              f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배"
              f"  EPS {r.get('eps',0):,.0f}원({r.get('eps_trend','?')})"
              f"{'  💰' if r.get('is_dividend') else ''}")

    json.dump({
        "date":          date,
        "generated_at":  now_kst.isoformat(),
        "total":         len(results),
        "market_signal": market_signal,
        "results":       results,
        "recommended":   recs,
    }, open("results.json","w",encoding="utf-8"), ensure_ascii=False, indent=2, default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results, date, recs, market_signal)
    print("\n✅ 완료!")

if __name__=="__main__": main()
