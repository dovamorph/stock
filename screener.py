"""
StockPilot KR — KIS OpenAPI 스크리닝
거래대금: FDR 시총 상위 100 → KIS inquire-price acml_tr_pbmn → 상위 30 정렬
재무: KIS API (PER/PBR/ROE/EPS/배당/외국인/PSR/EPS추세/20일등락)
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import requests, pandas as pd
    import FinanceDataReader as fdr
except ImportError:
    print("pip install requests pandas finance-datareader"); exit(1)

APP_KEY    = os.environ.get("KIS_APP_KEY","")
APP_SECRET = os.environ.get("KIS_APP_SECRET","")
DISCORD    = os.environ.get("DISCORD_WEBHOOK","")
BASE       = "https://openapi.koreainvestment.com:9443"
TOP_N      = 30
CAND_N     = 100  # 후보 종목 수

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

# ── 1단계: FDR 시총 상위 100 후보 ────────────────────────────────
def load_candidates():
    print(f"\n[1/4] 후보 {CAND_N}종목 로드 중 (FDR 시총 기준)...")
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
            lst=lst[lst["Marcap"]>0]
            rows.append(lst)
            print(f"  {m}: {len(lst)}종목")
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

# ── 2단계: KIS 현재가 조회 (거래대금 포함) ───────────────────────
def fetch_price_full(tok, ticker):
    """KIS inquire-price: 현재가 + 누적거래대금 + PER/PBR/EPS/배당"""
    r={"per":0.,"pbr":0.,"eps":0.,"bps":0.,"div":0.,"roe":0.,
       "close":0.,"acml_tr_pbmn":0.,"tvol_today":0}
    try:
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=H(tok,"FHKST01010100"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
        o=res.json().get("output",{})
        r["close"]        = sf(o.get("stck_prpr"))
        r["acml_tr_pbmn"] = sf(o.get("acml_tr_pbmn",0))  # 누적 거래대금 (원)
        r["tvol_today"]   = int(r["acml_tr_pbmn"])//100000000  # 억원
        r["per"]  = sf(o.get("per"))
        r["pbr"]  = sf(o.get("pbr"))
        r["eps"]  = sf(o.get("eps"))
        r["bps"]  = sf(o.get("bps"))
        r["div"]  = sf(o.get("d_rate"))
        if r["bps"]>0: r["roe"]=round(r["eps"]/r["bps"]*100,1)
    except Exception as e: print(f"    현재가오류({ticker}):{e}")
    return r

# ── 3단계: 거래대금 기준 상위 30 선정 ────────────────────────────
def select_top30(tok, candidates):
    print(f"\n[2/4] {len(candidates)}종목 거래대금 조회 중...")
    enriched=[]
    for i,c in enumerate(candidates):
        try:
            info=fetch_price_full(tok,c["ticker"])
            enriched.append({**c,**info})
        except: enriched.append({**c,"tvol_today":0,"acml_tr_pbmn":0})
        if (i+1)%10==0: print(f"  {i+1}/{len(candidates)} 조회 완료...")
        time.sleep(0.05)

    df=(pd.DataFrame(enriched)
        .sort_values("acml_tr_pbmn",ascending=False)
        .head(TOP_N).reset_index(drop=True))

    result=[]
    for i,row in df.iterrows():
        result.append({
            "rank":     i+1,
            "ticker":   row["ticker"],
            "name":     row["name"],
            "market":   row["market"],
            "tvol":     int(row.get("tvol_today",0)),
            "per":      row.get("per",0.),
            "pbr":      row.get("pbr",0.),
            "eps":      row.get("eps",0.),
            "bps":      row.get("bps",0.),
            "div":      row.get("div",0.),
            "roe":      row.get("roe",0.),
            "close":    row.get("close",0.),
        })
    print(f"\n  거래대금 상위 {len(result)}종목:")
    for r in result[:5]: print(f"    {r['rank']:2d}. {r['name']} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 4단계: 외국인 순매수 ──────────────────────────────────────────
def fetch_fgn(tok, ticker):
    r={"foreign_net":0,"foreign_ok":False}
    try:
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=H(tok,"FHKST01010900"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
        items=res.json().get("output",[])
        if items:
            net=sf(items[0].get("frgn_ntby_qty",0))
            r["foreign_net"]=int(net); r["foreign_ok"]=net>0
    except Exception as e: print(f"    외국인({ticker}):{e}")
    return r

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

# ── 6단계: PSR ────────────────────────────────────────────────────
def fetch_psr(tok, ticker, close):
    try:
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/finance/income-statement",
            headers=H(tok,"FHKST66430200"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,"fid_div_cls_code":"1"})
        items=res.json().get("output",[])
        if items:
            rev=sf(items[0].get("sale_account",0))
            if rev>0:
                res2=requests.get(f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=H(tok,"FHKST01010100"),timeout=10,
                    params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
                shares=sf(res2.json().get("output",{}).get("lstn_stcn",0))
                if shares>0 and close>0:
                    return round((close*shares)/(rev*1_000_000),2)
    except: pass
    return 0.

# ── 7단계: 20일 등락 ──────────────────────────────────────────────
def fetch_ch20(tok, ticker):
    r={"ch20":0.,"vol_trend":0.}
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
            vols=[sf(x.get("acml_vol")) for x in items[:20]]
            avg5=sum(vols[:5])/5 if vols[:5] else 0
            avgA=sum(vols)/len(vols) if vols else 0
            r["vol_trend"]=round((avg5-avgA)/avgA*100,1) if avgA>0 else 0.
    except: pass
    return r

# ── 추천 판단 ─────────────────────────────────────────────────────
def judge(d):
    per=d.get("per",0) or 0; pbr=d.get("pbr",0) or 0
    roe=d.get("roe",0) or 0; eps=d.get("eps",0) or 0
    psr=d.get("psr",0) or 0; div=d.get("div",0) or 0
    eps_trend=d.get("eps_trend","")
    checks={
        "roe_ok":  roe>=15,
        "per_ok":  0<per<=15,
        "pbr_ok":  0<pbr<=1.5,
        "psr_ok":  psr==0 or psr<=3,
        "psr_good":0<psr<=1.5,
        "eps_ok":  eps>=1,
        "eps_up":  eps_trend=="상승",
        "div_ok":  div>=3,
        "fgn_ok":  d.get("foreign_ok",False),
        "momentum":(d.get("ch20") or 0)>=20,
    }
    core=[checks["roe_ok"],checks["per_ok"],checks["eps_ok"],checks["eps_up"],checks["psr_ok"]]
    if pbr>0: core.append(checks["pbr_ok"])
    pc=sum(core); tot=len(core)
    return {**checks,"pass_count":pc,"total":tot,"recommended":pc>=tot-1}

# ── Discord ───────────────────────────────────────────────────────
def send_discord(results, date, recs):
    if not DISCORD: print("  ℹ️ DISCORD 미설정"); return
    dt=f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ei={"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}
    display=recs[:5] if recs else sorted(results,key=lambda x:x.get("pass_count",0),reverse=True)[:5]
    lines=[
        f"📊 **StockPilot KR — {dt}** (KIS 실시간)",
        f"거래대금 상위{TOP_N} | ROE≥15% · PER≤15배 · PBR≤1.5 · PSR≤3배 · EPS상승",
        f"✅ 추천: **{len(recs)}종목**","",
        "⭐ **추천 종목**" if recs else "📊 **조건 상위 종목** (추천 미충족)","─"*30,
    ]
    for r in display:
        f=r.get("filters",{}); pc=r.get("pass_count",0); tot=r.get("total",5)
        star="⭐ " if r.get("recommended") else ""
        eps_t=r.get("eps_trend","데이터없음"); eps_g=r.get("eps_growth",0)
        lines.append(f"{star}**{r['name']}** ({r['market']}) — 조건 {pc}/{tot} 충족")
        lines.append(f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  PBR {r.get('pbr',0):.2f}  PSR {r.get('psr',0):.1f}배  배당 {r.get('div',0):.1f}%")
        lines.append(f"  {ei.get(eps_t,'❓')} EPS {r.get('eps',0):,.0f}원{f'({eps_g:+.1f}%)' if eps_g else ''} ({eps_t})  외국인{'✅' if f.get('fgn_ok') else '❌'}")
        lines.append(f"  {'📈' if r.get('ch20',0)>0 else '📉'} 20일 {r.get('ch20',0):+.1f}%  거래대금 {r.get('tvol',0):,}억")
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

    date=datetime.now().strftime("%Y%m%d")
    print(f"  기준일: {date} ({datetime.now().strftime('%H:%M')} KST)")

    print("\n[0] KIS 토큰 발급 중...")
    try: tok=get_token()
    except Exception as e: print(f"❌ 토큰 실패: {e}"); return

    # 1) FDR 후보 로드
    candidates=load_candidates()
    if not candidates:
        print("❌ 후보 로드 실패"); return

    # 2) KIS 현재가로 거래대금 계산 → 상위 30 선정
    top30=select_top30(tok,candidates)
    if not top30:
        print("❌ 거래대금 계산 실패"); return

    # 3) 상세 분석
    print(f"\n[3/4] {len(top30)}종목 상세 분석 중...\n")
    results=[]
    for t in top30:
        tk=t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk})",end=" ... ",flush=True)
        try:
            fgn=fetch_fgn(tok,tk)
            eps_tr=fetch_eps_trend(tok,tk,t.get("eps",0))
            price=fetch_ch20(tok,tk)
            psr=fetch_psr(tok,tk,t.get("close",0))
            time.sleep(0.2)
            data={**t,**fgn,**eps_tr,**price,"psr":psr}
            f=judge(data)
            data.update({"filters":f,"recommended":f["recommended"],
                         "pass_count":f["pass_count"],"total":f["total"]})
            results.append(data)
            print(
                f"조건{f['pass_count']}/{f['total']}  "
                f"ROE:{t.get('roe',0):.1f}%  PER:{t.get('per',0):.1f}  "
                f"PBR:{t.get('pbr',0):.2f}  PSR:{psr:.1f}  "
                f"EPS:{t.get('eps',0):,.0f}({eps_tr.get('eps_trend','?')})  "
                f"배당:{t.get('div',0):.1f}%  "
                f"외국인{'✅' if fgn.get('foreign_ok') else '❌'}"
                f"{'  ⭐' if f['recommended'] else ''}"
            )
        except Exception: print("오류"); traceback.print_exc()
        time.sleep(0.3)

    recs=[r for r in results if r.get("recommended")]
    print(f"\n{'─'*70}\n  분석:{len(results)}종목  추천:{len(recs)}종목")
    for r in recs:
        print(f"  ⭐ {r['name']} ({r['market']}) 조건{r.get('pass_count',0)}/{r.get('total',5)}"
              f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배"
              f"  EPS {r.get('eps',0):,.0f}원({r.get('eps_trend','?')})  배당 {r.get('div',0):.1f}%")

    json.dump({"date":date,"generated_at":datetime.now().isoformat(),
               "total":len(results),"results":results,"recommended":recs},
              open("results.json","w",encoding="utf-8"),ensure_ascii=False,indent=2,default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results,date,recs)
    print("\n✅ 완료!")

if __name__=="__main__": main()

