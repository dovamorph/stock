"""
StockPilot KR — KIS OpenAPI 스크리닝
구현된 지표: 거래대금 상위30 / ROE / PER / PBR / EPS / PSR / 배당 / 외국인순매수 / 20일등락
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import requests, pandas as pd
except ImportError:
    print("pip install requests pandas"); exit(1)

APP_KEY    = os.environ.get("KIS_APP_KEY","")
APP_SECRET = os.environ.get("KIS_APP_SECRET","")
DISCORD    = os.environ.get("DISCORD_WEBHOOK","")
BASE       = "https://openapi.koreainvestment.com:9443"
TOP_N      = 30

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

# ── 1단계: 거래대금 상위 30 ──────────────────────────────────────
def fetch_top30(tok):
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회...")
    stocks=[]

    for params, desc in [
        # 시도 1: 전체 시장
        ({"fid_cond_mrkt_div_code":"J","fid_cond_scr_div_code":"20171",
          "fid_input_iscd":"0000","fid_div_cls_code":"1","fid_blng_cls_code":"0",
          "fid_trgt_cls_code":"111111111","fid_trgt_exls_cls_code":"000000",
          "fid_input_price_1":"","fid_input_price_2":"","fid_vol_cnt":"","fid_input_date_1":""}, "전체"),
        # 시도 2: KOSPI만
        ({"fid_cond_mrkt_div_code":"J","fid_cond_scr_div_code":"20171",
          "fid_input_iscd":"0001","fid_div_cls_code":"1","fid_blng_cls_code":"0",
          "fid_trgt_cls_code":"111111111","fid_trgt_exls_cls_code":"000000",
          "fid_input_price_1":"","fid_input_price_2":"","fid_vol_cnt":"","fid_input_date_1":""}, "KOSPI"),
    ]:
        try:
            r=requests.get(f"{BASE}/uapi/domestic-stock/v1/ranking/volume",
                headers=H(tok,"FHPST01710000"),params=params,timeout=15)
            # 원시 응답 확인
            print(f"  [{desc}] HTTP {r.status_code}, 응답길이={len(r.text)}")
            if r.status_code!=200:
                print(f"  응답내용: {r.text[:200]}")
                continue
            try:
                data=r.json()
            except Exception as je:
                print(f"  JSON파싱 실패: {je}")
                print(f"  응답내용: {r.text[:200]}")
                continue

            rc=data.get("rt_cd","?")
            print(f"  rt_cd={rc} msg={data.get('msg1','')[:60]}")

            if rc=="0":
                items=data.get("output",[])
                for item in items:
                    name=str(item.get("hts_kor_isnm","")).strip()
                    ticker=str(item.get("mksc_shrn_iscd","")).strip()
                    tvol=sf(item.get("acml_tr_pbmn",0))
                    if not name or not ticker or is_etf(name): continue
                    mkt="KOSPI"
                    stocks.append({"ticker":ticker,"name":name,"market":mkt,"tvol":int(tvol)//100000000})
                print(f"  [{desc}] {len(stocks)}종목 수집 완료")
                if stocks: break
            else:
                print(f"  [{desc}] 실패: rt_cd={rc}")
        except Exception as e:
            print(f"  [{desc}] 오류: {e}")
        time.sleep(0.5)

    # KOSDAQ 추가 시도
    if stocks:
        try:
            r=requests.get(f"{BASE}/uapi/domestic-stock/v1/ranking/volume",
                headers=H(tok,"FHPST01710000"),timeout=15,
                params={"fid_cond_mrkt_div_code":"J","fid_cond_scr_div_code":"20171",
                        "fid_input_iscd":"1001","fid_div_cls_code":"1","fid_blng_cls_code":"0",
                        "fid_trgt_cls_code":"111111111","fid_trgt_exls_cls_code":"000000",
                        "fid_input_price_1":"","fid_input_price_2":"","fid_vol_cnt":"","fid_input_date_1":""})
            if r.status_code==200:
                data=r.json()
                if data.get("rt_cd")=="0":
                    for item in data.get("output",[]):
                        name=str(item.get("hts_kor_isnm","")).strip()
                        ticker=str(item.get("mksc_shrn_iscd","")).strip()
                        tvol=sf(item.get("acml_tr_pbmn",0))
                        if not name or not ticker or is_etf(name): continue
                        stocks.append({"ticker":ticker,"name":name,"market":"KOSDAQ","tvol":int(tvol)//100000000})
                    print(f"  KOSDAQ 추가: 총 {len(stocks)}종목")
        except Exception as e:
            print(f"  KOSDAQ 추가 오류: {e}")

    if not stocks:
        print("  ❌ 거래대금 데이터 없음 — KIS API 접근 실패")
        return []

    df=(pd.DataFrame(stocks).drop_duplicates("ticker")
        .sort_values("tvol",ascending=False).head(TOP_N).reset_index(drop=True))
    result=[{"rank":i+1,"ticker":r.ticker,"name":r.name,"market":r.market,"tvol":int(r.tvol)}
             for i,r in df.iterrows()]
    print(f"\n  거래대금 상위 {len(result)}종목 확정:")
    for r in result[:5]: print(f"    {r['rank']:2d}. {r['name']} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 2단계: 현재가 조회 ────────────────────────────────────────────
def fetch_price(tok, ticker):
    r={"per":0.,"pbr":0.,"eps":0.,"bps":0.,"div":0.,"roe":0.,"close":0.}
    try:
        res=requests.get(f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=H(tok,"FHKST01010100"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
        o=res.json().get("output",{})
        r["close"]=sf(o.get("stck_prpr")); r["per"]=sf(o.get("per"))
        r["pbr"]=sf(o.get("pbr")); r["eps"]=sf(o.get("eps"))
        r["bps"]=sf(o.get("bps")); r["div"]=sf(o.get("d_rate"))
        if r["bps"]>0: r["roe"]=round(r["eps"]/r["bps"]*100,1)
    except Exception as e: print(f"    현재가:{e}")
    return r

# ── 3단계: 외국인 순매수 ──────────────────────────────────────────
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
    except Exception as e: print(f"    외국인:{e}")
    return r

# ── 4단계: EPS 추세 ───────────────────────────────────────────────
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

# ── 5단계: PSR ────────────────────────────────────────────────────
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

# ── 6단계: 20일 등락 ──────────────────────────────────────────────
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
        "roe_ok":  roe>=15,            # ROE 15% 이상
        "per_ok":  0<per<=15,          # PER 15배 이하
        "pbr_ok":  0<pbr<=1.5,         # PBR 1.5배 이하
        "psr_ok":  psr==0 or psr<=3,   # PSR 3배 이하 (없으면 통과)
        "psr_good":0<psr<=1.5,         # PSR 1.5배 이하 선호
        "eps_ok":  eps>=1,             # EPS 1원 이상
        "eps_up":  eps_trend=="상승",   # EPS 상승추세
        "div_ok":  div>=3,             # 배당 3% 이상 선호
        "fgn_ok":  d.get("foreign_ok",False),  # 외국인 순매수
        "momentum":(d.get("ch20") or 0)>=20,   # 20일 20% 이상
    }
    # 핵심 조건 (데이터 있는 것만)
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
    print(f"  필터: ROE≥15% | PER≤15배 | PBR≤1.5 | PSR≤3배 | EPS상승 | 배당≥3% 선호")

    print("\n[0/4] KIS 토큰 발급 중...")
    try: tok=get_token()
    except Exception as e: print(f"❌ 토큰 실패: {e}"); return

    top30=fetch_top30(tok)
    if not top30:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"거래대금 데이터 없음"},
                  open("results.json","w",encoding="utf-8"),ensure_ascii=False)
        return

    print(f"\n[2/4] {len(top30)}종목 분석 중...\n")
    results=[]
    for t in top30:
        tk=t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk})",end=" ... ",flush=True)
        try:
            info=fetch_price(tok,tk)
            fgn=fetch_fgn(tok,tk)
            eps_tr=fetch_eps_trend(tok,tk,info.get("eps",0))
            price=fetch_ch20(tok,tk)
            psr=fetch_psr(tok,tk,info.get("close",0))
            time.sleep(0.2)
            data={**t,**info,**fgn,**eps_tr,**price,"psr":psr}
            f=judge(data)
            data.update({"filters":f,"recommended":f["recommended"],
                         "pass_count":f["pass_count"],"total":f["total"]})
            results.append(data)
            print(
                f"조건{f['pass_count']}/{f['total']}  "
                f"ROE:{info.get('roe',0):.1f}%  PER:{info.get('per',0):.1f}  "
                f"PBR:{info.get('pbr',0):.2f}  PSR:{psr:.1f}  "
                f"EPS:{info.get('eps',0):,.0f}({eps_tr.get('eps_trend','?')})  "
                f"배당:{info.get('div',0):.1f}%  "
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
