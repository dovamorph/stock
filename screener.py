"""
StockPilot KR — screener.py (KIS OpenAPI)
거래대금 상위 30 / PER / PBR / ROE / EPS / PSR / 배당 / 외국인순매수 / 20일등락
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import requests, pandas as pd
except ImportError:
    print("pip install -r requirements.txt"); exit(1)

APP_KEY     = os.environ.get("KIS_APP_KEY","")
APP_SECRET  = os.environ.get("KIS_APP_SECRET","")
DISCORD_URL = os.environ.get("DISCORD_WEBHOOK","")
BASE_URL    = "https://openapi.koreainvestment.com:9443"

TOP_N=30; FILTER_ROE=15.0; FILTER_PER=15.0; FILTER_PBR=1.5
FILTER_PSR=3.0; FILTER_EPS=1.0; FILTER_DIV=3.0

def sf(v,d=0.0):
    try: val=float(str(v).replace(",","").strip() or 0); return d if val!=val else val
    except: return d

def get_token():
    r=requests.post(f"{BASE_URL}/oauth2/tokenP",
        json={"grant_type":"client_credentials","appkey":APP_KEY,"appsecret":APP_SECRET},timeout=10)
    r.raise_for_status(); print("  ✅ KIS 토큰 발급 완료"); return r.json().get("access_token","")

def hdr(token,tr_id):
    return {"Content-Type":"application/json","authorization":f"Bearer {token}",
            "appkey":APP_KEY,"appsecret":APP_SECRET,"tr_id":tr_id,"custtype":"P"}

ETF_NAMES=["ETF","ETN","KODEX","TIGER","레버리지","인버스","선물","RISE","ACE","KBSTAR","ARIRANG","HANARO","SOL","PLUS"]

def fetch_top30(token):
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회...")
    stocks=[]
    for iscd,mkt in [("0001","KOSPI"),("1001","KOSDAQ")]:
        try:
            r=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/ranking/volume",
                headers=hdr(token,"FHPST01710000"),timeout=10,
                params={"fid_cond_mrkt_div_code":"J","fid_cond_scr_div_code":"20171",
                        "fid_input_iscd":iscd,"fid_div_cls_code":"1","fid_blng_cls_code":"0",
                        "fid_trgt_cls_code":"111111111","fid_trgt_exls_cls_code":"000000",
                        "fid_input_price_1":"","fid_input_price_2":"","fid_vol_cnt":"","fid_input_date_1":""})
            data=r.json()
            if data.get("rt_cd")!="0": raise ValueError(data.get("msg1",""))
            cnt=0
            for item in data.get("output",[]):
                name=str(item.get("hts_kor_isnm","")).strip()
                ticker=str(item.get("mksc_shrn_iscd","")).strip()
                tvol=sf(item.get("acml_tr_pbmn",0))
                if not name or not ticker: continue
                if any(k in name for k in ETF_NAMES): continue
                stocks.append({"ticker":ticker,"name":name,"market":mkt,"tvol":int(tvol)//100000000})
                cnt+=1
            print(f"  {mkt}: {cnt}종목")
        except Exception as e:
            print(f"  {mkt} 오류: {e}")
        time.sleep(0.5)

    if not stocks: return []
    df=pd.DataFrame(stocks).drop_duplicates("ticker").sort_values("tvol",ascending=False).head(TOP_N).reset_index(drop=True)
    result=[{"rank":i+1,"ticker":r.ticker,"name":r.name,"market":r.market,"tvol":int(r.tvol)} for i,r in df.iterrows()]
    print(f"\n  상위 {len(result)}종목:")
    for r in result[:5]: print(f"    {r['rank']:2d}. {r['name']} ({r['market']}) — {r['tvol']:,}억")
    return result

def fetch_info(token,ticker):
    r={"per":0.,"pbr":0.,"eps":0.,"bps":0.,"div":0.,"roe":0.,"close":0.}
    try:
        res=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=hdr(token,"FHKST01010100"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
        o=res.json().get("output",{})
        r["close"]=sf(o.get("stck_prpr")); r["per"]=sf(o.get("per")); r["pbr"]=sf(o.get("pbr"))
        r["eps"]=sf(o.get("eps")); r["bps"]=sf(o.get("bps")); r["div"]=sf(o.get("d_rate"))
        if r["bps"]>0: r["roe"]=round(r["eps"]/r["bps"]*100,1)
    except Exception as e: print(f"    정보오류:{e}")
    return r

def fetch_foreign(token,ticker):
    r={"foreign_net":0,"foreign_ok":False}
    try:
        res=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=hdr(token,"FHKST01010900"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
        items=res.json().get("output",[])
        if items:
            net=sf(items[0].get("frgn_ntby_qty",0)); r["foreign_net"]=int(net); r["foreign_ok"]=net>0
    except Exception as e: print(f"    외국인오류:{e}")
    return r

def fetch_psr(token,ticker,close):
    try:
        res=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/finance/income-statement",
            headers=hdr(token,"FHKST66430200"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,"fid_div_cls_code":"1"})
        items=res.json().get("output",[])
        if items:
            rev=sf(items[0].get("sale_account",0))
            if rev>0:
                res2=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=hdr(token,"FHKST01010100"),timeout=10,
                    params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker})
                shares=sf(res2.json().get("output",{}).get("lstn_stcn",0))
                if shares>0 and close>0: return round((close*shares)/(rev*1_000_000),2)
    except: pass
    return 0.

def fetch_eps_trend(token,ticker,cur_eps):
    r={"eps_trend":"데이터없음","eps_growth":0.}
    try:
        res=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/finance/financial-ratio",
            headers=hdr(token,"FHKST66430300"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,"fid_div_cls_code":"1"})
        items=res.json().get("output",[])
        ev=[sf(x.get("eps")) for x in items[:3] if sf(x.get("eps"))!=0]
        if len(ev)>=2:
            growing=all(ev[i]>=ev[i+1] for i in range(len(ev)-1))
            if growing and ev[0]>=FILTER_EPS:
                gr=((ev[0]-ev[1])/abs(ev[1])*100) if ev[1]!=0 else 0
                r["eps_trend"]="상승"; r["eps_growth"]=round(gr,1)
            elif ev[0]>=FILTER_EPS: r["eps_trend"]="유지"
            else: r["eps_trend"]="부진"
        else: r["eps_trend"]="유지" if cur_eps>=FILTER_EPS else "부진"
    except: r["eps_trend"]="유지" if cur_eps>=FILTER_EPS else "부진"
    return r

def fetch_ch20(token,ticker):
    r={"ch20":0.,"vol_trend":0.}
    try:
        now=datetime.now(); start=(now-timedelta(days=45)).strftime("%Y%m%d"); end=now.strftime("%Y%m%d")
        res=requests.get(f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=hdr(token,"FHKST01010400"),timeout=10,
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,
                    "fid_org_adj_prc":"1","fid_period_div_code":"D",
                    "fid_input_date_1":start,"fid_input_date_2":end})
        items=res.json().get("output2",res.json().get("output",[]))
        prices=[sf(x.get("stck_clpr")) for x in items if sf(x.get("stck_clpr"))>0]
        if len(prices)>=20:
            r["ch20"]=round((prices[0]-prices[19])/prices[19]*100,1) if prices[19]>0 else 0.
            vols=[sf(x.get("acml_vol")) for x in items[:20]]
            avg5=sum(vols[:5])/5 if vols[:5] else 0; avgA=sum(vols)/len(vols) if vols else 0
            r["vol_trend"]=round((avg5-avgA)/avgA*100,1) if avgA>0 else 0.
    except Exception as e: print(f"    20일오류:{e}")
    return r

def judge(d):
    per=d.get("per",0) or 0; pbr=d.get("pbr",0) or 0; roe=d.get("roe",0) or 0
    eps=d.get("eps",0) or 0; psr=d.get("psr",0) or 0; div=d.get("div",0) or 0
    f={
        "roe_ok":roe>=FILTER_ROE,"per_ok":0<per<=FILTER_PER,"pbr_ok":0<pbr<=FILTER_PBR,
        "psr_ok":psr==0 or psr<=FILTER_PSR,"eps_ok":eps>=FILTER_EPS,
        "eps_growing":d.get("eps_trend","")=="상승","div_ok":div>=FILTER_DIV,
        "foreign_ok":d.get("foreign_ok",False),"momentum":(d.get("ch20") or 0)>=20.
    }
    core=[f["roe_ok"],f["per_ok"],f["eps_ok"],f["eps_growing"],f["psr_ok"]]
    if pbr>0: core.append(f["pbr_ok"])
    pc=sum(core); tot=len(core)
    return {**f,"pass_count":pc,"total":tot,"recommended":pc>=tot-1}

def send_discord(results,date,recommended):
    if not DISCORD_URL: print("  ℹ️ DISCORD 미설정"); return
    dt=f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ei={"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}
    display=recommended[:5] if recommended else sorted(results,key=lambda x:x.get("pass_count",0),reverse=True)[:5]
    lines=[
        f"📊 **StockPilot KR — {dt}** (KIS 실시간)",
        f"거래대금 상위{TOP_N} | ROE≥{FILTER_ROE}% PER≤{FILTER_PER}배 PBR≤{FILTER_PBR} PSR≤{FILTER_PSR}배 EPS상승",
        f"✅ 추천: **{len(recommended)}종목**","",
        "⭐ **추천 종목**" if recommended else "📊 **조건 상위 종목**","─"*30,
    ]
    for r in display:
        f=r.get("filters",{}); is_rec=r.get("recommended",False); pc=r.get("pass_count",0); tot=r.get("total",5)
        lines.append(f"{'⭐ ' if is_rec else ''}**{r['name']}** ({r['market']}) — 조건 {pc}/{tot}")
        lines.append(f"  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  PBR {r.get('pbr',0):.2f}  PSR {r.get('psr',0):.1f}배  배당 {r.get('div',0):.1f}%")
        eps_t=r.get("eps_trend","데이터없음"); eps_g=r.get("eps_growth",0)
        lines.append(f"  {ei.get(eps_t,'❓')} EPS {r.get('eps',0):,.0f}원{f'({eps_g:+.1f}%)' if eps_g else ''} ({eps_t})  외국인{'✅' if f.get('foreign_ok') else '❌'}")
        lines.append(f"  {'📈' if r.get('ch20',0)>0 else '📉'} 20일 {r.get('ch20',0):+.1f}%  거래대금 {r.get('tvol',0):,}억")
        lines.append("")
    lines.append("⚠️ 투자 손실 책임은 본인에게 있습니다.")
    msg="\n".join(lines)
    chunks=[]
    while len(msg)>1900: si=msg[:1900].rfind("\n"); chunks.append(msg[:si]); msg=msg[si:]
    chunks.append(msg)
    try:
        for chunk in chunks:
            res=requests.post(DISCORD_URL,json={"content":chunk},timeout=10)
            if res.status_code not in (200,204): print(f"  ⚠️ {res.status_code}")
            time.sleep(0.3)
        print(f"  ✅ Discord 전송 완료 ({len(chunks)}개)")
    except Exception as e: print(f"  ❌ Discord 실패: {e}")

def main():
    print("╔══════════════════════════════════╗\n║   StockPilot KR  KIS 스크리닝   ║\n╚══════════════════════════════════╝")
    if not APP_KEY or not APP_SECRET: print("❌ KIS 키 없음"); return
    date=datetime.now().strftime("%Y%m%d"); print(f"  기준일: {date}")
    print("\n[0/4] KIS 토큰 발급 중...")
    try: token=get_token()
    except Exception as e: print(f"❌ 토큰 실패: {e}"); return

    top30=fetch_top30(token)
    if not top30:
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),"results":[],"recommended":[],"error":"거래대금 없음"},
                  open("results.json","w",encoding="utf-8"),ensure_ascii=False); return

    print(f"\n[2/4] {len(top30)}종목 분석 중...\n")
    results=[]
    for t in top30:
        tk=t["ticker"]; print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk})",end=" ... ",flush=True)
        try:
            info=fetch_info(token,tk); fgn=fetch_foreign(token,tk)
            eps_tr=fetch_eps_trend(token,tk,info.get("eps",0))
            price=fetch_ch20(token,tk); psr=fetch_psr(token,tk,info.get("close",0))
            time.sleep(0.2)
            data={**t,**info,**fgn,**eps_tr,**price,"psr":psr}
            filt=judge(data); data.update({"filters":filt,"recommended":filt["recommended"],"pass_count":filt["pass_count"],"total":filt["total"]})
            results.append(data)
            print(f"조건{filt['pass_count']}/{filt['total']}  ROE:{info.get('roe',0):.1f}%  PER:{info.get('per',0):.1f}  PBR:{info.get('pbr',0):.2f}  PSR:{psr:.1f}  EPS:{info.get('eps',0):,.0f}({eps_tr.get('eps_trend','?')})  배당:{info.get('div',0):.1f}%  외국인{'✅' if fgn.get('foreign_ok') else '❌'}{'  ⭐' if filt['recommended'] else ''}")
        except Exception: print("오류"); traceback.print_exc()
        time.sleep(0.3)

    recommended=[r for r in results if r.get("recommended")]
    print(f"\n{'─'*70}\n  분석:{len(results)}종목  추천:{len(recommended)}종목")
    for r in recommended:
        print(f"  ⭐ {r['name']} ({r['market']}) 조건{r.get('pass_count',0)}/{r.get('total',5)}  ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  PBR {r.get('pbr',0):.2f}  EPS {r.get('eps',0):,.0f}원({r.get('eps_trend','?')})  배당 {r.get('div',0):.1f}%")

    json.dump({"date":date,"generated_at":datetime.now().isoformat(),"total":len(results),"results":results,"recommended":recommended},
              open("results.json","w",encoding="utf-8"),ensure_ascii=False,indent=2,default=str)
    print("\n  💾 results.json 저장 완료")
    send_discord(results,date,recommended)
    print("\n✅ 완료!")

if __name__=="__main__": main()
