"""
StockPilot KR — screener.py (KIS OpenAPI)
지표: 거래대금 상위 30 / PER / PBR / ROE / EPS / PSR / 배당 / 외국인순매수 / 20일등락
추천: 핵심 지표 조건 충족 여부로 판단
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import requests
    import pandas as pd
except ImportError:
    print("pip install -r requirements.txt 먼저 실행하세요")
    exit(1)

APP_KEY     = os.environ.get("KIS_APP_KEY", "")
APP_SECRET  = os.environ.get("KIS_APP_SECRET", "")
ACCOUNT     = os.environ.get("KIS_ACCOUNT", "")
DISCORD_URL = os.environ.get("DISCORD_WEBHOOK", "")
BASE_URL    = "https://openapi.koreainvestment.com:9443"

TOP_N           = 30
FILTER_ROE      = 15.0
FILTER_PER      = 15.0
FILTER_PBR      = 1.5
FILTER_PSR_MAX  = 3.0
FILTER_PSR_GOOD = 1.5
FILTER_EPS      = 1.0
FILTER_DIV      = 3.0

def safe_float(v, d=0.0):
    try:
        val = float(str(v).replace(",","").strip() or 0)
        return d if val != val else val
    except:
        return d

def get_token() -> str:
    res = requests.post(f"{BASE_URL}/oauth2/tokenP", json={
        "grant_type": "client_credentials",
        "appkey":     APP_KEY,
        "appsecret":  APP_SECRET,
    }, timeout=10)
    res.raise_for_status()
    token = res.json().get("access_token","")
    print(f"  ✅ KIS 토큰 발급 완료")
    return token

def hdrs(token, tr_id):
    return {
        "Content-Type":  "application/json",
        "authorization": f"Bearer {token}",
        "appkey":        APP_KEY,
        "appsecret":     APP_SECRET,
        "tr_id":         tr_id,
        "custtype":      "P",
    }

# ── 1단계: 거래대금 상위 30 ──────────────────────────────────────
def fetch_top30(token: str) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중...")
    all_stocks = []

    for mkt_code, mkt_name in [("J","KOSPI"), ("Q","KOSDAQ")]:
        try:
            # KIS 거래량 순위 (FHPST01710000)
            url = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/volume"
            params = {
                "fid_cond_mrkt_div_code": mkt_code,
                "fid_cond_scr_div_code":  "20171",
                "fid_input_iscd":         "0000",
                "fid_div_cls_code":       "1",   # 1=거래대금
                "fid_blng_cls_code":      "0",
                "fid_trgt_cls_code":      "111111111",
                "fid_trgt_exls_cls_code": "000000",
                "fid_input_price_1":      "",
                "fid_input_price_2":      "",
                "fid_vol_cnt":            "",
                "fid_input_date_1":       "",
            }
            res = requests.get(url, headers=hdrs(token,"FHPST01710000"), params=params, timeout=10)
            data = res.json()

            items = data.get("output", [])
            if not items and data.get("rt_cd") != "0":
                raise ValueError(f"rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}")

            cnt = 0
            for item in items:
                name   = str(item.get("hts_kor_isnm","")).strip()
                ticker = str(item.get("mksc_shrn_iscd","")).strip()
                tvol   = safe_float(item.get("acml_tr_pbmn", item.get("acml_vol",0)))
                if not name or not ticker:
                    continue
                if any(k in name for k in ["ETF","ETN","KODEX","TIGER","레버리지","인버스","선물","RISE","ACE","KBSTAR"]):
                    continue
                all_stocks.append({"ticker":ticker,"name":name,"market":mkt_name,"tvol":int(tvol)//100000000})
                cnt += 1
            print(f"  {mkt_name}: {cnt}종목")

        except Exception as e:
            print(f"  {mkt_name} 오류: {e}")
            # fallback: 호가 없이 종목 마스터에서 상위 종목 구성
            try:
                url2 = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/profit-loss-rate"
                params2 = {
                    "fid_cond_mrkt_div_code": mkt_code,
                    "fid_cond_scr_div_code":  "20193",
                    "fid_input_iscd":         "0000",
                    "fid_rank_sort_cls_code": "1",
                    "fid_input_cnt_1":        "0",
                    "fid_prc_cls_code":       "1",
                    "fid_input_price_1":      "",
                    "fid_input_price_2":      "",
                    "fid_vol_cnt":            "",
                    "fid_trgt_cls_code":      "0",
                    "fid_trgt_exls_cls_code": "0",
                    "fid_div_cls_code":       "0",
                    "fid_rsfl_rate1":         "",
                    "fid_rsfl_rate2":         "",
                }
                res2  = requests.get(url2, headers=hdrs(token,"FHPST01930000"), params=params2, timeout=10)
                data2 = res2.json()
                for item in data2.get("output",[]): 
                    name   = str(item.get("hts_kor_isnm","")).strip()
                    ticker = str(item.get("mksc_shrn_iscd","")).strip()
                    if not name or not ticker:
                        continue
                    if any(k in name for k in ["ETF","ETN","KODEX","TIGER","레버리지","인버스","선물"]):
                        continue
                    all_stocks.append({"ticker":ticker,"name":name,"market":mkt_name,"tvol":0})
            except Exception as e2:
                print(f"  {mkt_name} fallback 오류: {e2}")
        time.sleep(0.3)

    if not all_stocks:
        return []

    df = pd.DataFrame(all_stocks).drop_duplicates("ticker")
    df = df.sort_values("tvol", ascending=False).head(TOP_N).reset_index(drop=True)

    result = []
    for i, row in df.iterrows():
        result.append({"rank":i+1,"ticker":row["ticker"],"name":row["name"],
                       "market":row["market"],"tvol":int(row["tvol"])})
    print(f"\n  거래대금 상위 {len(result)}종목:")
    for r in result[:5]:
        print(f"    {r['rank']:2d}. {r['name']} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 2단계: 주식 현재가 (PER/PBR/EPS/배당) ────────────────────────
def fetch_price_info(token: str, ticker: str) -> dict:
    r = {"per":0.0,"pbr":0.0,"eps":0.0,"bps":0.0,"div":0.0,"roe":0.0,"close":0.0,"psr":0.0}
    try:
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=hdrs(token,"FHKST01010100"),
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker},
            timeout=10
        )
        o = res.json().get("output",{})
        r["close"] = safe_float(o.get("stck_prpr"))
        r["per"]   = safe_float(o.get("per"))
        r["pbr"]   = safe_float(o.get("pbr"))
        r["eps"]   = safe_float(o.get("eps"))
        r["bps"]   = safe_float(o.get("bps"))
        r["div"]   = safe_float(o.get("d_rate"))
        if r["bps"] > 0:
            r["roe"] = round(r["eps"] / r["bps"] * 100, 1)
    except Exception as e:
        print(f"    가격정보 오류: {e}")
    return r

# ── 3단계: 외국인 순매수 ──────────────────────────────────────────
def fetch_foreign(token: str, ticker: str) -> dict:
    r = {"foreign_net":0,"foreign_ok":False}
    try:
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=hdrs(token,"FHKST01010900"),
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker},
            timeout=10
        )
        items = res.json().get("output",[])
        if items:
            net = safe_float(items[0].get("frgn_ntby_qty",0))
            r["foreign_net"] = int(net)
            r["foreign_ok"]  = net > 0
    except Exception as e:
        print(f"    외국인 오류: {e}")
    return r

# ── 4단계: PSR (시가총액/매출액) ──────────────────────────────────
def fetch_psr(token: str, ticker: str, close: float) -> float:
    try:
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/finance/income-statement",
            headers=hdrs(token,"FHKST66430200"),
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,"fid_div_cls_code":"1"},
            timeout=10
        )
        items = res.json().get("output",[])
        if items:
            revenue = safe_float(items[0].get("sale_account",0))  # 백만원
            if revenue > 0:
                res2   = requests.get(
                    f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=hdrs(token,"FHKST01010100"),
                    params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker},
                    timeout=10
                )
                shares = safe_float(res2.json().get("output",{}).get("lstn_stcn",0))
                if shares > 0 and close > 0:
                    return round((close * shares) / (revenue * 1_000_000), 2)
    except:
        pass
    return 0.0

# ── 5단계: EPS 성장 추세 ──────────────────────────────────────────
def fetch_eps_trend(token: str, ticker: str, cur_eps: float) -> dict:
    r = {"eps_trend":"데이터없음","eps_growth":0.0}
    try:
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/finance/financial-ratio",
            headers=hdrs(token,"FHKST66430300"),
            params={"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,"fid_div_cls_code":"1"},
            timeout=10
        )
        items = res.json().get("output",[])
        ev = [safe_float(x.get("eps")) for x in items[:3] if safe_float(x.get("eps")) != 0]
        if len(ev) >= 2:
            growing = all(ev[i] >= ev[i+1] for i in range(len(ev)-1))
            if growing and ev[0] >= FILTER_EPS:
                gr = ((ev[0]-ev[1])/abs(ev[1])*100) if ev[1] != 0 else 0
                r["eps_trend"]  = "상승"
                r["eps_growth"] = round(gr,1)
            elif ev[0] >= FILTER_EPS:
                r["eps_trend"] = "유지"
            else:
                r["eps_trend"] = "부진"
        else:
            r["eps_trend"] = "유지" if cur_eps >= FILTER_EPS else "부진"
    except:
        r["eps_trend"] = "유지" if cur_eps >= FILTER_EPS else "부진"
    return r

# ── 6단계: 20일 등락률 ────────────────────────────────────────────
def fetch_ch20(token: str, ticker: str) -> dict:
    r = {"ch20":0.0,"vol_trend":0.0}
    try:
        now   = datetime.now()
        start = (now - timedelta(days=45)).strftime("%Y%m%d")
        end   = now.strftime("%Y%m%d")
        res   = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=hdrs(token,"FHKST01010400"),
            params={
                "fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker,
                "fid_org_adj_prc":"1","fid_period_div_code":"D",
                "fid_input_date_1":start,"fid_input_date_2":end,
            },
            timeout=10
        )
        items = res.json().get("output2", res.json().get("output",[]))
        prices = [safe_float(x.get("stck_clpr")) for x in items if safe_float(x.get("stck_clpr")) > 0]
        if len(prices) >= 20:
            r["ch20"] = round((prices[0]-prices[19])/prices[19]*100, 1) if prices[19] > 0 else 0.0
            vols = [safe_float(x.get("acml_vol")) for x in items[:20]]
            avg5 = sum(vols[:5])/5 if vols[:5] else 0
            avgA = sum(vols)/len(vols) if vols else 0
            r["vol_trend"] = round((avg5-avgA)/avgA*100,1) if avgA > 0 else 0.0
    except Exception as e:
        print(f"    20일 오류: {e}")
    return r

# ── 추천 판단 ─────────────────────────────────────────────────────
def judge(d: dict) -> dict:
    per = d.get("per",0) or 0
    pbr = d.get("pbr",0) or 0
    roe = d.get("roe",0) or 0
    eps = d.get("eps",0) or 0
    div = d.get("div",0) or 0
    psr = d.get("psr",0) or 0
    eps_trend = d.get("eps_trend","")

    f = {
        "roe_ok":      roe >= FILTER_ROE,
        "per_ok":      0 < per <= FILTER_PER,
        "pbr_ok":      0 < pbr <= FILTER_PBR,
        "psr_ok":      psr == 0 or psr <= FILTER_PSR_MAX,
        "psr_good":    0 < psr <= FILTER_PSR_GOOD,
        "eps_ok":      eps >= FILTER_EPS,
        "eps_growing": eps_trend == "상승",
        "div_ok":      div >= FILTER_DIV,
        "foreign_ok":  d.get("foreign_ok", False),
        "momentum":    (d.get("ch20") or 0) >= 20.0,
    }
    # 핵심 5개 기준 (PBR 데이터 없으면 제외하고 판단)
    core = [f["roe_ok"], f["per_ok"], f["eps_ok"], f["eps_growing"], f["psr_ok"]]
    if pbr > 0:
        core.append(f["pbr_ok"])
    pass_count = sum(core)
    total      = len(core)
    recommended = pass_count >= total - 1  # 1개 미충족 허용

    return {**f, "pass_count": pass_count, "total": total, "recommended": recommended}

# ── Discord 전송 ──────────────────────────────────────────────────
def send_discord(results: list, date: str, recommended: list):
    if not DISCORD_URL:
        print("  ℹ️  DISCORD_WEBHOOK 미설정"); return
    dt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ei = {"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}

    non_etf  = [r for r in results if not r.get("is_etf")]
    display  = recommended[:5] if recommended else sorted(
        non_etf, key=lambda x: x.get("pass_count",0), reverse=True
    )[:5]

    lines = [
        f"📊 **StockPilot KR — {dt}** (KIS 실시간)",
        f"거래대금 상위 {TOP_N}개 | ROE≥{FILTER_ROE}% · PER≤{FILTER_PER}배 · PBR≤{FILTER_PBR} · PSR≤{FILTER_PSR_MAX}배 · EPS상승",
        f"✅ 추천: **{len(recommended)}종목**",
        "",
        "⭐ **추천 종목**" if recommended else "📊 **조건 상위 종목** (추천 조건 미충족)",
        "─"*28,
    ]
    for r in display:
        f      = r.get("filters",{})
        is_rec = r.get("recommended",False)
        pc     = r.get("pass_count",0)
        tot    = r.get("total",5)
        per_s  = f"PER {r.get('per',0):.1f}배" if r.get("per",0)>0 else "PER -"
        pbr_s  = f"PBR {r.get('pbr',0):.2f}" if r.get("pbr",0)>0 else "PBR -"
        roe_s  = f"ROE {r.get('roe',0):.1f}%" if r.get("roe",0)>0 else "ROE -"
        psr_s  = f"PSR {r.get('psr',0):.1f}배" if r.get("psr",0)>0 else "PSR -"
        div_s  = f"배당 {r.get('div',0):.1f}%" if r.get("div",0)>0 else "배당 -"
        eps_t  = r.get("eps_trend","데이터없음")
        eps_g  = r.get("eps_growth",0)
        eps_s  = f"EPS {r.get('eps',0):,.0f}원" if r.get("eps",0)!=0 else "EPS -"
        eps_gs = f"({eps_g:+.1f}%)" if eps_g!=0 else ""
        ch20   = r.get("ch20",0)
        fgn_s  = "외국인✅" if f.get("foreign_ok") else "외국인❌"
        star   = "⭐ " if is_rec else ""

        lines.append(f"{star}**{r['name']}** ({r['market']}) — 조건 {pc}/{tot} 충족")
        lines.append(f"  {roe_s}  {per_s}  {pbr_s}  {psr_s}  {div_s}")
        lines.append(f"  {ei.get(eps_t,'❓')} {eps_s} {eps_gs} ({eps_t})  {fgn_s}")
        lines.append(f"  {'📈' if ch20>0 else '📉'} 20일 {ch20:+.1f}%  거래대금 {r.get('tvol',0):,}억")
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
            res = requests.post(DISCORD_URL, json={"content":chunk}, timeout=10)
            if res.status_code not in (200,204):
                print(f"  ⚠️ {res.status_code}")
            time.sleep(0.3)
        print(f"  ✅ Discord 전송 완료 ({len(chunks)}개)")
    except Exception as e:
        print(f"  ❌ Discord 실패: {e}")

# ── 메인 ──────────────────────────────────────────────────────────
def main():
    print("╔══════════════════════════════════╗")
    print("║   StockPilot KR  KIS 스크리닝   ║")
    print("╚══════════════════════════════════╝")

    if not APP_KEY or not APP_SECRET:
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음"); return

    date = datetime.now().strftime("%Y%m%d")
    print(f"  기준일: {date}")
    print(f"  필터: ROE≥{FILTER_ROE}% | PER≤{FILTER_PER}배 | PBR≤{FILTER_PBR} | PSR≤{FILTER_PSR_MAX}배 | EPS상승")

    print("\n[0/4] KIS 토큰 발급 중...")
    try:
        token = get_token()
    except Exception as e:
        print(f"❌ 토큰 발급 실패: {e}"); return

    # 1) 거래대금 상위 30
    top30 = fetch_top30(token)
    if not top30:
        print("❌ 거래대금 데이터 없음")
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"거래대금 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    # 2) 종목별 분석
    print(f"\n[2/4] {len(top30)}종목 분석 중...\n")
    results = []
    for t in top30:
        tk = t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk})", end=" ... ", flush=True)
        try:
            info    = fetch_price_info(token, tk)
            foreign = fetch_foreign(token, tk)
            eps_tr  = fetch_eps_trend(token, tk, info.get("eps",0))
            price   = fetch_ch20(token, tk)
            psr     = fetch_psr(token, tk, info.get("close",0))
            time.sleep(0.2)

            data = {**t, **info, **foreign, **eps_tr, **price, "psr": psr}
            filters = judge(data)
            data.update({
                "filters":     filters,
                "recommended": filters["recommended"],
                "pass_count":  filters["pass_count"],
                "total":       filters["total"],
            })
            results.append(data)

            rec_mark = "  ⭐추천" if filters["recommended"] else ""
            print(
                f"조건 {filters['pass_count']}/{filters['total']}  "
                f"ROE:{info.get('roe',0):.1f}%  "
                f"PER:{info.get('per',0):.1f}  "
                f"PBR:{info.get('pbr',0):.2f}  "
                f"PSR:{psr:.1f}  "
                f"EPS:{info.get('eps',0):,.0f}({eps_tr.get('eps_trend','?')})  "
                f"배당:{info.get('div',0):.1f}%  "
                f"외국인{'✅' if foreign.get('foreign_ok') else '❌'}"
                f"{rec_mark}"
            )
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.3)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*70}")
    print(f"  분석 완료: {len(results)}종목")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(
            f"  ⭐ {r['name']} ({r['market']}) "
            f"조건 {r.get('pass_count',0)}/{r.get('total',5)}  "
            f"ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  "
            f"PBR {r.get('pbr',0):.2f}  PSR {r.get('psr',0):.1f}배  "
            f"EPS {r.get('eps',0):,.0f}원({r.get('eps_trend','?')})  "
            f"배당 {r.get('div',0):.1f}%  거래대금 {r.get('tvol',0):,}억"
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
