"""
StockPilot KR — screener.py
KIS (한국투자증권) OpenAPI 기반
- 거래대금 상위 30 (실시간)
- PER / PBR / EPS / ROE / 배당수익률 (실제 데이터)
- 외국인 순매수
- 추천: 지표 부합 여부로 판단
"""
import os, json, time, traceback
from datetime import datetime, timedelta

try:
    import requests
    import pandas as pd
except ImportError:
    print("pip install -r requirements.txt 먼저 실행하세요")
    exit(1)

# ── 환경변수 ──────────────────────────────────────────────────────
APP_KEY     = os.environ.get("KIS_APP_KEY", "")
APP_SECRET  = os.environ.get("KIS_APP_SECRET", "")
ACCOUNT     = os.environ.get("KIS_ACCOUNT", "")  # 예: 44457068
DISCORD_URL = os.environ.get("DISCORD_WEBHOOK", "")

BASE_URL    = "https://openapi.koreainvestment.com:9443"

# ── 필터 기준 ──────────────────────────────────────────────────────
FILTER_ROE      = 15.0   # ROE 15% 이상
FILTER_PER_MAX  = 15.0   # PER 15배 이하
FILTER_PSR_MAX  = 3.0    # PSR 3배 이하
FILTER_PSR_GOOD = 1.5    # PSR 1.5배 이하 선호
FILTER_EPS      = 1.0    # EPS 1원 이상
FILTER_DIV      = 3.0    # 배당수익률 3% 이상 선호
TOP_N           = 30

# ── KIS API 토큰 발급 ─────────────────────────────────────────────
def get_token() -> str:
    url = f"{BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey":     APP_KEY,
        "appsecret":  APP_SECRET,
    }
    res = requests.post(url, json=body, timeout=10)
    res.raise_for_status()
    token = res.json().get("access_token", "")
    print(f"  ✅ KIS 토큰 발급 완료")
    return token

def kis_headers(token: str, tr_id: str) -> dict:
    return {
        "Content-Type":  "application/json",
        "authorization": f"Bearer {token}",
        "appkey":        APP_KEY,
        "appsecret":     APP_SECRET,
        "tr_id":         tr_id,
        "custtype":      "P",
    }

def safe_float(v, default=0.0) -> float:
    try:
        val = float(str(v).replace(",","").strip() or 0)
        return default if (val != val) else val
    except:
        return default

# ── 1단계: 거래대금 상위 30 ──────────────────────────────────────
def fetch_top30(token: str) -> list[dict]:
    print(f"\n[1/4] 거래대금 상위 {TOP_N} 조회 중...")
    all_stocks = []

    for market_code, market_name in [("J", "KOSPI"), ("Q", "KOSDAQ")]:
        try:
            # FHPST01710000: 주식 거래량/거래대금 순위
            url    = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/volume"
            params = {
                "fid_cond_mrkt_div_code": market_code,
                "fid_cond_scr_div_code":  "20171",
                "fid_input_iscd":         "0000",
                "fid_div_cls_code":       "0",
                "fid_blng_cls_code":      "0",
                "fid_trgt_cls_code":      "111111111",
                "fid_trgt_exls_cls_code": "000000",
                "fid_input_price_1":      "",
                "fid_input_price_2":      "",
                "fid_vol_cnt":            "",
                "fid_input_date_1":       "",
            }
            headers = kis_headers(token, "FHPST01710000")
            res = requests.get(url, headers=headers, params=params, timeout=10)
            data = res.json()

            if data.get("rt_cd") != "0":
                # 거래량 순위 대신 거래대금 순위로 재시도
                url2 = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/trading-volume"
                res  = requests.get(url2, headers=headers, params=params, timeout=10)
                data = res.json()

            items = data.get("output", [])
            count = 0
            for item in items:
                name   = str(item.get("hts_kor_isnm", "")).strip()
                ticker = str(item.get("mksc_shrn_iscd", "")).strip()
                tvol   = safe_float(item.get("acml_tr_pbmn", 0))  # 누적거래대금
                if not name or not ticker:
                    continue
                # ETF 제외
                if any(k in name for k in ["ETF","ETN","KODEX","TIGER","레버리지","인버스","선물"]):
                    continue
                all_stocks.append({
                    "ticker": ticker,
                    "name":   name,
                    "market": market_name,
                    "tvol":   int(tvol) // 100000000,  # 억원
                })
                count += 1
            print(f"  {market_name}: {count}종목")
        except Exception as e:
            print(f"  {market_name} 오류: {e}")
        time.sleep(0.3)

    # 거래대금 기준 합산 정렬
    df = pd.DataFrame(all_stocks)
    if df.empty:
        return []
    df = df.sort_values("tvol", ascending=False).drop_duplicates("ticker").head(TOP_N).reset_index(drop=True)

    result = []
    for i, row in df.iterrows():
        result.append({
            "rank":   i + 1,
            "ticker": row["ticker"],
            "name":   row["name"],
            "market": row["market"],
            "tvol":   int(row["tvol"]),
        })

    print(f"\n  거래대금 상위 {len(result)}종목:")
    for r in result[:5]:
        print(f"    {r['rank']:2d}. {r['name']} ({r['market']}) — {r['tvol']:,}억")
    return result

# ── 2단계: 개별 종목 기본 시세 + 재무 ────────────────────────────
def fetch_stock_info(token: str, ticker: str) -> dict:
    """주식 현재가 조회 (PER/PBR/EPS/배당 포함)"""
    result = {"per": 0.0, "pbr": 0.0, "eps": 0.0, "div": 0.0,
              "bps": 0.0, "roe": 0.0, "psr": 0.0, "close": 0.0}
    try:
        url    = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd":         ticker,
        }
        headers = kis_headers(token, "FHKST01010100")
        res  = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()

        if data.get("rt_cd") == "0":
            o = data.get("output", {})
            result["close"] = safe_float(o.get("stck_prpr", 0))
            result["per"]   = safe_float(o.get("per", 0))
            result["pbr"]   = safe_float(o.get("pbr", 0))
            result["eps"]   = safe_float(o.get("eps", 0))
            result["bps"]   = safe_float(o.get("bps", 0))
            result["div"]   = safe_float(o.get("d_rate", 0))   # 배당수익률

            # ROE = EPS / BPS * 100
            if result["bps"] > 0:
                result["roe"] = round(result["eps"] / result["bps"] * 100, 1)

    except Exception as e:
        print(f"    기본시세 오류: {e}")
    return result

# ── 3단계: 외국인 순매수 ──────────────────────────────────────────
def fetch_foreign(token: str, ticker: str) -> dict:
    """외국인 순매수 조회"""
    result = {"foreign_net": 0, "foreign_ok": False}
    try:
        url    = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd":         ticker,
        }
        headers = kis_headers(token, "FHKST01010900")
        res  = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()

        if data.get("rt_cd") == "0":
            items = data.get("output", [])
            if items:
                latest = items[0]
                net = safe_float(latest.get("frgn_ntby_qty", 0))  # 외국인 순매수 수량
                result["foreign_net"] = int(net)
                result["foreign_ok"]  = net > 0
    except Exception as e:
        print(f"    외국인 오류: {e}")
    return result

# ── 4단계: EPS 성장 추세 (52주 EPS 변화) ─────────────────────────
def fetch_eps_trend(token: str, ticker: str, current_eps: float) -> dict:
    """EPS 성장 추세 판단"""
    result = {"eps_trend": "데이터없음", "eps_growth": 0.0}
    try:
        # 재무비율 조회
        url    = f"{BASE_URL}/uapi/domestic-stock/v1/finance/financial-ratio"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd":         ticker,
            "fid_div_cls_code":       "1",  # 연간
        }
        headers = kis_headers(token, "FHKST66430300")
        res  = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()

        if data.get("rt_cd") == "0":
            items = data.get("output", [])
            eps_vals = []
            for item in items[:3]:  # 최근 3년
                eps_val = safe_float(item.get("eps", 0))
                if eps_val != 0:
                    eps_vals.append(eps_val)

            if len(eps_vals) >= 2:
                growing = all(eps_vals[i] >= eps_vals[i+1] for i in range(len(eps_vals)-1))
                latest  = eps_vals[0]
                if growing and latest >= FILTER_EPS:
                    gr = ((eps_vals[0]-eps_vals[1])/abs(eps_vals[1])*100) if eps_vals[1] != 0 else 0
                    result["eps_trend"]  = "상승"
                    result["eps_growth"] = round(gr, 1)
                elif latest >= FILTER_EPS:
                    result["eps_trend"] = "유지"
                else:
                    result["eps_trend"] = "부진"
            elif current_eps >= FILTER_EPS:
                result["eps_trend"] = "유지"
            else:
                result["eps_trend"] = "부진"
    except Exception as e:
        # EPS 현재값으로만 판단
        if current_eps >= FILTER_EPS:
            result["eps_trend"] = "유지"
        else:
            result["eps_trend"] = "부진"
    return result

# ── 5단계: 20일 등락률 ────────────────────────────────────────────
def fetch_ch20(token: str, ticker: str) -> dict:
    result = {"ch20": 0.0, "vol_trend": 0.0}
    try:
        today = datetime.now()
        start = (today - timedelta(days=45)).strftime("%Y%m%d")
        end   = today.strftime("%Y%m%d")

        url    = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd":         ticker,
            "fid_org_adj_prc":        "1",
            "fid_period_div_code":    "D",
            "fid_input_date_1":       start,
            "fid_input_date_2":       end,
        }
        headers = kis_headers(token, "FHKST01010400")
        res  = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()

        if data.get("rt_cd") == "0":
            items = data.get("output2", data.get("output", []))
            if len(items) >= 20:
                prices = [safe_float(x.get("stck_clpr", 0)) for x in items]
                prices = [p for p in prices if p > 0]
                if len(prices) >= 20:
                    p0 = prices[-1]  # 20일 전
                    p1 = prices[0]   # 최근
                    result["ch20"] = round((p1-p0)/p0*100, 1) if p0 > 0 else 0.0

                    vols = [safe_float(x.get("acml_vol", 0)) for x in items[:20]]
                    avg5 = sum(vols[:5]) / 5 if vols[:5] else 0
                    avgA = sum(vols) / len(vols) if vols else 0
                    result["vol_trend"] = round((avg5-avgA)/avgA*100, 1) if avgA > 0 else 0.0
    except Exception as e:
        print(f"    가격 오류: {e}")
    return result

# ── PSR 계산 (시총 / 매출액) ──────────────────────────────────────
def fetch_psr(token: str, ticker: str, close: float) -> float:
    """PSR = 시가총액 / 연간매출액"""
    try:
        url    = f"{BASE_URL}/uapi/domestic-stock/v1/finance/income-statement"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd":         ticker,
            "fid_div_cls_code":       "1",
        }
        headers = kis_headers(token, "FHKST66430200")
        res  = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()

        if data.get("rt_cd") == "0":
            items = data.get("output", [])
            if items:
                revenue = safe_float(items[0].get("sale_account", 0))  # 매출액 (백만원)
                if revenue > 0:
                    # 상장주식수 조회
                    url2    = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
                    params2 = {"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker}
                    res2    = requests.get(url2, headers=kis_headers(token,"FHKST01010100"), params=params2, timeout=10)
                    d2      = res2.json()
                    if d2.get("rt_cd") == "0":
                        shares  = safe_float(d2["output"].get("lstn_stcn", 0))
                        marcap  = close * shares  # 시가총액 (원)
                        psr     = marcap / (revenue * 1_000_000)  # PSR
                        return round(psr, 2)
    except:
        pass
    return 0.0

# ── 추천 판단 ─────────────────────────────────────────────────────
def judge(d: dict) -> dict:
    """지표별 Pass/Fail 및 추천 여부 판단"""
    per = d.get("per", 0) or 0
    pbr = d.get("pbr", 0) or 0
    roe = d.get("roe", 0) or 0
    eps = d.get("eps", 0) or 0
    div = d.get("div", 0) or 0
    psr = d.get("psr", 0) or 0
    eps_trend = d.get("eps_trend", "")
    foreign_ok = d.get("foreign_ok", False)

    f = {
        "roe_ok":      roe >= FILTER_ROE,
        "per_ok":      0 < per <= FILTER_PER_MAX,
        "psr_ok":      psr == 0 or psr <= FILTER_PSR_MAX,
        "psr_good":    0 < psr <= FILTER_PSR_GOOD,
        "eps_ok":      eps >= FILTER_EPS,
        "eps_growing": eps_trend == "상승",
        "div_ok":      div >= FILTER_DIV,
        "foreign_ok":  foreign_ok,
        "momentum":    (d.get("ch20") or 0) >= 20.0,
        "vol_ok":      (d.get("vol_trend") or 0) > -10,
    }

    # 추천: 핵심 지표 충족 여부
    pass_count = sum([
        f["roe_ok"],
        f["per_ok"],
        f["psr_ok"],
        f["eps_ok"],
        f["eps_growing"],
    ])
    recommended = pass_count >= 4  # 5개 중 4개 이상 충족

    return {**f, "pass_count": pass_count, "recommended": recommended}

# ── Discord 전송 ──────────────────────────────────────────────────
def send_discord(results: list, date: str, recommended: list):
    if not DISCORD_URL:
        print("  ℹ️  DISCORD_WEBHOOK 미설정"); return

    dt = f"{date[:4]}.{date[4:6]}.{date[6:]}"
    ge = {"A":"🟢","B":"🔵","C":"🟡","D":"🔴"}
    ei = {"상승":"📈","유지":"➡️","부진":"📉","데이터없음":"❓"}

    display = recommended[:5] if recommended else sorted(
        results, key=lambda x: x.get("pass_count", 0), reverse=True
    )[:5]

    lines = [
        f"📊 **StockPilot KR — {dt}** (KIS 실시간 데이터)",
        f"거래대금 상위 {TOP_N}개 분석",
        f"✅ 추천: **{len(recommended)}종목** (ROE≥{FILTER_ROE}% · PER≤{FILTER_PER_MAX}배 · PSR≤{FILTER_PSR_MAX}배 · EPS상승)",
        "",
        "⭐ **추천 종목**" if recommended else "📊 **점수 상위 종목** (추천 조건 미충족)",
        "─" * 28,
    ]

    for r in display:
        f      = r.get("filters", {})
        is_rec = r.get("recommended", False)
        star   = "⭐ " if is_rec else ""
        per_s  = f"PER {r.get('per',0):.1f}배" if r.get("per",0)>0 else "PER -"
        pbr_s  = f"PBR {r.get('pbr',0):.2f}" if r.get("pbr",0)>0 else "PBR -"
        roe_s  = f"ROE {r.get('roe',0):.1f}%" if r.get("roe",0)>0 else "ROE -"
        psr_s  = f"PSR {r.get('psr',0):.1f}배" if r.get("psr",0)>0 else "PSR -"
        div_s  = f"배당 {r.get('div',0):.1f}%" if r.get("div",0)>0 else "배당 -"
        eps_t  = r.get("eps_trend","데이터없음")
        eps_g  = r.get("eps_growth",0)
        eps_s  = f"EPS {r.get('eps',0):,.0f}원" if r.get("eps",0)!=0 else "EPS -"
        eps_gs = f"({eps_g:+.1f}%)" if eps_g!=0 else ""
        ch20   = r.get("ch20", 0)
        fgn_s  = "외국인✅" if f.get("foreign_ok") else "외국인❌"
        pass_c = r.get("pass_count", 0)

        lines.append(f"{'⭐ ' if is_rec else ''}" +
                     f"**{r['name']}** ({r['market']}) — 조건 {pass_c}/5 충족")
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
            r = requests.post(DISCORD_URL, json={"content": chunk}, timeout=10)
            if r.status_code not in (200, 204):
                print(f"  ⚠️ {r.status_code}")
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
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 환경변수 없음")
        return

    date = datetime.now().strftime("%Y%m%d")
    print(f"  기준일: {date}")
    print(f"  필터: ROE≥{FILTER_ROE}% | PER≤{FILTER_PER_MAX}배 | PSR≤{FILTER_PSR_MAX}배 | EPS≥{FILTER_EPS}·상승")

    # 토큰 발급
    print("\n[0/4] KIS 토큰 발급 중...")
    try:
        token = get_token()
    except Exception as e:
        print(f"❌ 토큰 발급 실패: {e}")
        return

    # 1) 거래대금 상위 30
    top30 = fetch_top30(token)
    if not top30:
        print("❌ 거래대금 데이터 없음")
        json.dump({"date":date,"generated_at":datetime.now().isoformat(),
                   "results":[],"recommended":[],"error":"거래대금 없음"},
                  open("results.json","w",encoding="utf-8"), ensure_ascii=False)
        return

    # 2) 종목별 재무 + 가격 분석
    print(f"\n[2/4] {len(top30)}종목 재무+가격 조회 중...\n")
    results = []
    for t in top30:
        tk = t["ticker"]
        print(f"  [{t['rank']:2d}] {t['name']:14s} ({tk})", end=" ... ", flush=True)
        try:
            info    = fetch_stock_info(token, tk)
            foreign = fetch_foreign(token, tk)
            eps_tr  = fetch_eps_trend(token, tk, info.get("eps", 0))
            price   = fetch_ch20(token, tk)
            psr     = fetch_psr(token, tk, info.get("close", 0))
            time.sleep(0.2)

            data = {
                **t,
                **info,
                **foreign,
                **eps_tr,
                **price,
                "psr": psr,
            }
            filters = judge(data)
            data.update({
                "filters":     filters,
                "recommended": filters["recommended"],
                "pass_count":  filters["pass_count"],
            })
            results.append(data)

            rec_mark = "  ⭐추천" if filters["recommended"] else ""
            print(
                f"조건 {filters['pass_count']}/5  "
                f"ROE:{info.get('roe',0):.1f}%  "
                f"PER:{info.get('per',0):.1f}  "
                f"PBR:{info.get('pbr',0):.2f}  "
                f"EPS:{info.get('eps',0):,.0f}({eps_tr.get('eps_trend','?')})  "
                f"외국인{'✅' if foreign.get('foreign_ok') else '❌'}"
                f"{rec_mark}"
            )
        except Exception:
            print("오류"); traceback.print_exc()
        time.sleep(0.3)

    recommended = [r for r in results if r.get("recommended")]
    print(f"\n{'─'*65}")
    print(f"  분석 완료: {len(results)}종목")
    print(f"  최종 추천: {len(recommended)}종목")
    for r in recommended:
        print(
            f"  ⭐ {r['name']} ({r['market']}) "
            f"조건 {r.get('pass_count',0)}/5  "
            f"ROE {r.get('roe',0):.1f}%  PER {r.get('per',0):.1f}배  "
            f"EPS {r.get('eps',0):,.0f}원({r.get('eps_trend','?')})"
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
