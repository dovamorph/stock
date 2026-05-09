#!/usr/bin/env python3
"""
StockPilot KR — 옵션 만기일 분석 (expiry.py)
매월 두 번째 목요일 만기일 기준 D-6부터 분석 시작
4대 지표: 베이시스 / 외국인 선물 순매수 / 풋콜비율 / 미결제약정
"""
import os, json, datetime, requests, time
from zoneinfo import ZoneInfo

KST      = ZoneInfo("Asia/Seoul")
APP_KEY  = os.environ.get("KIS_APP_KEY", "")
APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO", "")
BASE_URL = "https://openapi.koreainvestment.com:9443"
EXPIRY_FILE = "expiry_result.json"

# ── 만기일 계산 (매월 두 번째 목요일) ─────────────────────────────────
def get_expiry_date(ref: datetime.date) -> datetime.date:
    """ref 날짜 기준 당월 또는 차월 만기일 반환"""
    def second_thursday(year, month):
        first = datetime.date(year, month, 1)
        # 첫 번째 목요일
        days_to_thu = (3 - first.weekday()) % 7
        first_thu = first + datetime.timedelta(days=days_to_thu)
        return first_thu + datetime.timedelta(weeks=1)

    exp = second_thursday(ref.year, ref.month)
    # 이미 만기일이 지났으면 다음 달
    if ref > exp:
        if ref.month == 12:
            exp = second_thursday(ref.year + 1, 1)
        else:
            exp = second_thursday(ref.year, ref.month + 1)
    return exp

# ── KIS 토큰 ──────────────────────────────────────────────────────────
def get_token():
    r = requests.post(f"{BASE_URL}/oauth2/tokenP", json={
        "grant_type": "client_credentials",
        "appkey": APP_KEY, "appsecret": APP_SECRET
    }, timeout=10)
    return r.json()["access_token"]

# ── KIS API 공통 헤더 ─────────────────────────────────────────────────
def kis_headers(token, tr_id):
    return {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "content-type": "application/json"
    }

# ── ① 베이시스 (KOSPI200 선물 - KOSPI200 현물) ───────────────────────
def get_basis(token):
    try:
        # KOSPI200 지수 현재가
        r_spot = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-index-price",
            headers=kis_headers(token, "FHPUP02100000"),
            params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": "0002"},
            timeout=10
        )
        spot_data = r_spot.json().get("output", {})
        spot = float(spot_data.get("bstp_nmix_prpr", 0))

        # KOSPI200 선물 현재가 (근월물)
        r_fut = requests.get(
            f"{BASE_URL}/uapi/domestic-futureoption/v1/quotations/inquire-price",
            headers=kis_headers(token, "FHKIF03010100"),
            params={"FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": "101V06"},
            timeout=10
        )
        fut_data = r_fut.json().get("output1", {})
        futures = float(fut_data.get("futs_prpr", 0))

        if spot == 0 or futures == 0:
            return None

        basis = round(futures - spot, 2)
        state = "콘탱고" if basis > 0.5 else ("백워데이션" if basis < -0.5 else "중립")
        signal = "✅" if basis > 0.5 else ("❌" if basis < -0.5 else "⚠️")

        return {
            "spot": spot,
            "futures": futures,
            "basis": basis,
            "state": state,
            "signal": signal,
            "desc": f"선물 {futures:.2f} - 현물 {spot:.2f} = {basis:+.2f}p ({state})"
        }
    except Exception as e:
        print(f"    베이시스 조회 실패: {e}")
        return None

# ── ② 외국인 선물 순매수 ──────────────────────────────────────────────
def get_foreign_futures(token):
    try:
        r = requests.get(
            f"{BASE_URL}/uapi/domestic-futureoption/v1/quotations/inquire-futureoption-invest-trend",
            headers=kis_headers(token, "FHKIF04010200"),
            params={
                "FID_COND_MRKT_DIV_CODE": "F",
                "FID_INPUT_ISCD": "101V06",
                "FID_INPUT_DATE_1": "",
                "FID_BLNG_CLS_CODE": "0"
            },
            timeout=10
        )
        data = r.json().get("output", [])

        # 외국인 순매수 (매수 - 매도)
        foreign = next((d for d in data if "외국인" in d.get("mbcr_name", "")), None)
        if not foreign:
            return None

        net_buy = int(foreign.get("futs_net_buy_qty", 0).replace(",", "").replace("-", "") or 0)
        is_buy = foreign.get("futs_net_buy_qty", "0").startswith("-") is False
        net_signed = net_buy if is_buy else -net_buy

        signal = "✅" if net_signed > 500 else ("❌" if net_signed < -500 else "⚠️")
        direction = "매수세" if net_signed > 0 else "매도세"

        return {
            "net_qty": net_signed,
            "direction": direction,
            "signal": signal,
            "desc": f"외국인 선물 순매수 {net_signed:+,}계약 ({direction})"
        }
    except Exception as e:
        print(f"    외국인 선물 조회 실패: {e}")
        return None

# ── ③ 풋/콜 비율 ─────────────────────────────────────────────────────
def get_put_call_ratio(token):
    try:
        # 옵션 전체 거래량 조회
        r = requests.get(
            f"{BASE_URL}/uapi/domestic-futureoption/v1/quotations/inquire-futureoption-invest-trend",
            headers=kis_headers(token, "FHKIF04010200"),
            params={
                "FID_COND_MRKT_DIV_CODE": "O",
                "FID_INPUT_ISCD": "201V06",
                "FID_INPUT_DATE_1": "",
                "FID_BLNG_CLS_CODE": "0"
            },
            timeout=10
        )
        out = r.json().get("output1", {})
        put_vol  = float(out.get("put_vol",  0) or 0)
        call_vol = float(out.get("call_vol", 0) or 0)

        if call_vol == 0:
            return None

        pcr = round(put_vol / call_vol, 2)

        if pcr > 1.5:
            signal = "⚠️"
            desc_add = "과도한 풋 → 반등 가능성"
        elif pcr > 1.0:
            signal = "❌"
            desc_add = "풋 우세 → 하락 베팅 우위"
        else:
            signal = "✅"
            desc_add = "콜 우세 → 상승 베팅 우위"

        return {
            "put_vol": int(put_vol),
            "call_vol": int(call_vol),
            "ratio": pcr,
            "signal": signal,
            "desc": f"풋/콜 = {pcr:.2f} ({desc_add})"
        }
    except Exception as e:
        print(f"    풋/콜 비율 조회 실패: {e}")
        return None

# ── ④ 미결제약정 ──────────────────────────────────────────────────────
def get_open_interest(token):
    try:
        r = requests.get(
            f"{BASE_URL}/uapi/domestic-futureoption/v1/quotations/inquire-price",
            headers=kis_headers(token, "FHKIF03010100"),
            params={"FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": "101V06"},
            timeout=10
        )
        out = r.json().get("output1", {})
        oi_today = int(out.get("futs_opnint_qty", 0) or 0)
        oi_prev  = int(out.get("futs_opnint_qty_icdc", 0) or 0)  # 전일 대비

        if oi_today == 0:
            return None

        change_pct = round(oi_prev / oi_today * 100, 1) if oi_today != 0 else 0

        if oi_prev < -5000:
            signal = "⚠️"
            desc_add = "급감 → 변동성 확대 주의"
        elif oi_prev > 5000:
            signal = "❌"
            desc_add = "급증 + 하락 시 진짜 하락"
        else:
            signal = "✅"
            desc_add = "안정적"

        return {
            "oi": oi_today,
            "change": oi_prev,
            "change_pct": change_pct,
            "signal": signal,
            "desc": f"미결제약정 {oi_today:,}계약 (전일比 {oi_prev:+,}, {desc_add})"
        }
    except Exception as e:
        print(f"    미결제약정 조회 실패: {e}")
        return None

# ── 종합 판단 ─────────────────────────────────────────────────────────
def judge(indicators):
    danger = sum(1 for v in indicators.values() if v and v.get("signal") == "❌")
    warn   = sum(1 for v in indicators.values() if v and v.get("signal") == "⚠️")
    ok     = sum(1 for v in indicators.values() if v and v.get("signal") == "✅")
    total  = sum(1 for v in indicators.values() if v is not None)

    if total == 0:
        return {"level": "알 수 없음", "color": "gray", "action": "데이터 부족", "score": 0}

    if danger >= 3:
        return {"level": "🔴 위험", "color": "red",    "action": "매도 강력 고려", "score": danger}
    elif danger >= 2:
        return {"level": "🟠 경고", "color": "orange", "action": "일부 매도 고려", "score": danger}
    elif danger >= 1 or warn >= 2:
        return {"level": "🟡 주의", "color": "yellow", "action": "포지션 축소 검토", "score": danger}
    else:
        return {"level": "🟢 안전", "color": "green",  "action": "유지", "score": 0}

# ── 메인 ──────────────────────────────────────────────────────────────
def main():
    now  = datetime.datetime.now(KST)
    today = now.date()

    print(f"\n{'='*50}")
    print(f"  StockPilot KR — 옵션만기 분석  {now.strftime('%Y%m%d %H:%M KST')}")
    print(f"{'='*50}")

    # 만기일 계산
    expiry = get_expiry_date(today)
    d_day  = (expiry - today).days
    print(f"  다음 만기일: {expiry.strftime('%Y-%m-%d')} (D-{d_day})")

    # D-6 이전이면 기본 정보만 저장하고 종료
    if d_day > 6:
        result = {
            "updated": now.strftime("%Y-%m-%d %H:%M"),
            "expiry_date": str(expiry),
            "d_day": d_day,
            "active": False,
            "message": f"만기일까지 {d_day}일 남음. D-6부터 상세 분석 시작.",
            "indicators": {},
            "judgment": {"level": "분석 대기", "color": "gray", "action": f"D-{d_day}", "score": 0}
        }
        with open(EXPIRY_FILE, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  → D-{d_day}: 아직 분석 시작 전 (D-6부터 활성화)")
        return

    # D-6 이내: 본격 분석
    print(f"\n  ⚠️  만기일 D-{d_day} — 4대 지표 분석 시작")

    if not APP_KEY:
        print("  ⚠️  KIS API 키 없음 — 실전 계좌 키 필요")
        return

    try:
        token = get_token()
        print("  ✅ KIS 토큰 발급 완료")
    except Exception as e:
        print(f"  ⚠️ 토큰 발급 실패: {e}")
        return

    indicators = {}

    print("\n  [① 베이시스]")
    indicators["basis"] = get_basis(token)
    if indicators["basis"]:
        print(f"    {indicators['basis']['signal']} {indicators['basis']['desc']}")
    time.sleep(0.3)

    print("\n  [② 외국인 선물 순매수]")
    indicators["foreign"] = get_foreign_futures(token)
    if indicators["foreign"]:
        print(f"    {indicators['foreign']['signal']} {indicators['foreign']['desc']}")
    time.sleep(0.3)

    print("\n  [③ 풋/콜 비율]")
    indicators["pcr"] = get_put_call_ratio(token)
    if indicators["pcr"]:
        print(f"    {indicators['pcr']['signal']} {indicators['pcr']['desc']}")
    time.sleep(0.3)

    print("\n  [④ 미결제약정]")
    indicators["oi"] = get_open_interest(token)
    if indicators["oi"]:
        print(f"    {indicators['oi']['signal']} {indicators['oi']['desc']}")

    # 종합 판단
    verdict = judge(indicators)
    print(f"\n  📊 종합 판단: {verdict['level']} → {verdict['action']}")

    result = {
        "updated": now.strftime("%Y-%m-%d %H:%M"),
        "expiry_date": str(expiry),
        "d_day": d_day,
        "active": True,
        "indicators": indicators,
        "judgment": verdict
    }

    with open(EXPIRY_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n  💾 {EXPIRY_FILE} 저장 완료")
    print(f"\n✅ 옵션 만기 분석 완료!")

if __name__ == "__main__":
    main()
