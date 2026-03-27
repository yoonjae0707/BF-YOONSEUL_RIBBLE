import requests
import json
import os
from datetime import datetime
import pytz

# 1. 환경 변수 읽어오기
KEY = os.environ.get("SUPABASE_KEY")
URL = os.environ.get("SUPABASE_URL")
SOOP_ID = os.environ.get("SOOP_ID", "")

# 2. HEADERS 정의
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json"
}
CHZZK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://chzzk.naver.com/"
}

# 3. 데이터 가져올 URL 설정
GET_URL = f"{URL}/rest/v1/ARTIST?select=*"

def run_live_update():
    response = requests.get(GET_URL, headers=HEADERS)
    artists = response.json()
    soop_token = get_soop_token() if SOOP_ID else None
    
    # ... 나머지 로직 ...
    print(f"조회 성공: {len(artists)}명")

# 4. 함수 실행
if __name__ == "__main__":
    run_live_update()

# [기능 1] SOOP 토큰 가져오기 (API 문서 내용 반영)
def get_soop_token():
    # SOOP은 보안을 위해 '토큰'을 먼저 받아야 합니다.
    auth_url = "https://openapi.sooplive.co.kr/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": SOOP_ID,
        "client_secret": SOOP_SECRET
    }
    # post 요청을 보내서 응답을 받으세요.
    res = requests.post(auth_url, data=data).json()
    return res.get("access_token")

def run_live_update():
    # 2. DB에서 아티스트 리스트 가져오기
    get_url = f"{URL}/rest/v1/ARTIST?select=name,live_id,live_platform"
    artists = requests.get(get_url, headers=HEADERS).json()

    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

    soop_token = get_soop_token() if SOOP_ID else None

    for artist in artists:
        name = artist.get('name')
        l_id = artist.get('live_id')
        platform = artist.get('live_platform')
        
        is_live = False
        viewers = 0

        try:
            # --- [플랫폼 분기 로직] ---
            if platform == "치지직" and l_id:
                # 치지직 API 호출 (V2 live-detail)
                cz_url = f"https://api.chzzk.naver.com/service/v2/channels/{l_id}/live-detail"
                res = requests.get(cz_url, headers=CHZZK_HEADERS).json()
                content = res.get('content')
                if content and content.get('status') == "OPEN":
                    is_live = True
                    viewers = content.get('concurrentUserCount', 0)

            elif platform == "SOOP" and l_id and soop_token:
                # [기능 2] SOOP API 호출 (방금 분석한 문서 적용)
                soop_url = f"https://openapi.sooplive.co.kr/broad/free/v1/channel/{l_id}"
                s_headers = {"Authorization": f"Bearer {soop_token}"}
                s_res = requests.get(soop_url, headers=s_headers).json()
                
                broad = s_res.get("broad", {})
                if broad.get("is_broad") == True:
                    is_live = True
                    viewers = broad.get("total_view_cnt", 0)

            # 3. DB 업데이트 (ARTIST 테이블 PATCH)
            payload = {
                "live": is_live,
                "viewer_count": viewers,
                "last_updated": now
            }
            patch_url = f"{URL}/rest/v1/ARTIST?name=eq.{name}"
            requests.patch(patch_url, headers=HEADERS, data=json.dumps(payload))

            # 4. 로그 기록 (LIVE_LOG 테이블 POST)
            if is_live:
                log_payload = {"artist_name": name, "viewer_count": viewers}
                requests.post(f"{URL}/rest/v1/live_log", headers=HEADERS, data=json.dumps(log_payload))
                print(f"📝 {name} 로그 기록 시도 완료!")

        except Exception as e:
            print(f"❌ {name} 업데이트 중 에러 발생: {e}")

    print(f"🏁 [{now}] 모든 아티스트 업데이트 완료!")

if __name__ == "__main__":
    run_live_update()
