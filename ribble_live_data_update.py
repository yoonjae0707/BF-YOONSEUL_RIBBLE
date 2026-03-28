import requests
import json
import os
import logging
from datetime import datetime
import pytz

# ─────────────────────────────────────────
# 로깅 설정
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 환경 변수
# ─────────────────────────────────────────
SUPABASE_KEY    = os.environ.get("SUPABASE_KEY", "")
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
# ── SOOP (API 사용 범위 문의 전 / 확인 후 주석 해제) ──
# SOOP_ID     = os.environ.get("SOOP_ID", "")
# SOOP_SECRET = os.environ.get("SOOP_SECRET", "")
SOOP_ID     = ""  # 임시 비활성화
SOOP_SECRET = ""  # 임시 비활성화

if not SUPABASE_KEY or not SUPABASE_URL:
    raise EnvironmentError("SUPABASE_KEY 또는 SUPABASE_URL 환경 변수가 설정되지 않았습니다.")


# ─────────────────────────────────────────
# 공통 헤더
# ─────────────────────────────────────────
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

CHZZK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://chzzk.naver.com/"
}


# ─────────────────────────────────────────
# Supabase 유틸
# ─────────────────────────────────────────
def fetch_artists(session: requests.Session) -> list[dict]:
    """DB에서 아티스트 목록을 가져옵니다."""
    url = f"{SUPABASE_URL}/rest/v1/ARTIST?select=name,live_id,live_platform"
    res = session.get(url, headers=SUPABASE_HEADERS)
    res.raise_for_status()
    return res.json()


def update_artist_live_status(
    session: requests.Session,
    name: str,
    is_live: bool,
    viewers: int,
    now: str
) -> None:
    """아티스트 라이브 상태를 DB에 업데이트합니다."""
    url = f"{SUPABASE_URL}/rest/v1/ARTIST?name=eq.{name}"
    payload = {
        "live": is_live,
        "viewer_count": viewers,
        "last_updated": now
    }
    res = session.patch(url, headers=SUPABASE_HEADERS, data=json.dumps(payload))
    res.raise_for_status()


def insert_live_log(
    session: requests.Session,
    name: str,
    viewers: int
) -> None:
    """라이브 로그를 DB에 기록합니다."""
    url = f"{SUPABASE_URL}/rest/v1/live_log"
    payload = {"artist_name": name, "viewer_count": viewers}
    res = session.post(url, headers=SUPABASE_HEADERS, data=json.dumps(payload))
    res.raise_for_status()
    logger.info(f"📝 {name} 라이브 로그 기록 완료")


# ─────────────────────────────────────────
# SOOP (API 사용 범위 문의 후 활성화 예정)
# ─────────────────────────────────────────
# ⚠️ TODO: SOOP API 문의 완료 후 아래 주석 해제
#   - 인증 흐름: /auth/code → /auth/token (2단계)
#   - grant_type, endpoint URL 문서 확인 후 수정 필요

# def get_soop_token(session: requests.Session) -> str | None:
#     """SOOP OAuth 토큰을 발급받습니다."""
#     if not SOOP_ID or not SOOP_SECRET:
#         logger.warning("SOOP_ID 또는 SOOP_SECRET이 설정되지 않아 SOOP 토큰 발급을 건너뜁니다.")
#         return None
#
#     # TODO: 정확한 엔드포인트 및 인증 흐름 확인 필요
#     url = "https://openapi.sooplive.co.kr/auth/token"
#     data = {
#         "grant_type": "authorization_code",
#         "client_id": SOOP_ID,
#         "client_secret": SOOP_SECRET,
#         "code": "<1단계에서 발급받은 code>"
#     }
#     res = session.post(url, data=data)
#     res.raise_for_status()
#     token = res.json().get("access_token")
#     if not token:
#         logger.error("SOOP 토큰 발급 실패: access_token이 없습니다.")
#     return token


# def check_soop_live(session: requests.Session, live_id: str, token: str) -> tuple[bool, int]:
#     """SOOP 라이브 상태를 확인합니다."""
#     url = f"https://openapi.sooplive.co.kr/broad/free/v1/channel/{live_id}"
#     headers = {"Authorization": f"Bearer {token}"}
#     res = session.get(url, headers=headers)
#     res.raise_for_status()
#
#     broad = res.json().get("broad", {})
#     is_live = broad.get("is_broad") is True
#     viewers = broad.get("total_view_cnt", 0) if is_live else 0
#     return is_live, viewers


# ─────────────────────────────────────────
# 치지직
# ─────────────────────────────────────────
def check_chzzk_live(session: requests.Session, live_id: str) -> tuple[bool, int]:
    """치지직 라이브 상태를 확인합니다."""
    url = f"https://api.chzzk.naver.com/service/v2/channels/{live_id}/live-detail"
    res = session.get(url, headers=CHZZK_HEADERS)
    res.raise_for_status()

    content = res.json().get("content", {})
    is_live = content.get("status") == "OPEN"
    viewers = content.get("concurrentUserCount", 0) if is_live else 0
    return is_live, viewers


# ─────────────────────────────────────────
# 플랫폼 분기
# ─────────────────────────────────────────
def check_live_status(
    session: requests.Session,
    platform: str,
    live_id: str,
    soop_token: str | None  # ⚠️ SOOP 활성화 전까지 항상 None
) -> tuple[bool, int]:
    """플랫폼에 따라 라이브 상태를 확인합니다."""
    if platform == "치지직" and live_id:
        return check_chzzk_live(session, live_id)

    # ⚠️ SOOP: API 문의 완료 후 주석 해제
    # elif platform == "SOOP" and live_id and soop_token:
    #     return check_soop_live(session, live_id, soop_token)

    # 지원하지 않는 플랫폼이거나 ID 없음
    return False, 0


# ─────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────
def run_live_update() -> None:
    kst = pytz.timezone("Asia/Seoul")
    now = datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

    with requests.Session() as session:  # ✅ Session 재사용으로 성능 개선
        artists = fetch_artists(session)
        logger.info(f"총 {len(artists)}명의 아티스트 조회 완료")

        # ⚠️ SOOP: API 문의 완료 후 주석 해제
        # soop_token = get_soop_token(session) if SOOP_ID else None
        soop_token = None  # 임시 비활성화

        for artist in artists:
            name     = artist.get("name", "")
            live_id  = artist.get("live_id", "")
            platform = artist.get("live_platform", "")

            try:
                is_live, viewers = check_live_status(session, platform, live_id, soop_token)
                update_artist_live_status(session, name, is_live, viewers, now)

                if is_live:
                    logger.info(f"🔴 {name} 라이브 중 | 시청자: {viewers:,}명")
                    insert_live_log(session, name, viewers)
                else:
                    logger.info(f"⚫ {name} 오프라인")

            except requests.HTTPError as e:
                logger.error(f"❌ {name} HTTP 에러: {e.response.status_code} {e.response.text}")
            except Exception as e:
                logger.error(f"❌ {name} 업데이트 중 예외 발생: {e}")

    logger.info(f"🏁 [{now}] 모든 아티스트 업데이트 완료!")


if __name__ == "__main__":
    run_live_update()
