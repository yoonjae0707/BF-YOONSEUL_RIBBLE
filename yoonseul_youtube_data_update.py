import os
import json
import time
import logging
import requests
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
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
YT_API_KEY   = os.environ.get("YT_API_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_KEY 또는 SUPABASE_URL 환경 변수가 설정되지 않았습니다.")
if not YT_API_KEY:
    raise EnvironmentError("YT_API_KEY 환경 변수가 설정되지 않았습니다.")


# ─────────────────────────────────────────
# 공통 헤더
# ─────────────────────────────────────────
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

KST = pytz.timezone("Asia/Seoul")


# ─────────────────────────────────────────
# Supabase 유틸
# ─────────────────────────────────────────
def fetch_artists_with_youtube(session: requests.Session) -> list[dict]:
    """youtube_id가 있는 아티스트만 DB에서 가져옵니다."""
    url = f"{SUPABASE_URL}/rest/v1/ARTIST?select=name,youtube_id&youtube_id=not.is.null"
    res = session.get(url, headers=SUPABASE_HEADERS)
    res.raise_for_status()
    return res.json()


def update_artist_youtube_data(
    session: requests.Session,
    name: str,
    payload: dict
) -> None:
    """아티스트 유튜브 데이터를 DB에 업데이트합니다."""
    url = f"{SUPABASE_URL}/rest/v1/ARTIST?name=eq.{name}"
    res = session.patch(url, headers=SUPABASE_HEADERS, data=json.dumps(payload))
    res.raise_for_status()


# ─────────────────────────────────────────
# YouTube API 유틸
# ─────────────────────────────────────────
def fetch_youtube_channel(session: requests.Session, yt_id: str) -> dict | None:
    """YouTube API에서 채널 통계 및 정보를 가져옵니다."""
    url = (
        f"https://www.googleapis.com/youtube/v3/channels"
        f"?part=statistics,snippet&id={yt_id}&key={YT_API_KEY}"
    )
    res = session.get(url)
    res.raise_for_status()

    items = res.json().get("items", [])
    return items[0] if items else None


def parse_youtube_payload(channel: dict, now: str) -> dict:
    """YouTube 채널 데이터를 DB 업데이트용 payload로 변환합니다."""
    stats   = channel.get("statistics", {})
    snippet = channel.get("snippet", {})

    return {
        "yt_subs":          int(stats.get("subscriberCount", 0)),
        "youtube_views":    int(stats.get("viewCount", 0)),
        "youtube_ch_name":  snippet.get("title", ""),
        "last_updated":     now,
    }


# ─────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────
def update_youtube_data() -> None:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"🔄 [{now}] START YOUTUBE DATA SYNC...")

    with requests.Session() as session:
        artists = fetch_artists_with_youtube(session)
        logger.info(f"총 {len(artists)}명의 유튜브 아티스트 조회 완료")

        for artist in artists:
            name  = artist.get("name", "")
            yt_id = artist.get("youtube_id", "")

            try:
                channel = fetch_youtube_channel(session, yt_id)

                if not channel:
                    logger.warning(f"⚠️  {name}: 유튜브 채널 정보 없음 (ID: {yt_id})")
                    continue

                payload = parse_youtube_payload(channel, now)
                update_artist_youtube_data(session, name, payload)

                logger.info(
                    f"✅ {name}: 구독자 {payload['yt_subs']:,}명 / "
                    f"조회수 {payload['youtube_views']:,}회 갱신 완료"
                )

            except requests.HTTPError as e:
                logger.error(f"❌ {name} HTTP 에러: {e.response.status_code} {e.response.text}")
            except Exception as e:
                logger.error(f"❌ {name} 데이터 수집 중 예외 발생: {e}")

            # API 할당량 및 과부하 방지
            time.sleep(0.3)

    logger.info(f"🏁 [{now}] 유튜브 데이터 업데이트 완료!")


if __name__ == "__main__":
    update_youtube_data()
