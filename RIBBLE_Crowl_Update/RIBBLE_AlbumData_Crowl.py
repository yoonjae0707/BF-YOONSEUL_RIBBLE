import os
import re
import json
import time
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
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

if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_KEY 또는 SUPABASE_URL 환경 변수가 설정되지 않았습니다.")


# ─────────────────────────────────────────
# 공통 설정
# ─────────────────────────────────────────
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

KST = pytz.timezone("Asia/Seoul")

CRAWL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─────────────────────────────────────────
# 멜론 listType 상수
# ─────────────────────────────────────────
MELON_LIST_TYPE = {
    "ALL":        "0",   # 전체
    "REGULAR":    "1",   # 정규
    "SINGLE":     "2",   # 싱글/미니
    "OST":        "3",   # OST/방송
    "FEATURED":   "4",   # 참여
}

# 그룹: 전체 수집
# 솔로: 정규 + 싱글/미니 + OST만 수집 (참여 제외 → 그룹곡 중복 방지)
GROUP_LIST_TYPES = [MELON_LIST_TYPE["ALL"]]
SOLO_LIST_TYPES  = [
    MELON_LIST_TYPE["REGULAR"],
    MELON_LIST_TYPE["SINGLE"],
    MELON_LIST_TYPE["OST"],
]


# ─────────────────────────────────────────
# 멀티 아티스트 파싱 유틸
# ─────────────────────────────────────────
def parse_artists(artist_raw: str) -> tuple[str, list[str], str]:
    """
    멜론 아티스트 표기 문자열을 파싱합니다.
    예: "아카네 리제, 아이리 칸나" → ("아카네 리제", ["아카네 리제", "아이리 칸나"], "아카네 리제, 아이리 칸나")
    예: "이세계아이돌 (Feat. 주르르)" → ("이세계아이돌", ["이세계아이돌", "주르르"], "이세계아이돌 (Feat. 주르르)")

    반환: (main_artist, all_artists, artist_display)
    """
    if not artist_raw:
        return "", [], ""

    display = artist_raw.strip()

    # Feat./featuring 괄호 분리
    feat_artists: list[str] = []
    feat_match = re.search(r'\((?:Feat\.|feat\.|ft\.|Ft\.)\s*(.+?)\)', display)
    if feat_match:
        feat_part = feat_match.group(1)
        # 피처링 아티스트도 쉼표/&로 분리
        feat_artists = [a.strip() for a in re.split(r"[,&]", feat_part) if a.strip()]

    # 메인 아티스트 부분 (괄호 제거 후)
    main_part = re.sub(r'\((?:Feat\.|feat\.|ft\.|Ft\.).*?\)', '', display).strip()

    # 쉼표/&로 분리
    main_artists = [a.strip() for a in re.split(r"[,&]", main_part) if a.strip()]

    all_artists = main_artists + feat_artists
    main_artist = main_artists[0] if main_artists else display

    return main_artist, all_artists, display


# ─────────────────────────────────────────
# Supabase 유틸
# ─────────────────────────────────────────
def fetch_artists(session: requests.Session) -> list[dict]:
    """melon_id가 있는 아티스트 목록을 가져옵니다."""
    url = (
        f"{SUPABASE_URL}/rest/v1/ARTIST"
        f"?select=name,is_group,melon_id,bugs_id,genie_id,spotify_id"
        f"&melon_id=not.is.null"
    )
    res = session.get(url, headers=SUPABASE_HEADERS)
    res.raise_for_status()
    return res.json()


def fetch_saved_album_ids(session: requests.Session) -> set[str]:
    """이미 저장된 melon_album_id 목록을 가져옵니다. (앨범 중복 저장 방지)"""
    url = f"{SUPABASE_URL}/rest/v1/MusicData_Album?select=melon_album_id"
    res = session.get(url, headers=SUPABASE_HEADERS)
    res.raise_for_status()
    return {r["melon_album_id"] for r in res.json() if r.get("melon_album_id")}


def upsert_album(session: requests.Session, albums: list[dict]) -> None:
    """앨범 정보를 MusicData_Album 테이블에 upsert합니다."""
    if not albums:
        return
    url = f"{SUPABASE_URL}/rest/v1/MusicData_Album?on_conflict=melon_album_id"
    headers = {**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    res = session.post(url, headers=headers, data=json.dumps(albums))
    res.raise_for_status()


def upsert_track(session: requests.Session, tracks: list[dict]) -> None:
    """트랙 정보를 MusicData_track 테이블에 upsert합니다."""
    if not tracks:
        return
    has_key = [t for t in tracks if t.get("melon_track_id")]
    no_key  = [t for t in tracks if not t.get("melon_track_id")]

    if has_key:
        url = f"{SUPABASE_URL}/rest/v1/MusicData_track?on_conflict=melon_track_id"
        headers = {**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
        res = session.post(url, headers=headers, data=json.dumps(has_key))
        res.raise_for_status()

    if no_key:
        url = f"{SUPABASE_URL}/rest/v1/MusicData_track"
        res = session.post(url, headers=SUPABASE_HEADERS, data=json.dumps(no_key))
        res.raise_for_status()


# ─────────────────────────────────────────
# 멜론 — listType별 앨범 목록 수집
# URL: https://www.melon.com/artist/album.htm?artistId={id}&listType={type}
# ─────────────────────────────────────────
def crawl_melon_albums_by_type(
    session: requests.Session,
    artist_name: str,
    melon_artist_id: str,
    list_type: str,
    saved_album_ids: set[str],
    now: str
) -> list[dict]:
    """
    멜론 아티스트 앨범 페이지에서 특정 listType의 앨범 목록을 수집합니다.
    이미 DB에 저장된 앨범은 스킵합니다.
    """
    url = "https://www.melon.com/artist/album.htm"
    params = {"artistId": melon_artist_id, "listType": list_type}
    headers = {**CRAWL_HEADERS, "Referer": "https://www.melon.com/"}

    res = session.get(url, params=params, headers=headers, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    albums = []
    items = soup.select("ul.album11_ul > li.album11_li")

    for item in items:
        try:
            wrap = item.select_one("div.wrap_album04")
            if not wrap:
                continue

            # 앨범 ID
            melon_album_id = None
            for a_tag in wrap.select("a"):
                src = a_tag.get("href", "") + a_tag.get("onclick", "")
                m = re.search(r"goAlbumDetail\('?(\d+)'?\)", src)
                if m:
                    melon_album_id = m.group(1)
                    break
            if not melon_album_id:
                continue

            # 이미 저장된 앨범은 스킵 (artist_name 오버랩 방지)
            if melon_album_id in saved_album_ids:
                continue

            # 앨범명
            album_link = wrap.select_one("a.ellipsis[title]")
            album_name = ""
            if album_link:
                album_name = album_link.get("title", "").replace(" - 페이지 이동", "").strip()

            # 앨범 타입
            type_el = wrap.select_one("span.vdo_name")
            album_type = type_el.text.strip().strip("[]") if type_el else ""

            # 발매일
            date_el = wrap.select_one("span.cnt_view")
            release_date = date_el.text.strip() if date_el else ""

            # 수록곡 수
            count_el = wrap.select_one("span.tot_song")
            track_count = None
            if count_el:
                count_text = count_el.text.replace("곡", "").strip()
                if count_text.isdigit():
                    track_count = int(count_text)

            # 앨범 커버
            img_el = wrap.select_one("a.thumb img")
            album_image_url = img_el["src"] if img_el else None

            saved_album_ids.add(melon_album_id)  # 즉시 등록해서 같은 실행 내 중복 방지

            albums.append({
                "melon_album_id":          melon_album_id,
                "artist_name":             artist_name,
                "album_name":              album_name,
                "album_type":              album_type,
                "release_date":            release_date,
                "track_count":             track_count,
                "album_image_url":         album_image_url,
                "album_image_storage_url": None,
                "last_updated":            now,
            })

        except Exception as e:
            logger.warning(f"    ⚠️  멜론 앨범 파싱 실패: {e}")
            continue

    return albums


# ─────────────────────────────────────────
# 멜론 — 앨범 상세 → 트랙 목록 수집
# URL: https://www.melon.com/album/detail.htm?albumId={id}
# ─────────────────────────────────────────
def crawl_melon_album_detail(
    session: requests.Session,
    artist_name: str,
    melon_album_id: str,
    now: str
) -> list[dict]:
    """멜론 앨범 상세 페이지에서 전체 트랙 목록을 수집합니다."""
    url = "https://www.melon.com/album/detail.htm"
    params = {"albumId": melon_album_id}
    headers = {**CRAWL_HEADERS, "Referer": "https://www.melon.com/"}

    res = session.get(url, params=params, headers=headers, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    tracks = []
    items = soup.select("tr.lst50, tr.lst100")

    for item in items:
        try:
            chk = item.select_one("input[type='checkbox']")
            melon_track_id = chk["value"].strip() if chk else None
            if not melon_track_id:
                continue

            track_name_el = (
                item.select_one(".ellipsis.rank01 a") or
                item.select_one(".song_name a") or
                item.select_one(".tit_song a")
            )
            track_name = track_name_el.text.strip() if track_name_el else ""

            # 아티스트명 파싱 (멀티 아티스트 대응)
            track_artist_el = (
                item.select_one(".ellipsis.rank02 span.checkEllipsis") or
                item.select_one(".ellipsis.rank02 a")
            )
            track_artist_raw = track_artist_el.text.strip() if track_artist_el else artist_name
            main_artist, all_artists, artist_display = parse_artists(track_artist_raw)

            track_num_el = item.select_one(".t_num")
            track_number = None
            if track_num_el:
                num_text = track_num_el.text.strip()
                if num_text.isdigit():
                    track_number = int(num_text)

            duration_el = item.select_one(".t_time")
            duration = duration_el.text.strip() if duration_el else ""

            is_title        = bool(item.select_one(".ico_title"))
            lyrics_available = bool(item.select_one(".ico_lyrics"))

            like_el = item.select_one(".t_like")
            melon_like_count = None
            if like_el:
                like_text = like_el.text.strip().replace(",", "")
                if like_text.isdigit():
                    melon_like_count = int(like_text)

            melon_track_url = f"https://www.melon.com/song/detail.htm?songId={melon_track_id}"

            tracks.append({
                "melon_track_id":   melon_track_id,
                "melon_album_id":   melon_album_id,
                "artist_name":      artist_display,   # 원문 그대로 (표시용)
                "main_artist":      main_artist,       # 첫 번째 대표 아티스트
                "all_artists":      all_artists,       # 전체 아티스트 배열 (필터/검색용)
                "artist_display":   artist_display,    # 멜론 표기 원문
                "track_name":       track_name,
                "track_number":     track_number,
                "duration":         duration,
                "is_title":         is_title,
                "lyrics_available": lyrics_available,
                "melon_like_count": melon_like_count,
                "melon_track_url":  melon_track_url,
                "last_updated":     now,
            })

        except Exception as e:
            logger.warning(f"    ⚠️  멜론 트랙 파싱 실패: {e}")
            continue

    return tracks


def crawl_melon(
    session: requests.Session,
    artist_name: str,
    melon_artist_id: str,
    is_group: bool,
    saved_album_ids: set[str],
    now: str
) -> tuple[list[dict], list[dict]]:
    """
    멜론 아티스트 페이지에서 앨범 + 트랙을 수집합니다.
    - 그룹: listType=0 (전체)
    - 솔로: listType=1,2,3 (정규+싱글/미니+OST, 참여 제외)
    """
    list_types = GROUP_LIST_TYPES if is_group else SOLO_LIST_TYPES
    type_labels = {
        "0": "전체", "1": "정규", "2": "싱글/미니",
        "3": "OST/방송", "4": "참여"
    }

    all_albums: list[dict] = []
    all_tracks: list[dict] = []

    for list_type in list_types:
        label = type_labels.get(list_type, list_type)
        logger.info(f"    [{label}] 앨범 수집 중...")

        albums = crawl_melon_albums_by_type(
            session, artist_name, melon_artist_id,
            list_type, saved_album_ids, now
        )
        logger.info(f"    [{label}] 신규 앨범 {len(albums)}개")
        all_albums.extend(albums)

        # 각 앨범 상세 → 트랙
        for album in albums:
            try:
                tracks = crawl_melon_album_detail(
                    session, artist_name, album["melon_album_id"], now
                )
                all_tracks.extend(tracks)
                logger.info(
                    f"      📀 [{album['album_name']}] → 트랙 {len(tracks)}곡"
                )
                time.sleep(0.7)
            except Exception as e:
                logger.warning(
                    f"      ⚠️  앨범 {album['melon_album_id']} 상세 수집 실패: {e}"
                )
                continue

        time.sleep(0.5)

    logger.info(
        f"  🍈 멜론 최종: 앨범 {len(all_albums)}개 / 트랙 {len(all_tracks)}곡"
    )
    return all_albums, all_tracks


# ─────────────────────────────────────────
# 벅스 — 앨범 목록 → 트랙 수치 수집
# URL: https://music.bugs.co.kr/artist/{id}/albums
# ─────────────────────────────────────────
def crawl_bugs(
    session: requests.Session,
    artist_name: str,
    bugs_artist_id: str,
    now: str
) -> list[dict]:
    """벅스 아티스트 앨범 페이지에서 트랙 수치를 수집합니다."""
    url = f"https://music.bugs.co.kr/artist/{bugs_artist_id}/albums"
    headers = {**CRAWL_HEADERS, "Referer": "https://music.bugs.co.kr/"}

    res = session.get(url, headers=headers, timeout=15)
    res.raise_for_status()

    if "You need to enable JavaScript" in res.text or len(res.text) < 2000:
        logger.warning("  ⚠️  벅스: SPA 감지 → 스킵")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    stats = []
    album_ids_seen: set[str] = set()

    for link in soup.select("a[href*='/album/']"):
        m = re.search(r"/album/(\d+)", link.get("href", ""))
        if not m:
            continue
        bugs_album_id = m.group(1)
        if bugs_album_id in album_ids_seen:
            continue
        album_ids_seen.add(bugs_album_id)

        try:
            a_res = session.get(
                f"https://music.bugs.co.kr/album/{bugs_album_id}",
                headers=headers, timeout=15
            )
            a_res.raise_for_status()
            a_soup = BeautifulSoup(a_res.text, "html.parser")

            for item in a_soup.select("table.list tbody tr"):
                try:
                    bugs_track_id = None
                    t_link = item.select_one("a[href*='/track/']")
                    if t_link:
                        m2 = re.search(r"/track/(\d+)", t_link.get("href", ""))
                        if m2:
                            bugs_track_id = m2.group(1)

                    track_name_el = item.select_one(".title a")
                    track_name = track_name_el.text.strip() if track_name_el else ""

                    listener_el = item.select_one(".listener") or item.select_one(".t_listener")
                    bugs_listener_count = None
                    if listener_el:
                        txt = listener_el.text.strip().replace(",", "")
                        if txt.isdigit():
                            bugs_listener_count = int(txt)

                    like_el = item.select_one(".like_count") or item.select_one(".t_like")
                    bugs_like_count = None
                    if like_el:
                        txt = like_el.text.strip().replace(",", "")
                        if txt.isdigit():
                            bugs_like_count = int(txt)

                    bugs_track_url = (
                        f"https://music.bugs.co.kr/track/{bugs_track_id}"
                        if bugs_track_id else None
                    )

                    if track_name:
                        stats.append({
                            "artist_name":         artist_name,
                            "track_name":          track_name,
                            "bugs_track_id":       bugs_track_id,
                            "bugs_track_url":      bugs_track_url,
                            "bugs_listener_count": bugs_listener_count,
                            "bugs_like_count":     bugs_like_count,
                            "last_updated":        now,
                        })
                except Exception as e:
                    logger.warning(f"      ⚠️  벅스 트랙 파싱 실패: {e}")
                    continue

            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"    ⚠️  벅스 앨범 {bugs_album_id} 실패: {e}")
            continue

    logger.info(f"  🐛 벅스: {len(stats)}곡 수치 수집")
    return stats


# ─────────────────────────────────────────
# 지니뮤직 — 앨범 목록 → 트랙 수치 수집
# URL: https://www.genie.co.kr/detail/artistAlbum?xxnm={id}
# ─────────────────────────────────────────
def crawl_genie(
    session: requests.Session,
    artist_name: str,
    genie_artist_id: str,
    now: str
) -> list[dict]:
    """지니뮤직 아티스트 앨범 페이지에서 트랙 수치를 수집합니다."""
    url = "https://www.genie.co.kr/detail/artistAlbum"
    params = {"xxnm": genie_artist_id}
    headers = {**CRAWL_HEADERS, "Referer": "https://www.genie.co.kr/"}

    res = session.get(url, params=params, headers=headers, timeout=15)
    res.raise_for_status()

    if "You need to enable JavaScript" in res.text or len(res.text) < 2000:
        logger.warning("  ⚠️  지니: SPA 감지 → 스킵")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    stats = []
    album_ids_seen: set[str] = set()

    for link in soup.select("a[href*='albumInfo']") + soup.select("a[onclick*='axnm']"):
        genie_album_id = None
        for src in [link.get("href", ""), link.get("onclick", "")]:
            m = re.search(r"axnm[='\",]+(\d+)", src)
            if m:
                genie_album_id = m.group(1)
                break
        if not genie_album_id or genie_album_id in album_ids_seen:
            continue
        album_ids_seen.add(genie_album_id)

        try:
            a_res = session.get(
                f"https://www.genie.co.kr/detail/albumInfo?axnm={genie_album_id}",
                headers=headers, timeout=15
            )
            a_res.raise_for_status()
            a_soup = BeautifulSoup(a_res.text, "html.parser")

            for item in a_soup.select("tr.list"):
                try:
                    genie_track_id = None
                    song_el = (
                        item.select_one("a[onclick*='fnPlaySong']") or
                        item.select_one("a[href*='songInfo']")
                    )
                    if song_el:
                        for src in [song_el.get("onclick", ""), song_el.get("href", "")]:
                            m2 = re.search(r"xgnm[='\",]+(\d+)|fnPlaySong\(['\"]?(\d+)", src)
                            if m2:
                                genie_track_id = m2.group(1) or m2.group(2)
                                break

                    track_name_el = item.select_one(".title") or item.select_one(".song-name")
                    track_name = track_name_el.text.strip() if track_name_el else ""

                    listener_el = item.select_one(".listener") or item.select_one(".t_listener")
                    genie_listener_count = None
                    if listener_el:
                        txt = listener_el.text.strip().replace(",", "")
                        if txt.isdigit():
                            genie_listener_count = int(txt)

                    like_el = item.select_one(".like") or item.select_one(".t_like")
                    genie_like_count = None
                    if like_el:
                        txt = like_el.text.strip().replace(",", "")
                        if txt.isdigit():
                            genie_like_count = int(txt)

                    genie_track_url = (
                        f"https://www.genie.co.kr/detail/songInfo?xgnm={genie_track_id}"
                        if genie_track_id else None
                    )

                    if track_name:
                        stats.append({
                            "artist_name":          artist_name,
                            "track_name":           track_name,
                            "genie_track_id":       genie_track_id,
                            "genie_track_url":      genie_track_url,
                            "genie_listener_count": genie_listener_count,
                            "genie_like_count":     genie_like_count,
                            "last_updated":         now,
                        })
                except Exception as e:
                    logger.warning(f"      ⚠️  지니 트랙 파싱 실패: {e}")
                    continue

            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"    ⚠️  지니 앨범 {genie_album_id} 실패: {e}")
            continue

    logger.info(f"  🎵 지니: {len(stats)}곡 수치 수집")
    return stats


# ─────────────────────────────────────────
# Spotify — 웹 크롤링 (__NEXT_DATA__ SSR)
# ─────────────────────────────────────────
def crawl_spotify(
    session: requests.Session,
    artist_name: str,
    spotify_artist_id: str,
    now: str
) -> list[dict]:
    """Spotify 아티스트 페이지에서 트랙 데이터를 수집합니다."""
    url = f"https://open.spotify.com/artist/{spotify_artist_id}"
    headers = {
        **CRAWL_HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://open.spotify.com/",
    }

    res = session.get(url, headers=headers, timeout=15)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    stats = []

    # SSR JSON 우선 시도
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag:
        try:
            data = json.loads(next_data_tag.string)
            artist_data = (
                data.get("props", {})
                    .get("pageProps", {})
                    .get("state", {})
                    .get("data", {})
                    .get("artist", {})
            )
            discography = artist_data.get("discography", {})

            for section_key in ["albums", "singles", "compilations"]:
                for album_item in discography.get(section_key, {}).get("items", []):
                    releases = album_item.get("releases", {}).get("items", [{}])
                    album = releases[0] if releases else {}
                    for track in album.get("tracks", {}).get("items", []):
                        t = track.get("track", track)
                        sp_id = t.get("id") or t.get("uri", "").split(":")[-1]
                        sp_url = f"https://open.spotify.com/track/{sp_id}" if sp_id else None
                        track_name = t.get("name", "")
                        if track_name:
                            stats.append({
                                "artist_name":        artist_name,
                                "track_name":         track_name,
                                "spotify_track_id":   sp_id,
                                "spotify_track_url":  sp_url,
                                "spotify_popularity": t.get("playcount"),
                                "last_updated":       now,
                            })

            if stats:
                logger.info(f"  🎧 Spotify (SSR): {len(stats)}곡 수집")
                return stats
        except Exception as e:
            logger.warning(f"  ⚠️  Spotify SSR 파싱 실패: {e}")

    # HTML fallback
    for row in soup.select("div[data-testid='tracklist-row']"):
        try:
            name_el = row.select_one("div[data-testid='internal-track-link']") or \
                      row.select_one("a[href*='/track/']")
            track_name = name_el.text.strip() if name_el else ""

            t_link = row.select_one("a[href*='/track/']")
            sp_id, sp_url = None, None
            if t_link:
                m = re.search(r"/track/([A-Za-z0-9]+)", t_link.get("href", ""))
                if m:
                    sp_id  = m.group(1)
                    sp_url = f"https://open.spotify.com/track/{sp_id}"

            if track_name:
                stats.append({
                    "artist_name":        artist_name,
                    "track_name":         track_name,
                    "spotify_track_id":   sp_id,
                    "spotify_track_url":  sp_url,
                    "spotify_popularity": None,
                    "last_updated":       now,
                })
        except Exception as e:
            logger.warning(f"    ⚠️  Spotify HTML 파싱 실패: {e}")
            continue

    if not stats:
        logger.warning("  ⚠️  Spotify: 데이터 추출 실패 (SPA 가능성)")
    logger.info(f"  🎧 Spotify (HTML fallback): {len(stats)}곡 수집")
    return stats


# ─────────────────────────────────────────
# 수치 병합 유틸
# ─────────────────────────────────────────
def merge_stats_into_tracks(
    tracks: list[dict],
    stats_list: list[dict],
    fields: list[str]
) -> tuple[list[dict], list[dict]]:
    """track_name 기준으로 수치 데이터를 멜론 트랙에 병합합니다."""
    track_map: dict[str, dict] = {}
    for t in tracks:
        key = t.get("track_name", "").strip().lower()
        track_map[key] = t

    unmatched: list[dict] = []
    for stat in stats_list:
        key = stat.get("track_name", "").strip().lower()
        if key in track_map:
            for field in fields:
                if stat.get(field) is not None:
                    track_map[key][field] = stat[field]
        else:
            unmatched.append({
                f: stat.get(f)
                for f in ["artist_name", "track_name", *fields, "last_updated"]
            })

    return list(track_map.values()), unmatched


# ─────────────────────────────────────────
# 아티스트 단위 처리
# ─────────────────────────────────────────
def process_artist(
    session: requests.Session,
    artist: dict,
    saved_album_ids: set[str],
    now: str
) -> dict:
    """아티스트 1명의 전체 음원 데이터를 수집 후 DB에 저장합니다."""
    name       = artist["name"]
    is_group   = artist.get("is_group", False) or False
    melon_id   = artist.get("melon_id")
    bugs_id    = artist.get("bugs_id")
    genie_id   = artist.get("genie_id")
    spotify_id = artist.get("spotify_id")

    result = {"albums": 0, "tracks": 0}
    albums: list[dict] = []
    tracks: list[dict] = []

    group_label = "그룹" if is_group else "솔로"
    logger.info(f"  [{group_label}] 멜론 수집 시작...")

    # ── 1. 멜론 ──
    try:
        albums, tracks = crawl_melon(
            session, name, melon_id, is_group, saved_album_ids, now
        )
    except requests.HTTPError as e:
        logger.error(f"  ❌ 멜론 HTTP 에러: {e.response.status_code}")
    except Exception as e:
        logger.error(f"  ❌ 멜론 수집 실패: {e}")
    time.sleep(1)

    # ── 2. 벅스 ──
    if bugs_id:
        try:
            bugs_stats = crawl_bugs(session, name, bugs_id, now)
            tracks, unmatched = merge_stats_into_tracks(
                tracks, bugs_stats,
                ["bugs_track_id", "bugs_track_url", "bugs_listener_count", "bugs_like_count"]
            )
            if unmatched:
                upsert_track(session, unmatched)
        except requests.HTTPError as e:
            logger.error(f"  ❌ 벅스 HTTP 에러: {e.response.status_code}")
        except Exception as e:
            logger.error(f"  ❌ 벅스 수집 실패: {e}")
        time.sleep(1)
    else:
        logger.info("  ⏭️  벅스 ID 없음 → 스킵")

    # ── 3. 지니 ──
    if genie_id:
        try:
            genie_stats = crawl_genie(session, name, genie_id, now)
            tracks, unmatched = merge_stats_into_tracks(
                tracks, genie_stats,
                ["genie_track_id", "genie_track_url", "genie_listener_count", "genie_like_count"]
            )
            if unmatched:
                upsert_track(session, unmatched)
        except requests.HTTPError as e:
            logger.error(f"  ❌ 지니 HTTP 에러: {e.response.status_code}")
        except Exception as e:
            logger.error(f"  ❌ 지니 수집 실패: {e}")
        time.sleep(1)
    else:
        logger.info("  ⏭️  지니 ID 없음 → 스킵")

    # ── 4. Spotify ──
    if spotify_id:
        try:
            sp_stats = crawl_spotify(session, name, spotify_id, now)
            tracks, unmatched = merge_stats_into_tracks(
                tracks, sp_stats,
                ["spotify_track_id", "spotify_track_url", "spotify_popularity"]
            )
            if unmatched:
                upsert_track(session, unmatched)
        except requests.HTTPError as e:
            logger.error(f"  ❌ Spotify HTTP 에러: {e.response.status_code}")
        except Exception as e:
            logger.error(f"  ❌ Spotify 수집 실패: {e}")
        time.sleep(1)
    else:
        logger.info("  ⏭️  Spotify ID 없음 → 스킵")

    # ── 5. DB 저장 ──
    if albums:
        upsert_album(session, albums)
        result["albums"] = len(albums)

    if tracks:
        upsert_track(session, tracks)
        result["tracks"] = len(tracks)

    logger.info(
        f"  💾 저장 완료 — 앨범 {result['albums']}개 / 트랙 {result['tracks']}곡"
    )
    return result


# ─────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────
def update_music_data() -> None:
    now = datetime.now(KST).strftime("%Y-%m-%dT%H:%M:%S+09:00")
    logger.info(f"🔄 [{now}] START MUSIC DATA SYNC...")

    with requests.Session() as session:
        artists = fetch_artists(session)
        logger.info(f"🎤 수집 대상 아티스트 {len(artists)}명")

        # 실행 시작 시 저장된 앨범 ID 한 번만 로드 (전체 실행에서 공유)
        saved_album_ids = fetch_saved_album_ids(session)
        logger.info(f"🗂️  기존 저장 앨범 {len(saved_album_ids)}개 확인")

        total_albums = 0
        total_tracks = 0

        for artist in artists:
            name = artist.get("name", "")
            is_group = artist.get("is_group", False)
            label = "그룹" if is_group else "솔로"
            logger.info(f"\n🎤 [{name}] ({label}) 처리 중...")

            try:
                result = process_artist(session, artist, saved_album_ids, now)
                total_albums += result["albums"]
                total_tracks += result["tracks"]
            except Exception as e:
                logger.error(f"❌ {name} 전체 처리 실패: {e}")

            time.sleep(1.5)

    logger.info(
        f"\n🏁 [{now}] MUSIC DATA SYNC 완료 | "
        f"총 앨범 {total_albums}개 / 트랙 {total_tracks}곡 저장"
    )


if __name__ == "__main__":
    update_music_data()
