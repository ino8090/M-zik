import os
import sys
import time
import json
import logging
import subprocess
from typing import Optional, List
import requests
import yt_dlp

# --- LOGGING YAPILANDIRMASI ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- AYARLAR VE SABİTLER ---
M3U_URL = "https://raw.githubusercontent.com/ino8090/0101/refs/heads/main/max.m3u"
LOGO_URL = "https://raw.githubusercontent.com/ino8090/0101/refs/heads/main/file_000000007be48210a068edefa7260629.png"
RTMP_DEST = "rtmp://ssh101.bozztv.com:1935/ssh101/maxmuzik"
COOKIE_FILE = "cookies.txt"
LOGO_FILE = "logo.png"


def download_file(url: str, destination: str) -> bool:
    """Belirtilen URL'deki dosyayı yerel diske indirir."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        with open(destination, "wb") as f:
            f.write(response.content)
        logging.info(f"✅ Dosya indirildi: {destination}")
        return True
    except Exception as e:
        logging.error(f"❌ Dosya indirme hatası ({url}): {e}")
        return False


def validate_cookie_file(file_path: str) -> bool:
    """cookies.txt dosyasının varlığını ve Netscape formatında olup olmadığını denetler."""
    if not os.path.exists(file_path):
        return False
    
    if os.path.getsize(file_path) < 20:
        logging.warning("⚠️ cookies.txt çok küçük veya boş. Es geçiliyor.")
        return False

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
            # Netscape çerez dosyaları genellikle başlık veya tab formatı içerir
            if "# Netscape" in content or "\t" in content:
                logging.info("✅ Geçerli Netscape cookies.txt tespit edildi.")
                return True
            else:
                logging.warning("⚠️ cookies.txt Netscape formatına uymuyor. Devre dışı bırakıldı.")
                return False
    except Exception as e:
        logging.error(f"⚠️ Cookie okuma hatası: {e}")
        return False


def get_m3u_streams(m3u_url: str) -> List[str]:
    """M3U dosyasını indirip yayın URL'lerini ayıklar."""
    try:
        res = requests.get(m3u_url, timeout=15)
        res.raise_for_status()
        lines = [line.strip() for line in res.text.splitlines() if line.strip() and not line.startswith('#')]
        return lines
    except Exception as e:
        logging.error(f"❌ M3U listesi alınamadı: {e}")
        return []


def resolve_stream_url(raw_url: str) -> Optional[str]:
    """YouTube veya doğrudan medya bağlantılarını FFmpeg için oynatılabilir adrese dönüştürür."""
    if not ("youtube.com" in raw_url or "youtu.be" in raw_url):
        return raw_url

    logging.info("🔗 YouTube bağlantısı tespit edildi, URL çözümleniyor...")
    
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'quiet': True,
        'no_warnings': True,
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'ios', 'web']
            }
        }
    }

    if validate_cookie_file(COOKIE_FILE):
        ydl_opts['cookiefile'] = COOKIE_FILE

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(raw_url, download=False)
            stream_url = info.get('url')
            if stream_url:
                logging.info("✅ YouTube akış adresi başarıyla çözümlendi.")
                return stream_url
    except Exception as e:
        logging.error(f"❌ YouTube akış çözme hatası: {e}")
    
    return None


def run_ffmpeg_stream(stream_url: str) -> int:
    """FFmpeg sürecini başlatır ve RTMP hedefine yayın aktarır."""
    command = [
        'ffmpeg',
        '-hide_banner',
        '-loglevel', 'warning',
        '-re',
        '-i', stream_url,
        '-i', LOGO_FILE,
        '-filter_complex', '[0:v][1:v]overlay=W-w-10:10[outv]',
        '-map', '[outv]',
        '-map', '0:a?',
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-b:v', '2500k',
        '-maxrate', '2500k',
        '-bufsize', '5000k',
        '-pix_fmt', 'yuv420p',
        '-g', '50',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-ar', '44100',
        '-f', 'flv',
        RTMP_DEST
    ]

    logging.info("▶ FFmpeg başlatıldı, aktarım yapılıyor...")
    process = subprocess.Popen(command)
    return process.wait()


def main():
    # 1. Logo Hazırlığı
    if not os.path.exists(LOGO_FILE):
        if not download_file(LOGO_URL, LOGO_FILE):
            sys.exit(1)

    # 2. M3U İçeriğini Çek
    streams = get_m3u_streams(M3U_URL)
    if not streams:
        logging.error("❌ Oynatılacak yayın bulunamadı.")
        sys.exit(1)

    # 3. Yayın Döngüsü
    for index, raw_url in enumerate(streams, start=1):
        logging.info(f"📺 Oynatılan İçerik [{index}/{len(streams)}]: {raw_url}")
        
        real_stream_url = resolve_stream_url(raw_url)
        if not real_stream_url:
            logging.error("⚠️ Bağlantı çözülemedi, sonraki içeriğe geçiliyor...")
            continue

        return_code = run_ffmpeg_stream(real_stream_url)
        logging.warning(f"⚠️ Yayın kapandı (Çıkış Kodu: {return_code}).")
        time.sleep(2)


if __name__ == "__main__":
    main()
