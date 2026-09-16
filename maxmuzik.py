import os
import subprocess
import json
import time
import requests
import yt_dlp

# --- KONFİGÜRASYON ---
M3U_URL = "https://raw.githubusercontent.com/ino8090/0101/refs/heads/main/max.m3u"
LOGO_URL = "https://raw.githubusercontent.com/ino8090/0101/refs/heads/main/file_000000007be48210a068edefa7260629.png"
RTMP_DEST = "rtmp://ssh101.bozztv.com:1935/ssh101/maxmuzik"
STATE_FILE = "maxmuzik.json"
COOKIE_FILE = "cookies.txt"  # GitHub Secret üzerinden oluşturulan dosya

def resolve_youtube_url(url):
    """YouTube linklerini doğrudan akış (m3u8/mp4) adresine dönüştürür."""
    ydl_opts = {
        'format': 'best',
        'quiet': True,
        'no_warnings': True,
        # YouTube bot engellerini aşmak için Android/iOS istemcileri simüle et
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'ios']
            }
        }
    }
    
    # Eğer cookies.txt mevcutsa otomatik ekle
    if os.path.exists(COOKIE_FILE):
        ydl_opts['cookiefile'] = COOKIE_FILE

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get('url', None)
    except Exception as e:
        print(f"⚠️ YouTube akış çözme hatası: {e}")
        return None

def start_stream(media_url):
    """FFmpeg ile yayın başlatma işlemi."""
    ffmpeg_cmd = [
        'ffmpeg',
        '-re',
        '-i', media_url,
        '-i', 'logo.png',
        '-filter_complex', '[0:v][1:v]overlay=W-w-10:10[outv]',
        '-map', '[outv]',
        '-map', '0:a',
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-b:v', '2500k',
        '-maxrate', '2500k',
        '-bufsize', '5000k',
        '-r', '25',
        '-g', '50',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-ar', '44100',
        '-f', 'flv',
        RTMP_DEST
    ]
    
    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process

# --- ANA DÖNGÜ ---
if __name__ == "__main__":
    # 1. Logoyu İndir
    if not os.path.exists("logo.png"):
        r = requests.get(LOGO_URL)
        with open("logo.png", "wb") as f:
            f.write(r.content)
        print("✅ Logo başarıyla indirildi.")

    # 2. M3U İçeriğini Çek
    m3u_response = requests.get(M3U_URL)
    if m3u_response.status_code != 200:
        print("❌ M3U dosyası indirilemedi!")
        exit(1)

    # M3U içerisindeki ilk geçerli linki al
    lines = [line.strip() for line in m3u_response.text.splitlines() if line.strip() and not line.startswith('#')]
    raw_stream_url = lines[0] if lines else None

    if not raw_stream_url:
        print("❌ M3U içinde geçerli bir yayın bağlantısı bulunamadı!")
        exit(1)

    print(f"📡 Kaynak Yayın : {raw_stream_url}")

    # 3. YouTube Adresi Çözümleme
    if "youtube.com" in raw_stream_url or "youtu.be" in raw_stream_url:
        print("🔗 YouTube bağlantısı tespit edildi, akış adresi çözümleniyor...")
        stream_url = resolve_youtube_url(raw_stream_url)
        
        if not stream_url:
            print("❌ YouTube adresi çözülemedi! Bot engeli veya geçersiz link.")
            exit(1)
    else:
        stream_url = raw_stream_url

    # 4. Yayını Başlat
    print("▶ FFmpeg başlatıldı, 1080p 25fps @ 2500k yayın iletiliyor...")
    proc = start_stream(stream_url)
    proc.wait()
