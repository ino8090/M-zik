#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MİMARİ (RTMP kalıcı):

  [Okuyucu FFmpeg (her film için ayrı)]  --MPEG-TS pipe-->  [Çıkış FFmpeg (HİÇ KAPANMAZ)]  --RTMP-->  Sunucu

- Okuyucu: filmi açar, logo/yazı ekler, x264+AAC ile kodlar, MPEG-TS olarak stdout'a yazar.
- Çıkış: stdin'den MPEG-TS okur, yeniden kodlamadan (-c copy) RTMP'ye gönderir.
- Film bitince sadece okuyucu kapanır; çıkış süreci ve RTMP bağlantısı açık kalır.
- Sadece RTMP/çıkış süreci gerçekten ölürse yeniden başlatılır.

"BİTTİ" KARARI:
  Return code 0 tek başına yeterli değildir. Kaynak erken EOF verirse FFmpeg yine 0 döner.
  Bu yüzden film ancak toplam süreye (END_TOLERANCE_SECONDS payıyla) ulaşıldıysa bitmiş sayılır.
  Süre okunamadıysa da en az 60 saniye kesintisiz yayın yapılmış olması şartı aranır.
"""

import json
import os
import re
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field

import requests

# ===================== AYARLAR =====================
RTMP_URL = "rtmp://ssh101.bozztv.com:1935/ssh101"
STREAM_KEY = os.getenv("STREAM_KEY") or "maxdeneme"
RTMP_SERVER = f"{RTMP_URL}/{STREAM_KEY}"

M3U_URL = os.getenv("M3U_URL") or "https://raw.githubusercontent.com/ino8090/0101/refs/heads/main/yerli1.m3u"
LOGO_URL = os.getenv("LOGO_URL") or "https://raw.githubusercontent.com/ino8090/0101/refs/heads/main/file_000000007be48210a068edefa7260629.png"

STATE_FILE_NAME = os.getenv("STATE_FILE_NAME", "fixtv.json")
GITHUB_STEP_SUMMARY = os.getenv("GITHUB_STEP_SUMMARY")

STREAM_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
STREAM_REFERER = "https://vidmody.com/"
STREAM_ORIGIN = "https://vidmody.com"

LOGO_OPACITY = float(os.getenv("LOGO_OPACITY", "0.4"))
TEXT_OPACITY = float(os.getenv("TEXT_OPACITY", "0.5"))
BOLD_FONT_PATH = os.getenv("BOLD_FONT_PATH", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")

DECODER_THREADS = os.getenv("DECODER_THREADS", "1")

# İlerleme (time=) bu kadar sn gelmezse süreç donmuş sayılır.
WATCHDOG_TIMEOUT_SECONDS = int(os.getenv("WATCHDOG_TIMEOUT_SECONDS", "45"))
# İlk kare gelene kadar tanınan süre (HLS kaynaklar geç açılabilir).
WATCHDOG_STARTUP_SECONDS = int(os.getenv("WATCHDOG_STARTUP_SECONDS", "120"))

# Link değişip aynı film olduğunda geriden devam edilecek süre.
LINK_CHANGE_REWIND_SECONDS = int(os.getenv("LINK_CHANGE_REWIND_SECONDS", "15"))

# Filmin bittiği sayılması için toplam süreden en fazla bu kadar sn eksik kalınabilir.
END_TOLERANCE_SECONDS = int(os.getenv("END_TOLERANCE_SECONDS", "90"))

# Art arda erken/hatalı bitişlerde seek konumu bu kadar geri alınır (seek sorunlarına karşı).
SEEK_BACKOFF_SECONDS = int(os.getenv("SEEK_BACKOFF_SECONDS", "30"))
SEEK_BACKOFF_AFTER_FAILURES = int(os.getenv("SEEK_BACKOFF_AFTER_FAILURES", "2"))

# Bu kadar art arda hızlı hatadan sonra sıradaki içeriğe geçilir.
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "4"))
FAST_FAIL_THRESHOLD_SECONDS = 20
MAX_RETRY_DELAY_SECONDS = 120


# ===================== KALICI RTMP ÇIKIŞ SÜRECİ =====================
class RtmpOutput:
    """Tek FFmpeg süreci: stdin'den MPEG-TS alır, RTMP'ye kopyalar. Filmler arası KAPANMAZ."""

    def __init__(self, rtmp_url):
        self.rtmp_url = rtmp_url
        self.proc = None
        self.failed = threading.Event()
        self.stderr_tail = deque(maxlen=30)

    def start(self):
        self.stop(force=True)
        self.failed.clear()
        self.stderr_tail.clear()
        cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'warning', '-nostats',
            '-fflags', '+genpts+nobuffer',
            '-analyzeduration', '3000000',
            '-probesize', '3000000',
            '-f', 'mpegts', '-i', 'pipe:0',
            '-map', '0:v:0', '-map', '0:a:0?',
            '-c', 'copy',
            '-flvflags', 'no_duration_filesize',
            '-f', 'flv', self.rtmp_url,
        ]
        self.proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        threading.Thread(target=self._drain_stderr, args=(self.proc,), daemon=True).start()
        print(f"🔌 Kalıcı RTMP çıkış süreci başlatıldı => {self.rtmp_url}")

    def _drain_stderr(self, proc):
        """stderr dolup süreci kilitlemesin diye sürekli okunur."""
        try:
            for raw in iter(proc.stderr.readline, b''):
                self.stderr_tail.append(raw.decode('utf-8', 'replace').rstrip())
        except Exception:
            pass

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def write(self, data):
        try:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()
            return True
        except (BrokenPipeError, OSError, ValueError, AttributeError):
            self.failed.set()
            return False

    def stop(self, force=False):
        p = self.proc
        if p is None:
            return
        self.proc = None
        try:
            if force:
                p.kill()
            else:
                p.stdin.close()
        except Exception:
            pass
        try:
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
                p.wait(timeout=5)
            except Exception:
                pass


# ===================== YARDIMCI FONKSİYONLAR =====================
def format_hms(total_seconds):
    total_seconds = max(0, int(total_seconds))
    return f"{total_seconds // 3600:02d}:{(total_seconds % 3600) // 60:02d}:{total_seconds % 60:02d}"


def get_video_duration_ffprobe(video_url, retries=3, timeout=15):
    """
    PROFESYONEL FFPROBE SÜRE TESPİT MEKANİZMASI:
    - User-Agent & Referer eklenerek sunucu engellemeleri aşılır.
    - Ağ zaman aşımı (-rw_timeout) eklenerek kilitlenmeler önlenir.
    - JSON çıktısı analiz edilip hem format hem de stream seviyesinden süre çekilir.
    """
    ffprobe_cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        '-allowed_extensions', 'ALL',
        '-headers', f"User-Agent: {STREAM_USER_AGENT}\r\nReferer: {STREAM_REFERER}\r\nOrigin: {STREAM_ORIGIN}\r\n",
        '-rw_timeout', str(timeout * 1000000),  # Mikrosaniye (15 sn)
        '-analyzeduration', '10000000',         # Deep analysis (10s)
        '-probesize', '10000000',               # Deep analysis (10MB)
        video_url
    ]

    for attempt in range(1, retries + 1):
        try:
            print(f"⏱️ ffprobe analizi başlatılıyor (Deneme {attempt}/{retries})...")

            result = subprocess.run(
                ffprobe_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout + 5
            )

            if result.returncode != 0:
                if attempt < retries:
                    time.sleep(2)
                continue

            data = json.loads(result.stdout)

            # 1. YÖNTEM: Format alanından süre alma
            if 'format' in data and 'duration' in data['format']:
                try:
                    duration = float(data['format']['duration'])
                    if duration > 0:
                        print(f"✅ ffprobe ile süre okundu (Format): {duration:.1f} sn ({format_hms(duration)})")
                        return duration
                except (ValueError, TypeError):
                    pass

            # 2. YÖNTEM: Stream (Video/Audio) alanından süre alma
            if 'streams' in data and isinstance(data['streams'], list):
                for stream in data['streams']:
                    if 'duration' in stream:
                        try:
                            duration = float(stream['duration'])
                            if duration > 0:
                                print(f"✅ ffprobe ile süre okundu (Stream): {duration:.1f} sn ({format_hms(duration)})")
                                return duration
                        except (ValueError, TypeError):
                            continue

        except subprocess.TimeoutExpired:
            print(f"⚠️ ffprobe zaman aşımına uğradı (Deneme {attempt}/{retries}).")
        except json.JSONDecodeError:
            print(f"⚠️ ffprobe çıktısı çözümlenemedi (Deneme {attempt}/{retries}).")
        except Exception as e:
            print(f"⚠️ ffprobe beklenmeyen hata: {e}")

        if attempt < retries:
            time.sleep(2)

    print("⚠️ ffprobe ile toplam süre okunamadı (Canlı yayın veya korumalı akış olabilir).")
    return 0.0


def get_local_state():
    """(index, seconds, url, title) döndürür."""
    if not os.path.exists(STATE_FILE_NAME):
        print("ℹ️ Yerel state dosyası bulunamadı, 0'dan başlanıyor.")
        return 0, 0, "", ""
    if os.path.getsize(STATE_FILE_NAME) == 0:
        print(f"⚠️ Yerel state dosyası boş ({STATE_FILE_NAME}), 0'dan başlanıyor.")
        return 0, 0, "", ""
    try:
        with open(STATE_FILE_NAME, "r", encoding="utf-8") as f:
            data = json.load(f)
        idx = data.get("last_index", 0)
        sec = data.get("last_seconds", 0)
        print(f"✅ Yerel state okundu ({STATE_FILE_NAME}) => İndeks: {idx}, Saniye: {sec}")
        return idx, sec, data.get("last_url", ""), data.get("last_title", "")
    except Exception as e:
        print(f"⚠️ Yerel state okuma hatası: {e}")
        return 0, 0, "", ""


def update_local_state(index, seconds, url="", title=""):
    """Atomik yazım: yarım dosya kalma riskini önler."""
    tmp_name = STATE_FILE_NAME + ".tmp"
    try:
        data = {
            "last_index": int(index),
            "last_seconds": int(seconds),
            "last_url": url,
            "last_title": title,
        }
        with open(tmp_name, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_name, STATE_FILE_NAME)
        print(f"💾 Konum kaydedildi => İndeks: {index}, Saniye: {int(seconds)}")
    except Exception as e:
        print(f"⚠️ Yerel state yazma hatası: {e}")


def get_m3u_playlist(m3u_url):
    try:
        headers = {'User-Agent': STREAM_USER_AGENT, 'Referer': STREAM_REFERER}
        response = requests.get(m3u_url, headers=headers, timeout=15)
        if response.status_code == 200:
            playlist = []
            pending_title = None
            for raw_line in response.text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if line.startswith('#EXTINF'):
                    match = re.search(r',(.+)$', line)
                    pending_title = match.group(1).strip() if match else None
                elif not line.startswith('#') and line.startswith('http'):
                    title = pending_title or os.path.basename(line.split('?')[0])
                    playlist.append({"url": line, "title": title})
                    pending_title = None
            return playlist
    except Exception as e:
        print(f"⚠️ M3U çekme hatası: {e}")
    return [{"url": m3u_url, "title": os.path.basename(m3u_url)}]


def download_logo():
    try:
        response = requests.get(LOGO_URL, headers={'User-Agent': STREAM_USER_AGENT}, timeout=15)
        if response.status_code == 200 and len(response.content) > 0:
            with open('logo.png', 'wb') as f:
                f.write(response.content)
            print("✅ Logo başarıyla indirildi.")
    except Exception as e:
        print(f"⚠️ Logo indirme hatası: {e}")


def write_text_file(path, text):
    """drawtext reload=1 için yarım okumayı önlemek amacıyla atomik yazar."""
    tmp_name = path + ".tmp"
    try:
        with open(tmp_name, 'w', encoding='utf-8') as f:
            f.write(text)
        os.replace(tmp_name, path)
    except Exception as e:
        print(f"⚠️ {path} yazma hatası: {e}")


def print_dashboard(title, index, playlist_len, seconds, status="🟢 Yayında"):
    print("┌" + "─" * 58 + "┐")
    print(f"│ 🎬 İçerik         : {title[:36]:<36} │")
    print(f"│ 🔢 Sıra           : {f'{index + 1}/{playlist_len}':<36} │")
    print(f"│ ⏱️  Geçen Süre     : {format_hms(seconds):<36} │")
    print(f"│ 📡 Durum          : {status:<36} │")
    print("└" + "─" * 58 + "┘")


def write_step_summary(title, index, playlist_len, seconds, status="🟢 Yayında"):
    if not GITHUB_STEP_SUMMARY:
        return
    try:
        content = (
            "## 📺 Canlı Yayın Durumu (Maxanimasyon)\n\n"
            "| Alan | Değer |\n"
            "|---|---|\n"
            f"| 🎬 Şu an oynayan içerik | {title} |\n"
            f"| 🔢 Playlist sırası | {index + 1} / {playlist_len} |\n"
            f"| ⏱️ Geçen süre | {format_hms(seconds)} (sa:dk:sn) |\n"
            f"| 📡 Durum | {status} |\n"
            f"| 🕒 Son güncelleme | {time.strftime('%Y-%m-%d %H:%M:%S')} |\n"
        )
        with open(GITHUB_STEP_SUMMARY, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        print(f"⚠️ Step summary yazma hatası: {e}")


def iter_stderr_lines(stream):
    """FFmpeg stderr'ini hem \\r hem \\n ile böler (time= satırları \\r ile gelir)."""
    fd = stream.fileno()
    buf = b''
    while True:
        chunk = os.read(fd, 4096)
        if not chunk:
            if buf.strip():
                yield buf.decode('utf-8', 'replace')
            return
        buf += chunk
        parts = re.split(rb'[\r\n]', buf)
        buf = parts.pop()
        for p in parts:
            if p:
                yield p.decode('utf-8', 'replace')


def pump_reader_to_output(reader, output):
    """Okuyucunun stdout'unu (MPEG-TS) kalıcı RTMP çıkışına aktarır."""
    fd = reader.stdout.fileno()
    while True:
        try:
            chunk = os.read(fd, 65536)
        except OSError:
            break
        if not chunk:
            break
        if not output.write(chunk):
            try:
                reader.kill()
            except Exception:
                pass
            break


# ===================== OKUYUCU KOMUTU =====================
HEADERS_ARG = (
    f"User-Agent: {STREAM_USER_AGENT}\r\n"
    f"Referer: {STREAM_REFERER}\r\n"
    f"Origin: {STREAM_ORIGIN}\r\n"
)


def make_input_options(url):
    is_hls = '.m3u8' in url.lower()
    opts = [
        '-re',
        '-headers', HEADERS_ARG,
        '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
        '-err_detect', 'ignore_err',
        '-fflags', '+genpts+discardcorrupt',
        '-thread_queue_size', '1024',
        '-max_interleave_delta', '0',
        '-analyzeduration', '10000000',
        '-probesize', '10000000',
        '-reconnect', '1',
    ]
    if is_hls:
        opts += ['-allowed_extensions', 'ALL',
                 '-reconnect_streamed', '1',
                 '-reconnect_delay_max', '3']
    else:
        opts += ['-reconnect_at_eof', '1',
                 '-reconnect_streamed', '1',
                 '-reconnect_delay_max', '10']
    opts += ['-rw_timeout', '10000000', '-threads', DECODER_THREADS]
    return opts


def build_reader_command(target_url, seek_seconds):
    seek_args = ['-ss', str(int(seek_seconds))] if seek_seconds > 0 else []

    if ";" in target_url:
        video_url, audio_url = (p.strip() for p in target_url.split(";", 1))
        input_args = (
            make_input_options(video_url) + seek_args + ['-i', video_url] +
            make_input_options(audio_url) + seek_args + ['-i', audio_url]
        )
        audio_map = ['-map', '1:a:0?']
        logo_index = 2
    else:
        input_args = make_input_options(target_url) + seek_args + ['-i', target_url]
        audio_map = ['-map', '0:a:0?']
        logo_index = 1

    title_drawtext = (
        f"drawtext=textfile='title.txt':reload=1:fontfile='{BOLD_FONT_PATH}':"
        f"fontcolor=white@{TEXT_OPACITY}:fontsize=29:x=70:y=h-th-70"
    )
    base_scale = (
        '[0:v]scale=1920:1080:force_original_aspect_ratio=decrease,'
        'pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black,fps=25[main];'
    )

    has_logo = os.path.exists('logo.png') and os.path.getsize('logo.png') > 0
    if has_logo:
        logo_inputs = ['-i', 'logo.png']
        filter_str = (
            base_scale +
            f'[{logo_index}:v]scale=-2:91,format=rgba,colorchannelmixer=aa={LOGO_OPACITY}[logo1];'
            '[main][logo1]overlay=W-w-104:80[tmp1];'
            f'[tmp1]{title_drawtext}[v]'
        )
    else:
        logo_inputs = []
        filter_str = (
            base_scale +
            f'[main]{title_drawtext}[v]'
        )

    return (
        ['ffmpeg'] + input_args + logo_inputs + [
            '-filter_complex', filter_str,
            '-map', '[v]',
        ] + audio_map + [
            '-c:v', 'libx264', '-preset', 'veryfast', '-pix_fmt', 'yuv420p',
            '-r', '25', '-b:v', '2500k', '-maxrate', '2500k', '-bufsize', '3000k', '-g', '50',
            '-c:a', 'aac', '-b:a', '128k', '-ac', '2', '-ar', '44100',
            '-flush_packets', '1', '-muxdelay', '0', '-muxpreload', '0',
            '-mpegts_flags', '+resend_headers',
            '-f', 'mpegts', 'pipe:1',
        ]
    )


# ===================== OKUYUCUYU ÇALIŞTIR =====================
@dataclass
class ReaderResult:
    returncode: int
    stream_seconds: float
    stderr_tail: deque = field(default_factory=lambda: deque(maxlen=40))
    output_broken: bool = False


def run_reader(command, output, base_seconds, total_duration_sec, ctx):
    """Okuyucu FFmpeg'i çalıştırır, ilerlemeyi izler, sonucu döndürür."""
    process = subprocess.Popen(
        command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    pump_thread = threading.Thread(target=pump_reader_to_output, args=(process, output), daemon=True)
    pump_thread.start()

    now = time.time()
    last_save_time = now
    last_dashboard_time = now
    current_seconds = base_seconds
    stderr_tail = deque(maxlen=40)
    progress = [now, False]  # [son ilerleme zamanı, ilk ilerleme geldi mi]

    def watchdog():
        while process.poll() is None:
            time.sleep(5)
            limit = WATCHDOG_TIMEOUT_SECONDS if progress[1] else WATCHDOG_STARTUP_SECONDS
            if time.time() - progress[0] > limit:
                print(f"🚨 Watchdog: {limit} saniyedir ilerleme yok. Okuyucu zorla sonlandırılıyor.")
                try:
                    process.kill()
                except Exception as e:
                    print(f"⚠️ Watchdog sonlandırma hatası: {e}")
                break

    threading.Thread(target=watchdog, daemon=True).start()

    for line in iter_stderr_lines(process.stderr):
        stderr_tail.append(line.rstrip())
        if "time=" not in line:
            continue
        m = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
        if not m:
            continue

        hrs, mins, secs = m.groups()
        played = int(hrs) * 3600 + int(mins) * 60 + float(secs)
        current_seconds = base_seconds + played

        if total_duration_sec > 0:
            write_text_file('time.txt', format_hms(max(0, total_duration_sec - current_seconds)))
        else:
            write_text_file('time.txt', format_hms(current_seconds))

        now = time.time()
        progress[0] = now
        progress[1] = True

        if now - last_save_time > 30:
            update_local_state(ctx["index"], current_seconds, ctx["url"], ctx["title"])
            last_save_time = now
        if now - last_dashboard_time > 30:
            print_dashboard(ctx["title"], ctx["index"], ctx["playlist_len"], current_seconds)
            write_step_summary(ctx["title"], ctx["index"], ctx["playlist_len"], current_seconds)
            last_dashboard_time = now

    process.wait()
    pump_thread.join(timeout=15)

    output_broken = output.failed.is_set() or not output.alive() or pump_thread.is_alive()
    return ReaderResult(process.returncode, current_seconds, stderr_tail, output_broken)


def is_really_finished(result, base_seconds, total_duration_sec):
    """Film GERÇEKTEN bitti mi? Return code 0 tek başına yetmez."""
    if result.returncode != 0:
        return False

    played_seconds = result.stream_seconds - base_seconds

    # 10 saniyeden az oynatıldıysa kesinlikle kaynak erken koptu/seek hatası oluştu demektir.
    if played_seconds < 10:
        return False

    # Toplam süre biliniyorsa tolerans kontrolü yap.
    if total_duration_sec > 0:
        return result.stream_seconds >= total_duration_sec - END_TOLERANCE_SECONDS

    # Toplam süre bilinmiyorsa: Gerçekten bitti diyebilmek için en az 60 saniye kesintisiz yayın yapılmış olması gerekir.
    return played_seconds > 60


# ===================== ANA DÖNGÜ =====================
def start_m3u_stream():
    print(f"🔧 Kullanılan M3U   : {M3U_URL}")
    print(f"🔧 Kullanılan Logo  : {LOGO_URL}")
    print(f"🔧 State dosyası    : {STATE_FILE_NAME}")
    print(f"🔧 RTMP hedefi      : {RTMP_SERVER}")
    print(f"🔧 Decoder thread   : {DECODER_THREADS}")

    download_logo()
    current_index, last_seconds, last_url, last_title = get_local_state()

    consecutive_failures = 0
    output = RtmpOutput(RTMP_SERVER)

    try:
        while True:
            playlist = get_m3u_playlist(M3U_URL)
            if not playlist:
                time.sleep(10)
                continue

            if current_index >= len(playlist):
                current_index, last_seconds, last_url, last_title = 0, 0, "", ""

            item = playlist[current_index]
            target_url = item["url"]
            film_title = item["title"]
            playlist_len = len(playlist)

            # Link değiştiyse: aynı film ise geriden devam, farklıysa baştan
            if last_seconds > 0 and last_url and target_url != last_url:
                if last_title and film_title == last_title:
                    old = last_seconds
                    last_seconds = max(0, last_seconds - LINK_CHANGE_REWIND_SECONDS)
                    print(f"🔄 Sıra {current_index + 1}: link değişmiş ama film aynı ('{film_title}'). "
                          f"{old}s yerine {last_seconds}s'den devam edilecek.")
                else:
                    print(f"🆕 Sıra {current_index + 1}: içerik gerçekten değişmiş, baştan başlatılacak.")
                    last_seconds = 0

            last_url = target_url
            last_title = film_title
            write_text_file('title.txt', film_title)

            probe_url = target_url.split(";")[0].strip()
            total_duration_sec = get_video_duration_ffprobe(probe_url)
            write_text_file(
                'time.txt',
                format_hms(max(0, total_duration_sec - last_seconds) if total_duration_sec > 0 else 0),
            )

            print("=" * 60)
            print("📺 Maxanimasyon Canlı Aktarım Yayını (1080p 25fps - 2500k) Başlatılıyor")
            print(f"🎬 Oynatılan İçerik  : {film_title}")
            print(f"⏱️ Başlangıç Saniyesi: {last_seconds}")
            print(f"⏱ Toplam Süre      : {format_hms(total_duration_sec) if total_duration_sec > 0 else 'Bilinmiyor'}")
            print(f"🚀 Hedef RTMP       : {RTMP_SERVER}")

            print_dashboard(film_title, current_index, playlist_len, last_seconds, status="🟡 Başlatılıyor")
            write_step_summary(film_title, current_index, playlist_len, last_seconds, status="🟡 Başlatılıyor")

            if not output.alive():
                output.start()

            command = build_reader_command(target_url, last_seconds)
            print("▶ Okuyucu FFmpeg başlatıldı, kalıcı RTMP'ye aktarılıyor...")

            ctx = {"index": current_index, "url": target_url, "title": film_title, "playlist_len": playlist_len}
            result = run_reader(command, output, last_seconds, total_duration_sec, ctx)
            played = result.stream_seconds - last_seconds

            # ---------- 1) RTMP çıkışı koptu ----------
            if result.output_broken:
                print("🔴 Kalıcı RTMP çıkışı koptu. Çıkış süreci yeniden başlatılacak, film aynı saniyeden devam edecek.")
                if output.stderr_tail:
                    print("🧾 Çıkış FFmpeg son log satırları:")
                    for l in output.stderr_tail:
                        print(f"   {l}")
                output.stop(force=True)
                write_step_summary(film_title, current_index, playlist_len, result.stream_seconds,
                                   status="🔴 RTMP koptu, yeniden bağlanılıyor")
                last_seconds = result.stream_seconds  # film hatası değil, sayaç artmaz
                update_local_state(current_index, last_seconds, target_url, film_title)
                retry_delay = 5

            # ---------- 2) Film gerçekten bitti ----------
            elif is_really_finished(result, last_seconds, total_duration_sec):
                print("✅ İçerik bitti, sıradakine geçiliyor (RTMP açık kalıyor).")
                write_step_summary(film_title, current_index, playlist_len, result.stream_seconds,
                                   status="✅ Bitti, sıradakine geçiliyor")
                current_index += 1
                last_seconds, last_url, last_title = 0, "", ""
                update_local_state(current_index, 0, "", "")
                consecutive_failures = 0
                retry_delay = 0

            # ---------- 3) Kaynak koptu / erken bitti ----------
            else:
                if result.returncode == 0:
                    total_txt = format_hms(total_duration_sec) if total_duration_sec > 0 else "?"
                    print(f"⚠️ Okuyucu erken sonlandı (return code 0 ama film bitmedi): "
                          f"{format_hms(result.stream_seconds)} / {total_txt}")
                elif result.returncode == -6:
                    print("⚠ FFmpeg SIGABRT ile çöktü.")
                elif result.returncode == -9:
                    print("⚠️ Okuyucu FFmpeg watchdog tarafından donma nedeniyle sonlandırıldı.")
                print(f"⚠️ Okuyucu koptu (Return Code: {result.returncode}). Aynı saniyeden tekrar denenecek. (RTMP açık kalıyor)")
                if result.stderr_tail:
                    print("🧾 FFmpeg son log satırları:")
                    for l in result.stderr_tail:
                        print(f"   {l}")
                write_step_summary(film_title, current_index, playlist_len, result.stream_seconds,
                                   status="🔴 Kaynak koptu, tekrar denenecek")

                if played < FAST_FAIL_THRESHOLD_SECONDS:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"❌ {film_title} akışı sürekli hataya düştü. Sonraki içeriğe geçiliyor...")
                    current_index += 1
                    last_seconds, last_url, last_title = 0, "", ""
                    consecutive_failures = 0
                    update_local_state(current_index, 0, "", "")
                else:
                    last_seconds = result.stream_seconds
                    # Art arda hatada seek konumunu geri al (bozuk seek noktasına karşı)
                    if consecutive_failures >= SEEK_BACKOFF_AFTER_FAILURES and last_seconds > 0:
                        old = last_seconds
                        last_seconds = max(0, last_seconds - SEEK_BACKOFF_SECONDS)
                        print(f"⏪ Art arda hata: seek konumu {old}s -> {last_seconds}s geri alındı.")
                    last_url = target_url
                    update_local_state(current_index, last_seconds, last_url, film_title)

                retry_delay = (
                    min(5 * (2 ** consecutive_failures), MAX_RETRY_DELAY_SECONDS)
                    if consecutive_failures > 0 else 5
                )

            if retry_delay > 0:
                print(f"⚠️ {retry_delay} saniye sonra tekrar bağlanılıyor...")
                time.sleep(retry_delay)

    except KeyboardInterrupt:
        print("\n🛑 Durduruluyor...")
    finally:
        output.stop()


if __name__ == "__main__":
    start_m3u_stream()
