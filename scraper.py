#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ScreenScraper (screenscraper.fr) metadata scraper replicating the
EmulationStation scraping pipeline: same API endpoints, same developer
credentials compiled into the Batocera ES binary, same media layout
(./images/<stem>-image.png etc.) and same gamelist.xml field names, so
scraped results are indistinguishable from ES's own scraper output.

User credentials come from the ES scraper settings
(es_settings.cfg: ScreenScraperUser / ScreenScraperPass).

中文说明：
ScreenScraper（screenscraper.fr）元数据刮削器，复刻 EmulationStation 的
刮削流程：相同的 API 端点、编译进 Batocera ES 二进制的开发者凭据、相同的
媒体文件布局（./images/<stem>-image.png 等）与 gamelist.xml 字段名，
刮削结果与 ES 自带刮削器的输出完全一致。
用户凭据取自 ES 刮削设置（es_settings.cfg）。"""

import hashlib
import html
import json
import os
import re
import subprocess
import threading
import time
import urllib.parse
import zipfile
import zlib
import xml.etree.ElementTree as ET


def _txt(s):
    """ScreenScraper text fields contain HTML entities.
    中文：ScreenScraper 文本字段含 HTML 实体，需反转义并去除首尾空白。"""
    return html.unescape(s).strip() if s else ""

ES_SETTINGS = "/userdata/system/configs/emulationstation/es_settings.cfg"

# Developer credentials embedded in the Batocera EmulationStation binary
# (strings /usr/bin/emulationstation, next to api.screenscraper.fr/api2).
# 中文：内嵌于 Batocera ES 二进制中的开发者凭据（随二进制分发）。
DEVID = "batoceralinux"
DEVPASS = "omvIrirRPuY"
API = "https://api.screenscraper.fr/api2/"
SOFTNAME = "batocera-webmanager"

HASH_LIMIT = 256 * 1024 * 1024      # hash whole file only up to this size
REQ_DELAY = 0.4                     # seconds between API calls

# Batocera system name -> screenscraper systemeid (verified against
# systemesListe.php; covers the common libretro/standalone systems).
# 中文：Batocera 系统名 -> screenscraper systemeid 的映射表。
SYSID = {
    "megadrive": 1, "mastersystem": 2, "nes": 3, "snes": 4, "gb": 9,
    "gbc": 10, "virtualboy": 11, "gba": 12, "gc": 13, "gamecube": 13,
    "n64": 14, "nds": 15, "wii": 16, "sega32x": 19, "32x": 19,
    "saturn": 22, "dreamcast": 23, "ngp": 25, "atari2600": 26,
    "neogeo": 142, "pcengine": 31, "pcenginecd": 114, "supergrafx": 105,
    "psx": 57, "ps2": 58, "ps3": 59, "psp": 61, "psvita": 62,
    "neogeocd": 70, "mame": 75, "fbneo": 75, "fba": 75, "arcade": 75,
    "ngpc": 82, "fds": 106, "daphne": 49, "atomiswave": 53,
    "model2": 54, "model3": 55, "naomi": 56, "sg1000": 109,
    "msx": 113, "msx2": 116, "colecovision": 48, "atari7800": 41,
    "atari5200": 40, "atarist": 42, "amiga": 64, "amiga500": 64,
    "amiga1200": 64, "amigacd32": 130, "c64": 66, "zx81": 77,
    "zxspectrum": 76, "apple2": 86, "dos": 135, "scummvm": 123,
    "gw": 52, "wonderswan": 45, "wonderswancolor": 46, "pokemini": 211,
    "channelf": 80, "vectrex": 102, "intellivision": 115,
    "odyssey2": 104, "videopac": 104, "3do": 29, "jaguar": 27,
    "lynx": 28, "segacd": 20, "megacd": 20, "x68000": 79,
    "pcfx": 72, "3ds": 17, "wiiu": 18, "switch": 225, "satellaview": 107,
}

# filename region tags -> screenscraper region code
# 中文：文件名中的地区标签 -> screenscraper 地区代码。
REGIONS = [
    (r"\b(usa|us)\b", "us"), (r"\b(europe|pal|euro)\b", "eu"),
    (r"\bjapan\b", "jp"), (r"\bworld\b", "wor"), (r"\bfrance\b", "fr"),
    (r"\bgermany\b", "de"), (r"\bspain\b", "sp"), (r"\bitaly\b", "it"),
    (r"\bkorea\b", "kr"), (r"\bbrazil\b", "br"), (r"\bchina\b", "cn"),
    (r"\basia\b", "as"), (r"\baustralia\b", "au"),
]


def read_es_settings():
    """Scraper-related settings saved by EmulationStation.
    中文：读取 EmulationStation 保存的刮削相关设置（账号、视频/图片来源等）。"""
    out = {"user": "", "password": "", "videos": True, "imagesrc": "sstitle",
           "boxback": True, "fanart": True}
    try:
        root = ET.parse(ES_SETTINGS).getroot()
        for el in root.iter():
            name = el.get("name", "")
            val = el.get("value", "")
            if name == "ScreenScraperUser":
                out["user"] = val
            elif name == "ScreenScraperPass":
                out["password"] = val
            elif name == "ScrapeVideos":
                out["videos"] = val.lower() == "true"
            elif name == "ScrapperImageSrc":
                out["imagesrc"] = val or "sstitle"
            elif name == "ScrapeBoxBack":
                out["boxback"] = val.lower() == "true"
            elif name == "ScrapeFanart":
                out["fanart"] = val.lower() == "true"
    except Exception:
        pass
    return out


def rom_region(filename):
    low = filename.lower()
    for pat, code in REGIONS:
        if re.search(pat, low):
            return code
    return ""


def hash_rom(path):
    """(crc32, md5, sha1) of the rom content.  For zips, hashes the first
    inner file (what ScreenScraper indexes); for other files, hashes the
    file itself when it is not too large.
    中文：计算 ROM 的 (crc32, md5, sha1)；zip 取首个内部文件计算（与
    ScreenScraper 索引一致），其他文件在大小未超限时整体计算。"""
    try:
        if path.lower().endswith(".zip"):
            with zipfile.ZipFile(path) as z:
                for info in z.infolist():
                    if info.is_dir():
                        continue
                    crc = "%08x" % (info.CRC & 0xFFFFFFFF)
                    if info.file_size > HASH_LIMIT:
                        return crc, "", ""
                    md5 = hashlib.md5()
                    sha1 = hashlib.sha1()
                    with z.open(info) as f:
                        for chunk in iter(lambda: f.read(1 << 20), b""):
                            md5.update(chunk)
                            sha1.update(chunk)
                    return crc, md5.hexdigest(), sha1.hexdigest()
            return "", "", ""
        if os.path.getsize(path) > HASH_LIMIT:
            return "", "", ""
        crc = 0
        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                crc = zlib.crc32(chunk, crc)
                md5.update(chunk)
                sha1.update(chunk)
        return "%08x" % (crc & 0xFFFFFFFF), md5.hexdigest(), sha1.hexdigest()
    except (OSError, zipfile.BadZipFile):
        return "", "", ""


def _pick_by(items, key, prefs):
    """Pick item whose key value matches prefs in order; fallback first.
    中文：按偏好顺序挑选 key 值匹配的条目；无匹配时回退到第一条。"""
    if not isinstance(items, list) or not items:
        return None
    for pref in prefs:
        for it in items:
            if it.get(key) == pref and it.get("text"):
                return it
    for it in items:
        if it.get("text"):
            return it
    return None


def _pick_media(medias, mtype, region_pref):
    cand = [m for m in medias if m.get("type") == mtype and m.get("url")]
    if not cand:
        return None
    prefs = [region_pref, "wor", "us", "eu", "jp", "ss"]
    for pref in prefs:
        for m in cand:
            if m.get("region") == pref:
                return m
    return cand[0]


def _fmt_date(txt):
    """'1991-06-23'/'1991-06'/'1991' -> '19910623T000000' style."""
    if not txt:
        return ""
    m = re.match(r"(\d{4})(?:-(\d{1,2}))?(?:-(\d{1,2}))?", txt.strip())
    if not m:
        return ""
    y, mo, d = m.group(1), m.group(2) or "01", m.group(3) or "01"
    return "%s%02d%02dT000000" % (y, int(mo), int(d))


class ScrapeJob(threading.Thread):
    """Scrapes a list of rom files of one system via the ScreenScraper API.
    中文：通过 ScreenScraper API 刮削某一系统下的一组 ROM 文件（线程任务）。"""

    def __init__(self, system, items, settings, save_entry, log=None):
        """items: [{'rel', 'abs', 'size'}]; save_entry(rel, fields, downloads)
        persists one game's metadata (fields dict + media {suffix: url}).
        中文：items 为待刮文件列表；save_entry 负责持久化单个游戏的
        元数据字段与媒体文件映射。"""
        threading.Thread.__init__(self, daemon=True)
        self.system = system
        self.items = items
        self.cfg = settings
        self.save_entry = save_entry
        self.log = log or (lambda m: None)
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.status = "running"
        self.error = ""
        self.total = len(items)
        self.done = 0
        self.found = 0
        self.notfound = 0
        self.errors = 0
        self.current = ""

    def snapshot(self):
        with self.lock:
            return {"status": self.status, "error": self.error,
                    "total": self.total, "done": self.done,
                    "found": self.found, "notfound": self.notfound,
                    "errors": self.errors, "current": self.current}

    def stop(self):
        self.stop_event.set()

    # ---- screenscraper API ---- ScreenScraper API 请求构造
    def _api_get(self, script, params):
        """Build and run one API2 GET request via curl; returns decoded JSON.
        构造并通过 curl 发起一次 API2 GET 请求（含开发者与用户凭据），
        返回解析后的 JSON。"""
        q = {"devid": DEVID, "devpassword": DEVPASS, "softname": SOFTNAME,
             "output": "json", "ssid": self.cfg["user"],
             "sspassword": self.cfg["password"]}
        q.update(params)
        cmd = ["curl", "-s", "--max-time", "60", "-G", API + script]
        for k, v in q.items():
            cmd += ["--data-urlencode", "%s=%s" % (k, v)]
        p = subprocess.run(cmd, capture_output=True, timeout=90)
        if p.returncode != 0:
            raise IOError("curl exit %d" % p.returncode)
        return json.loads(p.stdout.decode("utf-8", "replace"))

    def query_game(self, it):
        """Look up one rom via jeuInfos.php by hashes/name/size; retries 3x.
        按哈希/文件名/大小查询 jeuInfos.php 匹配单个 ROM，最多重试 3 次。"""
        crc, md5, sha1 = hash_rom(it["abs"])
        params = {"romtype": "rom", "romnom": os.path.basename(it["abs"]),
                  "romtaille": str(it["size"])}
        sid = SYSID.get(self.system)
        if sid:
            params["systemeid"] = str(sid)
        if crc:
            params["crc32"] = crc
        if md5:
            params["md5"] = md5
        if sha1:
            params["sha1"] = sha1
        for attempt in (1, 2, 3):
            if self.stop_event.is_set():
                return None
            try:
                d = self._api_get("jeuInfos.php", params)
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(5 * attempt)
                continue
            err = ((d.get("header") or {}).get("error") or "")
            jeu = (d.get("response") or d.get("reponse") or {}).get("jeu")
            if jeu:
                return jeu
            if err:                     # game not in database
                return None
            if attempt < 3:             # empty but no error: transient
                time.sleep(5 * attempt)
        return None

    def _download(self, url, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if "screenscraper.fr" in url and "ssid=" not in url:
            url += ("&devid=%s&devpassword=%s&ssid=%s&sspassword=%s" %
                    (DEVID, DEVPASS, urllib.parse.quote(self.cfg["user"]),
                     urllib.parse.quote(self.cfg["password"])))
        p = subprocess.run(["curl", "-sfL", "--max-time", "180", "-o", dest,
                            url], capture_output=True, timeout=200)
        if p.returncode != 0:
            try:
                os.unlink(dest)
            except OSError:
                pass
            return False
        return os.path.getsize(dest) > 0

    # ---- per-game pipeline ---- 单个游戏的刮削流程
    def _scrape_one(self, it):
        """Fetch metadata + media for one rom and persist via save_entry.
        刮削单个 ROM 的元数据与媒体文件，并通过 save_entry 保存。"""
        jeu = self.query_game(it)
        if not jeu:
            return False
        stem = os.path.splitext(os.path.basename(it["abs"]))[0]
        region = rom_region(it["rel"])

        fields = {}
        nom = _pick_by(jeu.get("noms"), "region",
                       [region, "wor", "us", "eu", "ss", "jp"])
        if nom:
            fields["name"] = _txt(nom["text"])
        syn = _pick_by(jeu.get("synopsis"), "langue", ["en", "fr", "de", "es"])
        if syn:
            fields["desc"] = _txt(syn["text"])
            fields["lang"] = syn.get("langue", "en")
        note = (jeu.get("note") or {}).get("text", "")
        if note.isdigit():
            fields["rating"] = "%.2f" % (int(note) / 20.0)
        date = _pick_by(jeu.get("dates"), "region",
                        [region, "wor", "us", "eu", "jp"])
        if date:
            fields["releasedate"] = _fmt_date(date["text"])
        for src, dst in (("developpeur", "developer"), ("editeur", "publisher"),
                         ("joueurs", "players")):
            v = _txt((jeu.get(src) or {}).get("text", ""))
            if v and v != "-":
                fields[dst] = v
        genres = jeu.get("genres") or []
        for g in genres:
            if g.get("principale") == "1":
                genres = [g] + [x for x in genres if x is not g]
                break
        for g in genres:
            gn = _pick_by(g.get("noms"), "langue", ["en", "fr"])
            if gn:
                fields["genre"] = _txt(gn["text"])
                break
        if jeu.get("id"):
            fields["id"] = str(jeu["id"])
            fields["source"] = "ScreenScraper.fr"

        # media downloads (same layout as EmulationStation)
        medias = jeu.get("medias") or []
        img_types = [self.cfg["imagesrc"], "sstitle", "ss", "box-2D"]
        wanted = {"image": img_types, "thumbnail": ["ss"],
                  "marquee": ["wheel", "wheel-hd"]}
        if self.cfg["videos"]:
            wanted["video"] = ["video", "video-normalized"]
        if self.cfg["boxback"]:
            wanted["boxback"] = ["box-2D-back"]
        if self.cfg["fanart"]:
            wanted["fanart"] = ["fanart"]
        downloads = {}
        sub = os.path.dirname(it["rel"])
        base_dir = os.path.dirname(it["abs"])
        # gamelist tag -> filename suffix used by EmulationStation
        suffix = {"image": "image", "thumbnail": "thumb", "marquee": "marquee",
                  "boxback": "boxback", "fanart": "fanart"}
        for kind, types in wanted.items():
            m = None
            for tp in types:
                m = _pick_media(medias, tp, region)
                if m:
                    break
            if not m:
                continue
            fmt = (m.get("format") or "png").lower()
            if kind == "video":
                fmt = "mp4"
                dest = os.path.join(base_dir, "videos",
                                    "%s-video.%s" % (stem, fmt))
                rel_dest = "./%svideos/%s-video.%s" % (
                    (sub + "/") if sub else "", stem, fmt)
            else:
                dest = os.path.join(base_dir, "images",
                                    "%s-%s.%s" % (stem, suffix[kind], fmt))
                rel_dest = "./%simages/%s-%s.%s" % (
                    (sub + "/") if sub else "", stem, suffix[kind], fmt)
            if os.path.exists(dest) or self._download(m["url"], dest):
                if kind in ("image", "video", "marquee", "thumbnail"):
                    downloads[kind] = rel_dest
        self.save_entry(it["rel"], fields, downloads)
        return True

    def run(self):
        """Main scrape loop over all items with per-request throttling.
        刮削主循环：逐个处理条目，每次请求间按 REQ_DELAY 限速。"""
        try:
            for it in self.items:
                if self.stop_event.is_set():
                    break
                with self.lock:
                    self.current = os.path.basename(it["abs"])
                try:
                    ok = self._scrape_one(it)
                except Exception as e:
                    ok = False
                    with self.lock:
                        self.errors += 1
                    self.log("error %s: %s" % (it["rel"], e))
                else:
                    with self.lock:
                        if ok:
                            self.found += 1
                        else:
                            self.notfound += 1
                with self.lock:
                    self.done += 1
                time.sleep(REQ_DELAY)
            with self.lock:
                self.status = "stopped" if self.stop_event.is_set() else "done"
        except Exception as e:
            with self.lock:
                self.status = "error"
                self.error = str(e)
