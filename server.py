#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Batocera Web Manager - browser-based admin panel built on the standard library only.
Deployed at /userdata/system/webmanager/server.py, listens on port 80.
Batocera Web 管理面板：仅基于 Python 标准库构建的浏览器管理后台。
部署于 /userdata/system/webmanager/server.py，监听 80 端口。
"""

import btlib
import httplib
import scraper
import glob
import hmac
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import traceback
import urllib.parse
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(BASE, "backups")
TMP_DIR = os.path.join(BASE, "tmp")
USERDATA = "/userdata"
CONF_PATH = "/userdata/system/batocera.conf"
ES_INPUT_PATH = "/userdata/system/configs/emulationstation/es_input.cfg"
ES_SYSTEMS_PATH = "/usr/share/emulationstation/es_systems.cfg"
ROMS_DIR = "/userdata/roms"
SAVES_DIR = "/userdata/saves"
TORRENT_OUTPUT = "/userdata/system/torrent/output"
BOOT_DIR = "/boot"
UPGRADE_DIR = "/userdata/system/upgrade"
BIOS_DIR = "/userdata/bios"
PORT = 80
APP_VERSION = "Beta 1.0"  # panel version shown in the sidebar / 侧边栏显示的面板版本号

os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# Wayland environment detection (required by batocera-resolution)
WL_ENV = {}
for _sock in ("wayland-0", "wayland-1", "wayland-2"):
    if os.path.exists("/var/run/" + _sock):
        WL_ENV = {"XDG_RUNTIME_DIR": "/var/run", "WAYLAND_DISPLAY": _sock}
        break


def run_cmd(cmd, timeout=30):
    env = dict(os.environ)
    env.update(WL_ENV)
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        return p.returncode, p.stdout, p.stderr
    except Exception as e:
        return -1, "", str(e)


def sh(cmd, timeout=30):
    return run_cmd(["sh", "-c", cmd], timeout=timeout)


def _tail(path, n=15):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
        return "\n".join([l for l in lines if l.strip()][-n:])
    except OSError:
        return ""


def _write_json(path, obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f)
    except OSError:
        pass


# ---------------- parallel range downloader / 并行分段下载器 ----------------
# The update server gives ~25 KB/s per connection from here, but the mirror
# (mirrors.o2switch.fr) honors byte ranges and the throughput scales with
# parallel connections, so big images are fetched in parallel chunks.
# 更新服务器单连接仅约 25 KB/s，但镜像站支持 Range 且吞吐随连接数提升，故大镜像采用分段并行下载。

UPGRADE_PARTS = 12
_UPG = {"thread": None, "cancel": threading.Event(),
        # live downloader processes, killed precisely on cancel (B7)
        # 存活的下载进程，取消时按 PID 精确终止（B7）
        "procs": set(), "plock": threading.Lock()}


def _curl_head(url):
    """Follow redirects with HEAD requests; return {url, size, ranges} or None.
    用 HEAD 请求跟踪重定向，返回 {url, size, ranges}，失败返回 None。"""
    code, out, _ = run_cmd(["curl", "-sIL", "-A", "webmanager/1.0", url], timeout=40)
    if code != 0:
        return None
    final, size, ranges = url, 0, False
    for line in out.splitlines():
        low = line.strip().lower()
        if low.startswith("location:"):
            final = line.split(":", 1)[1].strip()
        elif low.startswith("content-length:"):
            try:
                size = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif low.startswith("accept-ranges:"):
            ranges = "bytes" in low
    return {"url": final, "size": size, "ranges": ranges}


def _download_ranges(url, dest, total, log):
    """Download url into dest using parallel HTTP range requests (one curl per
    chunk, resumable on retry). Returns '' on success, error message otherwise.
    以并行 HTTP Range 请求把 url 分段下载到 dest（每段一个 curl，重试可断点续传）。"""
    parts_dir = dest + ".parts"
    os.makedirs(parts_dir, exist_ok=True)
    chunk = (total + UPGRADE_PARTS - 1) // UPGRADE_PARTS
    tasks = []
    s = 0
    while s < total:
        e = min(total, s + chunk) - 1
        tasks.append((len(tasks), s, e))
        s = e + 1
    # resume support: reuse part files only when they match this run's layout
    meta = {"url": url, "total": total, "parts": UPGRADE_PARTS}
    meta_p = os.path.join(parts_dir, "meta.json")
    try:
        with open(meta_p) as f:
            old = json.load(f)
    except (OSError, ValueError):
        old = None
    if old != meta:
        shutil.rmtree(parts_dir, ignore_errors=True)
        os.makedirs(parts_dir, exist_ok=True)
        _write_json(meta_p, meta)

    def have(idx):
        p = os.path.join(parts_dir, "part%03d" % idx)
        return os.path.getsize(p) if os.path.exists(p) else 0

    def fetch(idx, start, end):
        want = end - start + 1
        if have(idx) >= want:
            return True
        try:
            # --speed-* aborts stalled connections so a retry can take over
            with open(os.path.join(parts_dir, "part%03d" % idx), "ab") as out:
                proc = subprocess.Popen(["curl", "-sf", "--connect-timeout", "20",
                                         "--speed-limit", "1024", "--speed-time", "60",
                                         "-A", "webmanager/1.0",
                                         "-r", "%d-%d" % (start + have(idx), end), url],
                                        stdout=out, stderr=subprocess.DEVNULL)
                with _UPG["plock"]:
                    _UPG["procs"].add(proc)
                try:
                    # poll so a cancel kills this exact process right away (B7)
                    # 轮询等待，取消时立即精确终止该进程（B7）
                    while proc.poll() is None:
                        if _UPG["cancel"].is_set():
                            proc.kill()
                            break
                        time.sleep(0.5)
                finally:
                    with _UPG["plock"]:
                        _UPG["procs"].discard(proc)
        except Exception:
            pass
        return have(idx) >= want

    def worker(t):
        idx, start, end = t
        time.sleep(idx * 0.4)  # stagger connection attempts
        for attempt in range(20):
            if _UPG["cancel"].is_set():
                return
            if fetch(idx, start, end):
                return
            time.sleep(min(30, 2 ** attempt))

    threads = [threading.Thread(target=worker, args=(t,), daemon=True) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # mop-up rounds: retry stragglers sequentially — the mirror appears to cap
    # parallel connections per IP, so some parts may never get a slot otherwise
    for rnd in range(4):
        pending = [t for t in tasks if have(t[0]) < t[2] - t[1] + 1]
        if not pending or _UPG["cancel"].is_set():
            break
        log.write("retry round %d: %d part(s) left\n" % (rnd + 1, len(pending)))
        log.flush()
        for t in pending:
            if _UPG["cancel"].is_set():
                break
            fetch(*t)
    if _UPG["cancel"].is_set():
        shutil.rmtree(parts_dir, ignore_errors=True)
        try:
            os.remove(dest)
        except OSError:
            pass
        return "cancelled"
    errors = ["part %d incomplete (%d/%d bytes)" % (i, have(i), e - s + 1)
              for i, s, e in tasks if have(i) < e - s + 1]
    if errors:
        return "; ".join(errors[:3]) + " (click update again to resume)"
    log.write("assembling %d parts ...\n" % len(tasks))
    log.flush()
    with open(dest, "wb") as out:
        for idx, _, _ in tasks:
            with open(os.path.join(parts_dir, "part%03d" % idx), "rb") as f:
                shutil.copyfileobj(f, out, 1048576)
    shutil.rmtree(parts_dir, ignore_errors=True)
    if os.path.getsize(dest) != total:
        return "size mismatch after assembly"
    return ""


# ---------------- full userdata backup / restore / 完整 userdata 备份与恢复 ----------------
# Everything NOT in these sets is regenerable system state (cache, logs,
# pacman store, torrent downloads, this webmanager) and is excluded on purpose.
# 不在这些集合中的内容（缓存、日志、pacman 仓库、种子下载、本面板）均可再生成，刻意排除。
BACKUP_ROOT = "/userdata/backups"

BACKUP_SETS = [
    ("roms",    ["roms"]),
    ("saves",   ["saves"]),
    ("bios",    ["bios"]),
    ("configs", ["system/configs", "system/wine-bottles",
                 "system/batocera.conf", "system/custom.sh"]),
    ("media",   ["music", "screenshots", "recordings", "splash"]),
    ("extras",  ["cheats", "decorations", "themes", "library", "kodi"]),
]

_BKUP = {"running": False, "action": "", "file": "", "bytes": 0, "total": 0,
         "error": "", "done": "", "cancel": threading.Event()}
_BKUP_SIZE_CACHE = {"t": 0.0, "data": None}
# embedded as first tar member so an archive is self-describing
MANIFEST_NAME = ".backup-manifest.json"


def _du_bytes(path):
    code, out, _ = run_cmd(["du", "-sb", path], timeout=300)
    if code == 0:
        try:
            return int(out.split()[0])
        except (ValueError, IndexError):
            pass
    return 0


def _du_many(paths):
    """{path: bytes} with ONE du process for all paths — spawning du per
    directory made /api/backup/info take seconds on large libraries (A3/B10).
    单个 du 进程统计多个路径的大小；此前逐目录 spawn 导致大库上接口耗时数秒。"""
    sizes = {p: 0 for p in paths}
    paths = [p for p in paths if os.path.exists(p)]
    if not paths:
        return sizes
    try:
        p = subprocess.run(["du", "-sb"] + paths, capture_output=True, timeout=300)
        if p.returncode == 0:
            for line in p.stdout.decode("utf-8", "replace").splitlines():
                sz, _, name = line.partition("\t")
                if name in sizes:
                    try:
                        sizes[name] = int(sz)
                    except ValueError:
                        pass
    except Exception:
        pass
    return sizes


def backup_set_sizes():
    """Per-set byte sizes for the backup UI (cached 5 min). / 各备份集合的字节大小（缓存 5 分钟）。"""
    if _BKUP_SIZE_CACHE["data"] is not None and time.time() - _BKUP_SIZE_CACHE["t"] < 300:
        return _BKUP_SIZE_CACHE["data"]
    rels_all = [r for _, rels in BACKUP_SETS for r in rels]
    sizes = _du_many([os.path.join(USERDATA, r) for r in rels_all])
    sets = []
    for key, rels in BACKUP_SETS:
        total, present = 0, False
        for rel in rels:
            p = os.path.join(USERDATA, rel)
            if os.path.exists(p):
                present = True
                total += sizes.get(p, 0)
        sets.append({"key": key, "bytes": total, "present": present})
    _BKUP_SIZE_CACHE.update({"t": time.time(), "data": sets})
    return sets


def backup_archive_sets(path, size):
    """Which backup sets an archive contains, from its embedded manifest.
    Falls back to scanning top-level dirs for archives made before manifests
    existed (skipped for very large archives).
    从归档内嵌清单判断其包含哪些备份集合（无清单的旧归档回退为扫描顶层目录）。"""
    try:
        with tarfile.open(path, "r:") as tf:
            for i, m in enumerate(tf):
                n = m.name[2:] if m.name.startswith("./") else m.name
                if n == MANIFEST_NAME:
                    sets = json.loads(tf.extractfile(m).read().decode("utf-8", "replace")).get("sets")
                    if sets is not None:
                        return sets
                if i >= 4:
                    break  # manifest is always the first member when present
    except Exception:
        pass
    if size > 8 * 1024 ** 3:
        return None
    try:
        tops = set()
        with tarfile.open(path, "r:") as tf:
            for m in tf:
                n = m.name[2:] if m.name.startswith("./") else m.name
                tops.add(n.split("/", 1)[0])
        return [key for key, rels in BACKUP_SETS
                if any(rel.split("/", 1)[0] in tops for rel in rels)]
    except Exception:
        return None


_SETS_CACHE_PATH = os.path.join(BACKUP_ROOT, ".sets-cache.json")


def backup_list():
    """Archive list for the backup UI. Scanning a legacy (manifest-less) tar's
    member headers costs seconds per GB, so per-archive results are cached in a
    sidecar json keyed by name+size+mtime.
    备份归档列表。扫描无清单的旧归档成员头每 GB 需数秒，因此按 文件名+大小+mtime
    把结果缓存到随附 json 中。"""
    os.makedirs(BACKUP_ROOT, exist_ok=True)
    try:
        with open(_SETS_CACHE_PATH, encoding="utf-8") as f:
            cache = json.load(f)
    except (OSError, ValueError):
        cache = {}
    dirty = False
    out = []
    for f in sorted(os.listdir(BACKUP_ROOT), reverse=True):
        p = os.path.join(BACKUP_ROOT, f)
        if not (f.endswith(".tar") and os.path.isfile(p)):
            continue
        st = os.stat(p)
        ent = cache.get(f)
        if not ent or ent.get("size") != st.st_size or ent.get("mtime") != int(st.st_mtime):
            ent = {"size": st.st_size, "mtime": int(st.st_mtime),
                   "sets": backup_archive_sets(p, st.st_size)}
            cache[f] = ent
            dirty = True
        out.append({"name": f, "size": st.st_size, "mtime": int(st.st_mtime),
                    "sets": ent["sets"]})
    if dirty:
        live = {e["name"] for e in out}
        cache = {k: v for k, v in cache.items() if k in live}
        _write_json(_SETS_CACHE_PATH, cache)
    return out


def backup_status():
    return {"ok": True, "running": _BKUP["running"], "action": _BKUP["action"],
            "file": _BKUP["file"], "bytes": _BKUP["bytes"], "total": _BKUP["total"],
            "error": _BKUP["error"], "done": _BKUP["done"]}


def backup_start(keys):
    """Start a background tar backup of the selected sets. / 后台启动所选集合的 tar 备份。"""
    if _BKUP["running"]:
        return "backup already in progress"
    rels = []
    used = []
    for key, r in BACKUP_SETS:
        if key in keys:
            used.append(key)
            rels.extend(rel for rel in r if os.path.exists(os.path.join(USERDATA, rel)))
    if not rels:
        return "nothing to backup"
    _BKUP.update(running=True, action="backup", file="", bytes=0, total=0,
                 error="", done="")
    _BKUP["cancel"].clear()

    def work():
        proc = None
        mtmp = os.path.join(USERDATA, MANIFEST_NAME)
        try:
            _BKUP["total"] = sum(_du_many([os.path.join(USERDATA, r) for r in rels]).values())
            free = shutil.disk_usage(USERDATA).free
            if _BKUP["total"] + 512 * 1048576 > free:
                raise RuntimeError("not enough free space: need %d MB, have %d MB"
                                   % (_BKUP["total"] // 1048576, free // 1048576))
            name = "batocera-backup-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".tar"
            dest = os.path.join(BACKUP_ROOT, name)
            _BKUP["file"] = name
            _write_json(mtmp, {"created": name, "sets": used})
            proc = subprocess.Popen(["tar", "cf", dest, "-C", USERDATA, MANIFEST_NAME] + rels)
            while proc.poll() is None:
                if _BKUP["cancel"].is_set():
                    proc.terminate()
                    proc.wait()
                    try:
                        os.remove(dest)
                    except OSError:
                        pass
                    _BKUP.update(running=False, done="cancelled")
                    return
                try:
                    _BKUP["bytes"] = os.path.getsize(dest)
                except OSError:
                    pass
                time.sleep(2)
            if proc.returncode != 0:
                raise RuntimeError("tar failed (rc=%d)" % proc.returncode)
            _BKUP["bytes"] = os.path.getsize(dest)
            _BKUP.update(running=False, done=name)
        except Exception as e:
            _BKUP.update(running=False, error=str(e))
        finally:
            try:
                os.remove(mtmp)
            except OSError:
                pass

    threading.Thread(target=work, daemon=True).start()
    return None


def backup_restore(name):
    """Restore a backup archive in the background (ES is stopped first). / 后台恢复备份归档（先停止 EmulationStation）。"""
    if _BKUP["running"]:
        return "backup already in progress"
    if not re.match(r"^[\w.-]+\.tar$", name or ""):
        return "bad file name"
    src = os.path.join(BACKUP_ROOT, name)
    if not os.path.isfile(src):
        return "file not found"
    _BKUP.update(running=True, action="restore", file=name, bytes=0,
                 total=os.path.getsize(src), error="", done="")
    _BKUP["cancel"].clear()

    def work():
        proc = None
        try:
            # stop ES so it cannot rewrite restored configs/saves on exit
            run_cmd(["/etc/init.d/S31emulationstation", "stop"], timeout=60)
            proc = subprocess.Popen(["tar", "xf", src, "-C", USERDATA])
            while proc.poll() is None:
                if _BKUP["cancel"].is_set():
                    proc.terminate()
                    proc.wait()
                    _BKUP.update(running=False, done="cancelled")
                    return
                time.sleep(2)
            if proc.returncode != 0:
                raise RuntimeError("tar failed (rc=%d)" % proc.returncode)
            # the embedded manifest must not stay behind in /userdata (B11)
            # 恢复后清理随归档解出的清单文件，避免残留在 /userdata（B11）
            try:
                os.remove(os.path.join(USERDATA, MANIFEST_NAME))
            except OSError:
                pass
            _BKUP.update(running=False, done="restored")
        except Exception as e:
            _BKUP.update(running=False, error=str(e))
        finally:
            run_cmd(["/etc/init.d/S31emulationstation", "start"], timeout=60)

    threading.Thread(target=work, daemon=True).start()
    return None


def backup_cancel():
    if not _BKUP["running"]:
        return "no backup running"
    _BKUP["cancel"].set()
    return None


def backup_delete(name):
    if not re.match(r"^[\w.-]+\.tar$", name or ""):
        return "bad file name"
    try:
        os.remove(os.path.join(BACKUP_ROOT, name))
    except OSError:
        return "cannot delete"
    return None


# ---------------- batocera.conf read/write / batocera.conf 读写 ----------------

def read_conf_lines():
    """Parse batocera.conf into typed line items (kv/comment/blank/other). / 把 batocera.conf 解析为带类型的行项（kv/comment/blank/other）。"""
    items = []
    if not os.path.exists(CONF_PATH):
        return items
    with open(CONF_PATH, encoding="utf-8", errors="replace") as f:
        for raw in f.read().splitlines():
            line = raw.strip()
            if not line:
                items.append({"type": "blank", "raw": raw})
            elif line.startswith("#"):
                items.append({"type": "comment", "raw": raw})
            else:
                m = re.match(r"^([^=]+?)\s*=\s*(.*)$", raw)
                if m:
                    items.append({"type": "kv", "key": m.group(1).strip(), "value": m.group(2), "raw": raw})
                else:
                    items.append({"type": "other", "raw": raw})
    return items


def conf_map():
    return {it["key"]: it["value"] for it in read_conf_lines() if it["type"] == "kv"}


def backup_file(path):
    """Timestamped .bak copy under backups/, keeping the last 10. / 在 backups/ 下生成带时间戳的 .bak 副本，仅保留最近 10 份。"""
    if not os.path.exists(path):
        return None
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = os.path.join(BACKUP_DIR, os.path.basename(path) + "." + ts + ".bak")
    shutil.copy2(path, dst)
    baks = sorted(glob.glob(os.path.join(BACKUP_DIR, os.path.basename(path) + ".*.bak")))
    for old in baks[:-10]:
        try:
            os.remove(old)
        except OSError:
            pass
    return dst


def write_conf(items):
    """Write line items back to batocera.conf (auto-backup first). / 把行项写回 batocera.conf（先自动备份）。"""
    backup_file(CONF_PATH)
    out = []
    for it in items:
        if it.get("type") == "kv":
            out.append(it["key"] + "=" + it["value"])
        else:
            out.append(it.get("raw", ""))
    with open(CONF_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")


def conf_set(key, value):
    """Set one key in batocera.conf. / 设置 batocera.conf 中的单个键。"""
    items = read_conf_lines()
    for it in items:
        if it["type"] == "kv" and it["key"] == key:
            it["value"] = value
            write_conf(items)
            return
    items.append({"type": "kv", "key": key, "value": value})
    write_conf(items)


# ---------------- DOS / Windows launcher autoconfig / DOS / Windows 启动器自动配置 ----------------
# Batocera expects each DOS or Windows game as a "<name>.pc" folder carrying
# its launch config (dosbox.bat / autorun.cmd).  Uploads usually arrive as an
# archive, so extract it into that folder and generate the config file.

# Uploads for these systems may be plain archives: they are unpacked into a
# .pc folder, so the archive extension is accepted even when es_systems does
# not list it (windows only lists .pc/.exe/...).
AUTO_PACK_SYSTEMS = ("dos", "windows", "windows_installers")


def _pick_exe(root):
    """Largest executable under root -> (relative dir, filename) or None.
    找出 root 下体积最大的可执行文件，返回 (相对目录, 文件名)，无则返回 None。"""
    best = None
    for dirpath, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fn in files:
            if os.path.splitext(fn)[1].lower() not in (".exe", ".bat", ".com"):
                continue
            fp = os.path.join(dirpath, fn)
            try:
                size = os.path.getsize(fp)
            except OSError:
                continue
            if best is None or size > best[0]:
                rel = os.path.relpath(dirpath, root).replace(os.sep, "/")
                best = (size, "" if rel == "." else rel, fn)
    if not best:
        return None
    return best[1], best[2]


def pc_autoconfig(system, dest_dir, filename):
    """Turn an uploaded dos/windows archive into a launchable .pc folder.
    Returns a short info dict (or None when nothing was done).
    把上传的 dos/windows 压缩包转换成可启动的 .pc 文件夹，返回信息 dict（未处理返回 None）。"""
    if system not in AUTO_PACK_SYSTEMS:
        return None
    stem, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in (".zip", ".7z"):
        return None
    src = os.path.join(dest_dir, filename)
    if not os.path.isfile(src):
        return None
    target = os.path.join(dest_dir, stem + ".pc")
    if os.path.isdir(target):
        return None
    try:
        os.makedirs(target)
        # p7zip here ships without its lib, so try bsdtar (zip/7z/rar) first,
        # then the format-specific tool.
        attempts = [["bsdtar", "-xf", src, "-C", target]]
        if ext == ".7z":
            attempts.append(["7zr", "x", "-y", "-o" + target, src])
        elif ext == ".zip":
            attempts.append(["unzip", "-qq", "-o", src, "-d", target])
        code, err = 1, ""
        for cmd in attempts:
            code, _, err = run_cmd(cmd, timeout=600)
            if code == 0:
                break
        if code != 0:
            raise IOError((err or "extract failed").strip()[:200])
        found = _pick_exe(target)
        if not found:
            raise IOError("no executable inside the archive")
        sub, exe = found
        if system == "dos":
            # dosbox.bat holds the launch command line, e.g. "MP.EXE"
            lines = []
            if sub:
                lines.append("cd " + sub.replace("/", "\\"))
            lines.append('"' + exe + '"' if " " in exe else exe)
            launcher = "dosbox.bat"
        else:
            lines = ["CMD=" + exe, "DIR=" + sub]
            launcher = "autorun.cmd"
        with open(os.path.join(target, launcher), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(src, os.path.join(target, filename))
        return {"pc": stem + ".pc", "launcher": launcher, "exe": exe}
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        return None


# ---------------- es_systems.cfg / es_systems.cfg 系统列表解析 ----------------

_systems_cache = {"t": 0.0, "data": None}

# Single-game engines / source ports: they have exactly one playable title,
# so they carry no game library and only clutter the system list.
SINGLE_GAME_SYSTEMS = {
    "abuse", "bk", "bstone", "cannonball", "catacomb", "cave3rd", "cavestory",
    "cdogs", "cgenius", "corsixth", "devilutionx", "doom3", "dxx-rebirth",
    "ecwolf", "eduke32", "etlegacy", "fallout1-ce", "fallout2-ce", "fury",
    "gzdoom", "halflife", "hurrican", "jazz2", "jkdf2", "jknight", "mohaa",
    "mrboom", "odcommander", "openjazz", "prboom", "quake", "quake2",
    "quake3", "raze", "reminiscence", "rott", "rtcw", "sdlpop", "sonic-mania",
    "sonic3-air", "sonicretro", "theforceengine", "thextech", "tyrian",
    "traider1", "traider2", "xrick", "zc210",
}

# Non-game entries shipped by Batocera (media viewers, remote streaming,
# package frontends, installer helper): they hold no ROMs at all.
NON_GAME_SYSTEMS = {
    "imageviewer", "library", "moonlight", "recordings", "steam", "flatpak",
    "vgmplay", "windows_installers",
}

HIDDEN_SYSTEMS = SINGLE_GAME_SYSTEMS | NON_GAME_SYSTEMS

# System category (type) mapping sourced from supportengine.xlsx.
SYSTEM_TYPES = {
    "3do": "家用机", "3ds": "掌机", "abuse": "移植引擎", "adam": "家用电脑",
    "advision": "家用机", "amiga1200": "家用电脑", "amiga500": "家用电脑",
    "amigacd32": "家用机", "amigacdtv": "家用机", "amstradcpc": "家用电脑",
    "apfm1000": "家用机", "apple2": "家用电脑", "apple2gs": "家用电脑",
    "arcadia": "家用机", "archimedes": "家用电脑", "arduboy": "掌机",
    "astrocade": "家用机", "atari2600": "家用机", "atari5200": "家用机",
    "atari7800": "家用机", "atari800": "家用电脑", "atarist": "家用电脑",
    "atom": "家用电脑", "atomiswave": "街机", "bbcmicro": "家用电脑",
    "beena": "家用机", "bennugd": "游戏引擎", "bk": "家用电脑",
    "bstone": "移植引擎", "c128": "家用电脑", "c20": "家用电脑",
    "c64": "家用电脑", "camplynx": "家用电脑", "cannonball": "移植引擎",
    "cassettevision": "家用机", "catacomb": "移植引擎", "cave3rd": "街机",
    "cavestory": "移植引擎", "cdi": "家用机", "cdogs": "移植引擎",
    "cgenie": "家用电脑", "cgenius": "移植引擎", "channelf": "家用机",
    "chihiro": "街机", "coco": "家用电脑", "colecovision": "家用机",
    "commanderx16": "家用电脑", "corsixth": "移植引擎", "cplus4": "家用电脑",
    "crvision": "家用机", "ctvboy": "家用机", "daphne": "街机",
    "devilutionx": "移植引擎", "dice": "街机", "doom3": "移植引擎",
    "dos": "PC 兼容机", "dragon64": "家用电脑", "dreamcast": "家用机",
    "dxx-rebirth": "移植引擎", "easyrpg": "引擎", "ecwolf": "移植引擎",
    "eduke32": "移植引擎", "electron": "家用电脑", "enterprise": "家用电脑",
    "etlegacy": "移植引擎", "fallout1-ce": "移植引擎", "fallout2-ce": "移植引擎",
    "fbneo": "街机", "fds": "家用机", "flash": "Flash 播放器",
    "flatpak": "通用容器", "fm7": "家用电脑", "fmtowns": "家用电脑",
    "fury": "移植引擎", "gaelco": "街机", "gamate": "掌机",
    "gameandwatch": "掌机", "gamecom": "掌机", "gamecube": "家用机",
    "gamegear": "掌机", "gamepock": "小众掌机", "gb": "掌机",
    "gb2players": "掌机", "gba": "掌机", "gbc": "掌机",
    "gbc2players": "掌机", "gmaster": "掌机", "gp32": "掌机",
    "gx4000": "家用机", "gzdoom": "移植引擎", "halflife": "移植引擎",
    "hcl": "街机", "hikaru": "街机", "hurrican": "移植引擎",
    "ikemen": "引擎", "imageviewer": "图片查看器", "intellivision": "家用机",
    "jaguar": "家用机", "jaguarcd": "家用机", "jazz2": "移植引擎",
    "jkdf2": "移植引擎", "jknight": "移植引擎", "laser310": "家用电脑",
    "lcdgames": "液晶掌机合集", "library": "媒体库", "lindbergh": "街机",
    "loopy": "家用机", "lowresnx": "虚拟主机", "lutro": "引擎",
    "lynx": "掌机", "macintosh": "家用电脑", "mame": "街机",
    "mastersystem": "家用机", "mc10": "家用电脑", "megacd": "家用机",
    "megadrive": "家用机", "megadrive-msu": "家用机", "megaduck": "掌机",
    "model2": "街机", "model3": "街机", "mohaa": "移植引擎",
    "moonlight": "串流客户端", "mrboom": "移植引擎", "msx1": "家用电脑",
    "msx2": "家用电脑", "msx2+": "家用电脑", "msxturbor": "家用电脑",
    "mugen": "格斗引擎", "multivision": "小众主机", "mz2000": "家用电脑",
    "mz2500": "家用电脑", "mz700": "家用电脑", "mz800": "家用电脑",
    "mz80k": "家用电脑", "n64": "家用机", "n64dd": "家用机外设",
    "namco22": "街机", "namco2x6": "街机", "naomi": "街机",
    "naomi2": "街机", "nds": "掌机", "neogeo": "街机 / 家用机",
    "neogeocd": "家用机", "nes": "家用机", "ngp": "掌机",
    "ngpc": "掌机", "odcommander": "移植引擎", "odyssey2": "家用机",
    "openbor": "引擎", "openjazz": "移植引擎", "oricatmos": "家用电脑",
    "pc60": "家用电脑", "pc80": "家用电脑", "pc88": "家用电脑",
    "pc98": "家用电脑", "pcengine": "家用机", "pcenginecd": "家用机外设",
    "pcfx": "家用机", "pcw": "家用电脑", "pdp1": "大型机",
    "pet": "家用电脑", "pico": "家用机", "pico8": "虚拟主机",
    "pokemini": "掌机", "ports": "移植游戏合集", "prboom": "移植引擎",
    "ps2": "家用机", "ps3": "家用机", "ps4": "家用机",
    "psp": "掌机", "psvita": "掌机", "psx": "家用机",
    "pv1000": "家用机", "pv2000": "家用机", "pygame": "Python 游戏引擎",
    "pyxel": "虚拟主机", "quake": "移植引擎", "quake2": "移植引擎",
    "quake3": "移植引擎", "raze": "移植引擎", "recordings": "录像回放",
    "reminiscence": "移植引擎", "rott": "移植引擎", "rtcw": "移植引擎",
    "rx78": "家用电脑", "samcoupe": "家用电脑", "satellaview": "外设",
    "saturn": "家用机", "sc3000": "家用电脑", "scummvm": "引擎",
    "scv": "家用机", "sdlpop": "移植引擎", "sega32x": "扩展模块",
    "segaai": "家用电脑", "sg1000": "家用机", "sgb": "外设",
    "sgb-msu1": "外设", "singe": "街机", "snes": "家用机",
    "snes-msu1": "家用机", "socrates": "教育主机", "solarus": "引擎",
    "sonic-mania": "移植引擎", "sonic3-air": "移植引擎",
    "sonicretro": "索尼克复古引擎", "spectravideo": "家用电脑",
    "steam": "Steam 游戏", "sufami": "外设", "superbroswar": "独立引擎",
    "supergrafx": "家用机", "supervision": "掌机", "supracan": "家用机",
    "sv8000": "家用机", "systemsp": "街机", "theforceengine": "移植引擎",
    "thextech": "移植引擎", "thomson": "家用电脑", "ti99": "家用电脑",
    "tic80": "虚拟主机", "traider1": "街机", "traider2": "街机",
    "triforce": "街机", "trs80": "家用电脑", "tutor": "教育主机",
    "tvc": "家用电脑", "tvgames": "家用机", "tyrian": "移植引擎",
    "uzebox": "自制主机", "vc4000": "家用机", "vectrex": "家用机",
    "vgmplay": "音频播放器", "videopacplus": "家用机", "vircon32": "虚拟主机",
    "virtualboy": "家用机", "vis": "家用电脑", "vpinball": "弹珠台引擎",
    "vsmile": "家用机", "wasm4": "虚拟主机", "wii": "家用机",
    "wiiu": "家用机", "windows": "PC Windows 游戏",
    "windows_installers": "安装包工具", "wswan": "掌机", "wswanc": "掌机",
    "x1": "家用电脑", "x68000": "家用电脑", "xbox": "家用机",
    "xbox360": "家用机", "xegs": "家用机", "xrick": "移植引擎",
    "zc210": "家用电脑", "zx81": "家用电脑", "zxspectrum": "家用电脑",
}


def get_systems():
    """Parse es_systems.cfg into the system list (cached 5 min). / 解析 es_systems.cfg 得到系统列表（缓存 5 分钟）。"""
    if _systems_cache["data"] is not None and time.time() - _systems_cache["t"] < 300:
        return _systems_cache["data"]
    tree = ET.parse(ES_SYSTEMS_PATH)
    systems = []
    for s in tree.getroot().findall("system"):
        name = s.findtext("name", "")
        emulators = []
        for emu in s.findall("emulators/emulator"):
            cores = []
            for c in emu.findall("cores/core"):
                cores.append({"name": (c.text or "").strip(), "default": c.get("default") == "true"})
            if cores:
                emulators.append({"name": emu.get("name", ""), "cores": cores})
        extensions = [e.strip().lower() for e in (s.findtext("extension", "") or "").split() if e.strip()]
        systems.append({
            "name": name,
            "fullname": s.findtext("fullname", name),
            "manufacturer": s.findtext("manufacturer", ""),
            "path": s.findtext("path", os.path.join(ROMS_DIR, name)),
            "extensions": extensions,
            "emulators": emulators,
            "type": SYSTEM_TYPES.get(name, "通用"),
        })
    _systems_cache["data"] = systems
    _systems_cache["t"] = time.time()
    return systems


def find_system(name):
    """Find a system by name. / 按名称查找系统。"""
    for s in get_systems():
        if s["name"] == name:
            return s
    return None


_count_cache = {}  # name -> (timestamp, count); TTL so externally added roms show up (B8)


def rom_count(sysinfo):
    """Count rom files under the system path (cached 2 min). / 统计系统目录下的 ROM 文件数（缓存 2 分钟）。"""
    cached = _count_cache.get(sysinfo["name"])
    if cached is not None and time.time() - cached[0] < 120:
        return cached[1]
    d = sysinfo["path"]
    if not os.path.isdir(d):
        return 0
    exts = set(sysinfo["extensions"])
    n = 0
    try:
        for root, dirs, files in os.walk(d):
            dirs[:] = [x for x in dirs if not x.startswith(".")]
            for f in files:
                if os.path.splitext(f)[1].lower() in exts:
                    n += 1
    except OSError:
        pass
    _count_cache[sysinfo["name"]] = (time.time(), n)
    return n


# ---------------- gamelist.xml / gamelist.xml 游戏元数据 ----------------

_gamelist_cache = {}  # path -> (mtime, meta)


def _norm_rel(p):
    p = p.replace("\\", "/")
    if p.startswith("./"):
        p = p[2:]
    return p.lower()


def gamelist_meta(sys_path):
    """gamelist.xml metadata keyed by normalized relative path. / 读取 gamelist.xml 元数据，按规范化相对路径索引。"""
    gl = os.path.join(sys_path, "gamelist.xml")
    if not os.path.exists(gl):
        return {}
    try:
        mtime = os.path.getmtime(gl)
    except OSError:
        return {}
    cached = _gamelist_cache.get(gl)
    if cached and cached[0] == mtime:
        return cached[1]
    meta = {}
    try:
        tree = ET.parse(gl)
        for g in tree.getroot().findall("game"):
            p = g.findtext("path", "")
            if not p:
                continue
            meta[_norm_rel(p)] = {
                "name": g.findtext("name", "") or "",
                "desc": g.findtext("desc", "") or "",
                "image": g.findtext("image", "") or "",
                "rating": g.findtext("rating", "") or "",
                "releasedate": g.findtext("releasedate", "") or "",
                "developer": g.findtext("developer", "") or "",
                "publisher": g.findtext("publisher", "") or "",
                "genre": g.findtext("genre", "") or "",
                "players": g.findtext("players", "") or "",
            }
    except Exception:
        pass
    _gamelist_cache[gl] = (mtime, meta)
    return meta


def gamelist_remove_entry(sys_path, rel):
    gl = os.path.join(sys_path, "gamelist.xml")
    if not os.path.exists(gl):
        return
    try:
        tree = ET.parse(gl)
        root = tree.getroot()
        removed = False
        for g in list(root.findall("game")):
            if _norm_rel(g.findtext("path", "")) == _norm_rel(rel):
                root.remove(g)
                removed = True
        if removed:
            backup_file(gl)
            ET.indent(tree, space="  ")
            tree.write(gl, encoding="utf-8", xml_declaration=True)
            _gamelist_cache.pop(gl, None)
    except Exception:
        pass


def gamelist_rename_entry(sys_path, old_rel, new_rel):
    gl = os.path.join(sys_path, "gamelist.xml")
    if not os.path.exists(gl):
        return
    try:
        tree = ET.parse(gl)
        root = tree.getroot()
        changed = False
        for g in root.findall("game"):
            p = g.find("path")
            if p is not None and _norm_rel(p.text or "") == _norm_rel(old_rel):
                p.text = "./" + new_rel
                changed = True
        if changed:
            backup_file(gl)
            ET.indent(tree, space="  ")
            tree.write(gl, encoding="utf-8", xml_declaration=True)
            _gamelist_cache.pop(gl, None)
    except Exception:
        pass


# ---------------- es_input.cfg gamepads / es_input.cfg 手柄配置 ----------------

def read_controllers():
    """Parse es_input.cfg into a device list. / 解析 es_input.cfg 得到手柄设备列表。"""
    devices = []
    if not os.path.exists(ES_INPUT_PATH):
        return devices
    tree = ET.parse(ES_INPUT_PATH)
    for cfg in tree.getroot().findall("inputConfig"):
        inputs = []
        for i in cfg.findall("input"):
            inputs.append({
                "name": i.get("name", ""),
                "type": i.get("type", ""),
                "id": i.get("id", ""),
                "value": i.get("value", ""),
                "code": i.get("code", ""),
            })
        devices.append({
            "type": cfg.get("type", ""),
            "deviceName": cfg.get("deviceName", ""),
            "deviceGUID": cfg.get("deviceGUID", ""),
            "inputs": inputs,
        })
    return devices


def save_controllers(devices):
    """Write the device list back to es_input.cfg (auto-backup first). / 把设备列表写回 es_input.cfg（先自动备份）。"""
    backup_file(ES_INPUT_PATH)
    root = ET.Element("inputList")
    for d in devices:
        el = ET.SubElement(root, "inputConfig", {
            "type": str(d.get("type", "")),
            "deviceName": str(d.get("deviceName", "")),
            "deviceGUID": str(d.get("deviceGUID", "")),
        })
        for i in d.get("inputs", []):
            attrib = {
                "name": str(i.get("name", "")),
                "type": str(i.get("type", "")),
                "id": str(i.get("id", "")),
                "value": str(i.get("value", "")),
            }
            if str(i.get("code", "")):
                attrib["code"] = str(i["code"])
            ET.SubElement(el, "input", attrib)
    tree = ET.ElementTree(root)
    ET.indent(tree, space="\t")
    with open(ES_INPUT_PATH, "wb") as f:
        f.write(b'<?xml version="1.0"?>\n')
        tree.write(f, encoding="utf-8")


# ---------------- streaming multipart parser (Python 3.14 removed the cgi module) / 流式 multipart 解析器（Python 3.14 已移除 cgi 模块） ----------------

class _NullSink:
    def write(self, b):
        pass


class _MP:
    """Incremental multipart reader over rfile. / 基于 rfile 的增量式 multipart 读取器。"""
    def __init__(self, rfile, boundary, length):
        self.rfile = rfile
        self.bnd = b"--" + boundary
        self.delim = b"\r\n--" + boundary
        self.left = length
        self.buf = b""
        self.pos = 0

    def _fill(self):
        if self.left <= 0:
            return
        chunk = self.rfile.read(min(65536, self.left))
        if chunk:
            self.left -= len(chunk)
            self.buf += chunk

    def readline(self):
        while True:
            idx = self.buf.find(b"\n", self.pos)
            if idx >= 0:
                line = self.buf[self.pos:idx + 1]
                self.pos = idx + 1
                if self.pos > 65536:
                    self.buf = self.buf[self.pos:]
                    self.pos = 0
                return line
            if self.left <= 0:
                if self.pos < len(self.buf):
                    line = self.buf[self.pos:]
                    self.pos = len(self.buf)
                    return line
                return None
            self._fill()

    def stream_until_boundary(self, out):
        data = self.buf[self.pos:]
        self.buf = b""
        self.pos = 0
        keep = len(self.delim) + 4
        while True:
            idx = data.find(self.delim)
            if idx >= 0:
                out.write(data[:idx])
                self.buf = data[idx + 2:]  # keep "--boundary..." for the next readline
                self.pos = 0
                return True
            if len(data) > keep:
                out.write(data[:-keep])
                data = data[-keep:]
            if self.left <= 0:
                return False
            chunk = self.rfile.read(min(65536, self.left))
            if not chunk:
                return False
            self.left -= len(chunk)
            data += chunk


def sanitize_filename(name):
    name = os.path.basename(name.replace("\\", "/"))
    return name.strip().strip(".") or "unnamed"


def parse_multipart(rfile, content_type, length, make_dest, max_files=50):
    """Stream a multipart/form-data body to disk (no in-memory buffering). / 把 multipart/form-data 请求体流式落盘（不在内存中缓冲）。"""
    m = re.search(r"boundary=(?P<b>[^;]+)", content_type)
    if not m:
        raise ValueError("no boundary")
    boundary = m.group("b").strip().strip('"').encode("latin1")
    mp = _MP(rfile, boundary, length)
    fields = {}
    files = []
    line = mp.readline()
    guard = 0
    while line is not None and guard < 10000:
        guard += 1
        stripped = line.strip()
        if stripped == mp.bnd + b"--":
            break
        if not stripped.startswith(mp.bnd):
            line = mp.readline()
            continue
        headers = {}
        while True:
            h = mp.readline()
            if h is None or h in (b"\r\n", b"\n", b""):
                break
            k, _, v = h.decode("latin1").partition(":")
            headers[k.strip().lower()] = v.strip()
        cd = headers.get("content-disposition", "")
        mn = re.search(r'name="([^"]*)"', cd)
        mf = re.search(r'filename="([^"]*)"', cd)
        name = mn.group(1) if mn else ""
        filename = mf.group(1) if mf else None
        if not filename:
            bio = io.BytesIO()
            mp.stream_until_boundary(bio)
            fields[name] = bio.getvalue().decode("utf-8", "replace")
        else:
            filename = sanitize_filename(filename)
            dest = make_dest(name, filename) if len(files) < max_files else None
            if dest:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest + ".part", "wb") as out:
                    mp.stream_until_boundary(out)
                os.replace(dest + ".part", dest)
                files.append({"field": name, "filename": filename, "path": dest,
                              "size": os.path.getsize(dest)})
            else:
                mp.stream_until_boundary(_NullSink())
        line = mp.readline()
    return fields, files


# ---------------- utilities / 工具函数 ----------------

def safe_join(base, *parts):
    """Join paths under base; reject directory escapes. / 在 base 下拼接路径，拒绝目录逃逸。"""
    p = os.path.realpath(os.path.join(base, *parts))
    base_r = os.path.realpath(base)
    if base_r == os.sep:
        return p
    if p != base_r and not p.startswith(base_r + os.sep):
        raise ValueError("path escape")
    return p


def dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def fmt_size(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return "%.1f%s" % (n, u)
        n /= 1024
    return "%.1fPB" % n


MIME = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml",
    ".mp4": "video/mp4", ".pdf": "application/pdf", ".zip": "application/zip",
    ".xml": "text/xml", ".txt": "text/plain", ".json": "application/json",
    ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css",
}


# ---------------- version-check cache (module-level, shared across all requests) / 版本检查缓存（模块级，跨请求共享） ----------------

_vc_cache = {"t": 0.0, "data": None}


# ---------------- BIOS check cache / BIOS 检查缓存 ----------------

_bios_cache = {"t": 0.0, "data": None}


def bios_check():
    """Missing/installed BIOS report via batocera-systems (cached 2 min). / 通过 batocera-systems 生成 BIOS 缺失/已安装报告（缓存 2 分钟）。"""
    if _bios_cache["data"] is not None and time.time() - _bios_cache["t"] < 120:
        return _bios_cache["data"]
    code, out, err = run_cmd(["batocera-systems"], timeout=300)
    missing = []
    cur = ""
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(">"):
            cur = line[1:].strip()
            continue
        parts = line.split()
        if len(parts) >= 3 and parts[0].upper() != "OK":
            missing.append({"system": cur, "status": parts[0], "md5": parts[1], "path": parts[-1]})
        elif len(parts) == 2:
            missing.append({"system": cur, "status": parts[0], "md5": "", "path": parts[-1]})
    installed = []
    for root, _, files in os.walk(BIOS_DIR):
        for f in files:
            fp = os.path.join(root, f)
            rel = os.path.relpath(fp, USERDATA).replace(os.sep, "/")
            try:
                installed.append({"path": rel, "size": os.path.getsize(fp)})
            except OSError:
                pass
    installed.sort(key=lambda x: x["path"])
    data = {"missing": missing, "installed": installed}
    _bios_cache["data"] = data
    _bios_cache["t"] = time.time()
    return data


# ---------------- downloadable game libraries (torrent based) / 可下载游戏库（基于 BT 种子） ----------------
# One entry per system: a game list JSON (built from a .torrent) plus the torrent
# itself.  Adding another system / source later only needs a new entry here.

# Download libraries live in gamedb/<system>/: a games JSON plus, for
# BitTorrent-sourced systems, the .torrent itself.  Disc systems (CHD sets on
# archive.org have no torrent) carry an "http" block inside the JSON instead
# and are fetched by httplib.  Adding a system = dropping a folder here.
# 下载库位于 gamedb/<system>/：BT 来源附带 .torrent；光盘系统（archive.org CHD 无种子）改用 JSON 内的
# "http" 块并由 httplib 下载。新增系统 = 在此放一个文件夹。

GAMEDB_DIR = os.path.join(BASE, "gamedb")
DL_SOURCES = {}
if os.path.isdir(GAMEDB_DIR):
    for _sys in sorted(os.listdir(GAMEDB_DIR)):
        _dir = os.path.join(GAMEDB_DIR, _sys)
        _json = os.path.join(_dir, _sys + ".json")
        if not os.path.isfile(_json):
            continue
        _entry = {"json": _json}
        _tor = os.path.join(_dir, _sys + ".torrent")
        if os.path.isfile(_tor):
            _entry["torrent"] = _tor
        DL_SOURCES[_sys] = _entry
_DL = {}
for _sys in DL_SOURCES:
    _DL[_sys] = {"lib": None, "torrent": None, "job": None, "job_names": [],
                 "queue": [], "pending": set(), "log": [], "lock": threading.Lock()}

# archive.org login cookies: the Redump CHD items sit in the "loggedin"
# collection, so HTTP downloads need a logged-in session. Free IA account.
# archive.org 登录 cookie：Redump CHD 位于 "loggedin" 合集，HTTP 下载需要已登录会话（免费 IA 账号即可）。
IA_COOKIE = os.path.join(GAMEDB_DIR, ".ia_cookies")


def _jar_cookie(path, name):
    """Value of cookie `name` in a Netscape cookie jar, '' when absent/deleted.
    HttpOnly cookies are stored with a '#HttpOnly_' domain prefix."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith("#HttpOnly_"):
                    line = line[len("#HttpOnly_"):]
                elif line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 7 and parts[5] == name and parts[6] != "deleted":
                    return parts[6]
    except OSError:
        pass
    return ""


def ia_login(email, password):
    """Log in to archive.org, storing the session cookie jar on success.
    IA's login is stateful: the first POST only establishes the ia-csrf
    session, a second POST carrying it actually authenticates — so retry.
    Returns '' on success, error message otherwise.
    登录 archive.org 并保存会话 cookie；其登录有状态，需携带 ia-csrf 会话重试认证。"""
    proxy = conf_map().get("webmanager.dl_proxy", "")
    tmp = IA_COOKIE + ".tmp"
    try:
        os.remove(tmp)
    except OSError:
        pass
    ok = False
    for _ in range(3):
        csrf = _jar_cookie(tmp, "ia-csrf")
        cmd = ["curl", "-s", "--connect-timeout", "20", "-c", tmp, "-b", tmp,
               "-o", os.devnull, "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
               "--data-urlencode", "username=" + email,
               "--data-urlencode", "password=" + password,
               "--data-urlencode", "submit=Log in",
               "--data-urlencode", "csrf=" + csrf,
               "-H", "X-CSRF-Token: " + csrf,
               "-H", "X-Requested-With: XMLHttpRequest",
               "https://archive.org/account/login"]
        if proxy:
            cmd += ["-x", proxy]
        run_cmd(cmd, timeout=60)
        if _jar_cookie(tmp, "logged-in-sig"):
            ok = True
            break
    if ok:
        os.replace(tmp, IA_COOKIE)
        return ""
    try:
        os.remove(tmp)
    except OSError:
        pass
    return "login failed (check email/password)"


def ia_auth_ok():
    """True only when a usable, non-expired archive.org session cookie exists
    (B12 — the old check matched a stale cookie file forever).
    仅当存在可用且未过期的 archive.org 会话 cookie 时返回真（B12）。"""
    try:
        with open(IA_COOKIE, encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith("#HttpOnly_"):
                    line = line[len("#HttpOnly_"):]
                elif line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 7 and parts[5] == "logged-in-sig" and parts[6] != "deleted":
                    try:
                        exp = int(parts[4])
                    except ValueError:
                        exp = 0
                    return exp == 0 or exp > time.time()  # 0 = session cookie
    except OSError:
        pass
    return False


def dl_available(system):
    return system in DL_SOURCES


# Shared inbound listener for every torrent job: incoming peer connections
# (LAN / port-forwarded) are routed by infohash, and its LSD announcer makes
# LAN seeders (e.g. the user's NAS) discoverable automatically.
# 所有种子任务共享的入站监听器：按 infohash 分发入站连接，并通过 LSD 自动发现局域网做种者（如 NAS）。
_BT_LISTENER = None
_BT_LOG = []


def _bt_log(msg):
    _BT_LOG.append(datetime.now().strftime("%H:%M:%S ") + msg)
    del _BT_LOG[:-200]


def bt_listener():
    """Shared inbound BitTorrent peer listener (lazy singleton). / 共享的 BT 入站连接监听器（懒加载单例）。"""
    global _BT_LISTENER
    if _BT_LISTENER is None:
        try:
            port = int(conf_map().get("webmanager.dl_port", "6881") or 6881)
        except ValueError:
            port = 6881
        _BT_LISTENER = btlib.PeerListener(port, log=_bt_log)
        _BT_LISTENER.start()
    return _BT_LISTENER


def dl_lib(system):
    """Lazy-load the system's games JSON. / 懒加载该系统的游戏清单 JSON。"""
    st = _DL[system]
    if st["lib"] is None:
        with open(DL_SOURCES[system]["json"], encoding="utf-8") as f:
            st["lib"] = json.load(f)
    return st["lib"]


def dl_torrent(system):
    """Lazy-load and parse the system's .torrent. / 懒加载并解析该系统的 .torrent。"""
    st = _DL[system]
    if st["torrent"] is None:
        with open(DL_SOURCES[system]["torrent"], "rb") as f:
            st["torrent"] = btlib.Torrent(f.read())
    return st["torrent"]


def dl_installed(system):
    """Stems of all entries present in roms/<system>; the frontend matches them
    against library game names, so any rom extension counts as installed.
    Directory stems count too — several engine ports use a directory as rom.
    roms/<system> 下已存在条目的主名集合：任意 ROM 扩展名（含目录主名）均视为已安装。"""
    out = set()
    try:
        for fn in os.listdir(os.path.join(ROMS_DIR, system)):
            if fn.endswith(".part") or fn.endswith(".parts"):
                continue
            stem = os.path.splitext(fn)[0]
            if stem and not stem.startswith(("_", ".")) and stem != "gamelist":
                out.add(stem)
    except OSError:
        pass
    return out


def dl_info(system):
    """Torrent/library summary for the UI header. / 供前端展示的下载库摘要信息。"""
    lib = dl_lib(system)
    if "torrent" in lib:
        t = lib["torrent"]
        return {k: t[k] for k in ("name", "infohash", "piece_length", "total", "pieces")}
    # http-sourced library (disc systems)
    return {"name": system, "infohash": "", "piece_length": 0,
            "total": sum(g.get("s", 0) for g in lib["games"]),
            "pieces": 0, "http": True}


def dl_residue(system):
    """Leftover partial-download entries in roms/<system>: '*.part' files and
    '*.parts' segment dirs, from failed or interrupted downloads.
    列出 roms/<system> 中下载失败/中断残留的 *.part 文件与 *.parts 分段目录。"""
    out = []
    try:
        d = os.path.join(ROMS_DIR, system)
        for fn in os.listdir(d):
            if not (fn.endswith(".part") or fn.endswith(".parts")):
                continue
            p = os.path.join(d, fn)
            try:
                if os.path.isdir(p):
                    sz = sum(os.path.getsize(os.path.join(dp, f))
                             for dp, _, fns in os.walk(p) for f in fns)
                else:
                    sz = os.path.getsize(p)
                out.append({"name": fn, "size": sz})
            except OSError:
                pass
    except OSError:
        pass
    out.sort(key=lambda x: x["name"].lower())
    return out


def dl_clean(system):
    """Delete residue entries; refuse while a job is running so we never
    remove data an active download is writing.  Returns (count, bytes).
    删除下载残留；有任务运行时拒绝操作，返回 (数量, 释放字节数)。"""
    st = _DL[system]
    if st["job"] is not None:
        raise RuntimeError("download in progress")
    cnt, freed = 0, 0
    for e in dl_residue(system):
        p = os.path.join(ROMS_DIR, system, e["name"])
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                os.remove(p)
            cnt += 1
            freed += e["size"]
        except OSError:
            pass
    return cnt, freed


def dl_logger(system):
    log = _DL[system]["log"]

    def add(msg):
        log.append(datetime.now().strftime("%H:%M:%S ") + msg)
        del log[:-200]

    return add


_NAMES_CACHE = {}


def dl_names(system, lang):
    """Localized title overrides, e.g. fbneo_names_zh.json (shortname -> title).
    本地化游戏名覆盖表，如 fbneo_names_zh.json（短名 -> 显示名）。"""
    if not lang or not re.match(r"^[a-z]{2,3}(_[A-Za-z]{2,3})?$", lang):
        return {}
    key = (system, lang)
    if key not in _NAMES_CACHE:
        path = os.path.join(GAMEDB_DIR, system, "%s_names_%s.json" % (system, lang))
        try:
            with open(path, encoding="utf-8") as f:
                _NAMES_CACHE[key] = json.load(f)
        except (OSError, ValueError):
            _NAMES_CACHE[key] = {}
    return _NAMES_CACHE[key]


def dl_state(system):
    """Snapshot of the download state for the UI poller. / 供前端轮询的下载状态快照。"""
    st = _DL[system]
    job = st["job"]
    return {"ok": True, "job": job.snapshot() if job else None,
            "queue": len(st["queue"]), "queue_names": st["queue"][:200],
            "log": st["log"][-6:], "torrent": dl_info(system)}


def dl_start(system, names, peers):
    """Add games to the download queue; starts immediately when idle.
    把游戏加入下载队列；空闲时立即开始下载。"""
    st = _DL[system]
    by = {g["n"]: g for g in dl_lib(system)["games"]}
    inst = dl_installed(system)
    with st["lock"]:
        job = st["job"]
        running = bool(job and job.status in ("running", "idle"))
        seeding = bool(job and job.status == "seeding")
        wanted = [n for n in names
                  if n in by and n not in inst and n not in st["pending"]][:200]
        # payload-less games (engine data ships with the device): creating
        # their launch stub is the whole "download"
        stubbed = 0
        for n in list(wanted):
            g = by[n]
            # torrent games carry a file index ("i"), not a payload path
            if "f" not in g and not g.get("files") and "i" not in g:
                wanted.remove(n)
                stub = g.get("stub")
                if stub:
                    try:
                        d = os.path.join(ROMS_DIR, system)
                        os.makedirs(d, exist_ok=True)
                        open(os.path.join(d, stub), "a").close()
                        dl_logger(system)("created " + stub)
                        stubbed += 1
                    except OSError as e:
                        dl_logger(system)("stub error: %s" % e)
        if not wanted:
            return "" if (running or seeding or stubbed) else \
                "nothing to download (already installed or unknown)"
        st["pending"].update(wanted)
        if running or seeding:
            for addr in peers:
                job.add_peer(addr)
            st["queue"].extend(wanted)
            dl_logger(system)("queued %d game(s): %s"
                              % (len(wanted), ", ".join(wanted[:6])))
            if seeding:
                # a seeding job never ends on its own; stopping it lets
                # _dl_watch wake up and launch the queued games
                job.stop()
            return ""
        _dl_launch(system, wanted, peers)
        return ""


def _dl_launch(system, names, peers):
    """Build and start a btlib/httplib job for the selected games. / 为选中的游戏创建并启动 btlib/httplib 下载任务。"""
    st = _DL[system]
    lib = dl_lib(system)
    by = {g["n"]: g for g in lib["games"]}
    dest = os.path.join(ROMS_DIR, system)
    if "torrent" in lib:
        sel = [by[n]["i"] for n in names if n in by and "i" in by[n]]
        if not sel:
            return
        saved = parse_peers(conf_map().get("webmanager.dl_peers", ""))
        job = btlib.Job(dl_torrent(system), dest, sel,
                        manual_peers=saved + peers, log=dl_logger(system),
                        allow_upload=conf_map().get("webmanager.dl_upload", "1") != "0",
                        proxy=conf_map().get("webmanager.dl_proxy", ""),
                        listener=bt_listener(),
                        seed=conf_map().get("webmanager.dl_seed", "1") != "0")
    else:
        base = lib["http"]["base"]
        # a game entry carries either a single payload ("f"/"fn"/"s" at the
        # top level) or several under "files" (e.g. etlegacy's three pk3)
        files = []
        extract = {}
        for n in names:
            g = by.get(n)
            if not g:
                continue
            for e in (g.get("files") or [g]):
                if "f" not in e:
                    continue
                fn = e.get("fn") or os.path.basename(e["f"])
                files.append((e["f"], fn, e["s"]))
                if e.get("extract"):
                    extract[fn] = e["extract"]
        if not files:
            return
        cookie = IA_COOKIE if os.path.exists(IA_COOKIE) else ""
        job = httplib.Job(base, files, dest, log=dl_logger(system),
                          proxy=conf_map().get("webmanager.dl_proxy", ""),
                          cookie=cookie, extract=extract)
    st["job"] = job
    st["job_names"] = names
    job.start()
    threading.Thread(target=_dl_watch, args=(system, job), daemon=True).start()


def _dl_watch(system, job):
    """When a job ends, carry on with whatever was queued meanwhile.
    任务结束后，继续启动在此期间排队等待的游戏。"""
    job.thread.join()
    st = _DL[system]
    _count_cache.pop(system, None)
    with st["lock"]:
        if st["job"] is not job:
            return  # superseded by a newer job; its own watcher continues
        st["job"] = None
        st["job_names"] = []
        queue = st["queue"]
        st["queue"] = []
        if not queue:
            st["pending"] = set()
            return
        inst = dl_installed(system)
        nxt = [n for n in queue if n not in inst]
        st["pending"] = set(nxt)
        if nxt:
            _dl_launch(system, nxt, list(job.manual_peers))


def dl_stop(system):
    """Stop the running download job and clear the queue. / 停止当前下载任务并清空队列。"""
    st = _DL[system]
    with st["lock"]:
        if st["job"]:
            st["job"].stop()
        st["queue"] = []
        st["pending"] = set()


def parse_peers(raw):
    peers = []
    for s in (raw or [])[:20]:
        try:
            ip, port = str(s).rsplit(":", 1)
            peers.append((ip.strip(), int(port)))
        except ValueError:
            pass
    return peers


# ---------------- screenscraper metadata scraping / ScreenScraper 元数据刮削 ----------------
# Mirrors EmulationStation's own scraper (same API, credentials, media
# layout and gamelist.xml fields), driven from the game list UI.
# 复刻 EmulationStation 自带刮削器（相同 API、凭据、媒体布局与 gamelist.xml 字段），由游戏列表页驱动。
_SCRAPE = {}
_SCRAPE_LOCK = threading.Lock()


def _scrape_st(system):
    with _SCRAPE_LOCK:
        return _SCRAPE.setdefault(system, {"job": None, "log": []})


def _scrape_log(system):
    log = _scrape_st(system)["log"]

    def add(msg):
        log.append(datetime.now().strftime("%H:%M:%S ") + msg)
        del log[:-200]

    return add


def scrape_missing(system):
    """Rom files (recursive) lacking any gamelist metadata (desc or image).
    递归找出缺少 gamelist 元数据（desc 或 image）的 ROM 文件。"""
    sysinfo = find_system(system)
    if not sysinfo:
        return []
    d = sysinfo["path"]
    exts = set(sysinfo["extensions"])
    meta = gamelist_meta(d)
    out = []
    for root, dirs, files in os.walk(d):
        dirs[:] = [x for x in dirs if not x.startswith(".") and
                   x.lower() not in ("images", "videos", "manuals")]
        for fn in files:
            if fn.startswith((".", "_")):
                continue
            if os.path.splitext(fn)[1].lower() not in exts:
                continue
            abs_p = os.path.join(root, fn)
            rel = os.path.relpath(abs_p, d).replace(os.sep, "/")
            m = meta.get(rel.lower())
            if m and (m.get("desc") or m.get("image")):
                continue
            try:
                size = os.path.getsize(abs_p)
            except OSError:
                continue
            out.append({"rel": rel, "abs": abs_p, "size": size})
    return out


def gamelist_upsert_entry(sys_path, rel, fields, downloads):
    """Create or update one <game> node; preserves fields we do not set
    (favorite, playcount, ...), exactly like ES's scraper.
    创建或更新一个 <game> 节点；保留未设置的字段（favorite、playcount 等），与 ES 刮削器一致。"""
    gl = os.path.join(sys_path, "gamelist.xml")
    try:
        tree = ET.parse(gl) if os.path.exists(gl) else None
    except Exception:
        tree = None
    if tree is None:
        tree = ET.ElementTree(ET.Element("gameList"))
    root = tree.getroot()
    node = None
    for g in root.findall("game"):
        if _norm_rel(g.findtext("path", "")) == _norm_rel(rel):
            node = g
            break
    if node is None:
        node = ET.SubElement(root, "game")
        ET.SubElement(node, "path").text = "./" + rel
    if fields.get("id"):
        node.set("id", str(fields["id"]))
        node.set("source", fields.get("source", "ScreenScraper.fr"))
    merged = {k: v for k, v in fields.items() if k not in ("id", "source")}
    merged.update(downloads)
    for k, v in merged.items():
        el = node.find(k)
        if el is None:
            el = ET.SubElement(node, k)
        el.text = v
    backup_file(gl)
    ET.indent(tree, space="\t")
    tree.write(gl, encoding="utf-8", xml_declaration=True)
    _gamelist_cache.pop(gl, None)


_missing_cache = {}  # system -> (timestamp, count); the 2.5 s UI poll must not rescan (B9)


def scrape_status(system):
    """Scrape job state plus credential/missing counts. / 刮削任务状态以及凭据/缺失数量。"""
    st = _scrape_st(system)
    cfg = scraper.read_es_settings()
    job = st["job"]
    ent = _missing_cache.get(system)
    if ent is None or time.time() - ent[0] > 10:
        missing = len(scrape_missing(system))
        _missing_cache[system] = (time.time(), missing)
    else:
        missing = ent[1]
    return {"ok": True,
            "creds": bool(cfg["user"] and cfg["password"]),
            "missing": missing,
            "job": job.snapshot() if job else None,
            "log": st["log"][-6:]}


def scrape_start(system):
    """Start a background scrape job for roms lacking metadata. / 为缺少元数据的 ROM 启动后台刮削任务。"""
    sysinfo = find_system(system)
    if not sysinfo:
        return "unknown system"
    cfg = scraper.read_es_settings()
    if not (cfg["user"] and cfg["password"]):
        return "no ScreenScraper credentials saved in EmulationStation"
    st = _scrape_st(system)
    if st["job"] and st["job"].is_alive():
        return "scrape already running"
    items = scrape_missing(system)
    if not items:
        st["job"] = None
        return ""
    st["job"] = scraper.ScrapeJob(
        system, items, cfg,
        lambda rel, fields, dl: gamelist_upsert_entry(
            sysinfo["path"], rel, fields, dl),
        log=_scrape_log(system))
    st["job"].start()
    return ""


def scrape_stop(system):
    st = _scrape_st(system)
    if st["job"]:
        st["job"].stop()


# ---------------- access control / 访问控制 ----------------
# Optional shared-token auth (A1): once webmanager.token is set in
# batocera.conf every API call must carry "Authorization: Bearer <token>"
# (the web UI asks once and keeps it in localStorage; ?token= exists for
# <img>/download URLs). With no token configured the panel stays open as
# before, but POSTs must then pass a same-origin Origin/Referer check so
# unrelated websites cannot drive the API through the user's browser (A5).
# 可选共享令牌认证（A1）：在 batocera.conf 配置 webmanager.token 后，所有 API
# 请求须携带令牌；未配置时面板保持开放，但 POST 需通过 Origin/Referer 同源校验，
# 防止恶意网页借用户浏览器调用 API（A5）。

class BadRequest(Exception):
    """Client input error -> HTTP 400 instead of a 500 (B15).
    客户端输入错误，返回 400 而非 500（B15）。"""


def web_token():
    """Configured access token; '' means auth is disabled. / 配置的访问令牌，空表示未启用认证。"""
    return conf_map().get("webmanager.token", "").strip()


# ---------------- HTTP handler / HTTP 请求处理器 ----------------

class Handler(BaseHTTPRequestHandler):
    """HTTP request handler: JSON API routes plus static/file responses. / HTTP 请求处理器：JSON API 路由与静态/文件响应。"""
    protocol_version = "HTTP/1.1"
    server_version = "BatoceraWebManager/1.0"

    def log_message(self, fmt, *args):
        pass

    # ---- access control (A1/A5) / 访问控制 ----
    def _auth_ok(self, q):
        tok = web_token()
        if not tok:
            return True
        h = self.headers.get("Authorization", "")
        if h.startswith("Bearer ") and hmac.compare_digest(h[7:].strip(), tok):
            return True
        t = q.get("token", "")
        return bool(t) and hmac.compare_digest(t, tok)

    def _csrf_ok(self):
        # A cross-site form cannot set Authorization, so a configured token
        # already blocks CSRF; this check guards the open (token-less) mode.
        # 跨站表单无法携带 Authorization，配置令牌后 CSRF 已被认证挡住；
        # 此校验保护未设令牌的开放模式。
        if web_token():
            return True
        host = self.headers.get("Host", "").lower()
        for hdr in ("Origin", "Referer"):
            val = self.headers.get(hdr)
            if val:
                try:
                    return urllib.parse.urlsplit(val).netloc.lower() == host
                except ValueError:
                    return False
        return True  # curl & co send neither header

    # ---- response helpers / 响应辅助方法 ----
    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, msg, status=400):
        self._json({"ok": False, "error": msg}, status)

    def _html(self, body):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path, download_name=None, ctype=None, inline=False):
        if not os.path.isfile(path):
            return self._error("file not found", 404)
        ext = os.path.splitext(path)[1].lower()
        ctype = ctype or MIME.get(ext, "application/octet-stream")
        size = os.path.getsize(path)
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("X-Content-Type-Options", "nosniff")
        if download_name:
            enc = urllib.parse.quote(download_name)
            self.send_header("Content-Disposition",
                             "attachment; filename*=UTF-8''" + enc)
        else:
            # static assets (locales, logo, art) are cacheable (B14)
            self.send_header("Cache-Control", "public, max-age=3600")
            if inline:
                self.send_header("Content-Disposition", "inline")
        self.end_headers()
        with open(path, "rb") as f:
            shutil.copyfileobj(f, self.wfile, 65536)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            raise BadRequest("bad Content-Length")
        if length < 0 or length > 10 * 1024 * 1024:
            raise BadRequest("bad Content-Length")
        if length == 0:
            return {}
        body = self.rfile.read(length)
        try:
            data = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise BadRequest("invalid JSON body")
        if not isinstance(data, dict):
            raise BadRequest("JSON object expected")
        return data

    def _drain(self):
        length = int(self.headers.get("Content-Length") or 0)
        while length > 0:
            chunk = self.rfile.read(min(65536, length))
            if not chunk:
                break
            length -= len(chunk)

    def _client_error(self, msg, status=400):
        """Best-effort error response; the client may already be gone. / 尽力返回错误；客户端可能已断连。"""
        try:
            self._error(msg, status)
        except Exception:
            pass

    # ---- routing / 路由分发 ----
    def do_GET(self):
        try:
            u = urllib.parse.urlsplit(self.path)
            path = u.path
            q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
            # open entry points: the page itself plus its locale files and
            # logo; everything else needs the token when one is set (A1)
            # 开放入口：页面本身、语言文件与 logo；配置令牌后其余均须认证
            if not (path in ("/", "/index.html") or path == "/api/logo"
                    or path.startswith("/api/locales/")) and not self._auth_ok(q):
                return self._error("unauthorized", 401)
            self.route_get(path, q)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except BadRequest as e:
            self._client_error(str(e))
        except Exception:
            traceback.print_exc()  # full stack to server.log, generic 500 out (B3)
            self._client_error("internal error", 500)

    def do_POST(self):
        try:
            u = urllib.parse.urlsplit(self.path)
            path = u.path
            q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
            if not self._auth_ok(q):
                return self._error("unauthorized", 401)
            if not self._csrf_ok():
                return self._client_error("cross-origin request rejected", 403)
            self.route_post(path, q)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except BadRequest as e:
            self._client_error(str(e))
        except Exception:
            traceback.print_exc()
            self._client_error("internal error", 500)

    # ================= GET 路由 =================
    def route_get(self, path, q):
        """Route table for all GET endpoints. / 所有 GET 端点的路由表。"""
        if path == "/" or path == "/index.html":
            with open(os.path.join(BASE, "index.html"), "rb") as f:
                return self._html(f.read())
        if path == "/api/system/info":
            return self._json(self.sys_info())
        if path == "/api/system/version-check":
            return self._json(self.version_check())
        if path == "/api/system/disk-usage":
            return self._json(self.disk_usage())
        if path == "/api/system/upgrade-status":
            return self.upgrade_status()
        if path == "/api/backup/info":
            return self._json({"ok": True, "sets": backup_set_sizes(),
                               "backups": backup_list()})
        if path == "/api/backup/status":
            return self._json(backup_status())
        if path == "/api/backup/download":
            name = q.get("f", "")
            if not re.match(r"^[\w.-]+\.tar$", name):
                return self._error("bad file name")
            return self._file(os.path.join(BACKUP_ROOT, name), download_name=name)
        # locale static JSON — shipped with the webmanager package
        m = re.match(r"^/api/locales/([A-Za-z0-9_-]+\.json)$", path)
        if m:
            return self._file(os.path.join(BASE, "locales", m.group(1)), ctype="application/json")
        if path == "/api/logo":
            return self._file("/usr/share/emulationstation/resources/logo.png", ctype="image/png", inline=True)
        if path == "/api/controller-overlay":
            return self.overlay(q.get("name", ""))
        if path == "/api/system-art":
            return self.system_art(q.get("name", ""))
        if path == "/api/controller-overlay/list":
            return self._json({"overlays": self.overlay_list()})
        if path == "/api/video/status":
            return self._json(self.video_status())
        if path == "/api/audio/status":
            return self._json(self.audio_status())
        if path == "/api/settings":
            return self._json({"lines": read_conf_lines()})
        if path == "/api/settings/backups":
            baks = sorted((os.path.basename(x) for x in glob.glob(BACKUP_DIR + "/*.bak")), reverse=True)
            return self._json({"backups": baks})
        if path == "/api/controllers":
            return self._json({"devices": read_controllers()})
        if path == "/api/controllers/raw":
            content = ""
            if os.path.exists(ES_INPUT_PATH):
                with open(ES_INPUT_PATH, encoding="utf-8", errors="replace") as f:
                    content = f.read()
            return self._json({"content": content})
        if path == "/api/systems":
            conf = conf_map()
            out = []
            for s in get_systems():
                if s["name"] in HIDDEN_SYSTEMS:
                    continue
                item = dict(s)
                item["romCount"] = rom_count(s)
                item["currentEmulator"] = conf.get(s["name"] + ".emulator", "")
                item["currentCore"] = conf.get(s["name"] + ".core", "")
                item["source"] = dl_available(s["name"])
                out.append(item)
            return self._json({"systems": out})
        if path == "/api/games":
            return self._json(self.games_list(q.get("system", ""), q.get("dir", "")))
        if path == "/api/media":
            return self.media(q)
        if path == "/api/dl/library":
            system = q.get("system", "")
            if not dl_available(system):
                return self._error("no download source for this system", 404)
            lib = dl_lib(system)
            return self._json({"ok": True, "games": lib["games"],
                               "installed": sorted(dl_installed(system)),
                               "names": dl_names(system, q.get("lang", "")),
                               "residue": dl_residue(system),
                               "proxy": conf_map().get("webmanager.dl_proxy", ""),
                               "ia_auth": ia_auth_ok(),
                               "torrent": dl_info(system)})
        if path == "/api/dl/status":
            system = q.get("system", "")
            if not dl_available(system):
                return self._error("no download source for this system", 404)
            return self._json(dl_state(system))
        if path == "/api/dl/ia_state":
            return self._json({"auth": ia_auth_ok(),
                               "proxy": conf_map().get("webmanager.dl_proxy", "")})
        if path == "/api/scrape/status":
            return self._json(scrape_status(q.get("system", "")))
        if path == "/api/bios":
            return self._json(bios_check())
        if path == "/api/files":
            return self.files_list(q.get("path", USERDATA))
        if path == "/api/files/download":
            fp = safe_join("/", q.get("path", ""))
            if not fp.startswith(os.path.realpath(USERDATA) + os.sep):
                return self._error("only /userdata is allowed", 403)
            return self._file(fp, download_name=os.path.basename(fp))
        return self._error("not found", 404)

    # ================= POST 路由 =================
    def route_post(self, path, q):
        """Route table for all POST endpoints. / 所有 POST 端点的路由表。"""
        ctype = self.headers.get("Content-Type", "")
        if path == "/api/power":
            data = self._read_json()
            return self.power(data.get("action", ""))
        if path == "/api/system/upgrade":
            data = self._read_json()
            channel = str(data.get("channel", "stable"))
            if channel not in ("stable", "butterfly"):
                return self._error("bad channel")
            if self._upgrade_running():
                return self._error("upgrade already in progress", 409)
            pre = self.upgrade_preflight(channel)
            if not pre["size"]:
                return self._error("cannot reach the update server")
            env, boost, err = self._boost_env(pre["need_mb"], pre["boot_avail_mb"])
            if err:
                return err
            extra = {"need_mb": pre["need_mb"], "boot_avail_mb": pre["boot_avail_mb"],
                     "boost_mb": boost}
            state = {"phase": "running", "stage": "download", "channel": channel,
                     "dl_total": pre["size"]}
            state.update(extra)
            self._upgrade_begin(state)
            dest = os.path.join(UPGRADE_DIR, "boot.tar.xz")
            os.makedirs(UPGRADE_DIR, exist_ok=True)
            # remove leftovers from earlier attempts (part files are kept for resume)
            for p in (dest, dest + ".md5"):
                try:
                    os.remove(p)
                except OSError:
                    pass

            # Download in the background (parallel ranges; the official server is
            # far too slow with a single connection), then install from the local
            # file. A manual reboot is needed afterwards.
            def later():
                err_msg = ""
                try:
                    with open(os.path.join(TMP_DIR, "upgrade.log"), "w") as log:
                        # md5 sidecar lets batocera-upgrade verify the image
                        run_cmd(["curl", "-sfL", "-A", "webmanager/1.0",
                                 "-o", dest + ".md5", pre["url"] + ".md5"], timeout=60)
                        info = _curl_head(pre["url"])
                        if info and info["ranges"] and info["size"] > 64 * 1048576:
                            log.write("parallel download: %d MB, %d connections\n"
                                      % (info["size"] // 1048576, UPGRADE_PARTS))
                            log.flush()
                            err_msg = _download_ranges(info["url"], dest, info["size"], log)
                        else:
                            log.write("plain download: %s\n" % pre["url"])
                            log.flush()
                            proc = subprocess.Popen(
                                ["curl", "-sfL", "-A", "webmanager/1.0",
                                 "-o", dest, pre["url"]],
                                stdout=log, stderr=subprocess.STDOUT)
                            with _UPG["plock"]:
                                _UPG["procs"].add(proc)
                            try:
                                while proc.poll() is None:
                                    if _UPG["cancel"].is_set():
                                        proc.kill()
                                        break
                                    time.sleep(0.5)
                            finally:
                                with _UPG["plock"]:
                                    _UPG["procs"].discard(proc)
                            rc = proc.returncode or 0
                            if rc:
                                err_msg = "download failed (rc=%d)" % rc
                except Exception as e:
                    err_msg = str(e)
                if err_msg:
                    # keep the parts directory — a retry resumes the download
                    for p in (dest, dest + ".md5"):
                        try:
                            os.remove(p)
                        except OSError:
                            pass
                    self._upgrade_finish(channel, err_msg, extra)
                    return
                self._start_install_thread(channel, dest, env, extra, verify=False)
            t = threading.Thread(target=later, daemon=True)
            _UPG["thread"] = t
            t.start()
            out = {"ok": True, "msg": "upgrade started"}
            out.update(extra)
            return self._json(out)
        if path == "/api/system/upgrade-local":
            # Install a boot.tar.xz the user fetched by other means (BT, PC with a
            # download manager) and copied to /userdata/system/upgrade/, e.g. via
            # the network share.
            if self._upgrade_running():
                return self._error("upgrade already in progress", 409)
            dest = os.path.join(UPGRADE_DIR, "boot.tar.xz")
            if not os.path.isfile(dest):
                return self._error("no local package: copy boot.tar.xz to "
                                   "/userdata/system/upgrade/ first (network share)")
            size = os.path.getsize(dest)
            if size < 400 * 1048576:
                return self._error("local package looks incomplete (%d MB)"
                                   % (size // 1048576))
            code, out, _ = run_cmd(["xz", "--robot", "-l", dest], timeout=120)
            if code != 0:
                return self._error("local package is not a valid xz archive (truncated?)")
            uncomp_mb = 0
            for line in out.splitlines():
                if line.startswith("totals"):
                    try:
                        uncomp_mb = int(line.split()[4]) // 1048576
                    except (IndexError, ValueError):
                        pass
            if not uncomp_mb:
                return self._error("cannot read uncompressed size of local package")
            boot_avail = self._boot_avail_mb()
            env, boost, err = self._boost_env(uncomp_mb + 10, boot_avail)
            if err:
                return err
            extra = {"need_mb": uncomp_mb + 10, "boot_avail_mb": boot_avail,
                     "boost_mb": boost, "dl_total": size}
            state = {"phase": "running", "stage": "verify", "channel": "local"}
            state.update(extra)
            self._upgrade_begin(state)
            try:
                os.remove(os.path.join(TMP_DIR, "upgrade.log"))
            except OSError:
                pass
            self._start_install_thread("local", dest, env, extra, verify=True)
            out = {"ok": True, "msg": "local upgrade started"}
            out.update(extra)
            return self._json(out)
        if path == "/api/system/upgrade-cancel":
            stage = ""
            try:
                with open(os.path.join(TMP_DIR, "upgrade.state")) as f:
                    stage = json.load(f).get("stage", "")
            except (OSError, ValueError):
                pass
            if stage == "install":
                return self._error("files are being written to /boot; cancelling now "
                                   "would corrupt the system", 409)
            if not self._upgrade_running():
                return self._error("no upgrade in progress")
            _UPG["cancel"].set()
            # kill only our own downloader processes, never pkill by name (B7)
            # 只终止本进程登记的下载进程，不再按名字 pkill 误伤其他进程（B7）
            with _UPG["plock"]:
                procs = list(_UPG["procs"])
            for p in procs:
                try:
                    p.kill()
                except OSError:
                    pass
            return self._json({"ok": True})
        if path == "/api/system/clean-upgrade":
            return self.clean_upgrade()
        if path == "/api/backup/create":
            data = self._read_json()
            err = backup_start([str(k) for k in data.get("sets", [])])
            return self._error(err) if err else self._json({"ok": True})
        if path == "/api/backup/restore":
            err = backup_restore(str(self._read_json().get("file", "")))
            return self._error(err) if err else self._json({"ok": True})
        if path == "/api/backup/cancel":
            err = backup_cancel()
            return self._error(err) if err else self._json({"ok": True})
        if path == "/api/backup/delete":
            err = backup_delete(str(self._read_json().get("file", "")))
            return self._error(err) if err else self._json({"ok": True})
        if path == "/api/video/output":
            data = self._read_json()
            out = data.get("output", "")
            if not re.match(r"^[\w\-]+$", out):
                return self._error("bad output name")
            code, so, se = run_cmd(["batocera-resolution", "setOutput", out], timeout=30)
            if conf_map().get("global.videooutput", "") != out:
                conf_set("global.videooutput", out)
            return self._json({"ok": code == 0, "output": so, "error": se})
        if path == "/api/video/mode":
            data = self._read_json()
            mode = data.get("mode", "")
            if not re.match(r"^[\w.\-:x@]+$", mode):
                return self._error("bad mode")
            code, so, se = run_cmd(["batocera-resolution", "setMode", mode], timeout=30)
            if conf_map().get("global.videomode", "") != mode:
                conf_set("global.videomode", mode)
            return self._json({"ok": code == 0, "output": so, "error": se})
        if path == "/api/audio/device":
            data = self._read_json()
            dev = data.get("device", "")
            if not re.match(r"^[\w.\-]+$", dev):
                return self._error("bad device")
            code, so, se = run_cmd(["batocera-audio", "set", dev], timeout=30)
            return self._json({"ok": code == 0, "output": so, "error": se})
        if path == "/api/audio/volume":
            data = self._read_json()
            vol = str(data.get("volume", ""))
            if not re.match(r"^(mute|unmute|mute-toggle|[+-]?\d{1,3})$", vol):
                return self._error("bad volume")
            code, so, se = run_cmd(["batocera-audio", "setSystemVolume", vol], timeout=30)
            return self._json({"ok": code == 0, "error": se})
        if path == "/api/settings/save":
            data = self._read_json()
            lines = data.get("lines", [])
            if not isinstance(lines, list) or len(lines) > 5000:
                return self._error("bad lines")
            clean = []
            for it in lines:
                if it.get("type") == "kv":
                    k = str(it.get("key", "")).strip()
                    if not k or "=" in k:
                        continue
                    clean.append({"type": "kv", "key": k, "value": str(it.get("value", ""))})
                elif it.get("type") in ("comment", "blank", "other"):
                    clean.append({"type": it["type"], "raw": str(it.get("raw", ""))})
            write_conf(clean)
            return self._json({"ok": True})
        if path == "/api/settings/set":
            data = self._read_json()
            k = str(data.get("key", "")).strip()
            if not k or "=" in k:
                return self._error("bad key")
            conf_set(k, str(data.get("value", "")))
            return self._json({"ok": True})
        if path == "/api/settings/restore":
            data = self._read_json()
            name = os.path.basename(str(data.get("file", "")))
            if not name.startswith("batocera.conf.") or not name.endswith(".bak"):
                return self._error("bad backup name")
            src = os.path.join(BACKUP_DIR, name)
            if not os.path.isfile(src):
                return self._error("backup not found", 404)
            backup_file(CONF_PATH)
            shutil.copy2(src, CONF_PATH)
            return self._json({"ok": True})
        if path == "/api/controllers/save":
            data = self._read_json()
            devices = data.get("devices", [])
            if not isinstance(devices, list) or len(devices) > 50:
                return self._error("bad devices")
            save_controllers(devices)
            return self._json({"ok": True})
        if path == "/api/controllers/raw":
            data = self._read_json()
            content = str(data.get("content", ""))
            try:
                ET.fromstring(content)
            except Exception as e:
                return self._error("invalid XML: " + str(e))
            backup_file(ES_INPUT_PATH)
            with open(ES_INPUT_PATH, "w", encoding="utf-8") as f:
                f.write(content)
            return self._json({"ok": True})
        if path == "/api/controllers/restore":
            data = self._read_json()
            name = os.path.basename(str(data.get("file", "")))
            if not name.startswith("es_input.cfg.") or not name.endswith(".bak"):
                return self._error("bad backup name")
            src = os.path.join(BACKUP_DIR, name)
            if not os.path.isfile(src):
                return self._error("backup not found", 404)
            backup_file(ES_INPUT_PATH)
            shutil.copy2(src, ES_INPUT_PATH)
            return self._json({"ok": True})
        if path == "/api/systems/emulator":
            data = self._read_json()
            sysname = str(data.get("system", ""))
            if not find_system(sysname):
                return self._error("unknown system", 404)
            emu = str(data.get("emulator", ""))
            core = str(data.get("core", ""))
            items = read_conf_lines()
            def setkey(k, v):
                for it in items:
                    if it["type"] == "kv" and it["key"] == k:
                        it["value"] = v
                        return
                items.append({"type": "kv", "key": k, "value": v})
            setkey(sysname + ".emulator", emu)
            setkey(sysname + ".core", core)
            write_conf(items)
            return self._json({"ok": True})
        if path == "/api/dl/download":
            data = self._read_json()
            system = str(data.get("system", ""))
            if not dl_available(system):
                self._drain()
                return self._error("no download source for this system", 404)
            err = dl_start(system, [str(x) for x in data.get("games", [])[:200]],
                           parse_peers(data.get("peers")))
            return self._error(err, 409) if err else self._json({"ok": True})
        if path == "/api/dl/stop":
            data = self._read_json()
            system = str(data.get("system", ""))
            if not dl_available(system):
                self._drain()
                return self._error("no download source for this system", 404)
            dl_stop(system)
            return self._json({"ok": True})
        if path == "/api/dl/clean":
            data = self._read_json()
            system = str(data.get("system", ""))
            if not dl_available(system):
                self._drain()
                return self._error("no download source for this system", 404)
            try:
                cnt, freed = dl_clean(system)
            except RuntimeError as e:
                return self._error(str(e), 409)
            return self._json({"ok": True, "removed": cnt, "freed": freed})
        if path == "/api/scrape/start":
            data = self._read_json()
            err = scrape_start(str(data.get("system", "")))
            return self._error(err, 409) if err else self._json({"ok": True})
        if path == "/api/scrape/stop":
            data = self._read_json()
            scrape_stop(str(data.get("system", "")))
            return self._json({"ok": True})
        if path == "/api/dl/peers":
            data = self._read_json()
            system = str(data.get("system", ""))
            st = _DL.get(system)
            added = 0
            job = st["job"] if st else None
            for addr in parse_peers(data.get("peers")):
                if job:
                    job.add_peer(addr)
                    added += 1
            return self._json({"ok": True, "added": added})
        if path == "/api/dl/ia_login":
            data = self._read_json()
            email = str(data.get("email", "")).strip()
            password = str(data.get("password", ""))
            if not email or not password:
                return self._error("email and password required")
            err = ia_login(email, password)
            return self._error(err) if err else self._json({"ok": True})
        if path == "/api/dl/ia_logout":
            try:
                os.remove(IA_COOKIE)
            except OSError:
                pass
            return self._json({"ok": True})
        if path == "/api/games/upload":
            sysinfo = find_system(q.get("system", ""))
            if not sysinfo:
                self._drain()
                return self._error("unknown system", 404)
            exts = set(sysinfo["extensions"])
            try:
                dest_base = safe_join(sysinfo["path"], q.get("dir", ""))
            except ValueError:
                self._drain()
                return self._error("bad dir")
            auto_pack = sysinfo["name"] in AUTO_PACK_SYSTEMS

            def make_dest(field, filename):
                ext = os.path.splitext(filename)[1].lower()
                if ext not in exts and not (auto_pack and ext in (".zip", ".7z")):
                    return None
                return safe_join(dest_base, filename)
            length = int(self.headers.get("Content-Length") or 0)
            fields, files = parse_multipart(self.rfile, ctype, length, make_dest)
            auto = []
            for f in files:
                info = pc_autoconfig(sysinfo["name"], dest_base, f["filename"])
                if info:
                    auto.append(dict(info, file=f["filename"]))
            _count_cache.pop(sysinfo["name"], None)
            return self._json({"ok": True, "uploaded": [f["filename"] for f in files],
                               "autoconfig": auto})
        if path == "/api/games/delete":
            data = self._read_json()
            sysinfo = find_system(str(data.get("system", "")))
            if not sysinfo:
                return self._error("unknown system", 404)
            deleted, errors = [], []
            for fn in data.get("files", [])[:200]:
                fn = str(fn).replace("\\", "/").lstrip("/")
                try:
                    fp = safe_join(sysinfo["path"], fn)
                    if os.path.isfile(fp):
                        os.remove(fp)
                        gamelist_remove_entry(sysinfo["path"], fn)
                        deleted.append(fn)
                except Exception as e:
                    errors.append(fn + ": " + str(e))
            _count_cache.pop(sysinfo["name"], None)
            return self._json({"ok": True, "deleted": deleted, "errors": errors})
        if path == "/api/games/rename":
            data = self._read_json()
            sysinfo = find_system(str(data.get("system", "")))
            if not sysinfo:
                return self._error("unknown system", 404)
            rel = str(data.get("rel", "")).replace("\\", "/").lstrip("/")
            new = sanitize_filename(str(data.get("new", "")))
            if not rel or not new:
                return self._error("bad name")
            src = safe_join(sysinfo["path"], rel)
            dst = safe_join(os.path.dirname(src), new)
            if not dst.startswith(os.path.realpath(sysinfo["path"]) + os.sep):
                return self._error("bad name")
            if not os.path.isfile(src):
                return self._error("file not found", 404)
            if os.path.exists(dst):
                return self._error("target exists")
            os.rename(src, dst)
            new_rel = os.path.relpath(dst, sysinfo["path"]).replace(os.sep, "/")
            gamelist_rename_entry(sysinfo["path"], rel, new_rel)
            return self._json({"ok": True})
        if path == "/api/bios/upload":
            relpath = q.get("relpath", "")
            if not relpath.startswith("bios/"):
                self._drain()
                return self._error("relpath must start with bios/")
            dest_dir = os.path.dirname(safe_join(USERDATA, relpath))
            length = int(self.headers.get("Content-Length") or 0)
            def make_dest(field, filename):
                return safe_join(dest_dir, sanitize_filename(filename))
            fields, files = parse_multipart(self.rfile, ctype, length, make_dest, max_files=1)
            _bios_cache["t"] = 0
            return self._json({"ok": True, "uploaded": [f["filename"] for f in files]})
        if path == "/api/files/upload":
            base = safe_join("/", q.get("path", USERDATA))
            ud = os.path.realpath(USERDATA)
            if base != ud and not base.startswith(ud + os.sep):
                self._drain()
                return self._error("only /userdata is allowed")
            if not os.path.isdir(base):
                self._drain()
                return self._error("directory not found", 404)
            length = int(self.headers.get("Content-Length") or 0)
            def make_dest(field, filename):
                return safe_join(base, filename)
            fields, files = parse_multipart(self.rfile, ctype, length, make_dest)
            return self._json({"ok": True, "uploaded": [f["filename"] for f in files]})
        if path == "/api/files/delete":
            data = self._read_json()
            deleted, errors = [], []
            for p in data.get("paths", [])[:200]:
                try:
                    fp = safe_join("/", str(p))
                    if not fp.startswith(os.path.realpath(USERDATA) + os.sep):
                        errors.append(p + ": out of bounds")
                        continue
                    if os.path.isdir(fp):
                        shutil.rmtree(fp)
                    else:
                        os.remove(fp)
                    deleted.append(p)
                except Exception as e:
                    errors.append(str(p) + ": " + str(e))
            return self._json({"ok": True, "deleted": deleted, "errors": errors})
        if path == "/api/files/rename":
            data = self._read_json()
            fp = safe_join("/", str(data.get("path", "")))
            if not fp.startswith(os.path.realpath(USERDATA) + os.sep):
                return self._error("only /userdata is allowed")
            new = sanitize_filename(str(data.get("new", "")))
            dst = os.path.join(os.path.dirname(fp), new)
            if os.path.exists(dst):
                return self._error("target exists")
            os.rename(fp, dst)
            return self._json({"ok": True})
        if path == "/api/files/mkdir":
            data = self._read_json()
            fp = safe_join("/", str(data.get("path", "")))
            if not fp.startswith(os.path.realpath(USERDATA) + os.sep):
                return self._error("only /userdata is allowed")
            target = os.path.join(fp, sanitize_filename(str(data.get("name", "new"))))
            if os.path.isdir(target):
                return self._json({"ok": True})  # already exists: be idempotent (B4)
            os.makedirs(target)
            return self._json({"ok": True})
        self._drain()
        return self._error("not found", 404)

    # ================= handlers / 各端点处理函数 =================
    def sys_info(self):
        """Version, hostname, IPs, uptime, memory, disk, temperatures. / 版本、主机名、IP、运行时长、内存、磁盘与温度。"""
        info = {}
        info["app_version"] = APP_VERSION
        code, out, _ = run_cmd(["batocera-version"])
        info["version"] = out.strip()
        info["hostname"] = conf_map().get("system.hostname", "") or sh("hostname")[1].strip()
        code, out, _ = sh("ip -4 addr show | grep -oP 'inet \\K[0-9.]+' | grep -v '^127\\.'")
        info["ips"] = [x for x in out.split() if x]
        try:
            up = float(open("/proc/uptime").read().split()[0])
            info["uptime"] = int(up)
        except Exception:
            info["uptime"] = 0
        try:
            info["load"] = open("/proc/loadavg").read().split()[:3]
        except Exception:
            info["load"] = []
        mem = {}
        try:
            for line in open("/proc/meminfo"):
                k, v = line.split(":")[0], line.split()[1]
                mem[k] = int(v)
            info["mem"] = {"total": mem.get("MemTotal", 0) * 1024, "avail": mem.get("MemAvailable", 0) * 1024}
        except Exception:
            info["mem"] = {"total": 0, "avail": 0}
        st = os.statvfs("/userdata")
        total = st.f_blocks * st.f_frsize
        free = st.f_bavail * st.f_frsize
        info["disk"] = {"total": total, "free": free, "used": total - free}
        temps = []
        for h in glob.glob("/sys/class/hwmon/hwmon*/name"):
            try:
                name = open(h).read().strip()
                d = os.path.dirname(h)
                for t in sorted(glob.glob(d + "/temp*_input"))[:1]:
                    temps.append({"name": name, "temp": int(open(t).read().strip()) // 1000})
            except Exception:
                pass
        info["temps"] = temps
        info["esRunning"] = sh("pgrep -x emulationstatio")[0] == 0
        return info

    def version_check(self):
        """Current + latest stable/butterfly versions (cached 10 min). / 当前版本与 stable/butterfly 频道最新版本（缓存 10 分钟）。"""
        # Returns current + latest stable + latest butterfly. Cached 10 min.
        now = time.time()
        if _vc_cache["data"] is not None and now - _vc_cache["t"] < 600:
            return _vc_cache["data"]
        code, cur, _ = run_cmd(["batocera-version"])
        cur = cur.strip()
        board = "x86-64-v3"
        try:
            with open("/boot/boot/batocera.board") as f:
                board = f.read().strip() or board
        except OSError:
            pass
        base = (conf_map().get("updates.url") or "https://updates.batocera.org").rstrip("/")
        channels = {}
        for ch in ("stable", "butterfly"):
            # NOTE: v43's batocera-upgrade starts downloading for real even with
            # --check-upgrade, so the check is done as a plain HTTP fetch of the
            # batocera.version sidecar instead.
            url = "%s/%s/%s/last/batocera.version" % (base, board, ch)
            c, out, err = run_cmd(["curl", "-sfL", "--max-time", "15",
                                   "-A", "webmanager/1.0", url], timeout=25)
            parts = out.strip().split(None, 1)
            channels[ch] = {"version": parts[0] if c == 0 and parts else "",
                            "date": parts[1] if c == 0 and len(parts) > 1 else "",
                            "error": "" if c == 0 else (err.strip() or "check failed")}
        result = {"current": cur, "channels": channels}
        _vc_cache["t"] = now
        _vc_cache["data"] = result
        return result

    def overlay(self, name):
        if not re.match(r"^[A-Za-z0-9_\-]+$", name):
            return self._error("bad name")
        # webmanager's own set (photos from the ES theme) wins over the
        # bundled Batocera drawings
        for d in (os.path.join(BASE, "overlays"),
                  "/usr/share/batocera/controller-overlays"):
            p = os.path.join(d, name + ".png")
            if os.path.isfile(p):
                return self._file(p, inline=True)
        return self._error("not found", 404)

    def overlay_list(self):
        result = set()
        for d in (os.path.join(BASE, "overlays"),
                  "/usr/share/batocera/controller-overlays"):
            if os.path.isdir(d):
                for f in os.listdir(d):
                    if f.endswith(".png"):
                        result.add(os.path.splitext(f)[0])
        return sorted(result)

    def system_art(self, name):
        """Console artwork from the default ES theme, shown next to the
        system name in the detail header.
        取默认 ES 主题中的主机图片，显示在系统详情页头部。"""
        if not re.match(r"^[A-Za-z0-9_\-]+$", name):
            return self._error("bad name")
        d = "/usr/share/emulationstation/themes/es-theme-carbon/art/background"
        for ext, ctype in ((".jpg", "image/jpeg"), (".png", "image/png")):
            p = os.path.join(d, name + ext)
            if os.path.isfile(p):
                return self._file(p, ctype=ctype, inline=True)
        return self._error("not found", 404)

    def disk_usage(self):
        # df for /userdata
        try:
            stat = os.statvfs(USERDATA)
            total = stat.f_frsize * stat.f_blocks
            free = stat.f_frsize * stat.f_bavail  # free for unprivileged
            used = total - free
        except Exception:
            total = used = free = 0
        # Per top-level /userdata subdirs — one du process for all of them (B10)
        breakdown = []
        names = [n for n in sorted(os.listdir(USERDATA))
                 if os.path.isdir(os.path.join(USERDATA, n))]
        sizes = _du_many([os.path.join(USERDATA, n) for n in names])
        for name in names:
            size = sizes.get(os.path.join(USERDATA, name), 0)
            if size > 0:
                breakdown.append({"name": name, "size": size})
        breakdown.sort(key=lambda x: -x["size"])
        return {
            "total": total, "used": used, "free": free,
            "breakdown": breakdown,
            "torrent_upgrade": self._torrent_list(),
        }

    def _torrent_list(self):
        if not os.path.isdir(TORRENT_OUTPUT):
            return []
        items = []
        for f in os.listdir(TORRENT_OUTPUT):
            p = os.path.join(TORRENT_OUTPUT, f)
            if os.path.isfile(p) and "boot" in f and ".tar" in f:
                try:
                    st = os.stat(p)
                    items.append({"name": f, "size": st.st_size, "mtime": int(st.st_mtime)})
                except OSError:
                    pass
        items.sort(key=lambda x: -x["mtime"])  # newest first
        return items

    def clean_upgrade(self):
        items = self._torrent_list()
        if not items:
            return self._json({"ok": True, "removed": 0})
        # If transmission-daemon is running, refuse — user should stop it first.
        if subprocess.run(["pgrep", "-f", "transmission-daemon"], capture_output=True).returncode == 0:
            return self._error("transmission-daemon is running; stop it before cleaning", 409)
        # Keep the most recent one (likely the currently in-use or newest downloaded).
        # Remove everything older.
        removed = []
        kept = items[0]["name"]
        for it in items[1:]:
            p = os.path.join(TORRENT_OUTPUT, it["name"])
            try:
                os.remove(p)
                removed.append(it["name"])
            except OSError as e:
                return self._error(str(e))
        return self._json({"ok": True, "removed": removed, "kept": kept})

    # ---------------- system upgrade / 系统升级 ----------------

    def _upgrade_running(self):
        t = _UPG.get("thread")
        if t is not None and t.is_alive():
            return True
        # anchor the match: a bare "batocera-upgrade" also matches our own curl
        # children when their args contain that string (old UA), causing the UI
        # to show a phantom "upgrading" state. Trailing space/EOL required.
        # 锚定匹配：裸串会命中参数里含该串的本进程 curl 子进程（旧 UA），
        # 导致前端误显示"升级中"。要求后跟空格或行尾。
        return subprocess.run(["pgrep", "-f", "batocera-upgrade( |$)"],
                              capture_output=True).returncode == 0

    def _upgrade_begin(self, state):
        _UPG["cancel"].clear()
        try:
            with open(os.path.join(TMP_DIR, "upgrade.lock"), "w") as f:
                f.write(str(os.getpid()))
        except OSError:
            pass
        _write_json(os.path.join(TMP_DIR, "upgrade.state"), state)

    def _upgrade_stage(self, stage):
        p = os.path.join(TMP_DIR, "upgrade.state")
        try:
            with open(p) as f:
                st = json.load(f)
        except (OSError, ValueError):
            st = {}
        st["stage"] = stage
        _write_json(p, st)

    def _upgrade_finish(self, channel, err, extra):
        tail = _tail(os.path.join(TMP_DIR, "upgrade.log"), 10)
        last = tail.splitlines()[-1] if tail else ""
        phase = "cancelled" if err == "cancelled" else ("failed" if err else "done")
        state = {"phase": phase, "channel": channel,
                 "error": err or last if phase == "failed" else err or "",
                 "tail": tail}
        state.update(extra or {})
        _write_json(os.path.join(TMP_DIR, "upgrade.state"), state)
        try:
            os.remove(os.path.join(TMP_DIR, "upgrade.lock"))
        except OSError:
            pass

    def _boost_env(self, need_mb, boot_avail_mb):
        """batocera-upgrade refuses to start unless df reports room for the whole
        image on /boot, but the 8 GB boot partition can never hold a second copy —
        the script extracts in place, replacing each file, so the real requirement
        is only the largest single member (~3.2 GB). When the pre-check would trip
        we run it with a df wrapper that pads the free space for /boot only.
        No files on /boot are touched.
        预检不满足时用 df 包装脚本仅对 /boot 虚增可用空间；不改动 /boot 上任何文件。"""
        env = dict(os.environ)
        boost = 0
        if need_mb > boot_avail_mb:
            if boot_avail_mb < 2800:
                return env, 0, self._error(
                    "not enough free space on /boot (%d MB) to update safely"
                    % boot_avail_mb)
            boost = need_mb + 40 - boot_avail_mb
        if boost:
            shim = os.path.join(TMP_DIR, "shim")
            os.makedirs(shim, exist_ok=True)
            with open(os.path.join(shim, "df"), "w") as f:
                f.write("#!/bin/sh\n"
                        "/bin/df \"$@\" | awk -v d=%d "
                        "'{ if ($NF==\"/boot\" || $NF==\"/boot/\") $4=$4+d; print }'\n"
                        % boost)
            os.chmod(os.path.join(shim, "df"), 0o755)
            env["PATH"] = shim + ":" + env.get("PATH", "")
        return env, boost, None

    def _start_install_thread(self, channel, dest, env, extra, verify):
        """Shared tail of both upgrade paths: optional integrity check, then
        batocera-upgrade --media-upgrade from the local file.
        两种升级路径共用的收尾：可选完整性校验后，从本地文件调用 batocera-upgrade 安装。"""
        log_path = os.path.join(TMP_DIR, "upgrade.log")

        def install():
            err = ""
            try:
                with open(log_path, "a") as log:
                    if verify:
                        self._upgrade_stage("verify")
                        log.write("verifying archive integrity (xz -t) ...\n")
                        log.flush()
                        rc = subprocess.run(["xz", "-t", dest],
                                            stdout=log, stderr=subprocess.STDOUT).returncode
                        if rc:
                            err = "archive failed integrity check"
                    if not err and _UPG["cancel"].is_set():
                        err = "cancelled"
                    if not err:
                        self._upgrade_stage("install")
                        log.write("installing to /boot ...\n")
                        log.flush()
                        rc = subprocess.Popen(
                            ["batocera-upgrade", "--media-upgrade", "--file", dest],
                            stdout=log, stderr=subprocess.STDOUT, env=env).wait()
                        if rc:
                            err = "install failed (rc=%d)" % rc
            except Exception as e:
                err = str(e)
            self._upgrade_finish(channel, err, extra)

        t = threading.Thread(target=install, daemon=True)
        _UPG["thread"] = t
        t.start()

    def _boot_avail_mb(self):
        try:
            st = os.statvfs(BOOT_DIR)
            return int(st.f_bavail * st.f_frsize / 1048576)
        except OSError:
            return 0

    def upgrade_preflight(self, channel):
        """Work out the image size and compare it with the free space batocera-upgrade demands.
        计算镜像大小，并与 batocera-upgrade 要求的 /boot 可用空间对比。"""
        board = "x86-64-v3"
        try:
            with open("/boot/boot/batocera.board") as f:
                board = f.read().strip() or board
        except OSError:
            pass
        base = (conf_map().get("updates.url") or "https://updates.batocera.org").rstrip("/")
        url = "%s/%s/%s/last/boot.tar.xz" % (base, board, channel)
        size = 0
        code, out, _ = run_cmd(["curl", "-sfIL", "-A", "webmanager/1.0", url], timeout=40)
        if code == 0:
            for line in out.splitlines():
                if line.lower().startswith("content-length:"):
                    try:
                        size = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
        need = int(size / 1048576 * 105 / 100) + 10 if size else 0
        return {"url": url, "size": size, "need_mb": need,
                "boot_avail_mb": self._boot_avail_mb()}

    def upgrade_status(self):
        """Report the current upgrade phase/stage and download progress. / 汇报当前升级阶段与下载进度。"""
        state = {}
        try:
            with open(os.path.join(TMP_DIR, "upgrade.state")) as f:
                state = json.load(f)
        except (OSError, ValueError):
            state = {}
        running = self._upgrade_running()
        phase = state.get("phase", "")
        if phase == "running" and not running:
            # interrupted without finishing (e.g. service restarted)
            phase = "failed"
            state.setdefault("error", "interrupted")
        dl_bytes = 0
        parts_dir = os.path.join(UPGRADE_DIR, "boot.tar.xz.parts")
        if os.path.isdir(parts_dir):
            for fn in os.listdir(parts_dir):
                try:
                    dl_bytes += os.path.getsize(os.path.join(parts_dir, fn))
                except OSError:
                    pass
        elif state.get("stage") == "download":
            try:
                dl_bytes = os.path.getsize(os.path.join(UPGRADE_DIR, "boot.tar.xz"))
            except OSError:
                pass
        msg = ""
        try:
            with open(os.path.join(UPGRADE_DIR, "upgrade_message.txt")) as f:
                msg = f.read().strip()
        except OSError:
            pass
        tail = ""
        if running or phase == "failed":
            tail = _tail(os.path.join(TMP_DIR, "upgrade.log"), 12) or state.get("tail", "")
        return self._json({
            "running": running,
            "phase": "running" if running else phase,
            "stage": state.get("stage", "") if running else "",
            "channel": state.get("channel", ""),
            "error": state.get("error", ""),
            "tail": tail,
            "message": msg,
            "dl_bytes": dl_bytes,
            "dl_total": state.get("dl_total", 0),
            "need_mb": state.get("need_mb", 0),
            "boot_avail_mb": state.get("boot_avail_mb", 0),
            "boot_free_mb": self._boot_avail_mb(),
        })

    def power(self, action):
        """Delayed reboot/shutdown/ES-restart. / 延迟执行重启/关机/重启 ES。"""
        if action == "es_restart":
            cmd = ["/etc/init.d/S31emulationstation", "restart"]
        elif action == "reboot":
            cmd = ["reboot"]
        elif action == "shutdown":
            cmd = ["poweroff"]
        else:
            return self._error("bad action")
        def later():
            time.sleep(1)
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        threading.Thread(target=later, daemon=True).start()
        return self._json({"ok": True})

    def video_status(self):
        _, outputs, _ = run_cmd(["batocera-resolution", "listOutputs"], timeout=20)
        _, cur_out, _ = run_cmd(["batocera-resolution", "currentOutput"], timeout=20)
        _, cur_mode, _ = run_cmd(["batocera-resolution", "currentMode"], timeout=20)
        _, modes_raw, _ = run_cmd(["batocera-resolution", "listModes"], timeout=20)
        modes = []
        for line in modes_raw.splitlines():
            line = line.strip()
            if not line:
                continue
            k, _, desc = line.partition(":")
            modes.append({"mode": k.strip(), "desc": desc.strip() or k.strip()})
        return {
            "outputs": [x.strip() for x in outputs.splitlines() if x.strip()],
            "currentOutput": cur_out.strip(),
            "modes": modes,
            "currentMode": cur_mode.strip(),
        }

    def audio_status(self):
        _, devs_raw, _ = run_cmd(["batocera-audio", "list"], timeout=20)
        devices = []
        for line in devs_raw.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            devices.append({"id": parts[0], "name": parts[1] if len(parts) > 1 else parts[0]})
        _, cur, _ = run_cmd(["batocera-audio", "get"], timeout=20)
        cur_id = cur.strip().split("\t")[0]
        _, vol, _ = run_cmd(["batocera-audio", "getSystemVolume"], timeout=20)
        _, mute, _ = run_cmd(["batocera-audio", "getSystemMute"], timeout=20)
        try:
            vol_i = int(vol.strip())
        except ValueError:
            vol_i = 0
        return {"devices": devices, "current": cur_id, "volume": vol_i,
                "muted": mute.strip().lower() in ("1", "true", "yes", "muted", "on")}

    def games_list(self, sysname, subdir=""):
        """List roms (with gamelist metadata) and subdirs of one system. / 列出一个系统的 ROM（含 gamelist 元数据）与子目录。"""
        sysinfo = find_system(sysname)
        if not sysinfo:
            return {"dirs": [], "games": []}
        d = sysinfo["path"]
        try:
            cur = safe_join(d, subdir)
        except ValueError:
            return {"dirs": [], "games": []}
        if not os.path.isdir(cur):
            return {"dirs": [], "games": []}
        exts = set(sysinfo["extensions"])
        meta = gamelist_meta(d)
        games, dirs = [], []
        try:
            for e in os.scandir(cur):
                if e.name.startswith("."):
                    continue
                if e.name.endswith((".part", ".parts")):
                    continue  # partial-download residue, never a playable entry
                if e.is_dir():
                    if e.name.lower() in ("images", "videos", "manuals"):
                        continue
                    dirs.append(e.name)
                    continue
                if os.path.splitext(e.name)[1].lower() not in exts:
                    continue
                rel = os.path.relpath(e.path, d).replace(os.sep, "/")
                g = {"file": e.name, "rel": rel, "size": e.stat().st_size}
                m = meta.get(rel.lower())
                if m:
                    g.update(m)
                    g["hasMeta"] = True
                else:
                    g["hasMeta"] = False
                games.append(g)
        except OSError:
            pass
        dirs.sort(key=str.lower)
        games.sort(key=lambda x: (x.get("name") or x["file"]).lower())
        return {"dirs": dirs, "games": games}

    def media(self, q):
        sysinfo = find_system(q.get("system", ""))
        if not sysinfo:
            return self._error("unknown system", 404)
        rel = q.get("path", "")
        if rel.startswith("./"):
            rel = rel[2:]
        fp = safe_join(sysinfo["path"], rel)
        ext = os.path.splitext(fp)[1].lower()
        if ext not in MIME or not MIME[ext].startswith(("image/", "video/")):
            return self._error("bad media type", 403)
        return self._file(fp)

    def files_list(self, path):
        """Directory listing restricted to /userdata. / 仅限 /userdata 范围内的目录列表。"""
        try:
            fp = safe_join("/", path)
        except ValueError:
            return self._error("invalid path")
        ud = os.path.realpath(USERDATA)
        if fp != ud and not fp.startswith(ud + os.sep):
            fp = ud
        if not os.path.isdir(fp):
            return self._error("directory not found", 404)
        entries = []
        try:
            for e in os.scandir(fp):
                try:
                    st = e.stat()
                    entries.append({
                        "name": e.name,
                        "isDir": e.is_dir(),
                        "size": 0 if e.is_dir() else st.st_size,
                        "mtime": int(st.st_mtime),
                    })
                except OSError:
                    pass
        except OSError as ex:
            return self._error(str(ex))
        entries.sort(key=lambda x: (not x["isDir"], x["name"].lower()))
        return self._json({"path": fp, "entries": entries})


class QuietHTTPServer(ThreadingHTTPServer):
    """Silence traceback spam from clients vanishing mid-request (A2): every
    browser refresh or aborted poll used to dump a full ConnectionResetError
    stack into server.log, drowning real errors. Genuine bugs still print.
    静默客户端中途断连的堆栈刷屏（A2）：此前每次刷新/中断轮询都会把完整的
    ConnectionResetError 堆栈写进 server.log，淹没真实错误；真正的 bug 仍会记录。"""

    def handle_error(self, request, client_address):
        if isinstance(sys.exc_info()[1], OSError):
            return  # connection reset / broken pipe / timeout — the client is gone
        traceback.print_exc()


def main():
    """Entry point: start the threaded HTTP server. / 入口：启动多线程 HTTP 服务器。"""
    server = QuietHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    print("Batocera Web Manager listening on port %d" % PORT, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
