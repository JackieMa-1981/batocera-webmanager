#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HTTP(S) parallel-range download engine with the same job interface as
btlib.Job, used for archive.org-hosted disc images (CHD sets have no torrent).

Job(base, files, dest_dir, log)
  base      = URL prefix, e.g. "https://archive.org/download/"
  files = [(relpath, filename, size), ...]  one entry per queued game
  dest_dir = directory the files are downloaded into (roms/<system>)

snapshot() keys mirror btlib.Job: status, bytes_total, bytes_done, speed,
peers, error, files{filename: {got, size, done}}.

中文说明：
HTTP(S) 分段并行下载引擎，提供与 btlib.Job 相同的任务接口，用于托管在
archive.org 上的光盘镜像（CHD 合集没有种子）。
base 为 URL 前缀；files 为 [(相对路径, 文件名, 大小), ...]；
dest_dir 为下载目标目录（roms/<system>）。
snapshot() 返回键与 btlib.Job 一致：状态、总字节、已完成、速度、peers、
错误信息及每个文件的进度。
"""

import os
import shutil
import subprocess
import threading
import time
import urllib.parse
import zipfile

SEGMENTS = 8
MIN_SEGMENT_FILE = 32 * 1024 * 1024  # small files: single connection 小文件单连接

_TAR_GNU = None


def _tar_needs_wildcards():
    """GNU tar treats member patterns literally unless --wildcards is passed;
    bsdtar patterns are always globs and rejects the flag.
    中文：检测 tar 实现；GNU tar 需加 --wildcards 才把成员模式当通配符，
    bsdtar 始终按通配符处理且拒绝该参数。"""
    global _TAR_GNU
    if _TAR_GNU is None:
        try:
            p = subprocess.run(["tar", "--version"], capture_output=True,
                               text=True, timeout=10)
            _TAR_GNU = "GNU tar" in (p.stdout or "") + (p.stderr or "")
        except Exception:
            _TAR_GNU = False
    return _TAR_GNU


def _extract(archive, dest_dir, spec):
    """Post-download unpack step for engine/port packages.
    spec: {"fmt": "zip"} extracts everything into dest_dir;
          {"fmt": "tar", "prefix": "userdata/roms/ports/"} extracts only
          members below prefix, stripped, into dest_dir;
          {"fmt": "7z"} extracts with the 7z CLI;
          {"fmt": "none"} keeps the downloaded file as-is.
    Optional keys:
          "dest":     unpack into this sub-directory of dest_dir;
          "prefix":   (zip) extract only members below this in-archive
                      directory, stripped;
          "only":     [member, ...] extract just these members (names after
                      prefix stripping);
          "collapse": wrapper directory created by unpacking whose contents
                      are moved up into the unpack dest;
          "rename":   {old_rel: new_rel} move extracted files (mkdir -p parents);
          "stub":     [name, ...] create empty launch stub files;
          "files":    {name: text} create launch stub files with content.
    rename/stub/files paths are relative to dest_dir. The archive is removed
    on success (except fmt "none"). Returns '' or an error message.
    中文：下载后的解包步骤。按 spec 处理 zip/tar/7z/none，支持子目录 dest、
    前缀裁剪 prefix、仅解指定成员 only、折叠包装目录 collapse、重命名
    rename、创建空启动文件 stub、写入文本文件 files；成功后删除压缩包，
    返回 '' 或错误信息。"""
    fmt = spec.get("fmt", "zip")
    dest = dest_dir
    if spec.get("dest"):
        dest = os.path.join(dest_dir, spec["dest"])
    prefix = spec.get("prefix", "")
    only = spec.get("only")
    try:
        if fmt == "none":
            pass
        elif fmt == "zip":
            os.makedirs(dest, exist_ok=True)
            with zipfile.ZipFile(archive) as z:
                if not prefix and not only:
                    z.extractall(dest)
                else:
                    real_dest = os.path.realpath(dest)
                    for name in z.namelist():
                        rel = name
                        if prefix:
                            if not rel.startswith(prefix):
                                continue
                            rel = rel[len(prefix):]
                        if not rel or rel.endswith("/"):
                            continue
                        if only and rel not in only:
                            continue
                        target = os.path.join(dest, rel)
                        # zip-slip guard (A4): a crafted member like ../../x
                        # must never escape dest — we run as root.
                        # zip 路径穿越防护（A4）：../../x 之类的恶意成员不得写出
                        # dest 之外，本进程以 root 运行。
                        if not os.path.realpath(target).startswith(real_dest + os.sep):
                            continue
                        parent = os.path.dirname(target)
                        if parent:
                            os.makedirs(parent, exist_ok=True)
                        with z.open(name) as src, open(target, "wb") as out:
                            shutil.copyfileobj(src, out, 1048576)
        elif fmt == "7z":
            os.makedirs(dest, exist_ok=True)
            # 7zr over 7z: the latter needs 7z.so from its own directory
            p = subprocess.run(["7zr", "x", "-y", "-o" + dest, archive],
                               capture_output=True, text=True, timeout=3600)
            if p.returncode != 0:
                return "7z: " + (p.stdout or "")[-200:].strip()
        else:
            os.makedirs(dest, exist_ok=True)
            cmd = ["tar", "-xf", archive, "-C", dest]
            if prefix:
                depth = prefix.strip("/").count("/") + 1
                if _tar_needs_wildcards():
                    cmd.append("--wildcards")
                cmd += ["--strip-components", str(depth),
                        prefix.strip("/") + "/*"]
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            if p.returncode != 0:
                return "tar: " + (p.stderr or p.stdout).strip()[:200]
        wrap = spec.get("collapse")
        if wrap:
            wdir = os.path.join(dest, wrap)
            if os.path.isdir(wdir):
                for item in os.listdir(wdir):
                    os.replace(os.path.join(wdir, item),
                               os.path.join(dest, item))
                os.rmdir(wdir)
        for old, new in (spec.get("rename") or {}).items():
            src = os.path.join(dest_dir, old)
            dst = os.path.join(dest_dir, new)
            parent = os.path.dirname(dst)
            if parent:
                os.makedirs(parent, exist_ok=True)
            os.replace(src, dst)
        for stub in (spec.get("stub") or []):
            fp = os.path.join(dest_dir, stub)
            parent = os.path.dirname(fp)
            if parent:
                os.makedirs(parent, exist_ok=True)
            open(fp, "a").close()
        for name, text in (spec.get("files") or {}).items():
            fp = os.path.join(dest_dir, name)
            parent = os.path.dirname(fp)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(fp, "w") as f:
                f.write(text)
    except Exception as e:
        return "%s: %s" % (fmt, e)
    if fmt != "none":
        try:
            os.remove(archive)
        except OSError:
            pass
    return ""


def _quote_url(url):
    """Percent-encode path components (spaces, brackets) keeping / : ? = & intact.
    中文：对 URL 路径部分做百分号编码（空格、方括号等），保留 / : ? = &。"""
    p = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit(
        (p.scheme, p.netloc, urllib.parse.quote(p.path), p.query, p.fragment))


def _curl_range(url, start, end, out_path, cancel, proxy="", cookie=""):
    """Append bytes [start,end] of url to out_path. Returns bytes written.
    中文：用 curl 下载 url 的 [start,end] 字节区间并追加到 out_path，
    支持断点续传、代理与 Cookie；返回已写入字节数。"""
    have = os.path.getsize(out_path) if os.path.exists(out_path) else 0
    want = end - start + 1
    if have >= want:
        return have
    cmd = ["curl", "-sfL", "--connect-timeout", "20",
           "--speed-limit", "10240", "--speed-time", "30",
           "-A", "batocera-webmanager",
           "-r", "%d-%d" % (start + have, end)]
    if proxy:
        cmd += ["-x", proxy]
    if cookie:
        cmd += ["-b", cookie]
    cmd.append(url)
    try:
        with open(out_path, "ab") as out:
            subprocess.run(cmd, stdout=out, stderr=subprocess.DEVNULL,
                           timeout=86400)
    except Exception:
        pass
    return os.path.getsize(out_path) if os.path.exists(out_path) else 0


class Job:
    """Parallel segmented download job with the btlib.Job interface.
    并行分段下载任务类，接口与 btlib.Job 保持一致。"""
    def __init__(self, base, files, dest_dir, log=None, proxy="", cookie="",
                 extract=None):
        self.base = base
        self.proxy = proxy
        self.cookie = cookie
        self.extract = extract or {}  # filename -> spec, see _extract
        self.queue = list(files)
        self.dest_dir = dest_dir
        self.log = log or (lambda msg: None)
        self.status = "idle"
        self.error = ""
        self.manual_peers = []
        self._cancel = threading.Event()
        self._lock = threading.Lock()
        self._prog = {fn: {"got": 0, "size": sz, "done": False}
                      for _, fn, sz in self.queue}
        self.bytes_total = sum(sz for _, _, sz in self.queue)
        self._done_bytes = 0
        self._speed = 0.0
        self.thread = None

    # -- interface compatible with btlib.Job -- 与 btlib.Job 兼容的对外接口
    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self._cancel.set()

    def add_peer(self, addr):
        pass

    def snapshot(self):
        with self._lock:
            files = {k: dict(v) for k, v in self._prog.items()}
            done = self._done_bytes
            speed = self._speed
        return {"status": self.status, "error": self.error,
                "bytes_total": self.bytes_total, "bytes_done": done,
                "speed": speed, "peers": 0, "files": files}

    # -- internals -- 内部实现
    def _account(self):
        with self._lock:
            self._done_bytes = sum(v["got"] for v in self._prog.values())

    def _speed_loop(self):
        last = 0
        while not self._cancel.is_set() and self.status == "running":
            time.sleep(2)
            with self._lock:
                cur = sum(v["got"] for v in self._prog.values())
            self._speed = max(0.0, (cur - last) / 2.0)
            last = cur

    def _run(self):
        """Worker thread main loop: download each queued file, then extract.
        工作线程主循环：依次下载队列中的文件，必要时执行解包。"""
        os.makedirs(self.dest_dir, exist_ok=True)
        self.status = "running"
        threading.Thread(target=self._speed_loop, daemon=True).start()
        for relpath, fname, size in self.queue:
            if self._cancel.is_set():
                break
            dest = os.path.join(self.dest_dir, fname)
            parent = os.path.dirname(dest)
            if parent:
                os.makedirs(parent, exist_ok=True)
            spec = self.extract.get(fname)
            if os.path.exists(dest) and os.path.getsize(dest) == size:
                if spec:  # downloaded earlier but never unpacked
                    err = _extract(dest, self.dest_dir, spec)
                    if err:
                        self.error = err
                        self.status = "error"
                        self.log("error: " + err)
                        return
                with self._lock:
                    self._prog[fname] = {"got": size, "size": size, "done": True}
                self._account()
                continue
            self.log("download %s (%d MB)" % (fname, size // 1048576))
            err = self._fetch(_quote_url(self.base + relpath), dest, fname, size)
            if err:
                self.error = err
                self.status = "error"
                self.log("error: " + err)
                return
            if spec:
                self.log("extract %s" % fname)
                err = _extract(dest, self.dest_dir, spec)
                if err:
                    self.error = err
                    self.status = "error"
                    self.log("error: " + err)
                    return
            with self._lock:
                self._prog[fname]["done"] = True
            self._account()
            self.log("done %s" % fname)
        if self._cancel.is_set():
            self.status = "cancelled"
        elif self.status == "running":
            self.status = "done"

    def _fetch(self, url, dest, fname, size):
        """Download one file: single connection when small, otherwise SEGMENTS
        parallel range parts merged afterwards. Returns '' or error message.
        下载单个文件：小文件走单连接；大文件按 SEGMENTS 分段并行下载后合并，
        返回 '' 或错误信息。"""
        tmp = dest + ".part"
        if size < MIN_SEGMENT_FILE:
            got = 0
            for _ in range(10):
                if self._cancel.is_set():
                    return "cancelled"
                got = _curl_range(url, 0, size - 1, tmp, self._cancel, self.proxy, self.cookie)
                self._prog[fname]["got"] = got
                if got == size:
                    break
                time.sleep(2)
            if got != size:
                return "incomplete (%d/%d)" % (got, size)
            os.replace(tmp, dest)
            return ""
        parts_dir = dest + ".parts"
        os.makedirs(parts_dir, exist_ok=True)
        chunk = (size + SEGMENTS - 1) // SEGMENTS
        tasks = []
        s = 0
        while s < size:
            e = min(size, s + chunk) - 1
            parts_dir_i = os.path.join(parts_dir, "part%02d" % len(tasks))
            tasks.append((parts_dir_i, s, e))
            s = e + 1

        # Retry loop per segment (up to 30 tries) 每个分段的重试循环（最多 30 次）
        def worker(part, s0, e0):
            for _ in range(30):
                if self._cancel.is_set():
                    return
                got = _curl_range(url, s0, e0, part, self._cancel, self.proxy, self.cookie)
                with self._lock:
                    self._prog[fname]["got"] = sum(
                        min(os.path.getsize(p), e - s + 1) if os.path.exists(p) else 0
                        for p, s, e in tasks)
                if got >= e0 - s0 + 1:
                    return
                time.sleep(3)

        threads = [threading.Thread(target=worker, args=t, daemon=True)
                   for t in tasks]
        for i, t in enumerate(threads):
            t.start()
            time.sleep(0.3)
        for t in threads:
            t.join()
        if self._cancel.is_set():
            return "cancelled"
        for part, s0, e0 in tasks:
            if not os.path.exists(part) or os.path.getsize(part) < e0 - s0 + 1:
                return "segment incomplete (%d/%d)" % (
                    os.path.getsize(part) if os.path.exists(part) else 0, e0 - s0 + 1)
        with open(tmp, "wb") as out:
            for part, _, _ in tasks:
                with open(part, "rb") as f:
                    shutil.copyfileobj(f, out, 1048576)
        shutil.rmtree(parts_dir, ignore_errors=True)
        if os.path.getsize(tmp) != size:
            return "size mismatch"
        os.replace(tmp, dest)
        return ""
