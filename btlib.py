#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Minimal pure-Python BitTorrent downloader: DHT peer discovery (BEP5),
tracker announce (UDP BEP15 + HTTP), selective file download, SHA1 piece
verification.  No external dependencies.

极简纯 Python BitTorrent 下载器：DHT 节点发现（BEP5）、Tracker 宣布
（UDP BEP15 + HTTP）、选择性文件下载、SHA1 分块校验。无外部依赖。"""

import base64
import hashlib
import json
import os
import random
import socket
import ssl
import struct
import threading
import time
import urllib.parse
import urllib.request

BLOCK = 16384
DHT_BOOT = [("dht.transmissionbt.com", 6881), ("router.bittorrent.com", 6881),
            ("router.utorrent.com", 6881), ("dht.libtorrent.org", 25401),
            ("dht.bt.am", 6881), ("bucket.cool", 6881)]
# Well-known public trackers queried in addition to the torrent's own
# announce list.  Every entry here was probe-verified reachable from a
# real device (2026-11): dead/DNS-failing ones were pruned.  The torrent's
# own trackers (archive.org) are tried too when reachable.
DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker-udp.gbitt.info:80/announce",
    "udp://retracker01-msk-virt.corbina.net:80/announce",
]


# ---------------- bencode 编解码 ----------------
# bencode 解码：将字节串解析为 Python 对象（int/bytes/list/dict）。
def bdec(data):
    def dec(i):
        c = data[i:i + 1]
        if c == b'i':
            j = data.index(b'e', i)
            return int(data[i + 1:j]), j + 1
        if c == b'l':
            i += 1
            out = []
            while data[i:i + 1] != b'e':
                v, i = dec(i)
                out.append(v)
            return out, i + 1
        if c == b'd':
            i += 1
            out = {}
            while data[i:i + 1] != b'e':
                k, i = dec(i)
                v, i = dec(i)
                out[k] = v
            return out, i + 1
        if c == b'e':
            raise ValueError("unexpected e")
        j = data.index(b':', i)
        n = int(data[i:j])
        return data[j + 1:j + 1 + n], j + 1 + n
    return dec(0)[0]


# bencode 编码：将 Python 对象序列化为 bencode 字节串。
def benc(o):
    if isinstance(o, bytes):
        return b'%d:%s' % (len(o), o)
    if isinstance(o, int):
        return b'i%de' % o
    if isinstance(o, list):
        return b'l' + b''.join(benc(x) for x in o) + b'e'
    if isinstance(o, dict):
        return b'd' + b''.join(benc(k) + benc(o[k]) for k in sorted(o)) + b'e'
    raise TypeError(type(o))


# Torrent 元信息：解析 .torrent 原始字节，得到名称、分块哈希、
# 文件列表、tracker 列表与 BEP19 webseed 地址。
class Torrent(object):
    def __init__(self, raw):
        top = bdec(raw)
        info = top[b'info']
        self.name = info[b'name'].decode('utf-8', 'replace')
        self.piece_length = info[b'piece length']
        self.pieces_raw = info[b'pieces']
        self.infohash = hashlib.sha1(benc(info)).digest()
        self.files = []
        off = 0
        for f in info.get(b'files', []):
            p = b'/'.join(f[b'path']).decode('utf-8', 'replace')
            self.files.append({"path": p, "size": f[b'length'], "offset": off,
                               "index": len(self.files)})
            off += f[b'length']
        if not self.files and b'length' in info:
            self.files.append({"path": self.name, "size": info[b'length'],
                               "offset": 0, "index": 0})
            off = info[b'length']
        self.total = off
        self.npieces = len(self.pieces_raw) // 20
        # tracker URLs declared by the torrent itself（种子自带的 tracker 地址）
        self.trackers = []
        ann = top.get(b'announce')
        if isinstance(ann, bytes):
            self.trackers.append(ann.decode('utf-8', 'replace'))
        for tier in top.get(b'announce-list', []):
            if isinstance(tier, list):
                for u in tier:
                    if isinstance(u, bytes):
                        self.trackers.append(u.decode('utf-8', 'replace'))
        seen = set()
        self.trackers = [u for u in self.trackers
                         if not (u in seen or seen.add(u))]
        # BEP19 web seeds (archive.org torrents always carry these: the IA
        # storage nodes serve every file over HTTP, which makes downloads
        # work even when the P2P swarm is empty)
        # BEP19 webseed：IA 存储节点通过 HTTP 提供文件，P2P 网络为空时也能下载。
        self.url_list = []
        urls = top.get(b'url-list', [])
        if isinstance(urls, bytes):
            urls = [urls]
        for u in urls:
            if isinstance(u, bytes):
                self.url_list.append(u.decode('utf-8', 'replace'))

    def piece_hash(self, idx):
        return self.pieces_raw[idx * 20:(idx + 1) * 20]

    def piece_size(self, idx):
        if idx == self.npieces - 1:
            return self.total - idx * self.piece_length
        return self.piece_length


# ---------------- DHT peer discovery (DHT 节点发现) ----------------
class DHTFinder(threading.Thread):
    """Continuously crawl the DHT for peers of one infohash.
    持续在 DHT 网络中爬取某个 infohash 的 peer 节点。"""

    def __init__(self, infohash, peers, lock, stop_event, log=None,
                 announce_port=0):
        threading.Thread.__init__(self, daemon=True)
        self.ih = infohash
        self.peers = peers
        self.lock = lock
        self.stop_event = stop_event
        self.log = log or (lambda m: None)
        self.nodes = {}      # addr -> id
        self.tokens = {}     # addr -> (token, last_announce_ts)
        self.announce_port = announce_port   # TCP listen port, 0 = disabled
        self.mid = bytes(random.getrandbits(8) for _ in range(20))
        self.tid = 0

    def _dist(self, a, b):
        return int.from_bytes(bytes(x ^ y for x, y in zip(a, b)), 'big')

    # 主循环：多轮 get_peers/find_node 查询，处理回复并周期性 announce。
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", 0))
        sock.settimeout(1.0)
        try:
            for rnd in range(40):
                if self.stop_event.is_set():
                    break
                cand = sorted(self.nodes.items(), key=lambda kv: self._dist(kv[1], self.ih))[:200]
                if not cand:
                    cand = [((h, p), self.mid) for h, p in DHT_BOOT]
                for addr, _ in cand:
                    self._query(sock, addr, b'get_peers', {b'info_hash': self.ih})
                if rnd % 4 == 0:
                    for h, p in DHT_BOOT:
                        self._query(sock, (h, p), b'find_node', {b'target': self.mid})
                end = time.time() + 6
                while time.time() < end and not self.stop_event.is_set():
                    try:
                        data, addr = sock.recvfrom(8192)
                    except socket.timeout:
                        continue
                    except OSError:
                        break
                    self._handle_dgram(data, addr)
                with self.lock:
                    n = len(self.peers)
                self.log("dht: peers=%d nodes=%d" % (n, len(self.nodes)))
                self.nodes = dict(list(self.nodes.items())[:1500])
                self._announce_round(sock)
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def _query(self, sock, addr, q, extra):
        self.tid += 1
        args = {b'id': self.mid}
        args.update(extra)
        # BEP5 recommends a 2-byte opaque tid; an ever-growing decimal string
        # (3+ bytes above 99) can be dropped by strict implementations (B6).
        # BEP5 建议 2 字节不透明事务 ID；递增十进制串超过 99 后超 2 字节，
        # 可能被严格实现丢弃（B6）。响应不按 tid 匹配，随机值即可。
        t = bytes((self.tid & 0xFF, random.getrandbits(8)))
        msg = benc({b't': t, b'y': b'q', b'q': q, b'a': args})
        try:
            if isinstance(addr[0], str):
                sock.sendto(msg, (addr[0], addr[1]))
            else:
                sock.sendto(msg, addr)
        except OSError:
            pass

    def _announce_round(self, sock):
        """BEP5 announce_peer: publish ourselves on nodes that answered our
        get_peers (they handed us a token), at most every 15 min per node.
        BEP5 announce_peer：向回复过 get_peers（发放了 token）的节点
        发布自己，每个节点最多每 15 分钟一次。"""
        if not self.announce_port:
            return
        now = time.time()
        for addr, (token, last) in list(self.tokens.items())[:200]:
            if now - last < 900:
                continue
            self.tokens[addr] = (token, now)
            self._query(sock, addr, b'announce_peer',
                        {b'info_hash': self.ih, b'port': self.announce_port,
                         b'token': token, b'implied_port': 0})

    # 处理收到的 DHT 数据报：提取 token、路由表节点与 peer 列表。
    def _handle_dgram(self, data, addr):
        try:
            r = bdec(data)
        except Exception:
            return
        if not isinstance(r, dict):
            return
        rr = r.get(b'r')
        if not isinstance(rr, dict):
            return
        token = rr.get(b'token')
        if isinstance(token, bytes):
            _, last = self.tokens.get(addr, (b'', 0))
            self.tokens[addr] = (token, last)
        nodes = rr.get(b'nodes')
        if isinstance(nodes, bytes):
            for i in range(0, len(nodes) - 25, 26):
                nid = nodes[i:i + 20]
                ip = socket.inet_ntoa(nodes[i + 20:i + 24])
                port = struct.unpack('>H', nodes[i + 24:i + 26])[0]
                if 0 < port < 65536 and ip != '127.0.0.1':
                    self.nodes.setdefault((ip, port), nid)
        vals = rr.get(b'values')
        if isinstance(vals, list):
            found = []
            for v in vals:
                if isinstance(v, bytes):
                    for i in range(0, len(v) - 5, 6):
                        ip = socket.inet_ntoa(v[i:i + 4])
                        port = struct.unpack('>H', v[i + 4:i + 6])[0]
                        if 0 < port < 65536 and ip != '127.0.0.1':
                            found.append((ip, port))
            if found:
                with self.lock:
                    self.peers.update(found)


# ---------------- tracker peer discovery (Tracker 节点发现) ----------------
class TrackerFinder(threading.Thread):
    """Announce to the torrent's trackers plus well-known public ones and
    harvest returned peers.  Complements DHT discovery (some peers only
    register with trackers).
    向种子自带及知名公共 tracker 宣布并收集返回的 peer，补充 DHT 发现。"""

    def __init__(self, infohash, trackers, left_fn, peers, lock, stop_event,
                 log=None, listen_port=6881, peer_id=None):
        threading.Thread.__init__(self, daemon=True)
        self.ih = infohash
        self.trackers = trackers
        self.left_fn = left_fn          # callable -> bytes still missing
        self.peers = peers
        self.lock = lock
        self.stop_event = stop_event
        self.log = log or (lambda m: None)
        self.listen_port = listen_port
        self.pid = peer_id or (b'-BTWEB01-' +
                               bytes(random.getrandbits(8) for _ in range(11)))
        self.key = random.getrandbits(32)
        self._completed_sent = False

    def run(self):
        started = True
        while not self.stop_event.is_set():
            left = self.left_fn()
            # BEP3: send event=completed once when the download finishes
            if left == 0 and not self._completed_sent and not started:
                event = "completed"
                self._completed_sent = True
            elif started:
                event = "started"
            else:
                event = ""
            for url in self.trackers:
                if self.stop_event.is_set():
                    return
                try:
                    if url.startswith("udp://"):
                        got = self._announce_udp(url, event, left)
                    elif url.startswith(("http://", "https://")):
                        got = self._announce_http(url, event, left)
                    else:
                        continue
                except Exception:
                    continue
                if got:
                    with self.lock:
                        self.peers.update(got)
                    self.log("tracker %s: +%d peers" % (url, len(got)))
            started = False
            # re-announce roughly every 3 minutes until stopped
            for _ in range(180):
                if self.stop_event.is_set():
                    return
                time.sleep(1)

    def _announce_udp(self, url, event, left):
        """BEP15 UDP tracker announce; returns [(ip, port), ...].
        BEP15 UDP tracker 宣布；返回 [(ip, port), ...]。"""
        netloc = urllib.parse.urlsplit(url).netloc
        host, _, port = netloc.partition(":")
        addr = (host, int(port) if port else 80)
        tid = random.getrandbits(32)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.settimeout(4)
            # connection handshake 连接握手
            sock.sendto(struct.pack('>QII', 0x41727101980, 0, tid), addr)
            data, _ = sock.recvfrom(2048)
            if len(data) < 16 or struct.unpack('>II', data[:8]) != (0, tid):
                return []
            ev = {"started": 2, "completed": 1, "stopped": 3}.get(event, 0)
            req = (data[8:16] + struct.pack('>II', 1, tid) + self.ih + self.pid +
                   struct.pack('>QQQ', 0, left, 0) +
                   struct.pack('>I', ev) +
                   struct.pack('>III', 0, self.key, 200) +
                   struct.pack('>H', self.listen_port))
            sock.sendto(req, addr)
            data, _ = sock.recvfrom(8192)
            if len(data) < 20 or struct.unpack('>I', data[4:8])[0] != tid:
                return []
            return self._compact_peers(data[20:])
        finally:
            sock.close()

    def _announce_http(self, url, event, left):
        """HTTP(S) tracker announce; returns [(ip, port), ...].
        HTTP(S) tracker 宣布；返回 [(ip, port), ...]。"""
        q = ("info_hash=%s&peer_id=%s&port=%d&uploaded=0&downloaded=0"
             "&left=%d&compact=1&numwant=200" %
             (urllib.parse.quote_from_bytes(self.ih),
              urllib.parse.quote_from_bytes(self.pid), self.listen_port, left))
        if event:
            q += "&event=" + event
        sep = "&" if "?" in url else "?"
        req = urllib.request.Request(url + sep + q,
                                     headers={"User-Agent": "btlib/1.0"})
        ctx = ssl._create_unverified_context()  # peer list needs no TLS trust
        with urllib.request.urlopen(req, timeout=8, context=ctx) as r:
            data = r.read(262144)
        top = bdec(data)
        peers = top.get(b"peers") if isinstance(top, dict) else None
        if isinstance(peers, bytes):
            return self._compact_peers(peers)
        out = []
        if isinstance(peers, list):
            for p in peers:
                try:
                    ip = p[b"ip"].decode("ascii", "replace")
                    port = int(p[b"port"])
                    if 0 < port < 65536 and not ip.startswith(("0.", "127.")):
                        out.append((ip, port))
                except Exception:
                    pass
        return out

    @staticmethod
    def _compact_peers(blob):
        out = []
        for i in range(0, len(blob) - 5, 6):
            ip = socket.inet_ntoa(blob[i:i + 4])
            port = struct.unpack('>H', blob[i + 4:i + 6])[0]
            if 0 < port < 65536 and not ip.startswith(('0.', '127.')):
                out.append((ip, port))
        return out


# ---------------- torrent job (下载任务) ----------------
class Job(object):
    """Downloads selected files of a torrent.  One instance per run.
    下载种子中选定的文件；每次运行对应一个实例。"""

    def __init__(self, torrent, dest_dir, selected, manual_peers=None, log=None,
                 allow_upload=True, proxy="", listener=None, seed=True):
        self.t = torrent
        self.dest_dir = dest_dir
        self.selected = list(selected)          # file indexes into torrent.files
        self.allow_upload = allow_upload
        self.proxy = proxy
        self.listener = listener
        self.seed = seed
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.peers = set()
        self.manual_peers = list(manual_peers or [])
        self.log = log or (lambda m: None)
        self.status = "idle"
        self.error = ""
        self.peer_conns = 0
        self.active_peers = 0
        self.live = set()
        self.bytes_done = 0
        self.bytes_up = 0
        self.speed = 0.0
        self.up_speed = 0.0
        self.peers_seen = 0
        self.thread = None
        self.peer_id = b'-BTWEB01-' + bytes(random.getrandbits(8)
                                            for _ in range(11))
        self._t0 = 0.0
        self._last_bytes = 0
        self._last_t = 0.0
        self._last_up = 0
        self._last_up_t = 0.0
        # BEP19 webseed state（webseed 状态）
        self.webseeds = list(torrent.url_list)
        self.web_dead = set()          # pieces webseeds failed repeatedly
        self._web_fails = {}
        self._finalized = False

        # piece bookkeeping（分块下载状态记录）
        self.need_pieces = []       # ordered piece indexes still needed
        self.done_pieces = set()
        self.claimed = {}           # piece -> worker id
        self.piece_total = 0
        self.files_map = {}         # file index -> descriptor
        self.handles = {}
        # upload bookkeeping: bitfield of completed pieces (whole torrent wide)
        # and the single file that fully contains each servable piece
        # 上传记录：已完成分块位图（针对整个种子）及每个可服务分块所属的文件。
        self._have_bits = bytearray((self.t.npieces + 7) // 8)
        self._serve_idx = {}

        # per-file piece ranges
        for idx in self.selected:
            f = self.t.files[idx]
            start = f["offset"]
            end = start + f["size"] - 1
            p0 = start // self.t.piece_length
            p1 = end // self.t.piece_length
            dest = os.path.join(dest_dir, os.path.basename(f["path"]))
            part = dest + ".part"
            self.files_map[idx] = {"dest": dest, "part": part, "p0": p0, "p1": p1,
                                   "offset": start, "size": f["size"], "got": 0}
            for p in range(p0, p1 + 1):
                if p not in self.need_pieces:
                    self.need_pieces.append(p)
            # a piece is uploadable only when one selected file covers it
            # entirely — bytes outside selected files are never on disk
            for p in range(p0, p1 + 1):
                if p in self._serve_idx:
                    continue
                if start <= p * self.t.piece_length and \
                        p * self.t.piece_length + self.t.piece_size(p) <= start + f["size"]:
                    self._serve_idx[p] = idx
        self.piece_total = len(self.need_pieces)
        self._need_set = set(self.need_pieces)

    # ---- progress helpers ----
    # 返回当前进度快照（状态、速度、peer 数、各文件进度），供前端轮询。
    def snapshot(self):
        with self.lock:
            files = {}
            for idx, d in self.files_map.items():
                files[self.t.files[idx]["path"].split('/')[-1]] = {
                    "size": d["size"], "got": d["got"],
                    "done": d["got"] >= d["size"],
                }
            return {
                "status": self.status,
                "error": self.error,
                "pieces_done": len(self.done_pieces),
                "pieces_total": self.piece_total,
                "bytes_done": self.bytes_done,
                "bytes_total": sum(d["size"] for d in self.files_map.values()),
                "speed": round(self.speed, 1),
                "bytes_up": self.bytes_up,
                "up_speed": round(self.up_speed, 1),
                "peers": len(self.live),
                "peers_seen": self.peers_seen,
                "files": files,
            }

    def start(self):
        self.status = "running"
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def add_peer(self, addr):
        if addr not in self.manual_peers:
            self.manual_peers.append(addr)
        with self.lock:
            self.peers.add(addr)

    # 计算剩余未下载字节数（供 tracker 宣布的 left 字段使用）。
    def _left(self):
        with self.lock:
            return sum(d["size"] - d["got"] for d in self.files_map.values())

    # 主流程：启动 DHT/Tracker/webseed 工作线程，循环为可达 peer 建立
    # 下载连接，全部分块完成后进入做种模式。
    def _run(self):
        registered = False
        try:
            self._prepare()
            announce_port = 0
            if self.listener and self.allow_upload:
                self.listener.register(self)
                registered = True
                announce_port = self.listener.port
            DHTFinder(self.t.infohash, self.peers, self.lock, self.stop_event,
                      self.log, announce_port=announce_port).start()
            trackers = self.t.trackers + [u for u in DEFAULT_TRACKERS
                                          if u not in self.t.trackers]
            TrackerFinder(self.t.infohash, trackers, self._left, self.peers,
                          self.lock, self.stop_event, self.log,
                          listen_port=announce_port or 6881,
                          peer_id=self.peer_id).start()
            self._t0 = time.time()
            self._last_t = self._t0
            web_workers = []
            if self.webseeds:
                for i in range(min(6, len(self.need_pieces))):
                    w = threading.Thread(target=self._web_worker, args=(i,),
                                         daemon=True)
                    w.start()
                    web_workers.append(w)
            idle_rounds = 0
            while not self.stop_event.is_set():
                with self.lock:
                    pending = len(self._need_set) - len(self.done_pieces)
                    available = list(self.peers)
                if pending <= 0:
                    break
                for p in self.manual_peers:
                    if p not in available:
                        available.append(p)
                # spawn workers up to 12 concurrent connections
                for addr in available:
                    if self.active_peers >= 12:
                        break
                    if self._peer_in_use(addr):
                        continue
                    if addr in self.manual_peers or self._connectable(addr):
                        w = threading.Thread(target=self._peer_worker, args=(addr,), daemon=True)
                        w.start()
                        self._mark_in_use(addr, True)
                time.sleep(3)
                web_alive = any(w.is_alive() for w in web_workers)
                if self.active_peers == 0 and not web_alive:
                    idle_rounds += 1
                else:
                    idle_rounds = 0
                if idle_rounds >= 40:
                    self.error = ("no reachable peers found "
                                  "(DHT/tracker gave %d candidates)") % len(available)
                    break
            if not self.error and len(self.done_pieces) >= self.piece_total \
                    and self.seed and self.allow_upload:
                self._enter_seed_mode()
            self.stop_event.set()
            with self.lock:
                if not self.error and len(self.done_pieces) >= self.piece_total:
                    self.status = "done"
                elif self.status == "running":
                    self.status = "stopped"
        except Exception as e:
            self.status = "error"
            self.error = str(e)
        finally:
            if registered:
                self.listener.unregister(self.t.infohash)
            for h in list(self.handles.values()):
                try:
                    h.close()
                except OSError:
                    pass
            if self.status == "done":
                self._finalize()

    # ---- seed mode: keep serving the swarm after the download finishes ----
    # 做种模式：下载完成后继续向 swarm 提供上传，直到手动停止。
    def _enter_seed_mode(self):
        self._finalize()
        self._finalized = True
        with self.lock:
            for idx, d in self.files_map.items():
                try:
                    self.handles[idx] = open(d["dest"], "rb")
                except OSError:
                    pass
        self.status = "seeding"
        self.log("download complete; seeding to the swarm (stop to end)")
        while not self.stop_event.is_set():
            time.sleep(1)

    # ---- BEP19 webseeds: pieces over plain HTTP range requests ----
    # BEP19 webseed：通过普通 HTTP Range 请求直接下载分块。
    def _web_url(self, base, f):
        if not base.endswith("/"):
            return base
        if len(self.t.files) == 1 and self.t.files[0]["path"] == self.t.name:
            rel = self.t.name
        else:
            rel = self.t.name + "/" + f["path"]
        return base + "/".join(urllib.parse.quote(s, safe="")
                               for s in rel.split("/"))

    def _claim_web_piece(self):
        with self.lock:
            for p in self.need_pieces:
                if p in self.done_pieces or p in self.claimed \
                        or p in self.web_dead:
                    continue
                self.claimed[p] = True
                return p
        return None

    # webseed 工作线程：认领分块 -> HTTP 下载 -> 校验 -> 写盘，失败三次放弃。
    def _web_worker(self, wid):
        opener = (urllib.request.build_opener(urllib.request.ProxyHandler(
            {"http": self.proxy, "https": self.proxy}))
            if self.proxy else urllib.request.build_opener())
        time.sleep(0.4 * wid)   # stagger the first burst a little
        while not self.stop_event.is_set():
            piece = self._claim_web_piece()
            if piece is None:
                return
            data = self._fetch_piece_web(opener, piece, wid)
            if data is not None:
                self._store_piece(piece, data)
            else:
                self._release_piece(piece)
                with self.lock:
                    n = self._web_fails.get(piece, 0) + 1
                    self._web_fails[piece] = n
                    if n >= 3:
                        self.web_dead.add(piece)
                        self.log("webseed: giving up on piece %d" % piece)

    # 从各 webseed 轮询按 Range 请求拼装一个完整分块，并做 SHA1 校验。
    def _fetch_piece_web(self, opener, piece, wid):
        ps = piece * self.t.piece_length
        pe = ps + self.t.piece_size(piece)
        nb = len(self.webseeds)
        order = [self.webseeds[(wid + k) % nb] for k in range(nb)]
        for base in order:
            try:
                buf = bytearray()
                for f in self.t.files:
                    fs, fe = f["offset"], f["offset"] + f["size"]
                    if fe <= ps or fs >= pe:
                        continue
                    a = max(fs, ps) - fs
                    b = min(fe, pe) - fs
                    req = urllib.request.Request(
                        self._web_url(base, f),
                        headers={"User-Agent": "btlib/1.0",
                                 "Range": "bytes=%d-%d" % (a, b - 1)})
                    with opener.open(req, timeout=30) as r:
                        chunk = r.read()
                    if len(chunk) != b - a:
                        raise IOError("short read (%d/%d bytes)"
                                      % (len(chunk), b - a))
                    buf += chunk
                data = bytes(buf)
                if len(data) != pe - ps:
                    raise IOError("assembled size mismatch")
                if hashlib.sha1(data).digest() != self.t.piece_hash(piece):
                    raise IOError("piece hash mismatch")
                return data
            except Exception as e:
                self.log("webseed %s: piece %d failed (%s)"
                         % (base.split("/")[2], piece, e))
        return None

    # ---- BEP10/11: peer exchange over the extension protocol ----
    # BEP10/11：基于扩展协议（LTEP）的 peer 交换（PEX）。
    def _send_ext_handshake(self, sock, ip):
        try:
            ipb = socket.inet_aton(ip)
        except OSError:
            ipb = b"\x00\x00\x00\x00"
        payload = benc({b"m": {b"ut_pex": 1}, b"v": b"btlib 1.0",
                        b"yourip": ipb})
        sock.sendall(struct.pack(">I", 2 + len(payload)) +
                     b"\x14\x00" + payload)

    def _send_pex(self, sock, pex):
        if not pex.get("id"):
            return
        with self.lock:
            live = list(self.live)[:40]
        added = b""
        for ip, port in live:
            try:
                added += socket.inet_aton(ip) + struct.pack(">H", port)
            except OSError:
                pass
        if not added:
            return
        payload = benc({b"added": added,
                        b"added.f": b"\x00" * (len(added) // 6),
                        b"dropped": b""})
        try:
            sock.sendall(struct.pack(">I", 2 + len(payload)) + b"\x14" +
                         bytes([pex["id"]]) + payload)
            pex["last"] = time.time()
        except OSError:
            pass

    def _handle_ext(self, sock, addr, body, pex):
        ext_id = body[1]
        if ext_id == 0:      # peer's LTEP handshake: learn its ut_pex msg id
            try:
                d = bdec(body[2:])
                pex["id"] = int(d.get(b"m", {}).get(b"ut_pex", 0) or 0)
            except Exception:
                return
            if pex["id"]:
                self._send_pex(sock, pex)
        elif ext_id == 1:    # ut_pex message using the id we advertised
            try:
                d = bdec(body[2:])
            except Exception:
                return
            added = d.get(b"added", b"")
            if isinstance(added, bytes) and added:
                new = TrackerFinder._compact_peers(added)
                if new:
                    with self.lock:
                        self.peers.update(new)
                    self.log("pex: +%d peers from %s" % (len(new), addr[0]))

    # ---- inbound peer (connected to our listen socket) ----
    def attach_inbound(self, sock, addr, peer_rsv):
        """Serve a peer that connected to us; upload-only.
        处理主动连入的 peer；仅提供上传服务。"""
        try:
            sock.settimeout(30)
            rsv = bytearray(8)
            rsv[5] |= 0x10                       # LTEP (BEP10)
            sock.sendall(b"\x13BitTorrent protocol" + bytes(rsv) +
                         self.t.infohash + self.peer_id)
            with self.lock:
                self.live.add(addr)
            if self.allow_upload:
                sock.sendall(struct.pack(">I", 1 + len(self._have_bits)) +
                             b"\x05" + bytes(self._have_bits))
            ul = {"interested": False, "unchoked": False}
            while not self.stop_event.is_set():
                ln = struct.unpack(">I", self._recv_exact(sock, 4))[0]
                if ln == 0:
                    continue
                body = self._recv_exact(sock, ln)
                if body[0] in (2, 3, 6):
                    self._handle_upload_msg(sock, body[0], body, ul)
        except Exception:
            pass
        finally:
            try:
                sock.close()
            except OSError:
                pass
            with self.lock:
                self.live.discard(addr)

    def _prepare(self):
        os.makedirs(self.dest_dir, exist_ok=True)
        # piece-level resume: trust the sidecar bitmap left by an
        # interrupted run (pieces were sha1-verified before being recorded)
        # 分块级断点续传：信任上次中断留下的旁车位图（记录前已通过 sha1 校验）。
        self._load_resume()
        for idx, d in self.files_map.items():
            # keep existing bytes for resume: "w+b" would zero the file
            # 保留已有数据以支持续传："w+b" 会把文件清零。
            mode = "r+b" if os.path.exists(d["part"]) else "w+b"
            self.handles[idx] = open(d["part"], mode)  # read back for uploads
            self.handles[idx].truncate(d["size"])

    # ---- piece-level resume (分块级断点续传) ----
    # 续传元数据为任务首个文件 .part 旁的 JSON 文件，按任务（infohash）区分。
    def _meta_path(self):
        first = self.files_map[next(iter(self.files_map))]
        return first["part"] + ".meta"

    # 将当前已完成分块位图写入旁车元数据，供中断后恢复；每完成一个分块调用。
    def _save_resume(self):
        if not self.files_map:
            return
        bits = bytearray((self.t.npieces + 7) // 8)
        for p in self.done_pieces:
            bits[p // 8] |= 1 << (7 - p % 8)
        meta = {"infohash": self.t.infohash.hex(),
                "size": self.t.total,
                "piece_length": self.t.piece_length,
                "pieces": base64.b64encode(bytes(bits)).decode("ascii")}
        try:
            with open(self._meta_path(), "w") as f:
                json.dump(meta, f)
        except OSError:
            pass

    # 启动时加载续传元数据：校验 infohash/总大小/分块长度及 .part 文件尺寸，
    # 全部匹配则信任位图，将对应分块标记为已完成（跳过重新下载）。
    def _load_resume(self):
        try:
            with open(self._meta_path()) as f:
                meta = json.load(f)
        except (OSError, ValueError):
            return
        if meta.get("infohash") != self.t.infohash.hex() or \
                meta.get("size") != self.t.total or \
                meta.get("piece_length") != self.t.piece_length:
            return
        for d in self.files_map.values():
            try:
                if os.path.getsize(d["part"]) != d["size"]:
                    return
            except OSError:
                return
        try:
            bits = base64.b64decode(meta.get("pieces", ""))
        except Exception:
            return
        for p in range(min(self.t.npieces, len(bits) * 8)):
            if bits[p // 8] & (1 << (7 - p % 8)) and p in self._need_set:
                self._resume_piece(p)
        if self.done_pieces:
            self.log("resume: %d/%d pieces restored from .part.meta"
                     % (len(self.done_pieces), self.piece_total))

    # 将续传恢复的分块计入各文件进度与上传位图（数据已在 .part 文件中）。
    def _resume_piece(self, piece):
        p0 = piece * self.t.piece_length
        pend = p0 + self.t.piece_size(piece)
        for idx, d in self.files_map.items():
            fs, fe = d["offset"], d["offset"] + d["size"]
            a, b = max(fs, p0), min(fe, pend)
            if a < b:
                d["got"] += b - a
                self.bytes_done += b - a
        self.done_pieces.add(piece)
        self._have_bits[piece // 8] |= 1 << (7 - piece % 8)

    def _connectable(self, addr):
        """Cheap filter: only try peers that look like real IP:port strings."""
        return isinstance(addr, tuple) and len(addr) == 2

    def _inuse_key(self, addr):
        return addr

    def _peer_in_use(self, addr):
        return self._inuse_key(addr) in getattr(self, "_inuse", set())

    def _mark_in_use(self, addr, v):
        if not hasattr(self, "_inuse"):
            self._inuse = set()
        if v:
            self._inuse.add(self._inuse_key(addr))
        else:
            self._inuse.discard(self._inuse_key(addr))

    # ---- one peer connection ----
    # 单个 peer 连接：完成 BT 握手（含 LTEP 扩展握手），随后循环认领并下载分块。
    def _peer_worker(self, addr):
        ip, port = addr
        sock = None
        try:
            sock = socket.create_connection((ip, port), timeout=8)
            sock.settimeout(12)
            rsv = bytearray(8)
            rsv[5] |= 0x10                       # LTEP (BEP10), enables ut_pex
            sock.sendall(b'\x13BitTorrent protocol' + bytes(rsv) +
                         self.t.infohash + self.peer_id)
            hdr = self._recv_exact(sock, 68)
            if hdr[1:20] != b'BitTorrent protocol' or hdr[28:48] != self.t.infohash:
                return
            with self.lock:
                self.active_peers += 1
                self.live.add((ip, port))
                if addr not in self.peers:
                    self.peers.add(addr)
            bitfield = None
            unchoked = False
            ul = {"interested": False, "unchoked": False}
            pex = {"id": 0, "last": 0.0}
            if hdr[25] & 0x10:  # peer speaks the extension protocol too
                self._send_ext_handshake(sock, ip)
            if self.allow_upload:
                # advertise completed pieces so the peer may request them
                sock.sendall(struct.pack('>I', 1 + len(self._have_bits)) +
                             b'\x05' + bytes(self._have_bits))
            sock.sendall(struct.pack('>I', 1) + b'\x02')  # interested
            while not self.stop_event.is_set():
                piece = self._claim_piece(bitfield)
                if piece is None:
                    break
                got = self._download_piece(sock, piece, bitfield, ul,
                                           (ip, port), pex)
                if got:
                    continue
                self._release_piece(piece)
                break
        except Exception:
            pass
        finally:
            self._mark_in_use((ip, port), False)
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass
            with self.lock:
                self.live.discard((ip, port))
                if self.active_peers > 0:
                    self.active_peers -= 1

    # 认领一个未完成、未被占用且对方拥有的分块。
    def _claim_piece(self, bitfield):
        with self.lock:
            for p in self.need_pieces:
                if p in self.done_pieces or p in self.claimed:
                    continue
                if bitfield is not None:
                    byte = p // 8
                    if byte >= len(bitfield) or not (bitfield[byte] >> (7 - p % 8)) & 1:
                        continue
                self.claimed[p] = True
                return p
        return None

    def _release_piece(self, p):
        with self.lock:
            self.claimed.pop(p, None)

    def _handle_upload_msg(self, sock, mid, body, ul):
        """Peer protocol messages related to us uploading (serving) pieces.
        处理与我们上传（提供分块）相关的 peer 协议消息。"""
        if mid == 2:        # peer is interested in what we have
            ul["interested"] = True
            if self.allow_upload and not ul["unchoked"]:
                try:
                    sock.sendall(struct.pack('>I', 1) + b'\x01')  # unchoke
                    ul["unchoked"] = True
                except OSError:
                    pass
        elif mid == 3:      # not interested
            ul["interested"] = False
        elif mid == 6 and ul["unchoked"] and len(body) >= 13:
            piece, begin, length = struct.unpack('>III', body[1:13])
            self._serve_block(sock, piece, begin, length)

    # 从磁盘读出对方请求的分块数据并发送（上传服务核心）。
    def _serve_block(self, sock, piece, begin, length):
        if not self.allow_upload or piece not in self.done_pieces:
            return
        idx = self._serve_idx.get(piece)
        if idx is None:
            return
        psize = self.t.piece_size(piece)
        if begin >= psize:
            return
        length = min(length, psize - begin, 128 * 1024)
        d = self.files_map[idx]
        with self.lock:
            h = self.handles.get(idx)
            if not h:
                return
            try:
                h.seek(piece * self.t.piece_length - d["offset"] + begin)
                data = h.read(length)
            except OSError:
                return
            self.bytes_up += len(data)
        try:
            sock.sendall(struct.pack('>I', 9 + len(data)) + b'\x07' +
                         struct.pack('>II', piece, begin) + data)
        except OSError:
            pass
        now = time.time()
        if now - self._last_up_t >= 1.0:
            self.up_speed = (self.bytes_up - self._last_up) / (now - self._last_up_t)
            self._last_up = self.bytes_up
            self._last_up_t = now

    # 分块下载主循环：滑动窗口流水线请求所有 block，收齐后 SHA1 校验并落盘。
    def _download_piece(self, sock, piece, bitfield, ul, addr, pex):
        psize = self.t.piece_size(piece)
        buf = bytearray(psize)
        nblocks = (psize + BLOCK - 1) // BLOCK
        pending = []
        next_block = 0
        filled = 0
        window = 24
        while not self.stop_event.is_set():
            while next_block < nblocks and len(pending) < window:
                off = next_block * BLOCK
                ln = min(BLOCK, psize - off)
                pending.append((off, ln))
                sock.sendall(struct.pack('>I', 13) + b'\x06' +
                             struct.pack('>III', piece, off, ln))
                next_block += 1
            if not pending:
                break
            try:
                ln = struct.unpack('>I', self._recv_exact(sock, 4))[0]
                if ln == 0:
                    continue
                body = self._recv_exact(sock, ln)
            except Exception:
                return False
            mid = body[0]
            if mid == 7 and len(body) >= 9:
                pidx, off = struct.unpack('>II', body[1:9])
                data = body[9:]
                if pidx == piece and off < psize:
                    want = min(len(data), psize - off)
                    buf[off:off + want] = data[:want]
                    filled += want
                    pending = [x for x in pending if not (x[0] == off)]
            elif mid == 0:      # choked
                return False
            elif mid in (4, 5):
                continue
            elif mid in (2, 3, 6):
                self._handle_upload_msg(sock, mid, body, ul)
            elif mid == 20 and len(body) >= 2:
                self._handle_ext(sock, addr, body, pex)
            if pex.get("id") and time.time() - pex["last"] > 60:
                self._send_pex(sock, pex)
            if filled >= psize:
                break
            if not pending and next_block >= nblocks and filled < psize:
                return False
        if filled < psize:
            return False
        if hashlib.sha1(bytes(buf)).digest() != self.t.piece_hash(piece):
            return False
        self._store_piece(piece, bytes(buf))
        return True

    # 将校验通过的分块写入所覆盖的各个文件，更新进度、位图与速度统计。
    def _store_piece(self, piece, data):
        p0 = piece * self.t.piece_length
        with self.lock:
            for idx, d in self.files_map.items():
                fs, fe = d["offset"], d["offset"] + d["size"]
                if fe <= p0 or fs >= p0 + len(data):
                    continue
                a = max(fs, p0) - p0
                b = min(fe, p0 + len(data)) - p0
                h = self.handles.get(idx)
                if not h:
                    continue
                h.seek(max(fs, p0) - fs)
                h.write(data[a:b])
                d["got"] += b - a
                self.bytes_done += b - a
            self.done_pieces.add(piece)
            self.claimed.pop(piece, None)
            self._have_bits[piece // 8] |= 1 << (7 - piece % 8)
            self._save_resume()  # keep the resume sidecar consistent / 保持续传元数据一致
        now = time.time()
        if now - self._last_t >= 1.0:
            self.speed = (self.bytes_done - self._last_bytes) / (now - self._last_t)
            self._last_bytes = self.bytes_done
            self._last_t = now

    # 收尾：关闭句柄并把 .part 临时文件改名为最终文件。
    def _finalize(self):
        with self.lock:
            if self._finalized:
                return
            self._finalized = True
            for idx, d in self.files_map.items():
                h = self.handles.pop(idx, None)
                try:
                    if h:
                        h.close()
                except OSError:
                    pass
                try:
                    os.replace(d["part"], d["dest"])
                except OSError:
                    pass
            # download finished: the resume sidecar is no longer needed
            # 下载完成：删除续传旁车元数据文件。
            try:
                os.remove(self._meta_path())
            except OSError:
                pass

    @staticmethod
    def _recv_exact(sock, n):
        buf = b''
        while len(buf) < n:
            d = sock.recv(n - len(buf))
            if not d:
                raise EOFError("peer closed")
            buf += d
        return buf


# ---------------- inbound listener + LAN discovery (连入监听 + 局域网发现) ----------------
class PeerListener(threading.Thread):
    """One TCP listen socket shared by all torrent jobs; incoming peer
    connections are routed by infohash.  NAT usually blocks this from the
    internet, but on the LAN (e.g. a NAS seeding the same torrents) it
    works — and the LSD announcer below makes such peers find us.
    所有下载任务共用一个 TCP 监听端口，按 infohash 分发连入的 peer；
    公网 NAT 通常会拦截，但局域网内（如做种的 NAS）可用，配合下方 LSD 使用。"""

    def __init__(self, port=6881, log=None):
        threading.Thread.__init__(self, daemon=True)
        self.log = log or (lambda m: None)
        self.jobs = {}               # infohash -> Job
        self.jobs_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind(("0.0.0.0", port))
        except OSError:
            self.sock.bind(("0.0.0.0", 0))
        self.sock.listen(20)
        self.sock.settimeout(1.0)
        self.port = self.sock.getsockname()[1]
        self.lsd = LSD(self)
        self.lsd.start()

    def register(self, job):
        with self.jobs_lock:
            self.jobs[job.t.infohash] = job

    def unregister(self, infohash):
        with self.jobs_lock:
            self.jobs.pop(infohash, None)

    def run(self):
        while not self.stop_event.is_set():
            try:
                conn, addr = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn, addr),
                             daemon=True).start()
        try:
            self.sock.close()
        except OSError:
            pass

    # 处理连入连接：读取 68 字节握手，按 infohash 路由到对应下载任务。
    def _handle(self, conn, addr):
        try:
            conn.settimeout(10)
            hdr = b""
            while len(hdr) < 68:
                d = conn.recv(68 - len(hdr))
                if not d:
                    conn.close()
                    return
                hdr += d
            if hdr[1:20] != b"BitTorrent protocol":
                conn.close()
                return
            with self.jobs_lock:
                job = self.jobs.get(hdr[28:48])
            if job is None or job.status not in ("running", "seeding"):
                conn.close()
                return
            self.log("inbound peer %s:%d" % (addr[0], addr[1]))
            job.attach_inbound(conn, addr, hdr[20:28])
        except Exception:
            try:
                conn.close()
            except OSError:
                pass


class LSD(threading.Thread):
    """BEP14 Local Service Discovery: multicast our infohashes on the LAN
    every 5 minutes and harvest peers other LAN clients announce (this is
    how a NAS seeding the same torrent gets found instantly).
    BEP14 局域网服务发现：每 5 分钟在局域网组播我们的 infohash，
    并收集其他局域网客户端宣布的 peer。"""

    MCAST = ("239.192.152.143", 6771)

    def __init__(self, listener):
        threading.Thread.__init__(self, daemon=True)
        self.listener = listener
        self.log = listener.log
        self.stop_event = listener.stop_event

    def run(self):
        try:
            rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                               socket.IPPROTO_UDP)
            rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rx.bind(("", self.MCAST[1]))
            rx.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                          socket.inet_aton(self.MCAST[0]) +
                          socket.inet_aton("0.0.0.0"))
            rx.settimeout(1.0)
        except OSError:
            return
        tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                           socket.IPPROTO_UDP)
        tx.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        next_ann = 0.0
        try:
            while not self.stop_event.is_set():
                if time.time() >= next_ann:
                    next_ann = time.time() + 300
                    self._announce(tx)
                try:
                    data, addr = rx.recvfrom(2048)
                except socket.timeout:
                    continue
                except OSError:
                    break
                self._handle(data, addr)
        finally:
            for s in (rx, tx):
                try:
                    s.close()
                except OSError:
                    pass

    # 组播 BT-SEARCH 报文：向局域网宣布我们的 infohash 与监听端口。
    def _announce(self, tx):
        with self.listener.jobs_lock:
            items = list(self.listener.jobs)
        for i in range(0, len(items), 5):      # up to 5 hashes per BT-SEARCH
            msg = ("BT-SEARCH * HTTP/1.1\r\nHost: %s:%d\r\nPort: %d\r\n"
                   % (self.MCAST[0], self.MCAST[1], self.listener.port))
            for ih in items[i:i + 5]:
                msg += "Infohash: %s\r\n" % ih.hex()
            msg += "cookie: btlib\r\n\r\n"
            try:
                tx.sendto(msg.encode("ascii"), self.MCAST)
            except OSError:
                pass

    # 解析收到的 BT-SEARCH：把持有同种子的局域网 peer 加入对应任务。
    def _handle(self, data, addr):
        try:
            text = data.decode("ascii", "replace")
            if not text.startswith("BT-SEARCH"):
                return
            port = 0
            hashes = []
            for line in text.split("\r\n"):
                low = line.lower()
                if low.startswith("port:"):
                    port = int(line.split(":", 1)[1].strip())
                elif low.startswith("infohash:"):
                    hashes.append(bytes.fromhex(line.split(":", 1)[1].strip()))
            if not port:
                return
            with self.listener.jobs_lock:
                jobs = dict(self.listener.jobs)
            for ih in hashes:
                job = jobs.get(ih)
                if job is not None:
                    job.add_peer((addr[0], port))
                    self.log("lsd: peer %s:%d" % (addr[0], port))
        except Exception:
            pass
