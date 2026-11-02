# Batocera Web Manager

**Beta 1.0**

A browser-based management panel for [Batocera](https://batocera.org/) — pure Python 3 standard library, **zero dependencies**. Manage games, download ROMs over BitTorrent/HTTP, scrape metadata, back up your userdata, and tweak system settings from any browser on your LAN.

基于浏览器的 [Batocera](https://batocera.org/) 管理面板 —— 纯 Python 3 标准库实现，**零依赖**。在局域网任意浏览器里管理游戏、通过 BT/HTTP 在线下载游戏、刮削元数据、备份用户数据、调整系统设置。

---

## Features / 功能特性

| EN | 中文 |
|---|---|
| **Dashboard** — hostname, IP, uptime, load, memory, disk, temperature | **仪表盘** —— 主机名、IP、运行时长、负载、内存、磁盘、温度 |
| **Game download library** — 100+ systems, per-game download | **游戏下载库** —— 100+ 个系统，按游戏单独下载 |
| **BitTorrent engine** — DHT (BEP5), trackers (UDP+HTTP), PEX, LSD, web seeds (BEP19), inbound listener, auto-seeding after download | **BT 引擎** —— DHT (BEP5)、tracker（UDP+HTTP）、PEX、LSD 局域网发现、webseed (BEP19)、入站监听、下载完成自动做种 |
| **HTTP downloads** — parallel range segments, resume, archive.org account support for restricted Redump sets | **HTTP 下载** —— 多线程分段、断点续传、支持 archive.org 账号下载受限 Redump 集 |
| **Metadata scraper** — ScreenScraper API, mirrors the ES scraper | **元数据刮削** —— ScreenScraper API，与 ES 内置刮削器一致 |
| **Backup / restore** — full userdata backup with selectable sets | **备份 / 恢复** —— 全量用户数据备份，可按数据集选择 |
| **File manager** — browse / upload / delete under `/userdata` | **文件管理** —— 浏览 / 上传 / 删除 `/userdata` 下的文件 |
| **Gamepad & keyboard mapping viewer** — read-only; edit in EmulationStation | **手柄 / 键盘映射查看** —— 只读；改键请用 ES 前台 |
| **BIOS check, system upgrade, power controls** | **BIOS 检查、系统升级、电源控制** |
| **5 UI languages** — English, 简体中文, 繁體中文, 日本語, 한국어 | **5 种界面语言** —— English、简体中文、繁體中文、日本語、한국어 |

---

## Requirements / 运行环境

* A Batocera device (x86_64 / ARM) with Python 3 — already shipped with Batocera.
  一台装有 Batocera 的设备（自带 Python 3）。
* A browser on the same LAN. 同一局域网内的浏览器。

No pip packages, no node, no build step. Standard library only.
无需 pip 安装任何包，无 node、无构建步骤，仅标准库。

---

## Install / 安装

```sh
# 1. Copy the whole webmanager folder to the device
#    将整个 webmanager 目录复制到设备
scp -r webmanager root@<batocera-ip>:/userdata/system/

# 2. Run the installer (adds autostart via custom.sh and starts the server)
#    运行安装脚本（写入 custom.sh 开机自启并立即启动）
ssh root@<batocera-ip> "sh /userdata/system/webmanager/install.sh"
```

Then open **http://\<batocera-ip\>/** — the server listens on port 80.
然后浏览器打开 **http://\<batocera-ip\>/** —— 服务监听 80 端口。

### Uninstall / 卸载

```sh
ssh root@<batocera-ip> "pkill -f webmanager/server.py; \
  sed -i '/webmanager/d' /userdata/system/custom.sh; \
  rm -rf /userdata/system/webmanager"
```

---

## Game downloads / 游戏下载

* ROM metadata lives in `gamedb/<system>/<system>.json` (+ `<system>.torrent` for BT systems).
  游戏库元数据位于 `gamedb/<系统>/<系统>.json`（BT 系统附带同名 `.torrent`）。
* Cartridge systems use **No-Intro** sets; disc systems use **Redump CHD** sets, all hosted on archive.org. Torrents are the **original archive.org item torrents** — every user of this panel joins the same swarm, so devices can fetch pieces from each other.
  卡带系统使用 **No-Intro** 集，光盘系统使用 **Redump CHD** 集，均托管于 archive.org。种子为 **archive.org 官方原始种子** —— 所有面板用户处于同一 P2P 网络，可互相传输加速。
* Every torrent carries the archive.org **web seed**, so downloads work even when the swarm is empty.
  每个种子都带 archive.org **webseed**，即使网络中没有其他 peer 也能下载。
* Finished downloads **keep seeding** (can be disabled in Settings). Forwarding the BT listen port (default 6881) on your router improves swarm connectivity; devices on the same LAN find each other automatically via LSD.
  下载完成后**自动继续做种**（可在设置中关闭）。在路由器上转发 BT 监听端口（默认 6881）可提升连通性；同局域网设备通过 LSD 自动互相发现。
* Restricted Redump items (Saturn / PSX / PS2 …) require a free **archive.org account** — log in on the Settings page.
  受限 Redump 资源（Saturn / PSX / PS2 等）需要免费的 **archive.org 账号** —— 在设置页登录。
* A proxy can be configured for networks that cannot reach archive.org directly (`webmanager.dl_proxy`).
  无法直连 archive.org 的网络可在设置中配置代理（`webmanager.dl_proxy`）。
* Interrupted/failed downloads leave `*.part` / `*.parts` residue: it is hidden from game lists and can be removed with one click from the download page.
  中断/失败的下载会留下 `*.part` / `*.parts` 残留：游戏列表会自动隐藏，下载页可一键清理。

> **Legal note / 法律提示**: Only download games you legally own. The authors do not host any ROM content; all downloads come from public archive.org items.
> 请只下载你合法拥有的游戏。本项目不托管任何 ROM 内容，所有下载均来自 archive.org 公开资源。

---

## Settings / 设置项

Stored in `batocera.conf` (`webmanager.*` keys) / 保存在 `batocera.conf` 的 `webmanager.*` 键中：

| Key | Default | EN | 中文 |
|---|---|---|---|
| `webmanager.dl_proxy` | *(empty)* | HTTP proxy for downloads | 下载用 HTTP 代理 |
| `webmanager.dl_upload` | `1` | Upload pieces to other peers | 向其他 peer 上传 |
| `webmanager.dl_seed` | `1` | Keep seeding after download finishes | 下载完成后继续做种 |
| `webmanager.dl_port` | `6881` | Inbound BT listen port | BT 入站监听端口 |
| `webmanager.dl_peers` | *(empty)* | Manual peers (`ip:port`, one per line) | 手动指定 peer（每行一个 `ip:port`） |
| `webmanager.token` | *(empty)* | Access token: when set, every browser must enter it to use the panel (sent as `Authorization: Bearer` header or `?token=` query); also blocks cross-site POSTs. Leave empty to disable. | 访问令牌：设置后每台浏览器须输入令牌才能使用面板（以 `Authorization: Bearer` 头或 `?token=` 参数携带），同时阻止跨站 POST。留空关闭。 |

---

## Architecture / 架构

```
webmanager/
├── server.py      # HTTP server (port 80), all API routes / HTTP 服务与全部 API
├── btlib.py       # BitTorrent client: DHT/tracker/PEX/LSD/webseed/seeding / BT 客户端
├── httplib.py     # parallel HTTP range downloader / 多线程分段 HTTP 下载器
├── scraper.py     # ScreenScraper metadata scraper / 元数据刮削器
├── index.html     # single-file frontend / 单文件前端
├── install.sh     # installer + autostart / 安装与自启脚本
├── locales/       # UI translations (en, zh, zh_TW, ja, ko) / 界面翻译
├── overlays/      # gamepad overlay images / 手柄布局图
└── gamedb/        # per-system download libraries / 各系统游戏下载库
```

* Pure stdlib: `http.server`, `socket`, `hashlib`, `tarfile`, `subprocess` (curl for HTTP ranges).
  纯标准库：`http.server`、`socket`、`hashlib`、`tarfile`、`subprocess`（HTTP 分段由 curl 执行）。
* No uTP (BEP29) — TCP peer transport only; DHT/PEX/LSD/trackers/web seeds provide plenty of connectivity.
  未实现 uTP (BEP29) —— 仅 TCP 对等传输；DHT/PEX/LSD/tracker/webseed 已提供充足连通性。

---

## Contributing / 参与贡献

Issues and PRs welcome. Please keep the code dependency-free (Python standard library only).
欢迎提交 Issue 和 PR。请保持零依赖（仅限 Python 标准库）。

## License / 许可

MIT
