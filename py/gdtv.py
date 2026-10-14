# -*- coding: utf-8 -*-
# ============================================================================
# 广东广电 gdtv.py（2026-10-13 重写 · 双模自包含版 v3）
# 站点：www.gdtv.cn / gdtv-api.gdtv.cn / tcdn-api.itouchtv.cn / tcdn-ws.itouchtv.cn
# 链路：签名头(HMAC-SHA256) → 频道表 → getParam 节点 → WS 换节点 → OPTIONS 预检
#       → GET 取动态 playUrl → 直出真 m3u8
# 契约：base.spider。双模：① 555 点播路径(18 台当列表项, playerContent 直出 m3u8)
#       ② liveContent 直播路径 + localProxy 302。两路共用同一取流内核。
# 网络：优先用引擎注入的 self.net；无则用内置 requests+纯Python WS 兜底。
# ★v3 修复：v1 只实现 liveContent/localProxy → ① 基类 ABCMeta 抽象方法未实现，实例化即崩；
#   ② 壳按点播源加载时 homeContent/categoryContent 为空 → 空白。本版补齐 555 全接口并双模出流。
# ============================================================================
# ★写源必加清单（缺则补·逐条落注释）
# ★必加①大图片：列表 item 走 _cw() 四件套 style={rect,1.78}+land+ratio+type_name，分类项走 _ct() 给 land/ratio（秀网阁同款，已落地）
#   直播卡片另走 liveContent 的 channel 结构（name/tvgName/tvgId/number/logo/urls/parse/format/ua/referer/origin）。
# ②二级分类/筛选器：本源单频道表(category=0)，无二级筛选 → 不加（有则加无则不加）。
# ③搜索：searchContent(key,quick,pg) 关键词本地过滤频道名，已加。
# ★写源必检查清单（交付前逐条核对）
# ①翻页：直播单表无分页 → page/pagecount/limit/total 四件齐全但不翻页（禁伪分页）。
# ②多线路接口检查：单一直播入口(CDN tcdn)，18 台逐台取流实测；失败台保留不剔除。
# ③播放源还能播放：playerContent 直出真 m3u8；localProxy 302 亦通（实测 200+#EXTM3U）。
# ④分类还少：category=0 全量 18 台已全取；直播源无 LABEL/排行榜概念。
# 必加/必检查清单结束
# ============================================================================
# ★写源前必调·三底座
# 底座①记忆库：知识库/影视源开发（01-规范契约 / 02-技能与工具链 / 03-攻坚档案）
# 底座②技能包：SKILL_INDEX v2 分层调度（L0 tvbox-py-v73 骨架 / L1 pySkill / L3 adaptive）
# 底座③母版：cp 母版 → 改 CONFIG ★区 → 改站点逻辑区（直播向取 base.spider 直播契约）
# SKILL_INDEX: /sdcard/Download/Operit/skills/SKILL_INDEX.md
# ============================================================================
import sys
sys.path.append('..')
import base64
import hashlib
import hmac
import json
import os
import socket
import ssl
import struct
import time
import uuid
from urllib.parse import urlsplit, parse_qs

try:
    import requests
except ImportError:
    requests = None

try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

try:
    from base.spider import Spider as BaseSpider
except ImportError:
    class BaseSpider(object):
        def __init__(self, *args, **kwargs):
            self._cache = {}

        def getProxyUrl(self, flag=False):
            return 'http://127.0.0.1:9978/proxy?do=py'


HOST = "https://www.gdtv.cn"
API_URL = "https://gdtv-api.gdtv.cn/api/tv/v2/tvChannel"
CATALOG_URL = API_URL + "?category=0"
NODE_URL = "https://tcdn-api.itouchtv.cn/getParam"
WS_URL = "wss://tcdn-ws.itouchtv.cn:3800/connect"
API_KEY = "89541943007407288657755311868534"
API_SECRET = b"dfkcY1c3sfuw1Cii9DWj8UO3iQy2hqlDxyvDXd1oVMxwYVDSgeB6phO9eW1dfuwX"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10
PLAY_CACHE_SECONDS = 30
CATALOG_CACHE_SECONDS = 300
PLAYER_HEADERS = {"User-Agent": USER_AGENT, "Referer": HOST + "/", "Origin": HOST}
PREFLIGHT_HEADERS = {
    **PLAYER_HEADERS,
    "Access-Control-Request-Method": "GET",
    "Access-Control-Request-Headers": (
        "x-itouchtv-ca-key,x-itouchtv-ca-signature,x-itouchtv-ca-timestamp,"
        "x-itouchtv-client,x-itouchtv-device-id"
    ),
}
GROUP_NAME = "广东频道"
# ★ 列表大图卡片(2026-10-07 统一): 分类项 _ct 给 land/ratio, 列表 item _cw 给 style/land/ratio/type_name
CARD_ON = 1
CARD_TYPE = "rect"
CARD_LAND = 1
CARD_RATIO = 1.78
CARD = {"type": CARD_TYPE, "ratio": CARD_RATIO}
CARD_SRC = GROUP_NAME


def _ws_once(url, headers, payload, timeout=10):
    u = urlsplit(url)
    host = u.hostname
    port = u.port or (443 if u.scheme == "wss" else 80)
    path = u.path or "/"
    if u.query:
        path += "?" + u.query
    raw = socket.create_connection((host, port), timeout=timeout)
    if u.scheme == "wss":
        raw = ssl._create_unverified_context().wrap_socket(raw, server_hostname=host)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    handshake = (
        "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n" % (path, host, key)
    )
    for k, v in (headers or {}).items():
        handshake += "%s: %s\r\n" % (k, v)
    handshake += "\r\n"
    raw.sendall(handshake.encode("utf-8"))
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = raw.recv(4096)
        if not chunk:
            raw.close()
            raise ValueError("WebSocket 握手失败")
        buf += chunk
    buf = buf.split(b"\r\n\r\n", 1)[1]
    data = json.dumps(payload).encode("utf-8")
    mask = os.urandom(4)
    frame = bytearray([0x81])
    n = len(data)
    if n < 126:
        frame.append(0x80 | n)
    elif n < 65536:
        frame.append(0x80 | 126)
        frame += struct.pack(">H", n)
    else:
        frame.append(0x80 | 127)
        frame += struct.pack(">Q", n)
    frame += mask
    frame += bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    raw.sendall(bytes(frame))

    def _read(size):
        nonlocal buf
        while len(buf) < size:
            chunk = raw.recv(4096)
            if not chunk:
                raise ValueError("WebSocket 连接中断")
            buf += chunk
        out, buf = buf[:size], buf[size:]
        return out

    try:
        while True:
            head = _read(2)
            opcode = head[0] & 0x0F
            length = head[1] & 0x7F
            if length == 126:
                length = struct.unpack(">H", _read(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", _read(8))[0]
            body = _read(length)
            if opcode == 0x01:
                return body.decode("utf-8", "ignore")
            if opcode == 0x08:
                raise ValueError("WebSocket 已关闭")
    finally:
        raw.close()


class _Net(object):
    def __init__(self, spider):
        self._sp = spider

    def req(self, url, opt=None):
        opt = opt or {}
        try:
            if requests is None:
                raise ValueError("requests 不可用")
            method = (opt.get("method") or "GET").upper()
            headers = opt.get("headers") or {}
            timeout = (opt.get("timeout") or REQUEST_TIMEOUT * 1000) / 1000.0
            if method == "OPTIONS":
                r = requests.options(url, headers=headers, timeout=timeout, verify=False)
            elif method == "POST":
                r = requests.post(url, data=opt.get("data"), headers=headers, timeout=timeout, verify=False)
            else:
                r = requests.get(url, headers=headers, timeout=timeout, verify=False)
            return {"code": r.status_code, "content": r.text, "error": None}
        except Exception as error:
            return {"error": str(error)}

    def json(self, url, opt=None):
        res = self.req(url, opt)
        if res.get("error"):
            raise ValueError(res["error"])
        return json.loads(res.get("content") or "null")

    def cached(self, key, opt, fn):
        ttl = (opt.get("ttl") or 0) / 1000.0
        now = time.time()
        hit = self._sp._cache.get(key)
        if hit and (ttl <= 0 or now - hit[0] < ttl):
            return hit[1]
        value = fn()
        self._sp._cache[key] = (now, value)
        return value

    def ws(self, url, opt=None):
        opt = opt or {}
        timeout = (opt.get("timeout") or REQUEST_TIMEOUT * 1000) / 1000.0
        text = _ws_once(url, opt.get("headers") or {}, opt.get("data"), timeout)
        return {"code": 101, "content": text, "error": None}


class Spider(BaseSpider):

    def init(self, extend=""):
        self.device_id = "WEB_" + str(uuid.uuid4())
        if not hasattr(self, "_cache"):
            self._cache = {}
        if getattr(self, "net", None) is None:
            self.net = _Net(self)

    def getName(self):
        return "广东广电"

    def homeContent(self, filter=False):
        return {"class": [self._ct("tv", GROUP_NAME)], "list": self._items()}

    def homeVideoContent(self):
        return {"list": self._items()}

    def categoryContent(self, tid, pg, filter, extend):
        items = self._items()
        return {"list": items, "page": 1, "pagecount": 1, "limit": len(items), "total": len(items)}

    def detailContent(self, ids):
        target = str((ids or [""])[0]) if isinstance(ids, (list, tuple)) else str(ids or "")
        for item in self._items():
            if item["vod_id"] == target:
                return {"list": [item]}
        return {"list": []}

    def searchContent(self, key, quick, pg="1"):
        key = (key or "").strip()
        items = [i for i in self._items() if key and key in i["vod_name"]]
        return {"list": items}

    def playerContent(self, flag, id, vipFlags=None):
        channel_id = self._channel_id(self._play_id(id))
        if not channel_id:
            return {"parse": 0, "url": "", "header": PLAYER_HEADERS}
        try:
            return {"parse": 0, "url": self._play_url(channel_id), "header": PLAYER_HEADERS}
        except (OSError, ValueError):
            return {"parse": 0, "url": "", "header": PLAYER_HEADERS}

    def isVideoFormat(self, url):
        return ".m3u8" in str(url or "").lower() or ".mp4" in str(url or "").lower()

    def manualVideoCheck(self):
        return False

    def liveContent(self, url):
        return [{"name": GROUP_NAME, "channel": self._channels()}]

    def localProxy(self, param):
        channel_id = self._channel_id(self._param_id(param))
        if not channel_id:
            return [400, "text/plain", "广东广电：频道代码错误"]
        try:
            return [302, "text/plain", "", {"Location": self._play_url(channel_id)}]
        except (OSError, ValueError) as error:
            return [502, "text/plain", "广东广电：" + str(error)]

    def destroy(self):
        pass

    @staticmethod
    def _play_id(value):
        s = str(value or "")
        return s.split("$")[-1] if "$" in s else s

    @staticmethod
    def _param_id(param):
        if isinstance(param, dict):
            value = param.get("id", "")
        elif isinstance(param, str):
            value = (parse_qs(param).get("id") or [""])[0]
        else:
            value = ""
        if isinstance(value, list):
            value = value[0] if value else ""
        return str(value or "")

    def _proxy_url(self, channel_id):
        try:
            base = self.getProxyUrl()
        except Exception:
            base = ""
        base = base or "http://127.0.0.1:9978/proxy?do=py"
        return base + ("&" if "?" in base else "?") + "id=" + channel_id

    @staticmethod
    def _pic(row):
        return str(row.get("coverUrl") or row.get("avatarUrl") or "")

    @staticmethod
    def _channel_id(value):
        if isinstance(value, str) and value.isascii() and value.isdigit():
            return value.lstrip("0")
        return ""

    def _catalog(self):
        try:
            return self.net.cached("catalog", {"ttl": CATALOG_CACHE_SECONDS * 1000},
                                   lambda: self._get(CATALOG_URL))
        except Exception:
            return []

    def _channels(self):
        rows = self._catalog()
        if not isinstance(rows, list):
            return []
        out, seen = [], set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            cid = self._channel_id(str(row.get("pk") or ""))
            name = str(row.get("name") or "").strip()
            if not cid or not name or cid in seen:
                continue
            seen.add(cid)
            out.append({
                "name": name,
                "tvgName": name,
                "tvgId": cid,
                "number": "{:02d}".format(len(out) + 1),
                "logo": self._pic(row),
                "urls": [self._proxy_url(cid)],
                "parse": 0,
                "format": "application/x-mpegURL",
                "ua": USER_AGENT,
                "referer": HOST + "/",
                "origin": HOST,
            })
        return out

    def _cw(self, it):
        if CARD_ON and isinstance(it, dict):
            it["style"] = dict(CARD)
            it.setdefault("land", CARD_LAND)
            it.setdefault("ratio", CARD_RATIO)
            it.setdefault("type_name", CARD_SRC)
        return it

    def _ct(self, tid, nm):
        c = {"type_id": str(tid), "type_name": nm}
        if CARD_ON:
            c["land"] = CARD_LAND
            c["ratio"] = CARD_RATIO
        return c

    def _items(self):
        items = []
        for ch in self._channels():
            cid = ch["tvgId"]
            name = ch["name"]
            items.append(self._cw({
                "vod_id": cid,
                "vod_name": name,
                "vod_pic": ch["logo"],
                "vod_remarks": "直播",
                "type_name": GROUP_NAME,
                "vod_play_from": "广东广电",
                "vod_play_url": name + "$" + cid,
            }))
        return items

    def _get(self, url):
        try:
            return self.net.json(url, {
                "method": "GET", "headers": self._headers(url), "timeout": REQUEST_TIMEOUT * 1000,
            })
        except Exception as error:
            raise ValueError("网络请求失败：" + str(error)) from None

    def _request(self, url, headers, method="GET"):
        result = self.net.req(url, {
            "method": method, "headers": headers, "timeout": REQUEST_TIMEOUT * 1000,
        })
        return self._content(result, range(200, 300))

    @staticmethod
    def _content(result, codes):
        if not isinstance(result, dict) or result.get("error"):
            raise ValueError("网络请求失败：" + str((result or {}).get("error")))
        if result.get("code") not in codes:
            raise ValueError("HTTP " + str(result.get("code") or "无响应"))
        return result["content"]

    def _play_url(self, channel_id):
        return self.net.cached("play:" + channel_id, {"ttl": PLAY_CACHE_SECONDS * 1000},
                               lambda: self._resolve(channel_id))

    def _resolve(self, channel_id):
        result = self._get(NODE_URL)
        node = result.get("node") if isinstance(result, dict) else None
        if not isinstance(node, str) or not node:
            raise ValueError("直播参数格式错误")
        if node.endswith("-1"):
            node = self._ws_node(node)
        encoded = base64.b64encode(node.encode("utf-8")).decode("ascii")
        url = API_URL + "/" + channel_id + "?tvChannelPk=" + channel_id + "&node=" + encoded
        self._request(url, PREFLIGHT_HEADERS, "OPTIONS")
        result = self._get(url)
        play_url = result.get("playUrl") if isinstance(result, dict) else None
        if not isinstance(play_url, str):
            raise ValueError("播放数据格式错误")
        streams = json.loads(play_url)
        stream = streams.get("hd") or streams.get("sd") if isinstance(streams, dict) else None
        parsed = urlsplit(stream) if isinstance(stream, str) else None
        if parsed is None or parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("播放地址为空或格式错误")
        return stream

    def _ws_node(self, node):
        response = self.net.ws(WS_URL, {
            "headers": PLAYER_HEADERS,
            "data": {"route": "getwsparam", "message": node},
            "timeout": REQUEST_TIMEOUT * 1000,
        })
        result = json.loads(self._content(response, (101,)))
        wsnode = result.get("wsnode") if isinstance(result, dict) else None
        if (not isinstance(result, dict) or result.get("status") != 201
                or not isinstance(wsnode, str) or not wsnode):
            raise ValueError("直播参数回应错误")
        return wsnode

    def _headers(self, url):
        timestamp = str(int(time.time() * 1000))
        message = "GET\n" + url + "\n" + timestamp + "\n"
        signature = base64.b64encode(
            hmac.new(API_SECRET, message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("ascii")
        return {
            **PLAYER_HEADERS,
            "X-ITOUCHTV-Ca-Key": API_KEY,
            "X-ITOUCHTV-Ca-Signature": signature,
            "X-ITOUCHTV-Ca-Timestamp": timestamp,
            "X-ITOUCHTV-CLIENT": "WEB_PC",
            "X-ITOUCHTV-DEVICE-ID": self.device_id,
        }