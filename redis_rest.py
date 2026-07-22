import os
import json
import time
import requests
import logging

logger = logging.getLogger(__name__)

_memory_store = {}

class UpstashRedisREST:
    """
    Cliente Redis REST para Upstash via HTTPS (porta 443).
    Com fallback em memória local para resiliência total contra erros de rede/autenticação.
    """
    def __init__(self, url: str = None, token: str = None):
        self.url = (url or os.getenv("UPSTASH_REDIS_REST_URL", "https://growing-aphid-186288.upstash.io")).rstrip("/")
        self.token = (token or os.getenv("UPSTASH_REDIS_REST_TOKEN", "AcdoAAIjcDFkODBhMjBiZmRkODI0MjdmYTNhMTE0ZTgxMzQwNGZhNHAxMA")).strip()
        if self.url and not (self.url.startswith("http://") or self.url.startswith("https://")):
            self.url = f"https://{self.url}"

    def _command(self, *args):
        if not self.url or not self.token:
            return "FALLBACK"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        cmd_args = [str(a) if not isinstance(a, (int, float)) else a for a in args]
        try:
            resp = requests.post(self.url, headers=headers, json=cmd_args, timeout=5)
            if resp.status_code == 401:
                return "FALLBACK"
            resp.raise_for_status()
            data = resp.json()
            return data.get("result")
        except Exception as e:
            return "FALLBACK"

    def get(self, key: str):
        res = self._command("GET", key)
        if res != "FALLBACK":
            return res
        item = _memory_store.get(key)
        if not item:
            return None
        val, exp = item
        if exp and time.time() > exp:
            _memory_store.pop(key, None)
            return None
        return val

    def set(self, key: str, value: str, ex: int = None, px: int = None, nx: bool = False, xx: bool = False):
        cmd = ["SET", key, str(value)]
        if ex is not None:
            cmd.extend(["EX", int(ex)])
        if px is not None:
            cmd.extend(["PX", int(px)])
        if nx:
            cmd.append("NX")
        if xx:
            cmd.append("XX")
        
        res = self._command(*cmd)
        if res != "FALLBACK":
            return res == "OK" or res is True

        now = time.time()
        item = _memory_store.get(key)
        exists = item is not None and (item[1] is None or item[1] > now)
        
        if nx and exists:
            return False
        if xx and not exists:
            return False
            
        ttl = None
        if ex is not None:
            ttl = now + int(ex)
        elif px is not None:
            ttl = now + (int(px) / 1000.0)
            
        _memory_store[key] = (str(value), ttl)
        return True

    def setex(self, key: str, time_seconds: int, value: str):
        return self.set(key, value, ex=time_seconds)

    def delete(self, *keys):
        if not keys:
            return 0
        res = self._command("DEL", *keys)
        if res != "FALLBACK":
            return int(res) if res is not None else 0
        count = 0
        for k in keys:
            if _memory_store.pop(k, None) is not None:
                count += 1
        return count

    def exists(self, *keys):
        if not keys:
            return 0
        res = self._command("EXISTS", *keys)
        if res != "FALLBACK":
            return int(res) if res is not None else 0
        now = time.time()
        count = 0
        for k in keys:
            item = _memory_store.get(k)
            if item and (item[1] is None or item[1] > now):
                count += 1
        return count

    def incr(self, key: str, amount: int = 1):
        res = self._command("INCRBY", key, amount)
        if res != "FALLBACK":
            return int(res) if res is not None else 0
        curr = int(self.get(key) or 0)
        new_val = curr + amount
        _memory_store[key] = (str(new_val), _memory_store.get(key, (None, None))[1])
        return new_val

    def decr(self, key: str, amount: int = 1):
        return self.incr(key, -amount)

    def ttl(self, key: str):
        res = self._command("TTL", key)
        if res != "FALLBACK":
            return int(res) if res is not None else -2
        item = _memory_store.get(key)
        if not item:
            return -2
        if item[1] is None:
            return -1
        rem = int(item[1] - time.time())
        return rem if rem > 0 else -2

    def expire(self, key: str, seconds: int):
        res = self._command("EXPIRE", key, seconds)
        if res != "FALLBACK":
            return res == 1 or res is True
        item = _memory_store.get(key)
        if not item:
            return False
        _memory_store[key] = (item[0], time.time() + int(seconds))
        return True

    def keys(self, pattern: str = "*"):
        res = self._command("KEYS", pattern)
        if res != "FALLBACK":
            return res if isinstance(res, list) else []
        return list(_memory_store.keys())

    def ping(self):
        res = self._command("PING")
        if res != "FALLBACK":
            return res == "PONG"
        return True
