import os
import json
import requests
import logging

logger = logging.getLogger(__name__)

class UpstashRedisREST:
    """
    Cliente Redis REST para Upstash via HTTPS (porta 443).
    Usado no cPanel para contornar o bloqueio de portas TCP de saída.
    """
    def __init__(self, url: str = None, token: str = None):
        self.url = (url or os.getenv("UPSTASH_REDIS_REST_URL", "https://growing-aphid-186288.upstash.io")).rstrip("/")
        self.token = token or os.getenv("UPSTASH_REDIS_REST_TOKEN", "AcdoAAIjcDFkODBhMjBiZmRkODI0MjdmYTNhMTE0ZTgxMzQwNGZhNHAxMA")
        if self.url and not (self.url.startswith("http://") or self.url.startswith("https://")):
            self.url = f"https://{self.url}"

    def _command(self, *args):
        if not self.url or not self.token:
            logger.warning("[UpstashREST] URL ou TOKEN não configurados.")
            return None
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        cmd_args = [str(a) if not isinstance(a, (int, float)) else a for a in args]
        try:
            resp = requests.post(self.url, headers=headers, json=cmd_args, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            return data.get("result")
        except Exception as e:
            logger.error(f"[UpstashREST] Erro no comando {args[0] if args else ''}: {e}")
            return None

    def get(self, key: str):
        return self._command("GET", key)

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
        return res == "OK" or res is True

    def setex(self, key: str, time_seconds: int, value: str):
        return self.set(key, value, ex=time_seconds)

    def delete(self, *keys):
        if not keys:
            return 0
        res = self._command("DEL", *keys)
        return int(res) if res is not None else 0

    def exists(self, *keys):
        if not keys:
            return 0
        res = self._command("EXISTS", *keys)
        return int(res) if res is not None else 0

    def incr(self, key: str, amount: int = 1):
        res = self._command("INCRBY", key, amount)
        return int(res) if res is not None else 0

    def decr(self, key: str, amount: int = 1):
        res = self._command("DECRBY", key, amount)
        return int(res) if res is not None else 0

    def ttl(self, key: str):
        res = self._command("TTL", key)
        return int(res) if res is not None else -2

    def expire(self, key: str, seconds: int):
        res = self._command("EXPIRE", key, seconds)
        return res == 1 or res is True

    def keys(self, pattern: str = "*"):
        res = self._command("KEYS", pattern)
        return res if isinstance(res, list) else []

    def ping(self):
        res = self._command("PING")
        return res == "PONG"
