import json
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Tuple
from urllib.parse import urlparse, unquote

import pymysql
from pymysql.cursors import DictCursor


JSON_COLUMNS = {
    "contacts": {"phones", "metadata"},
    "calls": {"metadata"},
    "campaigns": {"line_tokens"},
    "campaign_calls": {"debito_data"},
    "import_jobs": {"result"},
    "sip_groups": {"line_tokens"},
}


def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    if isinstance(value, str):
        value = value.strip().strip('"').strip("'")
        return value if value else default
    return value if value is not None else default


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass
class QueryResponse:
    data: Any = None
    count: int = 0


def create_client(*_args, **_kwargs):
    return MySQLClient()


class MySQLClient:
    def __init__(self):
        url_config = self._config_from_url(env("MYSQL_URL") or env("DATABASE_URL"))
        self.config = {
            "host": env("MYSQL_HOST") or env("MYSQLHOST") or url_config.get("host") or "localhost",
            "port": env_int("MYSQL_PORT", env_int("MYSQLPORT", url_config.get("port", 3306))),
            "user": env("MYSQL_USER") or env("MYSQLUSER") or url_config.get("user") or "root",
            "password": env("MYSQL_PASSWORD") or env("MYSQLPASSWORD") or url_config.get("password") or "",
            "database": env("MYSQL_DATABASE") or env("MYSQLDATABASE") or url_config.get("database") or "wavoip",
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
            "autocommit": True,
        }
        if env("MYSQL_SSL", "false").lower() in ("1", "true", "yes", "sim"):
            self.config["ssl"] = {}

    def _config_from_url(self, value: str) -> dict:
        if not value:
            return {}
        parsed = urlparse(value)
        return {
            "host": parsed.hostname or "",
            "port": parsed.port or 3306,
            "user": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "database": (parsed.path or "").lstrip("/"),
        }

    def table(self, name: str):
        return QueryBuilder(self, name)

    def connect(self):
        return pymysql.connect(**self.config)


class QueryBuilder:
    def __init__(self, client: MySQLClient, table: str):
        self.client = client
        self.table_name = table
        self.action = "select"
        self.columns = "*"
        self.payload = None
        self.filters: List[Tuple[str, str, Any]] = []
        self.order_by = None
        self.order_desc = False
        self.limit_value = None
        self.offset_value = None
        self.want_count = False
        self.conflict_column = None

    def select(self, columns: str = "*", count: str = None):
        self.action = "select"
        self.columns = columns or "*"
        self.want_count = count == "exact"
        return self

    def insert(self, payload):
        self.action = "insert"
        self.payload = payload
        return self

    def update(self, payload: dict):
        self.action = "update"
        self.payload = payload or {}
        return self

    def delete(self):
        self.action = "delete"
        return self

    def upsert(self, payload: dict, on_conflict: str = None):
        self.action = "upsert"
        self.payload = payload or {}
        self.conflict_column = on_conflict
        return self

    def eq(self, column: str, value):
        self.filters.append((column, "=", value))
        return self

    def neq(self, column: str, value):
        self.filters.append((column, "!=", value))
        return self

    def lt(self, column: str, value):
        self.filters.append((column, "<", value))
        return self

    def in_(self, column: str, values):
        self.filters.append((column, "IN", list(values or [])))
        return self

    def ilike(self, column: str, value):
        self.filters.append((column, "LIKE", value))
        return self

    def order(self, column: str, desc: bool = False):
        self.order_by = column
        self.order_desc = bool(desc)
        return self

    def limit(self, value: int):
        self.limit_value = int(value)
        return self

    def range(self, start: int, end: int):
        self.offset_value = int(start)
        self.limit_value = max(0, int(end) - int(start) + 1)
        return self

    def execute(self):
        with self.client.connect() as conn:
            with conn.cursor() as cur:
                if self.action == "select":
                    return self._execute_select(cur)
                if self.action == "insert":
                    return self._execute_insert(cur)
                if self.action == "update":
                    return self._execute_update(cur)
                if self.action == "delete":
                    return self._execute_delete(cur)
                if self.action == "upsert":
                    return self._execute_upsert(cur)
        return QueryResponse(data=[])

    def _execute_select(self, cur):
        where_sql, params = self._where()
        count_params = list(params)
        cols = self._columns_sql()
        sql = f"SELECT {cols} FROM `{self.table_name}`{where_sql}"
        if self.order_by:
            sql += f" ORDER BY `{self.order_by}` {'DESC' if self.order_desc else 'ASC'}"
        if self.limit_value is not None:
            sql += " LIMIT %s"
            params.append(self.limit_value)
            if self.offset_value is not None:
                sql += " OFFSET %s"
                params.append(self.offset_value)
        cur.execute(sql, params)
        rows = [self._normalize_row(row) for row in cur.fetchall()]
        total = len(rows)
        if self.want_count:
            count_sql = f"SELECT COUNT(*) AS total FROM `{self.table_name}`{where_sql}"
            cur.execute(count_sql, count_params)
            total = (cur.fetchone() or {}).get("total", 0)
        return QueryResponse(data=rows, count=total)

    def _execute_insert(self, cur):
        rows = self.payload if isinstance(self.payload, list) else [self.payload]
        inserted = []
        for row in rows:
            clean = self._prepare_payload(dict(row or {}), ensure_id=True)
            columns = list(clean.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO `{self.table_name}` ({', '.join(f'`{c}`' for c in columns)}) VALUES ({placeholders})"
            cur.execute(sql, [clean[c] for c in columns])
            inserted.append(self._normalize_row(dict(row, id=clean.get("id", row.get("id")))))
        return QueryResponse(data=inserted, count=len(inserted))

    def _execute_update(self, cur):
        clean = self._prepare_payload(dict(self.payload or {}), ensure_id=False)
        if not clean:
            return QueryResponse(data=[], count=0)
        assignments = ", ".join(f"`{col}` = %s" for col in clean)
        where_sql, params = self._where()
        sql = f"UPDATE `{self.table_name}` SET {assignments}{where_sql}"
        cur.execute(sql, list(clean.values()) + params)
        return QueryResponse(data=[], count=cur.rowcount)

    def _execute_delete(self, cur):
        where_sql, params = self._where()
        sql = f"DELETE FROM `{self.table_name}`{where_sql}"
        cur.execute(sql, params)
        return QueryResponse(data=[], count=cur.rowcount)

    def _execute_upsert(self, cur):
        clean = self._prepare_payload(dict(self.payload or {}), ensure_id=True)
        columns = list(clean.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        updates = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in columns if c != "id")
        sql = (
            f"INSERT INTO `{self.table_name}` ({', '.join(f'`{c}`' for c in columns)}) "
            f"VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        )
        cur.execute(sql, [clean[c] for c in columns])
        return QueryResponse(data=[self._normalize_row(clean)], count=1)

    def _where(self):
        if not self.filters:
            return "", []
        parts = []
        params = []
        for column, op, value in self.filters:
            if op == "IN":
                if not value:
                    parts.append("1 = 0")
                    continue
                parts.append(f"`{column}` IN ({', '.join(['%s'] * len(value))})")
                params.extend(value)
            else:
                parts.append(f"`{column}` {op} %s")
                params.append(value)
        return " WHERE " + " AND ".join(parts), params

    def _columns_sql(self):
        if self.columns == "*":
            return "*"
        cols = [c.strip() for c in str(self.columns).split(",") if c.strip()]
        return ", ".join(f"`{c}`" for c in cols) if cols else "*"

    def _prepare_payload(self, payload: dict, ensure_id: bool):
        if ensure_id and "id" not in payload and self.table_name not in {"line_overrides"}:
            payload["id"] = str(uuid.uuid4())
        out = {}
        for key, value in payload.items():
            if value == "now()":
                out[key] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            elif key in JSON_COLUMNS.get(self.table_name, set()):
                out[key] = json.dumps(value, ensure_ascii=False) if value is not None else None
            else:
                out[key] = value
        return out

    def _normalize_row(self, row: dict):
        out = {}
        for key, value in (row or {}).items():
            if key in JSON_COLUMNS.get(self.table_name, set()) and isinstance(value, str):
                try:
                    value = json.loads(value)
                except Exception:
                    pass
            elif isinstance(value, (datetime, date)):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = float(value)
            out[key] = value
        return out
