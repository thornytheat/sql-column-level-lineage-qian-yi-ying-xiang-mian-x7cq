"""SQLite 访问层（薄封装，无 ORM）。

表：
- meta                   seed 版本、active_run_id 等键值
- scripts                样例脚本（sql/ 与 sql_broken/）
- schemas                表结构（列名 + 类型）
- runs                   每次解析任务（仅全量成功的批次才会写入并成为结论）
- run_scripts            每个脚本的解析结论（含错误位置）
- run_table_edges        表级血缘边
- run_column_edges       列级血缘边（含变换链 / star 标记 / 证据行号）
- run_cycles             环检测结果
- run_batches            批次登记：应到脚本数、成功/失败数、状态与切换差异
- run_batch_scripts      批次内每个脚本的应到登记与最新解析结论（含失败位置）
- run_batch_edges        批次内已成功脚本的血缘边（重试只替换失败项的边）
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scripts (
  script_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL UNIQUE,
  set_name TEXT NOT NULL,
  sql TEXT NOT NULL,
  sort_order INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS schemas (
  table_name TEXT PRIMARY KEY,
  columns_json TEXT NOT NULL,
  comment TEXT
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  source_label TEXT NOT NULL,
  script_count INTEGER NOT NULL,
  parsed_count INTEGER NOT NULL,
  failed_count INTEGER NOT NULL,
  table_edge_count INTEGER NOT NULL,
  column_edge_count INTEGER NOT NULL,
  table_count INTEGER NOT NULL,
  cycle_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  duration_ms INTEGER NOT NULL DEFAULT 0,
  note TEXT
);

CREATE TABLE IF NOT EXISTS run_scripts (
  run_id TEXT NOT NULL,
  filename TEXT NOT NULL,
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  status TEXT NOT NULL,
  columns_json TEXT NOT NULL,
  line INTEGER NOT NULL,
  error_line INTEGER,
  error_col INTEGER,
  message TEXT,
  sql TEXT NOT NULL,
  PRIMARY KEY (run_id, filename)
);

CREATE TABLE IF NOT EXISTS run_table_edges (
  run_id TEXT NOT NULL,
  source_table TEXT NOT NULL,
  target_table TEXT NOT NULL,
  script TEXT NOT NULL,
  line INTEGER NOT NULL,
  target_kind TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rte_run ON run_table_edges(run_id);

CREATE TABLE IF NOT EXISTS run_column_edges (
  run_id TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_column TEXT NOT NULL,
  target_table TEXT NOT NULL,
  target_column TEXT NOT NULL,
  script TEXT NOT NULL,
  line INTEGER NOT NULL,
  expr TEXT NOT NULL,
  transforms_json TEXT NOT NULL,
  star_expanded INTEGER NOT NULL DEFAULT 0,
  union_branch INTEGER NOT NULL DEFAULT 0,
  target_kind TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rce_run ON run_column_edges(run_id);
CREATE INDEX IF NOT EXISTS idx_rce_src ON run_column_edges(run_id, source_table, source_column);

CREATE TABLE IF NOT EXISTS run_cycles (
  run_id TEXT NOT NULL,
  level TEXT NOT NULL,
  cycle_key TEXT NOT NULL,
  nodes_json TEXT NOT NULL,
  scripts_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rc_run ON run_cycles(run_id);

CREATE TABLE IF NOT EXISTS run_batches (
  batch_id TEXT PRIMARY KEY,
  batch_key TEXT NOT NULL,
  source_label TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expected_count INTEGER NOT NULL,
  parsed_count INTEGER NOT NULL,
  failed_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  active_run_id TEXT,
  previous_run_id TEXT,
  attempts INTEGER NOT NULL DEFAULT 1,
  switch_diff_json TEXT,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_rb_key ON run_batches(batch_key);
CREATE INDEX IF NOT EXISTS idx_rb_status ON run_batches(status);

CREATE TABLE IF NOT EXISTS run_batch_scripts (
  batch_id TEXT NOT NULL,
  filename TEXT NOT NULL,
  sort_order INTEGER NOT NULL,
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  status TEXT NOT NULL,
  columns_json TEXT NOT NULL,
  line INTEGER NOT NULL,
  error_line INTEGER,
  error_col INTEGER,
  message TEXT,
  sql TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (batch_id, filename)
);
CREATE INDEX IF NOT EXISTS idx_rbs_batch ON run_batch_scripts(batch_id);

CREATE TABLE IF NOT EXISTS run_batch_edges (
  batch_id TEXT NOT NULL,
  level TEXT NOT NULL,
  script TEXT NOT NULL,
  edge_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rbe_batch ON run_batch_edges(batch_id);
"""


def connect(path: str | Path | None = None) -> sqlite3.Connection:
    target = Path(path) if path else settings.DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


class transaction:
    """显式事务上下文：正常退出统一 commit，异常回滚，保证批次切换原子性。

    各 save_* 函数自带 commit，组合多步写入（落 run + 切 active + 更新批次）时
    用本上下文包起来，任一步失败整体回滚，避免出现「有新 run 但指针没切」的中间态。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    # 轻量迁移：老库的 runs 表没有 batch_id 列时补列（SQLite 支持 ADD COLUMN）。
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(runs)").fetchall()}
    if "batch_id" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN batch_id TEXT")
    conn.commit()


def now() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str, commit: bool = True) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    if commit:
        conn.commit()


def set_active_run_id(conn: sqlite3.Connection, run_id: str, commit: bool = True) -> None:
    set_meta(conn, "active_run_id", run_id, commit=commit)


def save_run(
    conn: sqlite3.Connection,
    analysis: dict,
    source_label: str,
    duration_ms: int = 0,
    batch_id: str | None = None,
    commit: bool = True,
) -> str:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{abs(hash(source_label)) % 1000:03d}"
    created = now()
    cycles = analysis["cycles"]
    conn.execute(
        """INSERT INTO runs(run_id, created_at, source_label, script_count, parsed_count, failed_count,
                            table_edge_count, column_edge_count, table_count, cycle_count, status, duration_ms, note, batch_id)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            run_id,
            created,
            source_label,
            len(analysis["scripts"]),
            analysis["parsed_count"],
            analysis["failed_count"],
            len(analysis["table_edges"]),
            len(analysis["column_edges"]),
            len(analysis["table_levels"]),
            len(cycles),
            "ok" if analysis["failed_count"] == 0 else "with_errors",
            duration_ms,
            analysis.get("note"),
            batch_id,
        ),
    )
    for s in analysis["scripts"]:
        conn.execute(
            """INSERT INTO run_scripts(run_id, filename, kind, target, status, columns_json, line,
                                       error_line, error_col, message, sql)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id,
                s["filename"],
                s["kind"],
                s["target"],
                s["status"],
                json.dumps(s["columns"], ensure_ascii=False),
                s["line"] or 0,
                s.get("error_line"),
                s.get("error_col"),
                s.get("message"),
                s["sql"],
            ),
        )
    for e in analysis["table_edges"]:
        conn.execute(
            """INSERT INTO run_table_edges(run_id, source_table, target_table, script, line, target_kind)
               VALUES(?,?,?,?,?,?)""",
            (run_id, e["source_table"], e["target_table"], e["script"], e["line"], e["target_kind"]),
        )
    for e in analysis["column_edges"]:
        conn.execute(
            """INSERT INTO run_column_edges(run_id, source_table, source_column, target_table, target_column,
                                            script, line, expr, transforms_json, star_expanded, union_branch, target_kind)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id,
                e["source_table"],
                e["source_column"],
                e["target_table"],
                e["target_column"],
                e["script"],
                e["line"],
                e["expr"],
                json.dumps(e["transforms"], ensure_ascii=False),
                1 if e["star_expanded"] else 0,
                e["union_branch"],
                e["target_kind"],
            ),
        )
    for c in cycles:
        conn.execute(
            "INSERT INTO run_cycles(run_id, level, cycle_key, nodes_json, scripts_json) VALUES(?,?,?,?,?)",
            (
                run_id,
                c["level"],
                c["key"],
                json.dumps(c["nodes"], ensure_ascii=False),
                json.dumps(c.get("scripts", []), ensure_ascii=False),
            ),
        )
    if commit:
        conn.commit()
    return run_id


def list_runs(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM runs ORDER BY created_at DESC, rowid DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def load_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    run = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    if run is None:
        return None
    scripts = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM run_scripts WHERE run_id = ? ORDER BY filename", (run_id,)
        ).fetchall()
    ]
    for s in scripts:
        s["columns"] = json.loads(s["columns_json"])
    table_edges = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM run_table_edges WHERE run_id = ? ORDER BY target_table, source_table, script, line",
            (run_id,),
        ).fetchall()
    ]
    column_edges = [
        dict(r)
        for r in conn.execute(
            """SELECT * FROM run_column_edges WHERE run_id = ?
               ORDER BY target_table, target_column, source_table, source_column""",
            (run_id,),
        ).fetchall()
    ]
    for e in column_edges:
        e["transforms"] = json.loads(e["transforms_json"])
        e["star_expanded"] = bool(e["star_expanded"])
    cycles = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM run_cycles WHERE run_id = ? ORDER BY level, cycle_key", (run_id,)
        ).fetchall()
    ]
    for c in cycles:
        c["nodes"] = json.loads(c["nodes_json"])
        c["scripts"] = json.loads(c["scripts_json"])
        c["key"] = c["cycle_key"]
    return {
        "run": dict(run),
        "scripts": scripts,
        "table_edges": table_edges,
        "column_edges": column_edges,
        "cycles": cycles,
    }


def load_schemas(conn: sqlite3.Connection) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for row in conn.execute("SELECT table_name, columns_json FROM schemas ORDER BY table_name").fetchall():
        out[row["table_name"]] = json.loads(row["columns_json"])
    return out


def list_scripts(conn: sqlite3.Connection, set_name: str | None = None) -> list[dict]:
    if set_name:
        rows = conn.execute(
            "SELECT * FROM scripts WHERE set_name = ? ORDER BY sort_order, filename", (set_name,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM scripts ORDER BY sort_order, filename").fetchall()
    return [dict(r) for r in rows]


def stats(conn: sqlite3.Connection) -> dict:
    def one(sql: str) -> int:
        row = conn.execute(sql).fetchone()
        return int(row[0]) if row else 0

    return {
        "sample_scripts": one("SELECT COUNT(*) FROM scripts WHERE set_name = 'sample'"),
        "broken_scripts": one("SELECT COUNT(*) FROM scripts WHERE set_name = 'broken'"),
        "schema_tables": one("SELECT COUNT(*) FROM schemas"),
        "runs": one("SELECT COUNT(*) FROM runs"),
        "active_run_id": get_meta(conn, "active_run_id"),
        "latest_run": (
            dict(conn.execute("SELECT * FROM runs ORDER BY created_at DESC, rowid DESC LIMIT 1").fetchone())
            if one("SELECT COUNT(*) FROM runs")
            else None
        ),
    }


# --------------------------------------------------------------------------
# 批次（run_batches / run_batch_scripts / run_batch_edges）
# --------------------------------------------------------------------------

def get_active_run_id(conn: sqlite3.Connection) -> str | None:
    """当前生效结论的 run_id；失效的批次不会替换它。"""
    return get_meta(conn, "active_run_id")


def get_open_batch(conn: sqlite3.Connection, batch_key: str) -> dict | None:
    """同 batch_key 且未切换成功的批次即视为同一批：重试复用、不新增重复批次。"""
    row = conn.execute(
        "SELECT * FROM run_batches WHERE batch_key = ? AND status != 'switched' ORDER BY rowid DESC LIMIT 1",
        (batch_key,),
    ).fetchone()
    return dict(row) if row else None


def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM run_batches WHERE batch_id = ?", (batch_id,)).fetchone()
    return dict(row) if row else None


def list_batches(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM run_batches ORDER BY rowid DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def list_batch_scripts(conn: sqlite3.Connection, batch_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM run_batch_scripts WHERE batch_id = ? ORDER BY sort_order, filename",
        (batch_id,),
    ).fetchall()
    out = [dict(r) for r in rows]
    for s in out:
        s["columns"] = json.loads(s["columns_json"])
    return out


def list_batch_edges(conn: sqlite3.Connection, batch_id: str, level: str | None = None) -> list[dict]:
    if level:
        rows = conn.execute(
            "SELECT edge_json FROM run_batch_edges WHERE batch_id = ? AND level = ?",
            (batch_id, level),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT edge_json FROM run_batch_edges WHERE batch_id = ?", (batch_id,)
        ).fetchall()
    return [json.loads(r["edge_json"]) for r in rows]


def new_batch_id(batch_key: str) -> str:
    return f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{abs(hash(batch_key)) % 1000:03d}"


def register_batch(conn: sqlite3.Connection, batch_id: str, batch_key: str, source_label: str,
                   expected_files: list[dict]) -> None:
    """按批次登记「应到脚本」清单：先把每个脚本以 pending 落库，缺失项以此为准对账。"""
    now_ts = now()
    conn.execute(
        """INSERT INTO run_batches(batch_id, batch_key, source_label, created_at, updated_at,
                                   expected_count, parsed_count, failed_count, status, attempts)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (batch_id, batch_key, source_label, now_ts, now_ts, len(expected_files), 0, 0, "open", 1),
    )
    for order, item in enumerate(expected_files):
        conn.execute(
            """INSERT INTO run_batch_scripts(batch_id, filename, sort_order, kind, target, status,
                                             columns_json, line, error_line, error_col, message, sql, attempts)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                batch_id,
                item["filename"],
                order,
                "-",
                "-",
                "pending",
                "[]",
                0,
                None,
                None,
                None,
                item.get("sql", ""),
                0,
            ),
        )
    conn.commit()


def update_batch_scripts(conn: sqlite3.Connection, batch_id: str, scripts: list[dict]) -> None:
    """用本轮解析结论 upsert 批次内脚本（重试只覆盖失败项）。"""
    for s in scripts:
        row = conn.execute(
            "SELECT attempts FROM run_batch_scripts WHERE batch_id = ? AND filename = ?",
            (batch_id, s["filename"]),
        ).fetchone()
        attempts = (row["attempts"] if row else 0) + 1
        conn.execute(
            """INSERT INTO run_batch_scripts(batch_id, filename, sort_order, kind, target, status,
                                             columns_json, line, error_line, error_col, message, sql, attempts)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(batch_id, filename) DO UPDATE SET
                 kind=excluded.kind, target=excluded.target, status=excluded.status,
                 columns_json=excluded.columns_json, line=excluded.line,
                 error_line=excluded.error_line, error_col=excluded.error_col,
                 message=excluded.message, sql=excluded.sql, attempts=excluded.attempts""",
            (
                batch_id,
                s["filename"],
                s.get("sort_order", 0),
                s["kind"],
                s["target"],
                s["status"],
                json.dumps(s.get("columns", []), ensure_ascii=False),
                s.get("line") or 0,
                s.get("error_line"),
                s.get("error_col"),
                s.get("message"),
                s["sql"],
                attempts,
            ),
        )
    conn.commit()


def replace_batch_edges(conn: sqlite3.Connection, batch_id: str, filenames: list[str],
                        table_edges: list[dict], column_edges: list[dict]) -> None:
    """删除并重写指定脚本产生的边：重试只替换失败项，成功项的边保持不动。"""
    placeholders = ",".join("?" for _ in filenames)
    conn.execute(
        f"DELETE FROM run_batch_edges WHERE batch_id = ? AND script IN ({placeholders})",
        [batch_id, *filenames],
    )
    for e in table_edges:
        conn.execute(
            "INSERT INTO run_batch_edges(batch_id, level, script, edge_json) VALUES(?,?,?,?)",
            (batch_id, "table", e["script"], json.dumps(e, ensure_ascii=False)),
        )
    for e in column_edges:
        conn.execute(
            "INSERT INTO run_batch_edges(batch_id, level, script, edge_json) VALUES(?,?,?,?)",
            (batch_id, "column", e["script"], json.dumps(e, ensure_ascii=False)),
        )
    conn.commit()


def update_batch_state(conn: sqlite3.Connection, batch_id: str, *, status: str,
                       parsed_count: int, failed_count: int, active_run_id: str | None = None,
                       previous_run_id: str | None = None, switch_diff: dict | None = None,
                       note: str | None = None, increment_attempt: bool = False,
                       commit: bool = True) -> None:
    row = conn.execute("SELECT attempts FROM run_batches WHERE batch_id = ?", (batch_id,)).fetchone()
    attempts = (row["attempts"] if row else 0) + (1 if increment_attempt else 0)
    conn.execute(
        """UPDATE run_batches
           SET updated_at=?, status=?, parsed_count=?, failed_count=?, attempts=?,
               active_run_id=COALESCE(?, active_run_id),
               previous_run_id=COALESCE(?, previous_run_id),
               switch_diff_json=COALESCE(?, switch_diff_json),
               note=?
           WHERE batch_id=?""",
        (
            now(),
            status,
            parsed_count,
            failed_count,
            attempts,
            active_run_id,
            previous_run_id,
            json.dumps(switch_diff, ensure_ascii=False) if switch_diff is not None else None,
            note,
            batch_id,
        ),
    )
    if commit:
        conn.commit()


def load_batch_view(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    """装配批次视图：批次头 + 应到脚本登记 + 当前已积累的全部边。"""
    batch = get_batch(conn, batch_id)
    if batch is None:
        return None
    if batch.get("switch_diff_json"):
        batch["switch_diff"] = json.loads(batch["switch_diff_json"])
    else:
        batch["switch_diff"] = None
    scripts = list_batch_scripts(conn, batch_id)
    return {
        "batch": batch,
        "scripts": scripts,
        "table_edges": list_batch_edges(conn, batch_id, "table"),
        "column_edges": list_batch_edges(conn, batch_id, "column"),
    }
