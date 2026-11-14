"""批次编排：登记应到脚本 → 解析 → 全部成功且零缺失后一次性切换结论。

设计要点（对应需求）：
- 按批次登记每个应到脚本（run_batch_scripts），缺失项以登记清单为准对账；
- 只有批次内脚本全部解析成功、缺失项为零，才生成新 run 并把血缘 / 影响面 / 迁移方案
  一次性切换到新结论（meta.active_run_id）；
- 失败批次不落新 run、不切换：血缘与迁移结论仍指向上一版，失败脚本保留行列位置；
- 重试同一批只处理失败项（run_batch_edges 里成功项的边原样保留），复用未关闭批次，
  不新增重复批次；
- 切换前后对表级 / 列级引用做三向对账：新增 added、消失 removed、仍未完成 unresolved。
"""
from __future__ import annotations

from . import lineage, pipeline, store

BATCH_STATUS_OPEN = "open"            # 批次登记后 / 存在失败项，结论未切换
BATCH_STATUS_SWITCHED = "switched"    # 全部成功，结论已原子切换


def make_batch_key(mode: str, filenames: list[str]) -> str:
    """同一批脚本清单 = 同一个 batch_key；清单变化即视为新批次。"""
    return f"{mode}:{'|'.join(filenames)}"


# --------------------------------------------------------------------------
# 引用对账
# --------------------------------------------------------------------------

def _table_ref(edge: dict) -> tuple[str, str]:
    return edge["source_table"], edge["target_table"]


def _column_ref(edge: dict) -> tuple[str, str, str, str]:
    return (
        edge["source_table"],
        edge["source_column"],
        edge["target_table"],
        edge["target_column"],
    )


def reference_diff(
    previous_table_edges: list[dict],
    previous_column_edges: list[dict],
    current_table_edges: list[dict],
    current_column_edges: list[dict],
    unresolved: list[dict],
) -> dict:
    """切换前后的三向对账：新增 / 消失 / 仍未完成的引用（表级与列级分别列出）。"""
    prev_t = {_table_ref(e) for e in previous_table_edges}
    curr_t = {_table_ref(e) for e in current_table_edges}
    prev_c = {_column_ref(e) for e in previous_column_edges}
    curr_c = {_column_ref(e) for e in current_column_edges}

    return {
        "table": {
            "added": [
                {"source_table": s, "target_table": t}
                for s, t in sorted(curr_t - prev_t)
            ],
            "removed": [
                {"source_table": s, "target_table": t}
                for s, t in sorted(prev_t - curr_t)
            ],
        },
        "column": {
            "added": [
                {"source_table": s, "source_column": sc, "target_table": t, "target_column": tc}
                for s, sc, t, tc in sorted(curr_c - prev_c)
            ],
            "removed": [
                {"source_table": s, "source_column": sc, "target_table": t, "target_column": tc}
                for s, sc, t, tc in sorted(prev_c - curr_c)
            ],
        },
        # 仍未完成：失败脚本引用了哪些来源表/列、失败位置在哪，防止迁移清单漏掉未知下游
        "unresolved": unresolved,
        "summary": {
            "table_added": len(curr_t - prev_t),
            "table_removed": len(prev_t - curr_t),
            "column_added": len(curr_c - prev_c),
            "column_removed": len(prev_c - curr_c),
            "unresolved_count": len(unresolved),
        },
    }


def _unresolved_refs(failed_scripts: list[dict]) -> list[dict]:
    """失败项的引用无法落边：登记脚本、目标与失败行列，作为「仍未完成」清单。"""
    out = []
    for s in failed_scripts:
        out.append(
            {
                "filename": s["filename"],
                "target": s.get("target") or "-",
                "line": s.get("error_line"),
                "col": s.get("error_col"),
                "message": s.get("message") or "解析失败",
            }
        )
    return sorted(out, key=lambda x: x["filename"])


# --------------------------------------------------------------------------
# 批次提交与重试
# ------------------------------------------------------------------

def _base_catalog(conn, batch_id: str, schemas: dict[str, list[str]]) -> dict[str, list[str]]:
    """重试时的基线 catalog：schema + 批次内已成功脚本登记的目标列。"""
    catalog = {name: list(cols) for name, cols in schemas.items()}
    for s in store.list_batch_scripts(conn, batch_id):
        if s["status"] == "ok" and s.get("columns"):
            catalog.setdefault(s["target"], s["columns"])
    return catalog


def submit_batch(
    conn,
    files: list[dict],
    schemas: dict[str, list[str]],
    source_label: str,
    mode: str,
) -> dict:
    """登记并解析一个批次；同 key 存在未关闭批次时走重试（只处理失败项）。"""
    batch_key = make_batch_key(mode, [f["filename"] for f in files])
    existing = store.get_open_batch(conn, batch_key)
    if existing is not None:
        return retry_batch(conn, existing["batch_id"], overrides=None, schemas=schemas)

    batch_id = store.new_batch_id(batch_key)
    store.register_batch(conn, batch_id, batch_key, source_label, files)
    return _process(conn, batch_id, files, schemas, source_label)


def retry_batch(
    conn,
    batch_id: str,
    overrides: dict[str, str] | None,
    schemas: dict[str, list[str]] | None = None,
) -> dict:
    """补齐后重试同一批：只重解析失败项，成功项与已落的边不重算、不新增批次。"""
    batch = store.get_batch(conn, batch_id)
    if batch is None:
        raise KeyError(batch_id)
    if batch["status"] == BATCH_STATUS_SWITCHED:
        return _already_switched(conn, batch)

    registered = store.list_batch_scripts(conn, batch_id)
    failed = [s for s in registered if s["status"] != "ok"]
    if not failed:
        # 没有失败项却重试：直接按现有登记做切换对账（幂等）。
        return _finalize_if_complete(conn, batch_id, schemas or store.load_schemas(conn))

    # 允许用修正后的 SQL 覆盖失败项（按文件名对应），不改变应到清单。
    files = []
    for s in failed:
        sql = overrides.get(s["filename"], s["sql"]) if overrides else s["sql"]
        files.append({"filename": s["filename"], "sql": sql})

    base_catalog = _base_catalog(conn, batch_id, schemas or store.load_schemas(conn))
    result = lineage.build_lineage(files, base_catalog)
    touched = [s["filename"] for s in result["scripts"]]
    store.update_batch_scripts(conn, batch_id, result["scripts"])
    store.replace_batch_edges(
        conn,
        batch_id,
        touched,
        result["table_edges"],
        result["column_edges"],
    )
    store.update_batch_state(
        conn,
        batch_id,
        status=BATCH_STATUS_OPEN,
        parsed_count=sum(1 for s in store.list_batch_scripts(conn, batch_id) if s["status"] == "ok"),
        failed_count=sum(1 for s in store.list_batch_scripts(conn, batch_id) if s["status"] != "ok"),
        increment_attempt=True,  # 批次尝试次数 +1；脚本行次数由 update_batch_scripts 维护
    )
    return _finalize_if_complete(conn, batch_id, schemas or store.load_schemas(conn))


def _process(conn, batch_id: str, files: list[dict], schemas: dict[str, list[str]],
             source_label: str) -> dict:
    result = lineage.build_lineage(files, schemas)
    store.update_batch_scripts(conn, batch_id, result["scripts"])
    store.replace_batch_edges(
        conn,
        batch_id,
        [s["filename"] for s in result["scripts"]],
        result["table_edges"],
        result["column_edges"],
    )
    parsed = sum(1 for s in result["scripts"] if s["status"] == "ok")
    failed_n = sum(1 for s in result["scripts"] if s["status"] != "ok")
    store.update_batch_state(
        conn, batch_id, status=BATCH_STATUS_OPEN, parsed_count=parsed, failed_count=failed_n
    )
    return _finalize_if_complete(conn, batch_id, schemas)


def _finalize_if_complete(conn, batch_id: str, schemas: dict[str, list[str]]) -> dict:
    """全部应到脚本成功且缺失项为零 → 组装全量结论并原子切换；否则保留原结论。"""
    view = store.load_batch_view(conn, batch_id)
    batch = view["batch"]
    registered = view["scripts"]
    expected = batch["expected_count"]
    ok_scripts = [s for s in registered if s["status"] == "ok"]
    failed_scripts = [s for s in registered if s["status"] != "ok"]
    missing = expected - len(registered)

    switched = False
    run_id = None
    switch_diff = None
    if not failed_scripts and missing == 0 and len(ok_scripts) == expected:
        previous_run_id = store.get_active_run_id(conn)
        previous = store.load_run(conn, previous_run_id) if previous_run_id else None
        prev_table = previous["table_edges"] if previous else []
        prev_column = previous["column_edges"] if previous else []

        analysis = pipeline.analyze(
            [{"filename": s["filename"], "sql": s["sql"]} for s in registered],
            schemas,
        )
        # 原子切换：落新 run、切 active 指针、更新批次状态在同一事务内，
        # 任一步失败整体回滚，绝不出现在新旧结论之间的中间态。
        with store.transaction(conn):
            run_id = store.save_run(conn, analysis, batch["source_label"],
                                    batch_id=batch_id, commit=False)
            switch_diff = reference_diff(
                prev_table,
                prev_column,
                analysis["table_edges"],
                analysis["column_edges"],
                unresolved=[],
            )
            store.set_active_run_id(conn, run_id, commit=False)
            store.update_batch_state(
                conn,
                batch_id,
                status=BATCH_STATUS_SWITCHED,
                parsed_count=len(ok_scripts),
                failed_count=0,
                active_run_id=run_id,
                previous_run_id=previous_run_id,
                switch_diff=switch_diff,
                commit=False,
            )
        switched = True
    else:
        # 失败批次：结论保持上一版，引用没有发生切换，因此只登记「仍未完成」项，
        # 不做新增/消失判定，避免把半成品误当成结论变化。
        switch_diff = {
            "table": {"added": [], "removed": []},
            "column": {"added": [], "removed": []},
            "unresolved": _unresolved_refs(failed_scripts),
            "summary": {
                "table_added": 0,
                "table_removed": 0,
                "column_added": 0,
                "column_removed": 0,
                "unresolved_count": len(failed_scripts),
            },
        }
        store.update_batch_state(
            conn,
            batch_id,
            status=BATCH_STATUS_OPEN,
            parsed_count=len(ok_scripts),
            failed_count=len(failed_scripts),
            note=(
                f"{len(failed_scripts)} 个脚本未解析、缺失 {missing} 项；"
                "当前血缘/影响面/迁移结论保持上一版，未做切换。"
            ),
        )

    refreshed = store.load_batch_view(conn, batch_id)
    return {
        "batch_id": batch_id,
        "batch_key": refreshed["batch"]["batch_key"],
        "status": refreshed["batch"]["status"],
        "switched": switched,
        "run_id": run_id,
        "active_run_id": store.get_active_run_id(conn),
        "expected_count": expected,
        "parsed_count": len(ok_scripts),
        "failed_count": len(failed_scripts),
        "missing_count": missing,
        "attempts": refreshed["batch"]["attempts"],
        "failures": _unresolved_refs(
            [s for s in refreshed["scripts"] if s["status"] != "ok"]
        ),
        "switch_diff": refreshed["batch"]["switch_diff"],
    }


def _already_switched(conn, batch: dict) -> dict:
    view = store.load_batch_view(conn, batch["batch_id"])
    return {
        "batch_id": batch["batch_id"],
        "batch_key": batch["batch_key"],
        "status": BATCH_STATUS_SWITCHED,
        "switched": True,
        "run_id": batch["active_run_id"],
        "active_run_id": store.get_active_run_id(conn),
        "expected_count": batch["expected_count"],
        "parsed_count": batch["parsed_count"],
        "failed_count": 0,
        "missing_count": 0,
        "attempts": batch["attempts"],
        "failures": [],
        "switch_diff": view["batch"]["switch_diff"],
    }
