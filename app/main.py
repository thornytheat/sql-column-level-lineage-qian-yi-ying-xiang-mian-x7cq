"""FastAPI 入口：路由保持薄，解析 / 血缘 / 图算法 / 迁移清单都在纯函数模块里。"""
from __future__ import annotations

import json
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import batch as batch_mod
from . import graph, migrate, pipeline, report, seed, settings, store

BASE = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE / "templates"))


def get_conn() -> sqlite3.Connection:
    return store.connect()


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = get_conn()
    try:
        store.init_schema(conn)
        try:
            app.state.seed_info = seed.ensure_seed(conn)
        except FileNotFoundError:
            app.state.seed_info = {"seeded": False, "reason": "未找到 sample_data，跳过 seed"}
    finally:
        conn.close()
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")


# --------------------------------------------------------------------------
# 通用
# --------------------------------------------------------------------------

def base_context(request: Request, active: str) -> dict:
    conn = get_conn()
    try:
        stats = store.stats(conn)
    finally:
        conn.close()
    return {
        "request": request,
        "app_name": settings.APP_NAME,
        "app_short": settings.APP_SHORT,
        "app_desc": settings.APP_DESC,
        "active": active,
        "page": active,
        "stats": stats,
    }


def _run_graph(conn, run_id: str):
    data = store.load_run(conn, run_id)
    if data is None:
        return None, None
    levels = graph.topo_levels(graph.table_adjacency(data["table_edges"]))
    return data, levels


def _active_run_id(conn) -> str | None:
    """当前生效结论；未登记或指向失效记录时回退最近一条 run（兼容老库）。"""
    run_id = store.get_active_run_id(conn)
    if run_id and store.load_run(conn, run_id):
        return run_id
    runs = store.list_runs(conn, limit=1)
    return runs[0]["run_id"] if runs else None


def _catalog(conn) -> dict[str, list[str]]:
    schemas = store.load_schemas(conn)
    run_id = _active_run_id(conn)
    if run_id:
        data = store.load_run(conn, run_id)
        if data:
            for e in data["table_edges"]:
                schemas.setdefault(e["target_table"], [])
            for s in data["scripts"]:
                if s["status"] == "ok":
                    schemas[s["target"]] = s["columns"]
    return schemas


def _errors_payload(analysis: dict) -> list[dict]:
    errors = []
    for s in analysis["scripts"]:
        if s["status"] != "error":
            continue
        errors.append(
            {
                "filename": s["filename"],
                "line": s.get("error_line"),
                "col": s.get("error_col"),
                "message": s.get("message") or "解析失败",
            }
        )
    for d in analysis["diagnostics"]:
        errors.append(
            {
                "filename": d["filename"],
                "line": d["line"],
                "col": d["col"],
                "message": d["message"],
            }
        )
    return errors


def _execute(conn, mode: str, sql: str, filename: str) -> tuple[str | None, dict, str | None, dict | None]:
    """返回 (run_id, analysis, error_message, batch_outcome)。

    样例批次走批次登记：只有全部脚本解析成功且零缺失才切换生效结论；
    失败时 run_id 为 None、原结论保留，batch_outcome 带失败位置与未完成引用。
    自定义单脚本仍是独立 run（不参与批次切换，避免污染生效结论）；
    全部解析失败 → 不落库并带诊断返回。
    """
    if mode in {"sample", "sample_broken"}:
        files = pipeline.load_sample_files(settings.SAMPLE_DIR, include_broken=(mode == "sample_broken"))
        schemas = pipeline.load_schemas(settings.SAMPLE_DIR)
        label = pipeline.SAMPLE_WITH_BROKEN_LABEL if mode == "sample_broken" else pipeline.SAMPLE_LABEL
        outcome = batch_mod.submit_batch(conn, files, schemas, label, mode)
        if outcome["switched"]:
            data = store.load_run(conn, outcome["run_id"])
            analysis = {
                "scripts": data["scripts"],
                "diagnostics": [],
                "parsed_count": data["run"]["parsed_count"],
                "failed_count": 0,
            }
            return outcome["run_id"], analysis, None, outcome
        # 失败批次：不产生新 run、不切换，保留原结论与失败位置。
        analysis = {
            "scripts": [],
            "diagnostics": [
                {
                    "filename": f["filename"],
                    "line": f["line"],
                    "col": f["col"],
                    "message": f["message"],
                }
                for f in outcome["failures"]
            ],
            "parsed_count": 0,
            "failed_count": len(outcome["failures"]),
        }
        msg = (
            f"批次 {outcome['parsed_count']}/{outcome['expected_count']} 解析成功，"
            f"{outcome['failed_count']} 个脚本未通过、缺失 {outcome['missing_count']} 项；"
            "血缘/影响面/迁移结论保持上一版，未切换。修复后可在工作台重试该批次（只处理失败项）。"
        )
        return None, analysis, msg, outcome

    if not sql or not sql.strip():
        return None, {}, "SQL 不能为空：请粘贴 CREATE TABLE/VIEW ... AS SELECT 或 INSERT INTO ... SELECT", None
    files = [{"filename": (filename or "自定义脚本.sql").strip() or "自定义脚本.sql", "sql": sql}]
    schemas = pipeline.load_schemas(settings.SAMPLE_DIR)
    if not schemas:
        schemas = store.load_schemas(conn)
    label = f"自定义录入 · {files[0]['filename']}"
    analysis = pipeline.analyze(files, schemas)
    if analysis["parsed_count"] == 0:
        first = next((s for s in analysis["scripts"] if s["status"] == "error"), None)
        msg = first["message"] if first else "全部脚本解析失败"
        return None, analysis, f"全部 {len(files)} 个脚本都未能解析：{msg}", None
    run_id = pipeline.persist(conn, analysis, label)
    return run_id, analysis, None, None


def _workbench_context(conn, ctx: dict) -> dict:
    """工作台右栏只读装配：生效结论的诊断 / 血缘 / 环，以及未关闭批次的失败项。"""
    active_id = _active_run_id(conn)
    latest = ctx["stats"]["latest_run"]
    failures: list[dict] = []
    column_groups: list[dict] = []
    cycles: list[dict] = []
    active_run = None
    if active_id:
        run_data, _ = _run_graph(conn, active_id)
        if run_data:
            active_run = run_data["run"]
            failures = report.failed_scripts(run_data["scripts"])
            column_groups = report.grouped_columns(run_data["column_edges"])
            cycles = [c for c in run_data["cycles"] if c["level"] == "table"]

    open_batches = [b for b in store.list_batches(conn, limit=10) if b["status"] != "switched"]
    open_views = []
    for b in open_batches:
        view = store.load_batch_view(conn, b["batch_id"])
        open_views.append(
            {
                "batch": view["batch"],
                "failures": report.failed_scripts(view["scripts"]),
                "scripts": view["scripts"],
            }
        )
    ctx.update(
        {
            "scripts": store.list_scripts(conn),
            "runs": store.list_runs(conn, limit=8),
            "latest": latest,
            "active_run": active_run,
            "active_run_id": active_id,
            "failures": failures,
            "column_groups": column_groups,
            "cycles": cycles,
            "open_batches": open_views,
        }
    )
    return ctx


# --------------------------------------------------------------------------
# 页面
# --------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    ctx = base_context(request, "index")
    conn = get_conn()
    try:
        scripts = store.list_scripts(conn)
        latest = ctx["stats"]["latest_run"]
        run_data = None
        cycles = []
        if latest:
            run_data, _ = _run_graph(conn, latest["run_id"])
            cycles = run_data["cycles"] if run_data else []
        ctx.update(
            {
                "scripts": scripts,
                "schemas": store.load_schemas(conn),
                "latest": latest,
                "active_run_id": _active_run_id(conn),
                "runs": store.list_runs(conn, limit=6),
                "run_data": run_data,
                "cycles": cycles,
            }
        )
    finally:
        conn.close()
    return templates.TemplateResponse(request, "index.html", ctx)


@app.get("/workbench", response_class=HTMLResponse)
def workbench(request: Request):
    conn = get_conn()
    try:
        ctx = _workbench_context(conn, base_context(request, "workbench"))
    finally:
        conn.close()
    return templates.TemplateResponse(request, "workbench.html", ctx)


@app.post("/runs/submit")
async def submit_run_form(request: Request):
    form = await request.form()
    mode = str(form.get("mode") or "sample")
    sql = str(form.get("sql") or "")
    filename = str(form.get("filename") or "自定义脚本.sql")
    conn = get_conn()
    try:
        run_id, analysis, err, outcome = _execute(conn, mode, sql, filename)
        if err or run_id is None:
            ctx = _workbench_context(conn, base_context(request, "workbench"))
            ctx.update(
                {
                    "error": err,
                    "diagnostics": _errors_payload(analysis),
                    "sql": sql,
                    "filename": filename,
                    "batch_outcome": outcome,
                }
            )
            return templates.TemplateResponse(request, "workbench.html", ctx, status_code=422)
    finally:
        conn.close()
    return RedirectResponse(url=f"/result/{run_id}", status_code=303)


@app.post("/batches/{batch_id}/retry")
async def retry_batch_form(request: Request, batch_id: str):
    """页面表单：重试未关闭批次，只处理失败项；支持随表提交修正后的 SQL。"""
    form = await request.form()
    overrides: dict[str, str] = {}
    for key, value in form.items():
        if key.startswith("sql:") and str(value).strip():
            overrides[key[4:]] = str(value)
    conn = get_conn()
    try:
        if store.get_batch(conn, batch_id) is None:
            ctx = _workbench_context(conn, base_context(request, "workbench"))
            ctx["error"] = f"找不到批次 {batch_id}"
            return templates.TemplateResponse(request, "workbench.html", ctx, status_code=404)
        schemas = pipeline.load_schemas(settings.SAMPLE_DIR) or store.load_schemas(conn)
        outcome = batch_mod.retry_batch(conn, batch_id, overrides=overrides or None, schemas=schemas)
        if outcome["switched"]:
            return RedirectResponse(url=f"/result/{outcome['run_id']}", status_code=303)
        ctx = _workbench_context(conn, base_context(request, "workbench"))
        ctx.update(
            {
                "error": (
                    f"批次仍有 {outcome['failed_count']} 个脚本未通过（第 {outcome['attempts']} 次尝试），"
                    "生效结论保持上一版；请按下方失败位置修复后再次重试。"
                ),
                "diagnostics": [
                    {"filename": f["filename"], "line": f["line"], "col": f["col"], "message": f["message"]}
                    for f in outcome["failures"]
                ],
                "batch_outcome": outcome,
            }
        )
        return templates.TemplateResponse(request, "workbench.html", ctx, status_code=422)
    finally:
        conn.close()


@app.get("/result/{run_id}", response_class=HTMLResponse)
def result_page(request: Request, run_id: str):
    conn = get_conn()
    try:
        data, levels = _run_graph(conn, run_id)
        if data is None:
            ctx = base_context(request, "result")
            ctx["error"] = f"找不到运行记录 {run_id}，请回工作台重新解析。"
            return templates.TemplateResponse(request, "result.html", ctx, status_code=404)
        run = data["run"]
        groups = report.grouped_columns(data["column_edges"])
        failures = report.failed_scripts(data["scripts"])
        switch_diff = None
        if run.get("batch_id"):
            batch = store.get_batch(conn, run["batch_id"])
            if batch and batch.get("switch_diff_json"):
                switch_diff = json.loads(batch["switch_diff_json"])
        table_groups: dict[str, list[dict]] = {}
        for e in data["table_edges"]:
            table_groups.setdefault(e["target_table"], []).append(e)
        seed_rows = [
            {"table": e["source_table"], "column": e["source_column"], "script": e["script"], "line": e["line"]}
            for e in data["column_edges"]
        ][:40]
        ctx = base_context(request, "result")
        ctx.update(
            {
                "run": run,
                "run_id": run_id,
                "scripts": data["scripts"],
                "column_groups": groups,
                "table_groups": dict(sorted(table_groups.items())),
                "cycles": [c for c in data["cycles"] if c["level"] == "table"],
                "column_cycles": [c for c in data["cycles"] if c["level"] == "column"],
                "failures": failures,
                "levels": levels,
                "switch_diff": switch_diff,
                "seed_rows": seed_rows,
                "star_edge_count": sum(1 for e in data["column_edges"] if e["star_expanded"]),
                "transform_edge_count": sum(1 for e in data["column_edges"] if e["transforms"]),
                "union_edge_count": sum(1 for e in data["column_edges"] if e["union_branch"]),
            }
        )
        return templates.TemplateResponse(request, "result.html", ctx)
    finally:
        conn.close()


@app.get("/review", response_class=HTMLResponse)
def review_page(
    request: Request,
    run_id: str | None = None,
    table: str | None = None,
    column: str | None = None,
    op: str = "drop",
    new_name: str | None = None,
):
    conn = get_conn()
    try:
        runs = store.list_runs(conn, limit=12)
        if not runs:
            ctx = base_context(request, "review")
            ctx["error"] = "还没有任何解析记录，请先到工作台执行样例解析。"
            return templates.TemplateResponse(request, "review.html", ctx)
        active_id = _active_run_id(conn)
        target_run = run_id or active_id or runs[0]["run_id"]
        data, levels = _run_graph(conn, target_run)
        if data is None:
            ctx = base_context(request, "review")
            ctx["error"] = f"找不到运行记录 {run_id}，已回退到当前生效结论。"
            target_run = active_id or runs[0]["run_id"]
            data, levels = _run_graph(conn, target_run)
        catalog = _catalog(conn)
        table_options = sorted({e["target_table"] for e in data["table_edges"]} | set(store.load_schemas(conn)))
        table_choice = table or "ods_orders"
        columns = catalog.get(table_choice, [])
        column_choice = column if column is not None and column != "" else (columns[2] if len(columns) > 2 else (columns[0] if columns else ""))
        impact = None
        migration = None
        page_error = None
        if table_choice:
            impact = graph.impact(
                data["table_edges"], data["column_edges"], levels, table_choice, column_choice or None
            )
            if not impact["found"]:
                page_error = impact["message"]
            migration = migrate.plan_migration(impact, op, new_name, data["cycles"])
        ctx = base_context(request, "review")
        ctx.update(
            {
                "runs": runs,
                "run_id": target_run,
                "active_run_id": active_id,
                "run": data["run"],
                "table_options": table_options,
                "table": table_choice,
                "columns": columns,
                "column": column_choice,
                "op": op,
                "new_name": new_name or "",
                "impact": impact,
                "migration": migration,
                "cycles": data["cycles"],
                "error": page_error,
            }
        )
        return templates.TemplateResponse(request, "review.html", ctx)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------

@app.get("/health")
def health():
    conn = get_conn()
    try:
        stats = store.stats(conn)
    finally:
        conn.close()
    return {
        "status": "ok",
        "app": settings.APP_SHORT,
        "seeded": stats["sample_scripts"] > 0,
        "sample_scripts": stats["sample_scripts"],
        "broken_scripts": stats["broken_scripts"],
        "schema_tables": stats["schema_tables"],
        "runs": stats["runs"],
        "active_run_id": stats["active_run_id"],
    }


@app.post("/api/runs")
def api_create_run(payload: dict = Body(default={})):
    mode = str(payload.get("mode") or "sample")
    sql = str(payload.get("sql") or "")
    filename = str(payload.get("filename") or "自定义脚本.sql")
    if mode not in {"sample", "sample_broken", "custom"}:
        raise HTTPException(status_code=400, detail=f"不支持的模式 {mode}；可选 sample / sample_broken / custom")
    conn = get_conn()
    try:
        run_id, analysis, err, outcome = _execute(conn, mode, sql, filename)
        if run_id is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "message": err,
                    "errors": _errors_payload(analysis),
                    "batch": outcome,
                },
            )
        data = store.load_run(conn, run_id)
        return {
            "run_id": run_id,
            "source_label": data["run"]["source_label"],
            "script_count": data["run"]["script_count"],
            "parsed_count": data["run"]["parsed_count"],
            "failed_count": data["run"]["failed_count"],
            "table_edge_count": data["run"]["table_edge_count"],
            "column_edge_count": data["run"]["column_edge_count"],
            "cycle_count": data["run"]["cycle_count"],
            "diagnostics": _errors_payload(analysis),
            "batch": outcome,
            "active_run_id": store.get_active_run_id(conn),
        }
    finally:
        conn.close()


@app.get("/api/runs")
def api_list_runs(limit: int = 10):
    conn = get_conn()
    try:
        return {"runs": store.list_runs(conn, limit=max(1, min(limit, 50)))}
    finally:
        conn.close()


@app.get("/api/batches")
def api_list_batches(limit: int = 20):
    conn = get_conn()
    try:
        rows = store.list_batches(conn, limit=max(1, min(limit, 100)))
        for b in rows:
            b["switch_diff"] = (
                json.loads(b["switch_diff_json"]) if b.get("switch_diff_json") else None
            )
        return {"batches": rows, "active_run_id": store.get_active_run_id(conn)}
    finally:
        conn.close()


@app.get("/api/batches/{batch_id}")
def api_get_batch(batch_id: str):
    conn = get_conn()
    try:
        view = store.load_batch_view(conn, batch_id)
        if view is None:
            raise HTTPException(status_code=404, detail=f"找不到批次 {batch_id}")
        view["scripts"] = [
            {
                "filename": s["filename"],
                "target": s["target"],
                "kind": s["kind"],
                "status": s["status"],
                "columns": s["columns"],
                "attempts": s["attempts"],
                "error": (
                    {"line": s.get("error_line"), "col": s.get("error_col"), "message": s.get("message")}
                    if s["status"] != "ok"
                    else None
                ),
            }
            for s in view["scripts"]
        ]
        view["active_run_id"] = store.get_active_run_id(conn)
        return view
    finally:
        conn.close()


@app.post("/api/batches/{batch_id}/retry")
def api_retry_batch(batch_id: str, payload: dict = Body(default={})):
    """补齐后重试同一批：只处理失败项，不新增重复批次；可携带修正后的 SQL。"""
    conn = get_conn()
    try:
        if store.get_batch(conn, batch_id) is None:
            raise HTTPException(status_code=404, detail=f"找不到批次 {batch_id}")
        raw_overrides = payload.get("scripts") or []
        overrides: dict[str, str] = {}
        for item in raw_overrides:
            name = item.get("filename")
            sql_text = item.get("sql")
            if name and sql_text:
                overrides[str(name)] = str(sql_text)
        outcome = batch_mod.retry_batch(conn, batch_id, overrides=overrides or None,
                                        schemas=pipeline.load_schemas(settings.SAMPLE_DIR) or store.load_schemas(conn))
        return outcome
    finally:
        conn.close()


@app.get("/api/runs/{run_id}")
def api_get_run(run_id: str):
    conn = get_conn()
    try:
        data, levels = _run_graph(conn, run_id)
        if data is None:
            raise HTTPException(status_code=404, detail=f"找不到运行记录 {run_id}")
        return {
            "run": data["run"],
            "scripts": [
                {
                    "filename": s["filename"],
                    "kind": s["kind"],
                    "target": s["target"],
                    "status": s["status"],
                    "columns": s["columns"],
                    "error": (
                        {"line": s.get("error_line"), "col": s.get("error_col"), "message": s.get("message")}
                        if s["status"] == "error"
                        else None
                    ),
                }
                for s in data["scripts"]
            ],
            "table_edges": data["table_edges"],
            "column_edge_count": len(data["column_edges"]),
            "column_groups": report.grouped_columns(data["column_edges"]),
            "cycles": data["cycles"],
            "levels": {k: v for k, v in levels.items()},
        }
    finally:
        conn.close()


@app.get("/api/tables")
def api_tables(run_id: str | None = None):
    conn = get_conn()
    try:
        catalog = _catalog(conn)
        kind_map: dict[str, str] = {}
        if run_id:
            data, _ = _run_graph(conn, run_id)
            if data:
                for e in data["table_edges"]:
                    kind_map[e["target_table"]] = e["target_kind"]
        return {
            "tables": [
                {"name": name, "kind": kind_map.get(name, "table"), "columns": cols}
                for name, cols in sorted(catalog.items())
            ]
        }
    finally:
        conn.close()


def _impact_for(run_id: str, table: str, column: str | None) -> tuple[dict, dict, dict]:
    conn = get_conn()
    try:
        data, levels = _run_graph(conn, run_id)
        if data is None:
            raise HTTPException(status_code=404, detail=f"找不到运行记录 {run_id}")
        active_id = _active_run_id(conn)
        # 影响面 / 迁移清单只能基于当前生效结论，避免失败批次的半成品导致漏删下游。
        if active_id and run_id != active_id:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"运行 {run_id} 不是当前生效结论（active_run_id={active_id}）；"
                    "失败批次不会切换血缘与迁移方案，请改用生效结论或先把批次重试到全部成功。"
                ),
            )
        if not table:
            raise HTTPException(status_code=400, detail="缺少 table 参数（例如 ods_orders）")
        impact = graph.impact(data["table_edges"], data["column_edges"], levels, table, column or None)
        return data, levels, impact
    finally:
        conn.close()


@app.post("/api/impact")
def api_impact(payload: dict = Body(default={})):
    run_id = str(payload.get("run_id") or "")
    table = str(payload.get("table") or "")
    column = payload.get("column")
    column = str(column) if column not in (None, "") else None
    data, levels, impact = _impact_for(run_id, table, column)
    if not impact["found"]:
        raise HTTPException(status_code=400, detail=impact["message"])
    return impact


@app.post("/api/migration")
def api_migration(payload: dict = Body(default={})):
    run_id = str(payload.get("run_id") or "")
    table = str(payload.get("table") or "")
    column = payload.get("column")
    column = str(column) if column not in (None, "") else None
    operation = str(payload.get("operation") or "drop")
    new_name = payload.get("new_name")
    new_name = str(new_name) if new_name not in (None, "") else None
    if operation not in {"drop", "rename"}:
        raise HTTPException(status_code=400, detail=f"不支持的操作 {operation}；可选 drop / rename")
    if not column:
        raise HTTPException(status_code=400, detail="迁移清单必须指定列，例如 ods_orders.amount")
    if operation == "rename" and not new_name:
        raise HTTPException(status_code=400, detail="改名操作必须提供 new_name")
    data, levels, impact = _impact_for(run_id, table, column)
    if not impact["found"]:
        raise HTTPException(status_code=400, detail=impact["message"])
    plan = migrate.plan_migration(impact, operation, new_name, data["cycles"])
    plan["run_id"] = run_id
    plan["impact"] = impact
    return plan


@app.get("/api/runs/{run_id}/export")
def api_export(run_id: str, format: str = "json"):
    conn = get_conn()
    try:
        data = store.load_run(conn, run_id)
        if data is None:
            raise HTTPException(status_code=404, detail=f"找不到运行记录 {run_id}")
        if format == "csv":
            body = report.export_csv(data["column_edges"])
            return Response(
                content=body,
                media_type="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": f'attachment; filename="lineagex_{run_id}_column_edges.csv"'
                },
            )
        payload = report.export_payload(
            data["run"], data["scripts"], data["table_edges"], data["column_edges"], data["cycles"]
        )
        return Response(
            content=report.dump_json(payload),
            media_type="application/json; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="lineagex_{run_id}.json"'
            },
        )
    finally:
        conn.close()


@app.get("/api/runs/{run_id}/impact_export")
def api_impact_export(run_id: str, table: str, column: str | None = None, operation: str = "drop", new_name: str | None = None):
    data, levels, impact = _impact_for(run_id, table, column)
    if not impact["found"]:
        raise HTTPException(status_code=400, detail=impact["message"])
    plan = migrate.plan_migration(impact, operation, new_name, data["cycles"])
    return JSONResponse(
        content={"impact": impact, "migration": plan},
        headers={
            "Content-Disposition": f'attachment; filename="lineagex_{table}_{column or "all"}_impact.json"'
        },
    )
