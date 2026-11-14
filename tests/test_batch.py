"""批次语义：应到登记、失败不切换、同批重试只处理失败项、引用三向对账。"""
from __future__ import annotations

BROKEN_FILENAME = "13_broken_channel.sql"

FIXED_SQL = """CREATE VIEW v_bad_channel AS
SELECT
  o.channel AS channel,
  ROUND(o.amount, 2) AS amount
FROM ods_orders o;
"""


def _open_batch_id(client) -> str:
    rows = client.get("/api/batches").json()["batches"]
    open_row = next(b for b in rows if b["status"] != "switched")
    return open_row["batch_id"]


def test_broken_batch_registers_all_expected_scripts(isolated_client):
    client = isolated_client
    res = client.post("/api/runs", json={"mode": "sample_broken"})
    assert res.status_code == 422
    batch_id = res.json()["detail"]["batch"]["batch_id"]
    detail = client.get(f"/api/batches/{batch_id}").json()
    # 应到 13 个脚本全部登记，缺失项为 0、失败项 1。
    assert len(detail["scripts"]) == 13
    statuses = {s["filename"]: s["status"] for s in detail["scripts"]}
    assert statuses[BROKEN_FILENAME] == "error"
    assert sum(1 for v in statuses.values() if v == "ok") == 12
    bad = next(s for s in detail["scripts"] if s["filename"] == BROKEN_FILENAME)
    assert bad["error"]["line"] == 5 and bad["error"]["col"] == 8
    assert detail["batch"]["status"] == "open"
    assert detail["active_run_id"]


def test_retry_same_batch_reuses_id_and_only_touches_failures(isolated_client):
    client = isolated_client
    client.post("/api/runs", json={"mode": "sample_broken"})
    batch_id = _open_batch_id(client)

    # 不带修正 SQL 重试：仍是同一批次、attempts 递增、不新增批次行。
    again = client.post(f"/api/batches/{batch_id}/retry", json={}).json()
    assert again["batch_id"] == batch_id
    assert again["switched"] is False
    assert again["attempts"] == 2
    assert again["failed_count"] == 1
    batches = [b for b in client.get("/api/batches").json()["batches"] if b["batch_id"] == batch_id]
    assert len(batches) == 1  # 重试不产生重复批次

    detail = client.get(f"/api/batches/{batch_id}").json()
    bad = next(s for s in detail["scripts"] if s["filename"] == BROKEN_FILENAME)
    good = next(s for s in detail["scripts"] if s["filename"] == "01_dwd_customer.sql")
    assert bad["attempts"] == 2   # 失败项被重复处理
    assert good["attempts"] == 1  # 成功项未被重算


def test_retry_with_fixed_sql_switches_and_lists_diff(isolated_client):
    client = isolated_client
    first = client.post("/api/runs", json={"mode": "sample"}).json()
    active_before = first["active_run_id"]

    client.post("/api/runs", json={"mode": "sample_broken"})
    batch_id = _open_batch_id(client)
    # 修复后重试同一批：只重解析坏脚本，成功后一次性切换。
    outcome = client.post(
        f"/api/batches/{batch_id}/retry",
        json={"scripts": [{"filename": BROKEN_FILENAME, "sql": FIXED_SQL}]},
    ).json()
    assert outcome["switched"] is True
    assert outcome["run_id"] != active_before
    assert outcome["active_run_id"] == outcome["run_id"]
    assert outcome["failed_count"] == 0 and outcome["missing_count"] == 0

    diff = outcome["switch_diff"]
    assert diff["summary"]["unresolved_count"] == 0
    # 修复后的 v_bad_channel 是新出现的目标对象：表级与列级引用都应计入「新增」。
    added_tables = {(e["source_table"], e["target_table"]) for e in diff["table"]["added"]}
    assert ("ods_orders", "v_bad_channel") in added_tables
    added_cols = {
        (e["source_table"], e["source_column"], e["target_table"], e["target_column"])
        for e in diff["column"]["added"]
    }
    assert ("ods_orders", "channel", "v_bad_channel", "channel") in added_cols
    # 旧结论里没有 v_bad_channel，因此不会有关于它的「消失」引用。
    assert all(e["target_table"] != "v_bad_channel" for e in diff["table"]["removed"])

    stored = client.get(f"/api/batches/{batch_id}").json()
    assert stored["batch"]["status"] == "switched"
    assert stored["batch"]["active_run_id"] == outcome["run_id"]


def test_failed_batch_keeps_previous_conclusion_for_impact(isolated_client):
    client = isolated_client
    sample = client.post("/api/runs", json={"mode": "sample"}).json()
    active_id = sample["active_run_id"]

    client.post("/api/runs", json={"mode": "sample_broken"})  # 422，不切换
    health = client.get("/health").json()
    assert health["active_run_id"] == active_id

    # 影响面与迁移仍基于上一版生效结论，可正常产出。
    impact = client.post(
        "/api/impact", json={"run_id": active_id, "table": "ods_orders", "column": "amount"}
    )
    assert impact.status_code == 200
    assert impact.json()["object_count"] == 5


def test_diff_reports_removed_references_when_script_dropped(client, sample_dir, schemas):
    """切换到少一个脚本的批次时，消失的引用必须进入 removed，防止迁移清单误删对象漏判。"""
    from app import batch as batch_mod
    from app import store

    conn = store.connect()
    try:
        files = [
            {"filename": "a.sql", "sql": "CREATE VIEW v_a AS SELECT o.order_id AS order_id FROM ods_orders o;"},
            {"filename": "b.sql", "sql": "CREATE VIEW v_b AS SELECT o.amount AS amount FROM ods_orders o;"},
        ]
        first = batch_mod.submit_batch(conn, files, schemas, "两脚本批次", "custom-set-1")
        assert first["switched"] is True

        smaller = [
            {"filename": "a.sql", "sql": "CREATE VIEW v_a AS SELECT o.order_id AS order_id FROM ods_orders o;"},
        ]
        second = batch_mod.submit_batch(conn, smaller, schemas, "单脚本批次", "custom-set-2")
        assert second["switched"] is True
        removed_t = {(e["source_table"], e["target_table"]) for e in second["switch_diff"]["table"]["removed"]}
        removed_c = {
            (e["source_table"], e["source_column"], e["target_table"], e["target_column"])
            for e in second["switch_diff"]["column"]["removed"]
        }
        assert ("ods_orders", "v_b") in removed_t
        assert ("ods_orders", "amount", "v_b", "amount") in removed_c
        assert second["switch_diff"]["summary"]["unresolved_count"] == 0
    finally:
        conn.close()


def test_retry_does_not_duplicate_edges_from_successful_scripts(client, sample_dir, schemas):
    from app import batch as batch_mod
    from app import store

    files = [
        {"filename": "good.sql", "sql": "CREATE VIEW v_good AS SELECT o.order_id AS order_id FROM ods_orders o;"},
        {"filename": "bad.sql", "sql": "CREATE VIEW v_bad AS SELECT x.id AS id FROM no_such_table x;"},
    ]
    conn = store.connect()
    try:
        out1 = batch_mod.submit_batch(conn, files, schemas, "混合批次", "mix-1")
        assert out1["switched"] is False
        assert len(store.list_batch_edges(conn, out1["batch_id"], "column")) == 1

        fixed = [{"filename": "bad.sql", "sql": "CREATE VIEW v_bad AS SELECT o.amount AS amount FROM ods_orders o;"}]
        out2 = batch_mod.retry_batch(conn, out1["batch_id"], overrides={f["filename"]: f["sql"] for f in fixed})
        assert out2["switched"] is True
        # good.sql 的边只保留一份；bad.sql 的旧失败边已被替换。
        edges = store.list_batch_edges(conn, out1["batch_id"], "column")
        assert len(edges) == 2
        assert {e["script"] for e in edges} == {"good.sql", "bad.sql"}
    finally:
        conn.close()


def test_switch_adds_previously_missing_downstream_to_migration(isolated_client):
    """失败批次修复切换后，此前未知的下游必须进入迁移清单，避免误删受影响对象。"""
    client = isolated_client
    active = client.get("/health").json()["active_run_id"]
    before = client.post(
        "/api/migration",
        json={"run_id": active, "table": "ods_orders", "column": "amount", "operation": "drop"},
    ).json()

    client.post("/api/runs", json={"mode": "sample_broken"})
    batch_id = _open_batch_id(client)
    out = client.post(
        f"/api/batches/{batch_id}/retry",
        json={"scripts": [{"filename": BROKEN_FILENAME, "sql": FIXED_SQL}]},
    ).json()
    assert out["switched"] is True

    after = client.post(
        "/api/migration",
        json={"run_id": out["run_id"], "table": "ods_orders", "column": "amount", "operation": "drop"},
    ).json()
    objects = {o["object"] for o in after["impact"]["downstream"]}
    assert "v_bad_channel" in objects
    assert after["impact"]["object_count"] == before["impact"]["object_count"] + 1


def test_form_retry_flow_renders_failure_then_redirect(isolated_client):
    client = isolated_client
    client.post("/api/runs", json={"mode": "sample_broken"})
    batch_id = _open_batch_id(client)
    # 表单重试（未修复）→ 422 工作台页，带失败定位。
    res = client.post(f"/batches/{batch_id}/retry", data={}, follow_redirects=False)
    assert res.status_code == 422
    assert "括号未闭合" in res.text
    # 修复后表单重试 → 303 跳转新结果页。
    res2 = client.post(
        f"/batches/{batch_id}/retry",
        data={f"sql:{BROKEN_FILENAME}": FIXED_SQL},
        follow_redirects=False,
    )
    assert res2.status_code == 303
    assert res2.headers["location"].startswith("/result/run_")
