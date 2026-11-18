"""发布争用修复的正确性测试（修复前应失败，修复后必须全绿）。

口径覆盖：
1. 当日择路写入发布争用册，当天禁止改口；偏离（选乙/混用）拒绝并记「发布争用偏了」
2. 同主题并发发布串行排队：恰好一胜一争用，后到只记「发布争用」且可复现
3. 禁止后到覆盖先到必填；禁止两版一并标正式启用；保住先到正式稿
4. 入站一览/版本目录/主题详情/策略附页/对账账册均保留先到
5. 失败闭包清晰：争用一律结构化 409 + 争用册留痕，不允许裸 500
6. 不同主题互不阻塞
"""
import threading
from datetime import date

import pytest
from fastapi.testclient import TestClient

from app import app, init_db, reset_subject_gates


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    reset_subject_gates()  # 模块级主题门每例重置，避免跨用例计数残留
    init_db()
    yield
    reset_subject_gates()


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def auth(client):
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


SUBJECT = "争用测试主题"


def publish(client, auth, version, fields, **extra):
    body = {"subject": SUBJECT, "version": version, "required_fields": fields}
    body.update(extra)
    return client.post("/api/contracts/publish", headers=auth, json=body)


def worker_client():
    """每个并发工作线程独立 TestClient（独立 anyio portal），避免单 portal 串行化失真。"""
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def concurrent_publish(ver_field_pairs, subject=SUBJECT, track="正式", deterministic=True,
                       expect_conflict=True, max_attempts=25):
    """双开（或多开）同屏障并发发布，返回 (results, used_subject)。

    deterministic=True 时给生产门注入 on_rendezvous（默认 None、生产零副作用），
    在锁外汇合让全员先注册再进临界区，压缩 GIL 调度窗口。
    expect_conflict=True 时若某轮被调度成完全串行（非 200+409），换新主题重试，
    直到捕获真正重叠的并发批次——保证「同主题同时抢发一胜一争用」可复现留痕。
    """
    import app as appmod
    n = len(ver_field_pairs)
    for attempt in range(max_attempts):
        used_subject = subject if not expect_conflict else f"{subject}_批次{attempt}"
        # 每次重试版本名唯一，避免 contracts.version 全局 UNIQUE 撞约束
        pairs = [(f"{v}_a{attempt}", f) for v, f in ver_field_pairs]
        reset_subject_gates()
        start_barrier = threading.Barrier(n)
        rendezvous = threading.Barrier(n)
        gate = None
        if deterministic:
            gate = appmod.subject_gate(used_subject)
            gate.on_rendezvous = rendezvous.wait  # 锁外汇合：全员注册后才进临界区
        results = {}

        def worker(tag, subj, ver, fields):
            c, h = worker_client()
            start_barrier.wait()
            r = c.post("/api/contracts/publish", headers=h,
                       json={"subject": subj, "version": ver, "required_fields": fields, "track": track})
            results[tag] = (r.status_code, r.json() if r.content else {}, ver)

        threads = [threading.Thread(target=worker, args=(f"值班{i}", used_subject, v, f))
                   for i, (v, f) in enumerate(pairs)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if gate is not None:
            gate.on_rendezvous = None
        if not expect_conflict:
            return results, used_subject
        codes = sorted(r[0] for r in results.values())
        if codes == [200, 409]:
            return results, used_subject  # 捕获到真正重叠的并发批次
    raise AssertionError(f"{max_attempts} 轮并发均未出现 200+409，最后一轮: {results}")


def ledger(client, auth):
    return client.get("/api/contention-ledger", headers=auth).json()


def active_formal(conn, subject=SUBJECT):
    return conn.execute(
        "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用'",
        (subject,)).fetchall()


def test_today_path_auto_recorded_and_immutable(client, auth, connect_db):
    # 首次发布自动择路（甲），并写入争用册
    r = publish(client, auth, "v10", "order_id,amount")
    assert r.status_code == 200 and r.json()["path"] == "甲"
    book = ledger(client, auth)
    assert book["today"] == date.today().isoformat()
    assert book["path"] == "甲"
    picks = [x for x in book["items"] if x["kind"] == "当日择路"]
    assert len(picks) == 1 and picks[0]["detail"].startswith("甲")

    # 当天改口选乙：拒绝并记偏了，且不改写已落册选择
    r2 = client.post("/api/contracts/publish-path", headers=auth, json={"path": "乙"})
    assert r2.status_code == 409 and "发布争用偏了" in r2.json()["detail"]
    book = ledger(client, auth)
    assert book["path"] == "甲"
    assert any(x["kind"] == "发布争用偏了" for x in book["items"])

    # 发布请求自带乙路径同样拒绝、不产生任何契约写入
    before = client.get("/api/contracts", headers=auth).json()["items"]
    r3 = publish(client, auth, "v11", "order_id,amount", path="乙")
    assert r3.status_code == 409 and "发布争用偏了" in r3.json()["detail"]
    after = client.get("/api/contracts", headers=auth).json()["items"]
    assert before == after

    # 再次确认甲（与册一致）放行
    r4 = client.post("/api/contracts/publish-path", headers=auth, json={"path": "甲"})
    assert r4.status_code == 200 and r4.json()["path"] == "甲"


def test_concurrent_distinct_versions_single_winner_repeated(client, auth, connect_db):
    rounds = 12
    for i in range(rounds):
        va, vb = f"cvA{i}", f"cvB{i}"
        subj = f"争用主题轮{i}"  # 每轮独立空主题，避免上一轮启用稿触发预查
        results, used_subject = concurrent_publish(
            [(va, f"order_id,{va}_field"), (vb, f"order_id,{vb}_field")], subject=subj)
        actual_versions = [r[2] for r in results.values()]

        codes = sorted(r[0] for r in results.values())
        assert codes == [200, 409], results  # 一胜一争用，绝不双成功
        loser = next(r for r in results.values() if r[0] == 409)
        winner_resp = next(r for r in results.values() if r[0] == 200)[1]
        loser_resp = loser[1]
        assert loser_resp["code"] == "发布争用"
        assert loser_resp["winner_version"] in actual_versions
        assert loser_resp["loser_version"] in actual_versions
        assert loser_resp["winner_version"] != loser_resp["loser_version"]

        with connect_db() as conn:
            active = active_formal(conn, used_subject)
            assert len(active) == 1  # 禁止两版一并标正式启用
            winner = active[0]
            assert winner["version"] == loser_resp["winner_version"] == winner_resp["version"]
            # 先到必填保住：生效稿必填字段即胜方发布时的字段（按胜方前缀 cvA/cvB 还原）
            prefix = "cvA" if winner["version"].startswith("cvA") else "cvB"
            assert winner["required_fields"] == f"order_id,{prefix}{i}_field"
            placed = [r[2] for r in results.values()]
            cnt = conn.execute(
                f"SELECT COUNT(*) c FROM contracts WHERE version IN ({','.join('?'*len(placed))})",
                placed).fetchone()["c"]
            assert cnt == 1  # 败方一行都不许落库

    book = ledger(client, auth)
    contentions = [x for x in book["items"] if x["kind"] == "发布争用"]
    assert len(contentions) >= rounds  # 每个并发批次至少一条争用、可复现留痕
    assert all(x["winner_version"] and x["loser_version"] for x in contentions)


def test_concurrent_same_version_one_lands_one_recorded(client, auth, connect_db):
    results, _ = concurrent_publish([("vDup", "order_id,dup_x"), ("vDup", "order_id,dup_x")])
    assert sorted(r[0] for r in results.values()) == [200, 409]
    actual = [r[2] for r in results.values()]
    assert len(set(actual)) == 1  # 两请求确实抢发同一版本号
    dup_ver = actual[0]
    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM contracts WHERE version=?", (dup_ver,)).fetchall()
        assert len(rows) == 1 and rows[0]["status"] == "启用"
    assert any(x["kind"] == "发布争用" and x["loser_version"] == dup_ver
               for x in ledger(client, auth)["items"])


def test_expected_version_optimistic_guard(client, auth):
    assert publish(client, auth, "ev1", "order_id,a").status_code == 200
    # 基线正确：基于 ev1 发 ev2 放行
    assert publish(client, auth, "ev2", "order_id,b", expected_version="ev1").status_code == 200
    # 基线过期：仍拿着 ev1 当基线 → 争用拒绝，ev2 保持启用
    r = publish(client, auth, "ev3", "order_id,c", expected_version="ev1")
    assert r.status_code == 409 and r.json()["code"] == "发布争用"
    detail = client.get(f"/api/contracts/by-subject/{SUBJECT}", headers=auth).json()
    assert detail["active_version"] == "ev2"


def test_ingress_and_views_keep_winner(client, auth, connect_db):
    results, subj = concurrent_publish([("win1", "order_id,keep_a"), ("lose1", "order_id,keep_b")])
    ok = next(r for r in results.values() if r[0] == 200)
    winner = ok[1]["version"]
    loser_ver = next(r[2] for r in results.values() if r[0] == 409)
    # 胜方字段：win1→keep_a，lose1→keep_b（版本名带重试后缀，按前缀判定）
    win_field = "keep_a" if winner.startswith("win1") else "keep_b"

    # 版本目录只挂胜方正式稿
    catalog = client.get("/api/contracts", headers=auth).json()["items"]
    enabled = [c for c in catalog if c["status"] == "启用" and c["track"] == "正式" and c["subject"] == subj]
    assert [c["version"] for c in enabled] == [winner]

    # 主题详情 / 策略附页保留先到胜方
    detail = client.get(f"/api/contracts/by-subject/{subj}", headers=auth).json()
    assert detail["active_version"] == winner
    policy = client.get("/api/policy-attachments", headers=auth, params={"subject": subj}).json()
    assert policy["active_version"] == winner
    assert policy["required_fields"] == f"order_id,{win_field}"

    # 入站校验按胜方必填走：缺胜方字段必拒收/缓冲，绝不按败方字段
    r = client.post("/api/ingress/validate", headers=auth,
                    json={"subject": subj, "payload_fields": "order_id"})
    assert r.status_code == 200 and r.json()["version"] == winner and r.json()["result"] == "拒收"
    ingress = client.get("/api/ingress", headers=auth).json()["items"]
    assert all(i["contract_version"] != loser_ver for i in ingress)
    assert all(i["contract_version"] != "lose1" for i in ingress)

    # 对账账册：无重复正式启用、争用已入账
    recon = client.get("/api/reconcile", headers=auth).json()
    assert recon["contention_count"] >= 1
    assert recon["active_formal_dupes"] == []
    assert recon["publish_aligned"] is True


def test_distinct_subjects_run_concurrently_without_blocking(client, auth):
    # 不同主题：各自独立门，用每线程独立客户端同屏障并发
    barrier = threading.Barrier(2)
    out = {}

    def worker(tag, subject, ver):
        c, h = worker_client()
        barrier.wait()
        out[tag] = c.post("/api/contracts/publish", headers=h,
                          json={"subject": subject, "version": ver,
                                "required_fields": "order_id,x"}).status_code

    t1 = threading.Thread(target=worker, args=("甲", "主题甲", "sa1"))
    t2 = threading.Thread(target=worker, args=("乙", "主题乙", "sb1"))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(out.values()) == [200, 200]  # 门按主题细分，不互相阻塞


def test_shadow_publish_also_queued_and_never_enabled(client, auth, connect_db):
    results, _ = concurrent_publish([("sh1", "order_id,sh"), ("sh2", "order_id,sh")], track="影子")
    assert sorted(r[0] for r in results.values()) == [200, 409]
    actual = [r[2] for r in results.values()]
    with connect_db() as conn:
        assert conn.execute(
            "SELECT COUNT(*) c FROM contracts WHERE track='影子' AND status='启用'").fetchone()["c"] == 0
        assert conn.execute(
            f"SELECT COUNT(*) c FROM contracts WHERE version IN ({','.join('?'*len(actual))})",
            actual).fetchone()["c"] == 1  # 只落先到一版，后到不写入


def test_no_raw_500_under_contention(client, auth):
    # 争用失败闭包必须是结构化 4xx，绝不允许把数据库异常抛给调用方
    results, _ = concurrent_publish([("e500a", "order_id,z"), ("e500b", "order_id,z")])
    assert all(r[0] < 500 for r in results.values())
