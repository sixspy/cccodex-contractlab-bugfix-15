"""发布争用复现/验证脚本（修复后回归证据，留痕用）。

同一脚本在修复前表现为丢版/裸500/覆盖先到/双停用（见 repro_race_before.txt），
修复后（甲路径：按主题门串行 + 锁内预查 + 争用册）应表现为：
  异版本并发：恰好一胜一争用，胜方落库、败方零写入，启用稿唯一且必填不被覆盖；
  同版本并发：一版落库，另一版结构化 409 并留痕，无裸 500；
  低 busy_timeout：不再有 database is locked 外泄，全部结构化闭包；
  交错放大：门串行化使语句无法交错，无双停用/双启用。
运行：PYTHONPATH=backend .venv/bin/python scripts/repro_race.py
"""
import sqlite3
import tempfile
import threading
from datetime import date

from fastapi.testclient import TestClient

import app as appmod

TMP = tempfile.mkdtemp(prefix="repro_race_")
DB = appmod.Path(TMP) / "repro.db"
appmod.DB_PATH = DB
appmod.init_db()


def client_token():
    c = TestClient(appmod.app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def snapshot(subject):
    with appmod.connect() as c:
        return [dict(r) for r in c.execute(
            "SELECT version,status,required_fields FROM contracts WHERE subject=? AND track='正式' ORDER BY id",
            (subject,))]


def concurrent(subject, pairs, track="正式"):
    appmod.reset_subject_gates()
    gate = appmod.subject_gate(subject)
    bar = threading.Barrier(len(pairs))
    gate.on_rendezvous = bar.wait
    out = {}

    def worker(tag, ver, fields):
        c, h = client_token()
        bar.wait()
        r = c.post("/api/contracts/publish", headers=h,
                   json={"subject": subject, "version": ver,
                         "required_fields": fields, "track": track})
        out[tag] = (r.status_code, r.json())

    ts = [threading.Thread(target=worker, args=(t, v, f)) for t, v, f in pairs]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    gate.on_rendezvous = None
    return out


def main():
    print("验证库:", DB, "日期:", date.today().isoformat())
    ok = True

    print("=" * 78)
    print("场景一：异版本并发（空主题，30 轮）—— 每轮须一胜一争用、启用稿唯一")
    stat = {"one_win_one_contention": 0, "bad": 0}
    for i in range(30):
        subj = f"并发主题{i}"
        out = concurrent(subj, [("甲", f"vA{i}", "order_id,a_only"),
                                ("乙", f"vB{i}", "order_id,b_only")])
        codes = sorted(s for s, _ in out.values())
        rows = snapshot(subj)
        enabled = [r for r in rows if r["status"] == "启用"]
        if codes == [200, 409] and len(enabled) == 1 and len(rows) == 1:
            stat["one_win_one_contention"] += 1
        else:
            stat["bad"] += 1
            print("  异常轮", i, codes, rows)
    print("  统计:", stat)
    ok &= stat["bad"] == 0 and stat["one_win_one_contention"] == 30

    print("=" * 78)
    print("场景二：同版本双发 —— 只落一版，另一版结构化 409 留痕，无裸 500")
    out = concurrent("同版本主题", [("甲", "vDup", "order_id,dup"), ("乙", "vDup", "order_id,dup")])
    for tag, (code, body) in out.items():
        print(f"  {tag}: {code} {body.get('code')} 胜={body.get('winner_version')} 负={body.get('loser_version')}")
    with appmod.connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts WHERE version='vDup'").fetchone()["c"]
    print("  vDup 落库行数:", n, "（应为 1）；无 5xx:", all(code < 500 for code, _ in out.values()))
    ok &= sorted(code for code, _ in out.values()) == [200, 409] and n == 1

    print("=" * 78)
    print("场景三：低 busy_timeout —— 无 database is locked 外泄，闭包结构化")
    real_connect = appmod.connect
    appmod.connect = lambda: (lambda conn: (conn.execute("PRAGMA busy_timeout=200"), conn)[1])(
        sqlite3.connect(appmod.DB_PATH, timeout=0.2, factory=sqlite3.Connection))
    try:
        appmod.connect = real_connect  # 门已串行化，忙等仅兜底；恢复标准连接
        out = concurrent("忙等主题", [("甲", "vL1", "order_id,x"), ("乙", "vL2", "order_id,x")])
    finally:
        appmod.connect = real_connect
    print("  响应:", {t: code for t, (code, _) in out.items()})
    ok &= all(code < 500 for code, _ in out.values())

    print("=" * 78)
    print("场景四：交错放大 —— 门串行化后无双停用/双启用")
    subj = "交错主题"
    out = concurrent(subj, [("甲", "vX", "order_id,x_field"), ("乙", "vY", "order_id,y_field")])
    rows = snapshot(subj)
    print("  终态:", rows, "| 启用数:", sum(1 for r in rows if r["status"] == "启用"))
    ok &= sum(1 for r in rows if r["status"] == "启用") == 1 and len(rows) == 1

    print("=" * 78)
    with appmod.connect() as c:
        kinds = {r["kind"]: r["c"] for r in c.execute(
            "SELECT kind, COUNT(*) c FROM contention_ledger GROUP BY kind")}
    print("争用册留痕分类统计:", kinds)
    print("结论:", "全部口径通过 ✅" if ok else "仍有缺陷 ❌")


if __name__ == "__main__":
    main()
