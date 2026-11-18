
import hashlib, secrets, sqlite3, threading
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}


class SubjectGate:
    """甲路径的按主题串行门（注册-汇合-临界区 两阶段）。

    生产语义（on_rendezvous=None，零行为变化）：
    - 同主题发布在同一把锁上严格串行，临界区读-查-写单事务，杜绝交错；
    - 阶段一在锁内登记本请求版本并快照此前已在场的先到版本 prior：
      prior 非空即并发后到，待阶段二取锁、先到完成后只记发布争用、不写入；
      无在场先到（prior 为空）的合法串行发布正常放行；
    - 不同主题各自独立一把锁，互不阻塞。

    可复现测试支持（on_rendezvous 注入 Barrier.wait）：
    - 注册在锁内登记后立刻释放锁，再在「锁外」汇合，保证并发请求都完成注册才进临界区，
      消除 GIL 把两请求完全串行化的窗口；钩子不在持锁内等待，不会停滞或死锁。
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._holders: list[str] = []  # 当前已注册在场版本（按注册次序），供后到者识别先到胜方
        self.on_rendezvous = None  # 默认 None=纯生产路径；测试可注入 Barrier.wait

    @contextmanager
    def acquire(self, version: str | None = None):
        # 阶段一：锁内注册，捕获进入前同主题已在场的先到版本
        with self._lock:
            prior = list(self._holders)
            self._holders.append(version)
        # 锁外汇合（生产为 no-op），不占用门的锁
        if self.on_rendezvous is not None:
            self.on_rendezvous()
        # 阶段二：重新取锁，同主题发布严格串行进入临界区
        self._lock.acquire()
        try:
            # prior 非空即并发后到；胜方取最早注册的先到版本（注册次序即到达次序）
            yield (prior[0] if prior else None)
        finally:
            if version in self._holders:
                self._holders.remove(version)
            self._lock.release()


_GATES: "defaultdict[str, SubjectGate]" = defaultdict(SubjectGate)
_GATES_GUARD = threading.Lock()
# 当日发布写路径只允许择定一次；争用册路径选择以此为进程内闸，DB 行做持久落册
_PATH_LOCK = threading.RLock()
# 仅甲路径实现并启用；乙路径（版本号条件更新）按争用册约束整体禁用，混用即偏了
CANONICAL_PATHS = ("甲", "乙")
ENABLED_PATH = "甲"


def subject_gate(subject: str) -> SubjectGate:
    with _GATES_GUARD:
        return _GATES[subject]


def reset_subject_gates() -> None:
    """清空主题门单例。仅测试在每例开始时调用，避免模块级门跨用例残留计数。"""
    with _GATES_GUARD:
        _GATES.clear()

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")  # 跨进程撞写锁时排队等待而非立刻抛 locked
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contention_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          day TEXT NOT NULL,
          kind TEXT NOT NULL,
          subject TEXT,
          winner_version TEXT,
          loser_version TEXT,
          operator TEXT,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        now = datetime.now().isoformat(timespec="seconds")
        if missing:
            if ctr["mode"] == "兼容":
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}


# ===== 发布事务层：甲路径（按主题加锁 → 锁内预查冲突 → 拒绝后到且不写入）=====
class PublishIn(BaseModel):
    subject: str
    version: str
    required_fields: str
    mode: str = "严格"
    track: str = "正式"          # 正式 / 影子
    expected_version: str | None = None  # 调用方认定的当前启用正式稿基线
    path: str | None = None      # 显式声明写路径；须与当日争用册一致，否则记偏了


class PathIn(BaseModel):
    path: str


class ContentionHTTP(HTTPException):
    """争用类失败统一闭包：结构化 409，调用方拿到胜负双方与原因。"""
    def __init__(self, code: str, detail: str, winner: str | None = None,
                 loser: str | None = None, subject: str | None = None):
        super().__init__(409, detail)
        self.payload = {"code": code, "detail": detail, "winner_version": winner,
                        "loser_version": loser, "subject": subject}


def _ledger_write(conn, kind: str, detail: str, subject: str | None = None,
                  winner: str | None = None, loser: str | None = None,
                  operator: str | None = None) -> None:
    conn.execute(
        """INSERT INTO contention_ledger(day,kind,subject,winner_version,loser_version,operator,detail,created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (date.today().isoformat(), kind, subject, winner, loser, operator,
         detail, datetime.now().isoformat(timespec="seconds")))


def _today_pick(conn):
    return conn.execute(
        "SELECT * FROM contention_ledger WHERE day=? AND kind='当日择路' ORDER BY id LIMIT 1",
        (date.today().isoformat(),)).fetchone()


def _ensure_today_path(conn, requested: str | None, operator: str) -> str:
    """当日择定一次写入争用册，当天禁止改口；偏离（选乙/混用）拒绝并记偏了。

    注意：争用册留痕必须先 commit，再抛 ContentionHTTP——外层会 rollback 业务事务，
    未提交的留痕会被一并抹掉。
    """
    pick = _today_pick(conn)
    if pick is None:
        chosen = requested or ENABLED_PATH
        if chosen != ENABLED_PATH:
            _ledger_write(conn, "发布争用偏了",
                          f"当日首请写路径={chosen or '空'}，本仓仅启用甲路径，拒绝并保持未择路",
                          operator=operator)
            conn.commit()
            raise ContentionHTTP("发布争用偏了",
                                 "发布争用偏了：本仓发布争用册仅启用甲路径（按主题加锁预查），乙路径禁用，禁止两条写路径混用")
        _ledger_write(conn, "当日择路", f"甲：按主题加锁后预查冲突，拒绝后到且不写入；乙路径当日禁用",
                      operator=operator)
        conn.commit()
        return ENABLED_PATH
    chosen = pick["detail"][0]
    if requested is not None and requested != chosen:
        _ledger_write(conn, "发布争用偏了",
                      f"请求写路径={requested} 与当日争用册={chosen} 不一致，拒绝且不改册",
                      operator=operator)
        conn.commit()
        raise ContentionHTTP("发布争用偏了",
                             f"发布争用偏了：当日已择定{chosen}路径，当天禁止改口，禁止两条写路径混用")
    if requested is not None and requested not in CANONICAL_PATHS:
        _ledger_write(conn, "发布争用偏了", f"未知写路径={requested}，拒绝", operator=operator)
        conn.commit()
        raise ContentionHTTP("发布争用偏了", f"发布争用偏了：未知写路径 {requested}")
    return chosen


@app.post("/api/contracts/publish-path")
def publish_path(body: PathIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    with _PATH_LOCK, connect() as conn:
        try:
            chosen = _ensure_today_path(conn, body.path, operator)
            conn.commit()
        except ContentionHTTP as e:
            conn.rollback()
            return JSONResponse(status_code=409, content=e.payload,
                                headers={"X-Contention": "1"})
        except sqlite3.Error:
            conn.rollback()
            raise
    return {"ok": True, "day": date.today().isoformat(), "path": chosen}


@app.post("/api/contracts/publish")
def publish(body: PublishIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    if body.track not in ("正式", "影子"):
        raise HTTPException(400, "track 仅支持 正式/影子")
    if not body.required_fields.strip():
        raise HTTPException(400, "必填字段不能为空")

    # 甲路径第一步：按主题加门串行；prior_winner 非空即后到，且值为先到在飞版本
    gate = subject_gate(body.subject)
    with gate.acquire(body.version) as prior_winner:
        # BEGIN IMMEDIATE 立即取得 SQLite 写锁，门内读-查-写构成单事务，杜绝跨连接交错
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            chosen = _ensure_today_path(conn, body.path, operator)  # 偏了直接抛 409 并回滚
            if chosen != "甲":
                # 兜底：册内若为乙，甲实现拒绝执行，绝不混用
                _ledger_write(conn, "发布争用偏了", "册定乙路径而运行时仅甲实现，拒绝执行",
                              subject=body.subject, loser=body.version, operator=operator)
                raise ContentionHTTP("发布争用偏了", "发布争用偏了：写路径与争用册不符，禁止混用")

            active = conn.execute(
                "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1",
                (body.subject,)).fetchone()
            dup = conn.execute("SELECT 1 FROM contracts WHERE version=? LIMIT 1",
                               (body.version,)).fetchone()

            def reject(kind: str, reason: str, winner: str | None):
                # 预查冲突：拒绝后到，且不做任何业务写入（仅争用册留痕）
                _ledger_write(conn, kind, reason, subject=body.subject,
                              winner=winner, loser=body.version, operator=operator)
                conn.commit()  # 只落争用册
                raise ContentionHTTP(kind, reason, winner=winner,
                                     loser=body.version, subject=body.subject)

            if dup is not None:
                reject("发布争用",
                       f"发布争用：版本号 {body.version} 已存在，后到请求拒绝且不写入，先到正式稿保持启用",
                       active["version"] if active else None)

            # 后到判定（注册在锁内、汇合在锁外，与调度时序无关、可复现）：已有先到发布在场
            if prior_winner is not None:
                # 胜方取门内最早注册的先到版本；若其已提交且为正式稿，与库内启用稿交叉确认
                winner = prior_winner
                reject("发布争用",
                       f"发布争用：同主题已有先到发布 {winner} 在飞，后到请求排队后拒绝且不写入，先到稿保持不变",
                       winner)

            if body.track == "正式":
                if body.expected_version is not None:
                    base = body.expected_version
                    if active is None or active["version"] != base:
                        reject("发布争用",
                               f"发布争用：基线版本 {base} 已过期（当前启用 {active['version'] if active else '无'}），拒绝后到覆盖先到",
                               active["version"] if active else None)
                # 无基线参数时：同主题已有启用正式稿即视为争用，后到只记争用、不覆盖先到必填
                if active is not None and body.expected_version is None:
                    reject("发布争用",
                           f"发布争用：主题已有先到正式稿 {active['version']} 启用中，后到发布排队后拒绝，先到必填保持不变",
                           active["version"])

            # 预查通过：单事务完成 插新版 + 旧正式稿停用，任何异常整体回滚
            conn.execute(
                "INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                (body.version, body.subject, body.mode, body.track,
                 "启用" if body.track == "正式" else "试跑", body.required_fields))
            if body.track == "正式":
                cur = conn.execute(
                    "UPDATE contracts SET status='停用' WHERE subject=? AND track='正式' AND status='启用' AND version<>?",
                    (body.subject, body.version))
                # 甲路径不变量：同主题正式启用稿至多一版
                n_active = conn.execute(
                    "SELECT COUNT(*) c FROM contracts WHERE subject=? AND track='正式' AND status='启用'",
                    (body.subject,)).fetchone()["c"]
                if cur.rowcount > 1 or n_active != 1:
                    conn.rollback()
                    raise HTTPException(500, "发布事务自检失败，已整体回滚")
            _ledger_write(conn, "发布落版",
                          f"甲路径串行放行 {body.track} 版本 {body.version}",
                          subject=body.subject, winner=body.version, operator=operator)
            conn.commit()
        except ContentionHTTP as e:
            conn.rollback()
            # 争用册留痕在 reject/偏了分支已单独 commit；回滚不影响已提交留痕
            return JSONResponse(status_code=409, content=e.payload,
                                headers={"X-Contention": "1"})
        except HTTPException:
            conn.rollback()
            raise
        except sqlite3.Error as exc:
            conn.rollback()
            # 失败闭包清晰：数据库异常不外泄为裸 500，转结构化争用/忙等响应
            raise HTTPException(409, f"发布争用：数据库忙或约束冲突，事务已回滚未写入（{type(exc).__name__}）")
        finally:
            conn.close()

    return {"ok": True, "path": "甲", "version": body.version,
            "status": "启用" if body.track == "正式" else "试跑"}


@app.get("/api/contention-ledger")
def contention_ledger(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT * FROM contention_ledger ORDER BY id DESC").fetchall()]
        pick = _today_pick(c)
    return {"today": date.today().isoformat(),
            "path": pick["detail"][0] if pick else None,
            "items": rows}


@app.get("/api/contracts/by-subject/{subject}")
def contract_by_subject(subject: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        items = [dict(r) for r in c.execute(
            "SELECT * FROM contracts WHERE subject=? ORDER BY id", (subject,)).fetchall()]
        active = c.execute(
            "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1",
            (subject,)).fetchone()
    return {"subject": subject,
            "active_version": active["version"] if active else None,
            "active": dict(active) if active else None,
            "items": items}


@app.get("/api/policy-attachments")
def policy_attachments(subject: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        active = c.execute(
            "SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1",
            (subject,)).fetchone()
        shadows = [dict(r) for r in c.execute(
            "SELECT * FROM contracts WHERE subject=? AND track='影子' ORDER BY id", (subject,)).fetchall()]
        pick = _today_pick(c)
    return {"subject": subject,
            "active_version": active["version"] if active else None,
            "required_fields": active["required_fields"] if active else "",
            "mode": active["mode"] if active else None,
            "shadow_versions": [r["version"] for r in shadows],
            "publish_path": pick["detail"][0] if pick else None}


@app.get("/api/reconcile/detail")
def reconcile_detail(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        dupes = [dict(r) for r in c.execute(
            """SELECT subject, GROUP_CONCAT(version) versions, COUNT(*) c
               FROM contracts WHERE track='正式' AND status='启用'
               GROUP BY subject HAVING c>1""").fetchall()]
        k_counts = {r["kind"]: r["c"] for r in c.execute(
            "SELECT kind, COUNT(*) c FROM contention_ledger GROUP BY kind").fetchall()}
    return {"active_formal_dupes": dupes,
            "contention_count": k_counts.get("发布争用", 0),
            "deviation_count": k_counts.get("发布争用偏了", 0),
            "publish_aligned": not dupes}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        dupes = [dict(r) for r in c.execute(
            """SELECT subject, GROUP_CONCAT(version) versions, COUNT(*) c
               FROM contracts WHERE track='正式' AND status='启用'
               GROUP BY subject HAVING c>1""").fetchall()]
        k_counts = {r["kind"]: r["c"] for r in c.execute(
            "SELECT kind, COUNT(*) c FROM contention_ledger GROUP BY kind").fetchall()}
    return {"ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
            "buffer_rows": buf_rows, "aligned": buffered == buf_rows,
            "active_formal_dupes": dupes,
            "contention_count": k_counts.get("发布争用", 0),
            "deviation_count": k_counts.get("发布争用偏了", 0),
            "publish_aligned": not dupes}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
