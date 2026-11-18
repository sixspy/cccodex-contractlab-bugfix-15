
import pytest
from fastapi.testclient import TestClient
from app import app, init_db

@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path/"t.db"); init_db(); yield

def test_validate_paths():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "入站契约"
    tok = c.post("/api/login", json={"username":"admin","password":"123456"}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}
    # strict reject missing
    r = c.post("/api/ingress/validate", headers=h, json={"subject":"订单入站","payload_fields":"order_id,amount"})
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    ok = c.post("/api/ingress/validate", headers=h, json={"subject":"订单入站","payload_fields":"order_id,amount,currency"})
    assert ok.json()["result"] == "通过"
    assert c.get("/api/reconcile", headers=h).json()["passed"] >= 1
