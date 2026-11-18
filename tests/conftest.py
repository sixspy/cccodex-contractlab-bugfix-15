import pytest

import app as appmod


@pytest.fixture()
def connect_db():
    """测试内直接连接当前 monkeypatch 后的库，绕过 HTTP 校验真实落库状态。"""
    def _connect():
        return appmod.connect()
    return _connect
