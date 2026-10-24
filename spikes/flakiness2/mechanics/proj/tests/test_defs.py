import json
import pathlib
import time


def test_limit():
    time.sleep(0.02)
    p = pathlib.Path(__file__).parent.parent / "data" / "defs.json"
    assert json.loads(p.read_text())["limit"] == 10
