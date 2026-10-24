import os
import time


def test_toggle():
    time.sleep(0.02)
    assert os.environ.get("MECH_FAIL") != "1"
