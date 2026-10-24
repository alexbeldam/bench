import time
from mod_f import calc


def test_f0():
    time.sleep(0.02)
    assert calc(1) == 6


def test_f1():
    time.sleep(0.02)
    assert calc(2) == 12


def test_f2():
    time.sleep(0.02)
    assert calc(3) == 18
