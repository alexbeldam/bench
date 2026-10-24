import time
from mod_b import calc


def test_b0():
    time.sleep(0.02)
    assert calc(1) == 2


def test_b1():
    time.sleep(0.02)
    assert calc(2) == 4


def test_b2():
    time.sleep(0.02)
    assert calc(3) == 6


def test_b3():
    time.sleep(0.02)
    assert calc(4) == 8


def test_b4():
    time.sleep(0.02)
    assert calc(5) == 10
