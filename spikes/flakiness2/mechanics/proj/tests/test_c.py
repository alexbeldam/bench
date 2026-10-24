import time
from mod_c import calc


def test_c0():
    time.sleep(0.02)
    assert calc(1) == 3


def test_c1():
    time.sleep(0.02)
    assert calc(2) == 6


def test_c2():
    time.sleep(0.02)
    assert calc(3) == 9


def test_c3():
    time.sleep(0.02)
    assert calc(4) == 12


def test_c4():
    time.sleep(0.02)
    assert calc(5) == 15
