import time
from mod_d import calc


def test_d0():
    time.sleep(0.02)
    assert calc(1) == 4


def test_d1():
    time.sleep(0.02)
    assert calc(2) == 8


def test_d2():
    time.sleep(0.02)
    assert calc(3) == 12


def test_d3():
    time.sleep(0.02)
    assert calc(4) == 16


def test_d4():
    time.sleep(0.02)
    assert calc(5) == 20
