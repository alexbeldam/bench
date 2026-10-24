import time
from mod_e import calc


def test_e0():
    time.sleep(0.02)
    assert calc(1) == 5


def test_e1():
    time.sleep(0.02)
    assert calc(2) == 10


def test_e2():
    time.sleep(0.02)
    assert calc(3) == 15


def test_e3():
    time.sleep(0.02)
    assert calc(4) == 20


def test_e4():
    time.sleep(0.02)
    assert calc(5) == 25
