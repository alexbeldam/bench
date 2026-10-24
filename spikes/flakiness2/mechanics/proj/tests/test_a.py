import time
from mod_a import calc


def test_a0():
    time.sleep(0.02)
    assert calc(1) == 1


def test_a1():
    time.sleep(0.02)
    assert calc(2) == 2


def test_a2():
    time.sleep(0.02)
    assert calc(3) == 3


def test_a3():
    time.sleep(0.02)
    assert calc(4) == 4


def test_a4():
    time.sleep(0.02)
    assert calc(5) == 5
