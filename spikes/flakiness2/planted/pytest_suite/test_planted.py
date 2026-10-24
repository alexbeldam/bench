"""The in-process planted tests exposed to pytest, one parametrized test per registry entry (in-memory and no-engine sets).
Used to check that pytest-randomly and pytest-repeat behave as the in-process runner models them, and to time both mechanisms.
FL2P_ONLY (comma list of names) restricts the parametrization, so repeat plugins keep working with a selection."""
import os

import pytest

from fl2p.core import REG, Ctx, run_one, selected

CTX = Ctx()  # one state for the whole pytest process, as in a real session
ONLY = {x for x in os.environ.get("FL2P_ONLY", "").split(",") if x}


@pytest.mark.parametrize("name", [t.name for t in selected("all") if not ONLY or t.name in ONLY])
def test_planted(name):
    r = run_one(REG[name], CTX)
    assert r.ok, r.err
