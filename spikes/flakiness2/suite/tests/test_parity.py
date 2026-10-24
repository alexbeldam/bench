"""Scenario by backend. The same coroutine runs on A, B and C."""
import inspect

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import pytest

from scenarios import SCENARIOS

# scenarios a backend cannot run by design (reason shown in the report); everything else runs on every backend
NOT_RUN: dict[tuple[str, str], str] = {}
# known, expected failures: (scenario, backend) -> (exception type name, reason). strict: an unexpected pass fails the run
KNOWN_FAIL: dict[tuple[str, str], tuple[type, str]] = {
    ("s27_kit_bulk_writer_in_scope", "C"): (TypeError, "mongomock bulk_write() rejects Beanie's `comment` argument"),
    ("s28_stock_bulk_writer_outside_scope", "C"): (TypeError, "mongomock bulk_write() rejects Beanie's `comment` argument"),
    ("s31_bulk_writer_search_visibility", "C"): (TypeError, "mongomock bulk_write() rejects Beanie's `comment` argument"),
}


@pytest.mark.parametrize("name", list(SCENARIOS))
async def test_scenario(name, backend, env, real_session_client, request):
    if (name, backend) in NOT_RUN:
        pytest.skip(NOT_RUN[(name, backend)])
    if (name, backend) in KNOWN_FAIL:
        exc, why = KNOWN_FAIL[(name, backend)]
        request.applymarker(pytest.mark.xfail(raises=exc, reason=why, strict=True))
    fn = SCENARIOS[name]
    try:
        if "real_session" in inspect.signature(fn).parameters:
            async with real_session_client.start_session() as s:
                await fn(env, real_session=s)
        else:
            await fn(env)
    finally:
        request.node._kit_notes = dict(env.notes)
