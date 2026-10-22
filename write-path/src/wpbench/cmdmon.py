"""Driver command monitoring: counts commands, and separately the Atlas Search queries."""

import threading
from collections import Counter

from pymongo import monitoring

IGNORED = {"hello", "isMaster", "ismaster", "ping", "saslStart", "saslContinue", "endSessions", "buildInfo"}


def is_search(cmd: dict) -> bool:
    pipeline = cmd.get("pipeline")
    if not isinstance(pipeline, list) or not pipeline:
        return False
    first = pipeline[0]
    return isinstance(first, dict) and ("$search" in first or "$searchMeta" in first)


class CommandCounter(monitoring.CommandListener):
    """Thread-safe counter. `snapshot()` returns counts since construction or the last `reset()`."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self.commands: Counter = Counter()
            self.search_queries = 0

    def started(self, event) -> None:
        name = event.command_name
        if name in IGNORED:
            return
        with self._lock:
            self.commands[name] += 1
            if name == "aggregate" and is_search(event.command):
                self.search_queries += 1

    def succeeded(self, event) -> None:
        pass

    def failed(self, event) -> None:
        pass

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "commands": dict(self.commands),
                "total": sum(self.commands.values()),
                "search_queries": self.search_queries,
            }
