"""Every log event and structured-log extra key the package emits is documented."""

import ast
import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
DOC = Path(__file__).resolve().parent.parent / "docs" / "monitoring.md"


#: Events are emitted two ways: an inline `extra={"event": "..."}`, and as
#: the first argument to a Worker helper that builds the extra itself,
#: _log_extra or _complete. Both are matched, or the coverage claim below
#: is not true.
_INLINE = re.compile(r'"event":\s*"([a-z_]+)"')
_HELPER = re.compile(r'\b(?:_log_extra|_complete)\(\s*"([a-z_]+)"')
#: The first cell of a table row, where the events table names each event.
_ROW = re.compile(r"^\| `([a-z_]+)` \|", re.MULTILINE)


def emitted_events() -> set[str]:
    events: set[str] = set()
    for path in SRC.rglob("*.py"):
        text = path.read_text()
        events |= set(_INLINE.findall(text)) | set(_HELPER.findall(text))
    return events


def test_every_event_is_documented():
    # An operator alerts on these names, so one that exists and is written
    # down nowhere is a signal nobody knows to watch for.
    documented = DOC.read_text()
    events = emitted_events()
    # A floor, so a regex that stops matching reports an empty set and fails
    # here rather than passing with nothing to check.
    assert len(events) >= 30, f"the event scanner found only {len(events)}"
    # A row of its own rather than a mention: most events are also named in
    # the key table or the prose, which would still be there for an event
    # whose own row was deleted.
    rows = set(_ROW.findall(documented))
    missing = sorted(events - rows)
    assert not missing, f"events with no row in docs/monitoring.md: {missing}"


# There is deliberately no test for the reverse direction, a documented
# event the code never emits. monitoring.md holds several tables and the
# field and metric names in them are indistinguishable from event names by
# any cheap parse, so such a check would either need the doc structure
# hard-coded or pass by matching too little. A test weakened until it
# passes is worse than the gap it covers.


_KEY_TABLE_START = "| Key | Present on | Meaning |"


def _literal_str_keys(node: ast.AST) -> set[str] | None:
    """Return string keys of a dict literal, or None if it is not one."""
    if not isinstance(node, ast.Dict):
        return None
    keys: set[str] = set()
    for key in node.keys:
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            keys.add(key.value)
        else:
            return None
    return keys


def log_extra_fixed_keys(helper: ast.FunctionDef) -> set[str]:
    """The keys a _log_extra() definition always attaches, from its return."""
    returns = [node for node in ast.walk(helper) if isinstance(node, ast.Return)]
    if len(returns) != 1 or not isinstance(returns[0].value, ast.Dict):
        raise AssertionError(
            "_log_extra must return one dict literal for its keys to be inventoried"
        )
    own_extra = helper.args.kwarg.arg if helper.args.kwarg else None
    keys: set[str] = set()
    for key, value in zip(returns[0].value.keys, returns[0].value.values, strict=True):
        if key is None:
            # The helper's own **extra: those keys are the call's keywords.
            # Any other spread would add keys no call site shows.
            if isinstance(value, ast.Name) and value.id == own_extra:
                continue
            raise AssertionError(
                f"_log_extra spreads a mapping the scanner cannot read: "
                f"{ast.dump(value)}"
            )
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            raise AssertionError(
                f"_log_extra returns a key the scanner cannot read: {ast.dump(key)}"
            )
        keys.add(key.value)
    return keys


def emitted_extra_keys() -> set[str]:
    """Collect structured-log extra keys the package actually emits."""
    trees = {
        path: ast.parse(path.read_text(), filename=str(path))
        for path in SRC.rglob("*.py")
    }
    # Every _log_extra call is credited with one definition's fixed keys, so
    # a second definition anywhere would have its own keys go unread.
    helpers = [
        node
        for tree in trees.values()
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_log_extra"
    ]
    if len(helpers) != 1:
        raise AssertionError(
            f"src/ must define _log_extra exactly once, not {len(helpers)} times"
        )
    fixed = log_extra_fixed_keys(helpers[0])
    keys: set[str] = set()
    for path, tree in trees.items():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            extra_kw = next((kw for kw in node.keywords if kw.arg == "extra"), None)
            if extra_kw is not None:
                parsed = _literal_str_keys(extra_kw.value)
                if parsed is not None:
                    keys |= parsed
                else:
                    value = extra_kw.value
                    helper = None
                    if isinstance(value, ast.Call):
                        func = value.func
                        helper = (
                            func.attr
                            if isinstance(func, ast.Attribute)
                            else func.id
                            if isinstance(func, ast.Name)
                            else None
                        )
                    # extra=self._log_extra(...) is inventoried when the
                    # helper call itself is walked; it is not an unknown form.
                    if helper != "_log_extra":
                        raise AssertionError(
                            f"{path}:{node.lineno}: extra= form the scanner "
                            "cannot understand: "
                            f"{ast.dump(extra_kw.value, include_attributes=False)}"
                        )
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else (func.id if isinstance(func, ast.Name) else None)
            )
            # Keyword extras on a _log_extra() call are collected at each call;
            # _complete() is not treated as a keyword helper because its extras
            # live in a literal dictionary on the logger call.
            if name == "_log_extra":
                keys |= fixed
                for kw in node.keywords:
                    if kw.arg is None:
                        raise AssertionError(
                            f"{path}:{node.lineno}: _log_extra(**kwargs) is "
                            "not a form the scanner can inventory"
                        )
                    keys.add(kw.arg)
    return keys


def documented_extra_keys() -> set[str]:
    """Backticked keys in the first cell of the structured-log key table."""
    text = DOC.read_text()
    start = text.find(_KEY_TABLE_START)
    assert start != -1, "structured log key table is missing from docs/monitoring.md"
    rest = text[start:]
    # The table ends at the next blank line after its header.
    lines = rest.splitlines()
    table: list[str] = []
    for line in lines:
        if not line.strip():
            if table:
                break
            continue
        table.append(line)
    keys: set[str] = set()
    for line in table[2:]:  # skip header and separator
        if not line.startswith("|"):
            break
        first = line.split("|", 2)[1]
        keys |= set(re.findall(r"`([a-z_]+)`", first))
    return keys


def _helper(source: str) -> ast.FunctionDef:
    helper = ast.parse(source).body[0]
    assert isinstance(helper, ast.FunctionDef)
    return helper


def test_fixed_keys_are_read_from_the_helper_return():
    helper = _helper(
        "def _log_extra(self, event, db_task, **extra):\n"
        "    return {'event': event, 'new_fixed_key': 1, **extra}\n"
    )
    assert log_extra_fixed_keys(helper) == {"event", "new_fixed_key"}


@pytest.mark.parametrize(
    "entry",
    ["**self.identity()", "KEY_NAME: 1"],
    ids=["other-spread", "non-literal-key"],
)
def test_fixed_keys_refuse_an_entry_the_scanner_cannot_read(entry):
    helper = _helper(
        "def _log_extra(self, event, db_task, **extra):\n"
        f"    return {{'event': event, {entry}, **extra}}\n"
    )
    with pytest.raises(AssertionError, match="the scanner cannot read"):
        log_extra_fixed_keys(helper)


def test_every_emitted_extra_key_is_documented():
    # This guard requires emitted keys to be documented. It does not
    # require every documented key to have an emitter the scanner can see.
    documented = documented_extra_keys()
    emitted = emitted_extra_keys()
    # A floor, as for events: a scanner that stops matching reports too few
    # keys and fails here rather than passing with little to check.
    assert len(emitted) >= 35, f"the extra-key scanner found only {len(emitted)}"
    missing = sorted(emitted - documented)
    assert not missing, (
        f"structured log keys with no row in docs/monitoring.md: {missing}"
    )
