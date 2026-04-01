from typing import Any, Callable

import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]

JsonObj = dict[str, Any]
NamedValue = tuple[str, ir.Value]
ReturnsTable = Callable[..., ir.Table]
ReturnsValue = Callable[..., NamedValue]

TABLE_REGISTRY: dict[str, ReturnsTable] = {}
VALUE_REGISTRY: dict[str, ReturnsValue] = {}


def table_handler(tag: str) -> Callable[..., ReturnsTable]:
    def deco(func: ReturnsTable) -> ReturnsTable:
        TABLE_REGISTRY[tag] = func
        return func

    return deco


def value_handler(tag: str) -> Callable[..., ReturnsValue]:
    def deco(func: ReturnsValue) -> ReturnsValue:
        VALUE_REGISTRY[tag] = func
        return func

    return deco


def tagged(node: JsonObj) -> tuple[str, Any]:
    if len(node) != 1:
        raise ValueError(f"Expected single-key tagged dict, got: {node!r}")
    return next(iter(node.items()))


def translate_table(node: JsonObj, *, table: ir.Table) -> ir.Table:
    tag, payload = tagged(node)
    try:
        return TABLE_REGISTRY[tag](payload, table=table)
    except KeyError as e:
        raise NotImplementedError(f"No table handler for {tag!r}") from e


def translate_value(node: Any) -> NamedValue:
    tag, payload = tagged(node)
    try:
        return VALUE_REGISTRY[tag](payload)
    except KeyError as e:
        raise NotImplementedError(f"No value handler for {tag!r}") from e


@table_handler("Scan")
@table_handler("DataFrameScan")
def handle_source(payload: JsonObj, *, table: ir.Table) -> ir.Table:
    return table
