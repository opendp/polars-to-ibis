import re
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


def split_tag_payload(node: JsonObj) -> tuple[str, Any]:
    if len(node) != 1:
        raise ValueError(f"Expected single-key tagged dict, got: {node!r}")
    return next(iter(node.items()))


def node_to_ibis_table(node: JsonObj, table: ir.Table) -> ir.Table:
    tag, payload = split_tag_payload(node)
    try:
        func = TABLE_REGISTRY[tag]
    except KeyError as e:
        raise NotImplementedError(f"No table handler for {tag!r}") from e
    return func(payload, table=table)


def node_to_ibis_value(node: Any) -> NamedValue:
    tag, payload = split_tag_payload(node)
    try:
        func = VALUE_REGISTRY[tag]
    except KeyError as e:
        raise NotImplementedError(f"No value handler for {tag!r}") from e
    return func(payload)


def nodes_to_ibis_values(nodes: list[Any]) -> dict[str, ir.Value]:
    return dict(map(node_to_ibis_value, nodes))


@table_handler("Scan")
@table_handler("DataFrameScan")
def handle_source(payload: JsonObj, table: ir.Table) -> ir.Table:
    return table


@table_handler("Select")
def handle_select(payload: JsonObj, table: ir.Table) -> ir.Table:
    input_table = node_to_ibis_table(payload["input"], table=table)
    return input_table.aggregate(**nodes_to_ibis_values(payload.get("expr", [])))


@table_handler("MapFunction")
def handle_map_function(payload: JsonObj, table: ir.Table) -> ir.Table:
    input_table = node_to_ibis_table(payload["input"], table=table)
    stats = payload["function"]["Stats"]
    match stats:
        case "Sum":
            return table.aggregate(
                [
                    getattr(getattr(input_table, col), stats.lower())()
                    for col in table.columns
                ]
            ).rename(lambda name: re.sub(r"^\w+\((.*)\)$", r"\1", name))

        case _:
            raise ValueError(f"unsupported stats type: {stats}")


@value_handler("Agg")
def handle_agg(payload: Any) -> NamedValue:
    agg, agg_payload = split_tag_payload(payload)

    match agg:
        case "Sum":
            name, value = node_to_ibis_value(agg_payload)
            return name, value.sum()

        case _:
            raise ValueError(f"unsupported agg type: {agg}")
