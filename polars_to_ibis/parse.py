from typing import Any, Callable

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]
from ibis import _ as defer  # pyright: ignore[reportMissingTypeStubs]

PolarsPlan = dict[str, Any]
NamedValue = tuple[str, ir.Value]
ReturnsTable = Callable[..., ir.Table]
ReturnsValue = Callable[..., NamedValue]

TABLE_REGISTRY: dict[str, ReturnsTable] = {}
VALUE_REGISTRY: dict[str, ReturnsValue] = {}


# Decorators:


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


# Helpers:


def split_tag_payload(polars_plan: PolarsPlan) -> tuple[str, Any]:
    match list(polars_plan.items()):
        case [[tag, payload]]:
            return tag, payload
        case _:
            raise ValueError(
                f"Expected single-key tagged dict, got: {polars_plan!r}"
            )  # pragma: no cover


def update_polars_to_ibis(polars_plan: PolarsPlan, table: ir.Table) -> ir.Table:
    tag, payload = split_tag_payload(polars_plan)
    try:
        func = TABLE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No table handler for {tag!r}") from e
    return func(payload, table=table)


def polars_expr_to_ibis_value(polars_expr: PolarsPlan) -> NamedValue:
    tag, payload = split_tag_payload(polars_expr)
    try:
        func = VALUE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No value handler for {tag!r}") from e
    return func(payload)


# def polars_plans_to_ibis_values(plans: list[PolarsPlan]) -> dict[str, ir.Value]:
#     return dict(map(polars_plan_to_ibis_value, plans))


def parse_sort_by_column(col_list: list[dict[str, str]]) -> list[str]:
    return [list(col.values())[0] for col in col_list]


def parse_select_expr(col_list: list[dict[str, Any]]) -> tuple[dict, list]:  # type: ignore
    """
    Given a polars serialization,
    return tuple of (kw)args to be used for select() or drop().

    >>> parse_select_expr([{'Column': 'keep'}, {'Column': 'two'}])
    ({'keep': 'keep', 'two': 'two'}, [])

    >>> parse_select_expr([{'Alias': [{'Column': 'old_name'}, 'new_name']}])
    ({'new_name': _['old_name']}, [])

    >>> parse_select_expr([{'Selector': {'Difference': ['Wildcard', {'ByName': {
    ...     'names': ['cols', 'to', 'drop'],
    ...     'strict': True
    ... }}]}}])
    ({}, ['cols', 'to', 'drop'])

    """
    select_kwargs = {}
    drop_args = []
    for col in col_list:
        tag, payload = split_tag_payload(col)
        match (tag, payload):
            case ("Column", _):
                select_kwargs[payload] = payload
            case ("Alias", [expr, new_name]):
                select_kwargs[new_name] = polars_expr_to_ibis_value(expr)
            case (
                "Selector",
                {
                    "Difference": [
                        "Wildcard",
                        {"ByName": {"names": names, "strict": True}},
                    ]
                },
            ):
                drop_args += names
            case ("Selector", _):  # pragma: no cover
                raise NotImplementedError(f"No support for {tag} with {payload}")
            case _:  # pragma: no cover
                raise NotImplementedError(f"No support for {tag}")
    return (select_kwargs, drop_args)  # type: ignore


# Value handlers:


@value_handler("Literal")
def handle_literal(payload: PolarsPlan):
    match payload:
        case (
            {"Dyn": {"Int": value}}
            | {"Dyn": {"Float": value}}
            | {"Scalar": {"Boolean": value}}
        ):
            return value
        case {"Scalar": {"String": value}}:
            return ibis.literal(value)  # pyright: ignore[reportUnknownMemberType]
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unimplemented Literal {payload}")


@value_handler("Column")
def handle_column(payload: PolarsPlan):
    return defer[payload]  # pyright: ignore[reportArgumentType]


@value_handler("Function")
def handle_function(payload: PolarsPlan):  # type: ignore
    match payload:
        case {"input": [left, right], "function": {"Pow": "Generic"}}:
            return polars_expr_to_ibis_value(left) ** polars_expr_to_ibis_value(right)  # type: ignore
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unimplemented Function {payload}")


@value_handler("BinaryExpr")
def handle_binary_expr(payload: PolarsPlan):
    match payload:
        case {"left": left, "op": op, "right": right}:
            from operator import add, mod, mul, sub, truediv

            func = {
                "Plus": add,
                "Minus": sub,
                "Multiply": mul,
                "TrueDivide": truediv,
                "Modulus": mod,
            }[op]
            return func(
                polars_expr_to_ibis_value(left), polars_expr_to_ibis_value(right)
            )
            # return polars_expr_to_ibis_value(left) + polars_expr_to_ibis_value(right)
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unimplemented BinaryExpr {payload}")


# Table handlers:


@table_handler("Scan")
@table_handler("DataFrameScan")
def handle_scan(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    match payload:
        case {"df": _, "schema": {"fields": _}}:
            return table
        case {"df": _, "schema": schema}:
            raise NotImplementedError(f"Unexpected schema keys: {schema.keys()}")
        case _:
            raise NotImplementedError(f"Unexpected payload keys: {payload.keys()}")


@table_handler("Select")
def handle_select(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    select_kwargs, drop_args = parse_select_expr(payload["expr"])  # type: ignore
    if select_kwargs:
        input_table = input_table.select(**select_kwargs)  # type: ignore
    if drop_args:
        input_table = input_table.drop(*drop_args)  # type: ignore
    return input_table


@table_handler("Slice")
def handle_slice(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {
            "len": len,
            "offset": offset,
            **_rest,
        }:
            if offset < 0:
                raise NotImplementedError(f"Negative offsets not supported: {offset}")
            return input_table.limit(len, offset=offset)
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unexpected Slice payload: {payload}")


@table_handler("Sort")
def handle_sort(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {
            "by_column": by_column,
            "sort_options": {
                "descending": descending,
                "nulls_last": _nulls_last,
                "multithreaded": True,
                "maintain_order": False,
                "limit": None,
            },
            **_rest,
        }:
            undirected_sort_keys = parse_sort_by_column(by_column)
            directed_sort_keys = [
                ibis.desc(key) if desc else key
                for key, desc in zip(undirected_sort_keys, descending)
            ]
            return update_polars_to_ibis(
                payload["input"],
                input_table,
            ).order_by(
                *directed_sort_keys  # type: ignore
            )
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unexpected Sort payload: {payload}")


@table_handler("GroupBy")
def handle_group_by(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)

    match payload:
        case {
            "keys": keys,
            "aggs": [{"Agg": agg_payload}],
            "maintain_order": False,
            "options": {"dynamic": None, "rolling": None, "slice": None},
        }:
            group_by_keys = parse_sort_by_column(keys)
            grouped_table = input_table.group_by(group_by_keys)

            agg_payload_tag, agg_payload_payload = split_tag_payload(agg_payload)
            agg_col = agg_payload_payload["Column"]
            match agg_payload_tag:
                case "Sum" | "Mean" | "Median" | "Max" | "Min":
                    return grouped_table.aggregate(  # type: ignore
                        **{agg_col: getattr(defer[agg_col], agg_payload_tag.lower())()}
                    )
                case _:  # pragma: no cover
                    raise NotImplementedError(f"Not implemented: {agg_payload_tag}")
        case _:  # pragma: no cover
            raise NotImplementedError(f"Not implemented: {payload}")


@table_handler("MapFunction")
def handle_map_function(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    stats = payload["function"]["Stats"]
    match stats:
        case "Sum" | "Mean" | "Median" | "Max" | "Min":
            return table.aggregate(
                **{
                    col: getattr(getattr(input_table, col), stats.lower())()
                    for col in table.columns
                }
            )

        case {"Var": {"ddof": 1}}:
            return table.aggregate(
                **{col: getattr(input_table, col).var() for col in table.columns}
            )

        case {"Std": {"ddof": 1}}:
            return table.aggregate(
                **{col: getattr(input_table, col).std() for col in table.columns}
            )

        # TODO: Does not support general quantiles
        case {
            "Quantile": {
                "quantile": {"Literal": {"Dyn": {"Float": 0.5}}},
                "method": "Nearest",
            }
        }:
            return table.aggregate(
                **{
                    col: getattr(input_table, col).quantile(0.5)
                    for col in table.columns
                }
            )

        case _:  # pragma: no cover
            raise ValueError(f"unsupported stats type: {stats}")
