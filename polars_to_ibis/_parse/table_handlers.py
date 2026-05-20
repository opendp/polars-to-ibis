"""
This is a private module: The API may change.
"""

from pprint import pformat
from typing import Any, Callable

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]
from ibis import _ as defer  # pyright: ignore[reportMissingTypeStubs]

from .._utils import replace
from .utils import assert_no_extras, split_tag_payload
from .value_handlers import polars_expr_to_ibis_value

PolarsPlan = dict[str, Any]
NamedValue = tuple[str, ir.Value]
ReturnsTable = Callable[..., ir.Table]
ReturnsValue = Callable[..., NamedValue]


# Main:


def update_polars_to_ibis(polars_plan: PolarsPlan, table: ir.Table) -> ir.Table:
    tag, payload = split_tag_payload(polars_plan)
    try:
        func = TABLE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No table handler for {tag!r}") from e
    try:
        return func(payload, table=table)
    except NotImplementedError as e:
        replace(polars_plan, "DataFrameScan", lambda _: "...")
        raise NotImplementedError(f"{e}:\n{pformat(polars_plan)}")


# Registry:

TABLE_REGISTRY: dict[str, ReturnsTable] = {}


def table_handler(tag: str) -> Callable[..., ReturnsTable]:
    def deco(func: ReturnsTable) -> ReturnsTable:
        TABLE_REGISTRY[tag] = func
        return func

    return deco


# Helpers:


def parse_sort_by_column(col_list: list[dict[str, str]]) -> list[str]:
    return [list(col.values())[0] for col in col_list]


def parse_select_expr(
    col_list: list[dict[str, Any]],
) -> tuple[dict[str, ir.Value], dict[str, ir.Value], list[str]]:
    """
    Given a polars serialization,
    return tuple of (kw)args to be used for select() or drop().

    >>> parse_select_expr([{'Column': 'keep'}, {'Column': 'two'}])
    ({'keep': 'keep', 'two': 'two'}, {}, [])

    >>> parse_select_expr([{'Alias': [{'Column': 'old_name'}, 'new_name']}])
    ({'new_name': _['old_name']}, {}, [])

    >>> parse_select_expr([{'Alias':
    ...     [{'Agg': {'Mean': {'Column': 'old_name'}}}, 'new_name']
    ... }])
    ({}, {'new_name': _['old_name'].mean().cast('float32')}, [])

    >>> parse_select_expr([{'Selector': {'Difference': ['Wildcard', {'ByName': {
    ...     'names': ['cols', 'to', 'drop'],
    ...     'strict': True
    ... }}]}}])
    ({}, {}, ['cols', 'to', 'drop'])

    """
    select_kwargs: dict[str, ir.Value] = {}
    agg_kwargs: dict[str, ir.Value] = {}
    drop_args: list[str] = []
    for col in col_list:
        tag, payload = split_tag_payload(col)
        match (tag, payload):
            case ("Column", _):
                select_kwargs[payload] = payload
            case ("Alias", [expr, new_name]):
                ibis_value = polars_expr_to_ibis_value(expr)
                if split_tag_payload(expr)[0] == "Agg":
                    agg_kwargs[new_name] = ibis_value.cast("float32")
                else:
                    select_kwargs[new_name] = ibis_value
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
            case _:  # pragma: no cover
                raise NotImplementedError(f"Unsupported {tag}")
    return (select_kwargs, agg_kwargs, drop_args)


# Table handlers:


@table_handler("Scan")
@table_handler("DataFrameScan")
def handle_scan(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    match payload:
        case {"df": _, "schema": {"fields": _}, **extras}:
            assert_no_extras(extras)
            return table
        case _:
            raise NotImplementedError("Unsupported Scan")


@table_handler("Select")
def handle_select(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)

    match payload:
        case {
            "expr": expr,
            "options": {
                "duplicate_check": True,
                "run_parallel": True,
                "should_broadcast": True,
            },
            **extras,
        }:
            assert_no_extras(extras)
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unsupported Select: {payload}")

    match expr:
        case ["Len"]:
            return input_table.aggregate(len=input_table.count())
        case _:
            select_kwargs, agg_kwargs, drop_args = parse_select_expr(payload["expr"])
            if select_kwargs:
                input_table = input_table.select(**select_kwargs)
            if agg_kwargs:
                input_table = input_table.aggregate(**agg_kwargs)  # type: ignore
            if drop_args:
                input_table = input_table.drop(*drop_args)
            return input_table


@table_handler("Filter")
def handle_filter(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {"predicate": predicate, **extras}:
            assert_no_extras(extras)
            value = polars_expr_to_ibis_value(predicate)
            return input_table.filter(value)  # type: ignore
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Filter")


@table_handler("Slice")
def handle_slice(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {"len": len, "offset": offset, **extras}:
            assert_no_extras(extras)
            if offset < 0:
                raise NotImplementedError(f"Unsupported offset: {offset}")
            return input_table.limit(len, offset=offset)
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Slice")


@table_handler("Sort")
def handle_sort(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {
            "by_column": by_column,
            "sort_options": {
                "descending": descending,
                "nulls_last": nulls_last,
                "multithreaded": True,
                "maintain_order": False,
                "limit": None,
            },
            "slice": None,
            **extras,
        }:
            assert_no_extras(extras)
            if any(nulls_last):
                raise NotImplementedError(f"Unsupported nulls_last: {nulls_last}")
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
            raise NotImplementedError("Unsupported Sort")


@table_handler("HStack")
def handle_hstack(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {
            "exprs": [
                {
                    "Cast": {
                        "dtype": {"Literal": dtype_literal},
                        "expr": {"Selector": "Wildcard"},
                        "options": "Strict",
                    }
                }
            ],
            "input": input,
            "options": {
                "duplicate_check": True,
                "run_parallel": True,
                "should_broadcast": True,
            },
            **extras,
        }:
            assert_no_extras(extras)
            all_columns = input["MapFunction"]["input"]["DataFrameScan"]["schema"][
                "fields"
            ].keys()
            return update_polars_to_ibis(input, table=table).cast(  # type: ignore
                {col: dtype_literal.lower() for col in all_columns}
            )
        case {
            "exprs": [
                {
                    "Function": {
                        "input": [
                            {
                                "Selector": {
                                    "Union": [
                                        {
                                            "ByDType": {
                                                "AnyOf": [
                                                    "Int8",
                                                    "Int16",
                                                    "Int32",
                                                    "Int64",
                                                    "Int128",
                                                    "UInt8",
                                                    "UInt16",
                                                    "UInt32",
                                                    "UInt64",
                                                    "Float32",
                                                    "Float64",
                                                ]
                                            }
                                        },
                                        {"ByDType": "Decimal"},
                                    ]
                                }
                            },
                            fill_expr,
                        ],
                        "function": function,
                    }
                }
            ],
            "options": {
                "run_parallel": True,
                "duplicate_check": True,
                "should_broadcast": True,
            },
            **extras,
        }:
            assert_no_extras(extras)
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported HStack")

    value = polars_expr_to_ibis_value(fill_expr)
    match function:
        case "FillNull":
            return input_table.fill_null(value)  # type: ignore
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unsupported HStack function: {function}")


@table_handler("GroupBy")
def handle_group_by(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)

    match payload:
        case {
            "keys": keys,
            "aggs": [{"Agg": agg_payload}],
            "maintain_order": False,
            "options": {"dynamic": None, "rolling": None, "slice": None},
            **extras,
        }:
            if "apply" in extras and extras["apply"] is None:
                # Added in new polars versions.
                del extras["apply"]  # pragma: no cover
            assert_no_extras(extras)
            group_by_keys = parse_sort_by_column(keys)
            grouped_table = input_table.group_by(group_by_keys)
            agg_payload_tag, agg_payload_payload = split_tag_payload(agg_payload)
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported GroupBy")

    match agg_payload_payload:
        case {"Column": column, **extras}:
            assert_no_extras(extras)
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported GroupBy")

    match agg_payload_tag:
        case "Sum" | "Mean" | "Median" | "Max" | "Min":
            return grouped_table.aggregate(  # type: ignore
                **{column: getattr(defer[column], agg_payload_tag.lower())()}
            )
        case _:  # pragma: no cover
            raise NotImplementedError(f"Unsupported Agg: {agg_payload_tag}")


@table_handler("MapFunction")
def handle_map_function(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    match payload:
        case {"function": {"Stats": stats}, **extras}:
            assert_no_extras(extras)
        case {"function": {"FillNan": fill_nan_expr}, **extras}:
            assert_no_extras(extras)
            fill_nan_value = polars_expr_to_ibis_value(fill_nan_expr)
            # No ibis "fill_nan()", so we do it by hand:
            return input_table.select(
                **{
                    col: input_table[col]
                    .isnan()  # type: ignore
                    .ifelse(fill_nan_value, input_table[col])
                    for col in input_table.columns
                }
            )
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported MapFunction")

    match stats:
        case "Mean":
            return table.aggregate(
                **{
                    col: getattr(getattr(input_table, col), stats.lower())().cast(
                        "float32"
                    )
                    for col in table.columns
                }
            )

        case "Sum" | "Median" | "Max" | "Min":
            return table.aggregate(
                **{
                    col: getattr(getattr(input_table, col), stats.lower())()
                    for col in table.columns
                }
            )

        case {"Var": {"ddof": 1}, **extras}:
            assert_no_extras(extras)
            return table.aggregate(
                **{
                    col: getattr(input_table, col).var().cast("float32")
                    for col in table.columns
                }
            )

        case {"Std": {"ddof": 1}, **extras}:
            assert_no_extras(extras)
            return table.aggregate(
                **{
                    col: getattr(input_table, col).std().cast("float32")
                    for col in table.columns
                }
            )

        case {
            "Quantile": {
                "quantile": {"Literal": {"Dyn": {"Float": quantile}}},
                "method": "Nearest",
            },
            **extras,
        }:
            assert_no_extras(extras)
            return table.aggregate(
                **{
                    col: getattr(input_table, col).quantile(quantile)
                    for col in table.columns
                }
            )

        case _:  # pragma: no cover
            raise ValueError(f"unsupported stats type: {stats}")
