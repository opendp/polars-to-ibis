from typing import Any, Callable

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]

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


# def value_handler(tag: str) -> Callable[..., ReturnsValue]:
#     def deco(func: ReturnsValue) -> ReturnsValue:
#         VALUE_REGISTRY[tag] = func
#         return func

#     return deco


# Helpers:


def split_tag_payload(polars_plan: PolarsPlan) -> tuple[str, Any]:
    if len(polars_plan) != 1:
        raise ValueError(
            f"Expected single-key tagged dict, got: {polars_plan!r}"
        )  # pragma: no cover
    return next(iter(polars_plan.items()))


def update_polars_to_ibis(polars_plan: PolarsPlan, table: ir.Table) -> ir.Table:
    tag, payload = split_tag_payload(polars_plan)
    try:
        func = TABLE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No table handler for {tag!r}") from e
    return func(payload, table=table)


# def polars_plan_to_ibis_value(polars_plan: PolarsPlan) -> NamedValue:
#     tag, payload = split_tag_payload(polars_plan)
#     try:
#         func = VALUE_REGISTRY[tag]
#     except KeyError as e:
#         raise NotImplementedError(f"No value handler for {tag!r}") from e
#     return func(payload)


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
    ({'new_name': 'old_name'}, [])

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
        match tag:
            case "Column":
                select_kwargs[payload] = payload
            case "Alias":
                select_kwargs[payload[1]] = payload[0]["Column"]
            case "Selector":
                drop_args += payload["Difference"][1]["ByName"]["names"]
            case _:  # pragma: no cover
                raise NotImplementedError(f"No support for {tag}")
    return (select_kwargs, drop_args)  # type: ignore


# Handlers:


@table_handler("Scan")
@table_handler("DataFrameScan")
def handle_scan(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    return table


@table_handler("Select")
def handle_select(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    select_kwargs, drop_args = parse_select_expr(payload["expr"])  # type: ignore
    if select_kwargs:
        table = table.select(**select_kwargs)  # type: ignore
    if drop_args:
        table = table.drop(*drop_args)  # type: ignore
    return table


@table_handler("Slice")
def handle_slice(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    if offset := payload["offset"] < 0:
        raise NotImplementedError("Negative offsets not supported")  # pragma: no cover
    return table.limit(payload["len"], offset=offset)


@table_handler("Sort")
def handle_sort(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    undirected_sort_keys = parse_sort_by_column(payload["by_column"])
    descending = payload["sort_options"]["descending"]
    # TODO: Error if unsupported sort options are used.
    directed_sort_keys = [
        ibis.desc(key) if desc else key
        for key, desc in zip(undirected_sort_keys, descending)
    ]
    return table.order_by(*directed_sort_keys)  # type: ignore


# @table_handler("GroupBy")
# def handle_group_by(payload: PolarsPlan, table: ir.Table) -> ir.Table:
#     # input_table = update_polars_to_ibis(payload["input"], table=table)
#     # TODO: hard-coded!
#     return table.group_by("ints").aggregate(
#         floats=_["floats"].sum()
#     )


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


# @value_handler("Agg")
# def handle_agg(payload: Any) -> NamedValue:
#     agg, agg_payload = split_tag_payload(payload)

#     match agg:
#         case "Sum":
#             name, value = polars_plan_to_ibis_value(agg_payload)
#             return name, value.sum()  # type: ignore

#         case _:
#             raise ValueError(f"unsupported agg type: {agg}")
