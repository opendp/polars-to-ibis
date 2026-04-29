from typing import Any, Callable

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


# Handlers:


@table_handler("Scan")
@table_handler("DataFrameScan")
def handle_source(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    return table


# @table_handler("Select")
# def handle_select(payload: PolarsPlan, table: ir.Table) -> ir.Table:
#     input_table = polars_plan_to_ibis_table(payload["input"], table=table)
#     ibis_values = polars_plans_to_ibis_values(payload.get("expr", []))
#     return input_table.aggregate(**ibis_values)  # type: ignore


@table_handler("MapFunction")
def handle_map_function(payload: PolarsPlan, table: ir.Table) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table)
    stats = payload["function"]["Stats"]
    match stats:
        case "Sum" | "Mean" | "Median":
            return table.aggregate(
                **{
                    col: getattr(getattr(input_table, col), stats.lower())()
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
