"""
This is a private module: The API may change.
"""

import logging
from collections.abc import Callable
from typing import Any

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]

from polars_to_ibis._utils import abbreviate

from . import tags
from .selectors import expand_expr
from .utils import assert_no_extras, indent, outdent, split_tag_payload
from .value_handlers import infer_name, polars_expr_to_ibis_value

logger = logging.getLogger(__name__)

PolarsPlan = dict[str, Any]
ReturnsTable = Callable[..., ir.Table]


# Main:


def update_polars_to_ibis(
    polars_plan: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    tag, payload = split_tag_payload(polars_plan)
    try:
        func = TABLE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No table handler for {tag!r}") from e
    try:
        return func(payload, table=table, backend=backend)
    except NotImplementedError as e:
        raise NotImplementedError(f"{e}\nin {abbreviate(polars_plan)}")


# Registry:

TABLE_REGISTRY: dict[str, ReturnsTable] = {}


def table_handler(tag: str) -> Callable[..., ReturnsTable]:
    def deco(func: ReturnsTable) -> ReturnsTable:
        def wrapped_func(*args, **kwargs):
            if logger.isEnabledFor(logging.DEBUG):  # pragma: no cover
                # Just so we don't call abbreviate unless needed.
                logger.debug(
                    f"{indent()}--> polars table {tag}:\n{abbreviate(args[0])} "
                )
            to_return = func(*args, **kwargs)
            if logger.isEnabledFor(logging.DEBUG):  # pragma: no cover
                logger.debug(f"{outdent()}<-- ibis table:\n{to_return}")
            return to_return

        TABLE_REGISTRY[tag] = wrapped_func
        return func

    return deco


# Helpers:


def parse_sort_by_column(col_list: list[dict[str, str]]) -> list[str]:
    return [next(iter(col.values())) for col in col_list]


def translate_named_exprs(exprs: list[PolarsPlan], table: ir.Table) -> dict[str, Any]:
    values = {}
    for expr in exprs:
        for expanded in expand_expr(expr, table):
            name = infer_name(expanded)
            if name in values:
                raise ValueError(f"Duplicate output column: {name}")
            values[name] = polars_expr_to_ibis_value(expanded)
    return values


def apply_select_expr(exprs: list[PolarsPlan], table: ir.Table) -> ir.Table:
    values = table.bind(**translate_named_exprs(exprs, table))
    scalars = tuple(value for value in values if isinstance(value, ir.Scalar))
    if len(scalars) == len(values):
        return table.aggregate(scalars)
    return table.select(*values)


# Table handlers:


@table_handler(tags.table.IR)
def handle_ir(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    match payload:
        case {"dsl": dsl, "version": _version, **extras_1}:
            assert_no_extras(extras_1)
            return update_polars_to_ibis(dsl, table=table, backend=backend)
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.IR}")


@table_handler(tags.table.SCAN)
@table_handler(tags.table.DATA_FRAME_SCAN)
def handle_scan(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    # Serialization changes between polars versions,
    # so only one of these will be covered in a given test run.
    match payload:  # pragma: no cover
        case {
            "df": _,
            "schema": {"fields": _, "metadata": None, **extras_1},
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            return table
        case {
            "df": _,
            "schema": {"fields": _, **extras_1},
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            return table
        case _:
            raise NotImplementedError(
                f"Unsupported {tags.table.SCAN} or {tags.table.DATA_FRAME_SCAN}"
            )


@table_handler(tags.table.SELECT)
def handle_select(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table, backend=backend)

    match payload:
        case {
            "expr": expr,
            "options": {
                "duplicate_check": True,
                "run_parallel": True,
                "should_broadcast": True,
                **extras_1,
            },
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.SELECT}")

    return apply_select_expr(expr, input_table)


@table_handler(tags.table.DISTINCT)
def handle_distinct(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    match payload:
        case {
            "input": input_expr,
            "options": {
                "keep_strategy": keep_strategy,
                "maintain_order": True,
                "subset": subset,
                **extras_1,
            },
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            if keep_strategy not in {"Any", "First"}:
                # Some Ibis backends do not support 'keep="last"'.
                raise NotImplementedError(f"Unsupported {keep_strategy=}")
            match subset:
                case None:
                    names = None
                case [
                    {
                        "Selector": {
                            "ByName": {
                                "names": names,
                                "strict": True,
                                **extras_1,
                            },
                            **extras_2,
                        },
                        **extras_3,
                    }
                ]:
                    assert_no_extras(extras_1, extras_2, extras_3)
                case _:
                    raise NotImplementedError(f"Unsupported {subset=}")
            input_table = update_polars_to_ibis(
                input_expr, table=table, backend=backend
            )
            return input_table.distinct(on=names, keep="first")
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.DISTINCT}")


@table_handler(tags.table.FILTER)
def handle_filter(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table, backend=backend)
    match payload:
        case {"predicate": predicate, **extras}:
            assert_no_extras(extras)
            value = polars_expr_to_ibis_value(predicate)
            return input_table.filter(value)  # type: ignore
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.FILTER}")


@table_handler(tags.table.SLICE)
def handle_slice(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table, backend=backend)
    match payload:
        case {"len": len, "offset": offset, **extras}:
            assert_no_extras(extras)
            if offset < 0:
                raise NotImplementedError(f"Unsupported offset: {offset}")
            return input_table.limit(len, offset=offset)
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.SLICE}")


@table_handler(tags.table.SORT)
def handle_sort(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table, backend=backend)
    match payload:
        case {
            "by_column": by_column,
            "sort_options": {
                "descending": descending,
                "multithreaded": True,
                "limit": None,
                # The test suite covers both True or False for maintain_order,
                # and [True] and [False] for nulls_last.
                # but Ibis order_by() does not have any options.
                "maintain_order": _maintain_order,
                "nulls_last": _nulls_last,
                **extras_1,
            },
            "slice": None,
            "input": _,
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            undirected_sort_keys = parse_sort_by_column(by_column)
            directed_sort_keys = [
                ibis.desc(key) if desc else key
                for key, desc in zip(undirected_sort_keys, descending)
            ]
            return input_table.order_by(*directed_sort_keys)  # type: ignore
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.SORT}")


@table_handler(tags.table.H_STACK)
def handle_hstack(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    match payload:
        case {
            "exprs": exprs,
            "input": source,
            "options": {
                "duplicate_check": True,
                "run_parallel": True,
                "should_broadcast": True,
                **extras_1,
            },
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.H_STACK} payload")

    input_table = update_polars_to_ibis(source, table=table, backend=backend)
    # All with_columns expressions evaluate against the same input relation.
    return input_table.mutate(**translate_named_exprs(exprs, input_table))


@table_handler(tags.table.GROUP_BY)
def handle_group_by(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    match payload:
        case {
            "input": source,
            "keys": keys,
            "aggs": aggs,
            "maintain_order": False,
            "options": {"dynamic": None, "rolling": None, "slice": None, **extras_1},
            **extras,
        }:
            assert_no_extras(extras_1)
            if extras.get("apply") is not None or extras.get("predicates"):
                raise NotImplementedError("Unsupported GroupBy options")
            assert_no_extras(
                {
                    key: value
                    for key, value in extras.items()
                    if key not in ("apply", "predicates")
                }
            )
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.GROUP_BY}")

    input_table = update_polars_to_ibis(source, table=table, backend=backend)
    by = translate_named_exprs(keys, input_table)
    metrics = translate_named_exprs(aggs, input_table)
    if not metrics:
        return input_table.select(**by).distinct()
    return input_table.group_by(**by).aggregate(**metrics)


def _stats_expr(stats: Any, column: str) -> PolarsPlan:
    """Convert a whole-table statistic to an ordinary aggregate expression."""
    tag, options = (
        split_tag_payload(stats) if isinstance(stats, dict) else (stats, None)
    )
    source = {tags.value.COLUMN: column}

    if tag in (tags.value.SUM, tags.value.MEAN, tags.value.MEDIAN):
        payload = source
    elif tag in (tags.value.MIN, tags.value.MAX):
        payload = {"input": source, "propagate_nans": False}
    elif tag in (tags.value.VAR, tags.value.STD) and options == {"ddof": 1}:
        payload = [source, 1]
    elif tag == tags.value.QUANTILE and isinstance(options, dict):
        payload = {"expr": source, **options}
    else:
        raise NotImplementedError(f"Unsupported Stats: {stats!r}")

    return {tags.value.AGG: {tag: payload}}


@table_handler(tags.table.MAP_FUNCTION)
def handle_map_function(
    payload: PolarsPlan,
    table: ir.Table,
    backend: ibis.BaseBackend,
) -> ir.Table:
    input_table = update_polars_to_ibis(payload["input"], table=table, backend=backend)
    match payload:
        case {"function": {"Stats": stats, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return input_table.aggregate(
                **{
                    name: polars_expr_to_ibis_value(_stats_expr(stats, name))
                    for name in input_table.columns
                }
            )
        case {"function": {"FillNan": fill_nan_expr, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            fill_nan_value = polars_expr_to_ibis_value(fill_nan_expr)
            if not backend.has_operation(ibis.expr.operations.IsNan):
                return input_table
            # No ibis "fill_nan()", so we do it by hand:
            return input_table.select(
                **{
                    col: input_table[col]
                    .isnan()  # type: ignore
                    .ifelse(fill_nan_value, input_table[col])
                    for col in input_table.columns
                }
            )
        case _:
            raise NotImplementedError(f"Unsupported {tags.table.MAP_FUNCTION}")

