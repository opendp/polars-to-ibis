"""Lower OpenDP's logical sparse histogram relation, not its score arithmetic.

The native aggregate returns sparse structs (and Polars flattens those structs).
SQL instead counts contribution rows directly.  The private observed-key domain
is retained before expression-local filters; it is not a public-key sanitizer.
"""

import io
import pickle
import pickletools
from typing import Any

import ibis
import ibis.expr.types as ir
import polars as pl
from ibis.common.deferred import Deferred

from .value_handlers import polars_expr_to_ibis_value

_SYMBOL = "discrete_quantile_histogram_plugin"


class _PrimitiveUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        raise ValueError("OpenDP histogram parameters cannot contain pickle globals")

    def persistent_load(self, pid):
        raise ValueError("OpenDP histogram parameters cannot contain persistent IDs")


def _parameter_dict(data):
    data = bytes(data)
    # EXT opcodes can bypass find_class through pickle's process-wide extension
    # cache. Only primitive containers/scalars are part of the Rust contract.
    executable = {
        "GLOBAL",
        "STACK_GLOBAL",
        "REDUCE",
        "BUILD",
        "INST",
        "OBJ",
        "NEWOBJ",
        "NEWOBJ_EX",
        "EXT1",
        "EXT2",
        "EXT4",
        "PERSID",
        "BINPERSID",
    }
    if any(op.name in executable for op, _, _ in pickletools.genops(data)):
        raise ValueError(
            "OpenDP histogram parameters cannot contain pickle globals or objects"
        )
    return _PrimitiveUnpickler(io.BytesIO(data)).load()


def _selector_names(selector):
    match selector:
        case {"ByName": {"names": names, "strict": True}}:
            return names
        case {"Union": selectors}:
            return [name for child in selectors for name in _selector_names(child)]
        case _:
            raise NotImplementedError("Unsupported histogram flatten selector")


def _bind(expr, table):
    value = polars_expr_to_ibis_value(expr)
    if isinstance(value, Deferred):
        value = value.resolve(table)
    return value if isinstance(value, ir.Value) else ibis.literal(value)


def _and(left, right):
    if left is None:
        return right
    if right is None or left == right:
        return left
    return {"BinaryExpr": {"left": left, "op": "And", "right": right}}


def _or_null(condition, value):
    return {
        "BinaryExpr": {
            "left": condition,
            "op": "Or",
            "right": {
                "Function": {"input": [value], "function": {"Boolean": "IsNull"}}
            },
        }
    }


def _row_expression(expr):
    """Return a row expression, its selection predicate, and literal origin.

    This is expression-to-SQL lowering.  Removing an aggregation-local filter
    changes neither the source relation nor other heads.  Its predicate is
    applied only to this histogram's contribution rows.  Independent positional
    selections cannot be replaced with a conjunction, so reject that translation
    rather than silently pair different observations.
    """
    match expr:
        case {"Literal": _}:
            return expr, None, True
        case {"Column": _}:
            return expr, None, False
        case {"Alias": [child, name]}:
            value, mask, literal = _row_expression(child)
            return {"Alias": [value, name]}, mask, literal
        case {"Filter": {"input": child, "by": predicate}}:
            value, mask, literal = _row_expression(child)
            by, by_mask, by_literal = _row_expression(predicate)
            if by_mask is not None:
                raise NotImplementedError(
                    "A filtered histogram predicate is positional"
                )
            return value, _and(mask, by), literal and by_literal
        case {"Function": {"input": [child], "function": op}} if op in (
            "DropNulls",
            "DropNans",
        ):
            value, mask, literal = _row_expression(child)
            condition = {
                "Function": {
                    "input": [value],
                    "function": {
                        "Boolean": "IsNotNull" if op == "DropNulls" else "IsNotNan"
                    },
                }
            }
            if op == "DropNans":
                # drop_nans retains nulls; an outer fill_null can turn them
                # into observations. SQL WHERE must not discard UNKNOWN here.
                condition = _or_null(condition, value)
            return value, _and(mask, condition), literal
        case {"Cast": payload}:
            children, rebuild = (
                [payload["expr"]],
                lambda v: {"Cast": {**payload, "expr": v[0]}},
            )
        case {"Function": payload}:
            children, rebuild = (
                payload["input"],
                lambda v: {"Function": {**payload, "input": v}},
            )
        case {"BinaryExpr": payload}:
            children = [payload["left"], payload["right"]]
            rebuild = lambda v: {"BinaryExpr": {**payload, "left": v[0], "right": v[1]}}
        case {"Ternary": payload}:
            fields = ("predicate", "truthy", "falsy")
            children = [payload[field] for field in fields]
            rebuild = lambda v: {"Ternary": dict(zip(fields, v))}
        case _:
            raise NotImplementedError("Unsupported histogram contribution expression")
    lowered = [_row_expression(child) for child in children]
    masks = [mask for _, mask, _ in lowered if mask is not None]
    mask = masks[0] if masks else None
    if any(other != mask for other in masks) or (
        mask is not None and any(m is None and not lit for _, m, lit in lowered)
    ):
        raise NotImplementedError("Independent positional histogram selections")
    return (
        rebuild([value for value, _, _ in lowered]),
        mask,
        all(lit for _, _, lit in lowered),
    )


def _bucket(value, candidates, start=0, end=None):
    """A backend-independent typed CASE implementation; not a splitter policy."""
    end = len(candidates) if end is None else end
    if start == end:
        return ibis.literal(2 * start, type="uint64")
    mid = start + (end - start) // 2
    pivot = ibis.literal(candidates[mid], type=str(candidates.dtype).lower())
    return ibis.ifelse(
        value < pivot,
        _bucket(value, candidates, start, mid),
        ibis.ifelse(
            value == pivot,
            ibis.literal(2 * mid + 1, type="uint64"),
            _bucket(value, candidates, mid + 1, end),
        ),
    )


def try_histogram_relation(plan: dict[str, Any], table, backend, translate):
    """Recognize the native sparse aggregate/flatten relation, or return None."""
    outer = plan.get("MapFunction", {})
    unnest = outer.get("function", {}).get("Unnest")
    if unnest is None:
        return None
    names = _selector_names(unnest["columns"])
    source = outer["input"]
    exploded = source.get("MapFunction", {})
    explosion = exploded.get("function", {}).get("Explode")
    if explosion is not None:
        if _selector_names(explosion["columns"]) != names:
            return None
        source = exploded["input"]
    if "GroupBy" in source:
        payload = source["GroupBy"]
        expressions, keys = payload["aggs"], payload["keys"]
        if explosion is None:
            return None
    elif "Select" in source:
        payload = source["Select"]
        expressions, keys = payload["expr"], []
        if explosion is not None:
            return None
    else:
        return None
    if len(expressions) != 1:
        return None
    match expressions[0]:
        case {
            "Alias": [
                {
                    "Function": {
                        "input": [input_expr],
                        "function": {"FfiPlugin": plugin},
                    }
                },
                name,
            ]
        } if names == [name]:
            if plugin.get("symbol") != _SYMBOL or "opendp" not in plugin.get("lib", ""):
                return None
        case _:
            return None
    args = _parameter_dict(plugin["kwargs"])
    candidates = pl.DataFrame.deserialize(
        io.BytesIO(args["scorer"]["candidates"])
    ).to_series()
    bucket_name, count_name = args["bucket_name"], args["count_name"]
    base = translate(payload["input"], table=table, backend=backend)
    key_values = {}
    for key in keys:
        match key:
            case {"Column": name}:
                key_values[name] = base[name]
            case {"Alias": [expr, name]}:
                key_values[name] = _bind(expr, base)
            case _:
                raise NotImplementedError("Unsupported histogram grouping expression")
    key_names = list(key_values)
    row, mask, literal = _row_expression(input_expr)
    domain = (
        base.mutate(**key_values).select(key_names).distinct()
        if keys
        else ibis.literal(0).as_table()
    )
    if literal:
        contributions = domain
        if mask is not None:
            contributions = contributions.filter(_bind(mask, contributions))
        value = _bind(row, contributions)
    else:
        # Bind before materializing key aliases: an output key alias may have
        # the same name as a source value used by the scorer.
        used = set(base.columns) | set(key_names) | {bucket_name, count_name}

        def fresh(suffix):
            name = f"{bucket_name}_{suffix}"
            while name in used:
                name += "_"
            used.add(name)
            return name

        value_name, mask_name = fresh("value"), fresh("selected")
        extra = {value_name: _bind(row, base)}
        if mask is not None:
            extra[mask_name] = _bind(mask, base)
        contributions = base.mutate(**extra).mutate(**key_values)
        if mask is not None:
            contributions = contributions.filter(contributions[mask_name])
        value = contributions[value_name]
    counted = contributions.mutate(**{bucket_name: _bucket(value, candidates)})
    counts = counted.group_by(key_names + [bucket_name]).aggregate(
        **{count_name: counted.count().cast("uint64")}
    )
    # A missing contribution is an observed group with an all-zero histogram,
    # not an absent private group.  Keep public-domain imputation above heads.
    predicates = [domain[name].identical_to(counts[name]) for name in key_names]
    joined = domain.left_join(counts, predicates or [ibis.literal(True)])
    return joined.select(
        **{name: domain[name] for name in key_names},
        **{
            bucket_name: counts[bucket_name].fill_null(ibis.literal(0, type="uint64")),
            count_name: counts[count_name].fill_null(ibis.literal(0, type="uint64")),
        },
    )
