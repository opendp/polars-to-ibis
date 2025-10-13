"""Convert Polars expressions to Ibis expressions"""

import json
from pathlib import Path

import ibis
import polars as pl

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()


def polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    polars_json = lf.serialize(format="json")
    polars_plan = json.loads(polars_json)

    polars_schema = lf.collect_schema()
    ibis_schema = _polars_schema_to_ibis(polars_schema)
    ibis_table = ibis.table(ibis_schema, name=table_name)

    return _apply_polars_plan_to_ibis_table(polars_plan, ibis_table)


class UnhandledPolarsException(Exception):
    pass


def _apply_polars_plan_to_ibis_table(polars_plan: dict, table: ibis.Table):
    # TODO: More general!
    if "Slice" in polars_plan:
        slice_params = polars_plan["Slice"]
        return table.limit(slice_params["len"], offset=slice_params["offset"])
    raise UnhandledPolarsException("Unhandled polars plan")


_type_map = {
    # TODO: Expand this!
    pl.Int64: "int64",
    pl.UInt32: "uint32",
    pl.Float64: "float64",
    pl.String: "string",
    pl.Boolean: "boolean",
}


def _polars_schema_to_ibis(schema: pl.Schema):
    return {k: _type_map[v] for k, v in schema.items()}
