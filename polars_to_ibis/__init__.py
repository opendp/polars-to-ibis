"""Convert Polars expressions to Ibis expressions"""

from pathlib import Path

import ibis
import polars as pl

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()


def polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    # TODO:
    # - Serialize LazyFrame
    # - Read schema
    # - Translate expr into Ibis

    # pl_json = lf.serialize(format='json')
    # import json
    # pl_dict = json.loads(pl_json)
    # del pl_dict['Slice']["input"]["DataFrameScan"]["df"]
    # pl_json_clean = json.dumps(pl_dict)

    ibis_schema = {
        "ints": "int64",
        "floats": "float64",
        "strings": "string",
        "bools": "boolean",
    }
    ibis_table = ibis.table(ibis_schema, name=table_name).head(1)
    return ibis_table


# def polars_expr_to_ibis(pl_expr: pl.Expr) -> ibis.common.deferred.Deferred:
#     # JSON serialization is deprecated,
#     # but use it to start from something.
#     # Down the road maybe we convert the binary to JSON?
#     pl_expr_json = pl_expr.meta.serialize(format="json")
#     pl_expr_dict = json.loads(pl_expr_json)
#     return _polars_dict_to_ibis(pl_expr_dict)


# def _polars_dict_to_ibis(pl_expr_dict: dict) -> ibis.common.deferred.Deferred:
#     # TODO! Transform pl_expr_dict into the AST for Ibis
#     node = ast.parse('ibis._.group_by("bar").agg(foo=ibis._.foo.sum())', mode="eval")

#     py_expr = ast.unparse(node)
#     return eval(py_expr, {"ibis": ibis})
