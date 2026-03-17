"""
LazyFrame.serialize() does not return stable results between Polars versions,
and may be dropped in the future.
Pulling out the serialization and validation logic keeps the rest of the code simple.
"""

import json

import polars as pl


class UnexpectedPolarsException(Exception):
    """
    JSON structure is not what we expected.
    """

    pass


class Serialization:
    def __init__(self, lf: pl.LazyFrame):
        self._serial = json.loads(lf.serialize(format="json"))
        self._validate()

    def _validate(self):
        if len(self.keys()) != 1:
            raise UnexpectedPolarsException(  # pragma: no cover
                f"Expected only a single key, not: {self.keys()}"
            )

    def __getitem__(self, name: str):
        return self._serial[name]

    def keys(self):  # type: ignore
        return self._serial.keys()  # type: ignore

    def values(self):  # type: ignore
        return self._serial.values()  # type: ignore
