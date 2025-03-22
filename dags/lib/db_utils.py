from datetime import datetime, timezone
from typing import List

import pandas as pd


# def add_metadata_to_dataframe(df: pd.DataFrame, username: str) -> None:
#     """add metadata such as created_date, created_by, updated_date, update_by
#     to the dataframe."""

#     df.loc[:, "created_date"] = datetime.now(timezone.utc)
#     df.loc[:, "created_by"] = username
#     df.loc[:, "updated_date"] = datetime.now(timezone.utc)
#     df.loc[:, "updated_by"] = username


def get_tuples(df: pd.DataFrame):
    tuples = [tuple(x) for x in df.to_numpy()]
    return tuples


def get_columns(df: pd.DataFrame) -> str:
    cols = ",".join(list(df.columns))
    return cols

def get_columns_in_list(df: pd.DataFrame) -> List:
    return df.columns.tolist()

def prepare_dataframe_to_insertion(df: pd.DataFrame, table: str, username: str = None):
    # add 4 metadata columns to df
    # add_metadata_to_dataframe(df, username)

    # create tuples of values
    # and string of df columns
    tuples = get_tuples(df)
    columns = get_columns(df)

    query = "INSERT INTO %s(%s) VALUES %%s" % (table, columns)

    return query, tuples
