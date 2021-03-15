from typing import Union, Dict, Tuple, List
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import arcpy
from arcpy._mp import Table, Layer
from collections import OrderedDict



def make_arcpy_query(fc: Union[Path, str, Layer, Table], fields: Union[list, str]="*", where:Union[None, str]=None) -> Union[dict, Dict[int, OrderedDict]]:
    """
    Read a feature class or feature layer with a search cursor and adds
    each field to an ordered dict with k=field name, v=value. The ordered_dict
    is then added to another ordered dict with k=OID and v=attributes.
    """
    rows = OrderedDict()
    s_fields = ["OID@", "SHAPE@"] + list(arcpy.da.SearchCursor(fc, fields).fields)
    with arcpy.da.SearchCursor(fc, s_fields, where) as scursor:
        for row in scursor:
            this_row = OrderedDict()
            for field, value in zip(s_fields[1:], row[1:]):
                this_row[field] = value
            if this_row:
                rows[row[0]] = this_row
    return rows


def arcpy_to_df(fc: Union[Path, str, Layer, Table], fields: Union[list, str]="*", where:Union[None, str]=None) -> DataFrame:
    arcpy_data = make_arcpy_query(fc, fields, where)
    return pd.DataFrame.from_dict(arcpy_data, "index")