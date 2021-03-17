from typing import Union, Dict, Tuple, List
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import arcpy
from arcpy._mp import Table, Layer
from collections import OrderedDict
import time



def make_arcpy_query(fc: Union[Path, str, Layer, Table], fields: Union[list, str]="*", where:Union[None, str]=None, output="dict") -> Union[dict, Dict[int, OrderedDict]]:
    """
    Read a feature class or feature layer with a search cursor and adds
    each field to an ordered dict with k=field name, v=value. The ordered_dict
    is then added to another ordered dict with k=OID and v=attributes.
    """
    if type(fields) is str:
        fields = [fields]
    scursor = arcpy.da.SearchCursor(fc, ["OID@", "SHAPE@"]+ fields, where)
    s_fields = scursor.fields
    if output=="dict":
        rows = OrderedDict()
        for row in scursor:
            this_row = OrderedDict()
            for field, value in zip(s_fields[1:], row[1:]):
                this_row[field] = value
            if this_row:
                rows[row[0]] = this_row
    elif output=="list":
        rows = list()
        for row in scursor:
            rows.append(row)

    del scursor

    return rows, s_fields

def fc_reader(fc: Union[Path, str, Layer, Table], fields: Union[list, str]="*", where:Union[None, str]=None):
    with arcpy.da.SearchCursor(fc, fields, where) as scursor:
        for row in scursor:
            yield row


def arcpy_to_df(fc: Union[Path, str, Layer, Table], fields: Union[list, str]="*", where:Union[None, str]=None) -> DataFrame:
    arcpy_data, s_fields = make_arcpy_query(fc, fields, where, "list")
    return pd.DataFrame(arcpy_data, columns=s_fields)

if __name__ == "__main__":
    import os
    from config import CONNECTION_FILE
    # ss_mains_df = feature_class_to_pandas_data_frame(os.path.join(CONNECTION_FILE, "RPUD.ssGravityMain"), "*")
    start_time = time.time()
    ss_mains_df = arcpy_to_df(os.path.join(CONNECTION_FILE, "RPUD.ssGravityMain"))
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("here")