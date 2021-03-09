import arcpy
import os
from pathlib import Path

from arcpy.management import MakeFeatureLayer
from config import WAKE_PARCELS_URL, DURHAM_PARCELS_URL, FRANKLIN_PARCELS_URL, GRAVITY_MAINS, SEWER_BASINS_URL, APRX
from arcpy_functions import make_arcpy_query

arcpy.env.parallelProcessingFactor = "50%"
arcpy.env.overwriteOutput = True

TEMP_OUT_GDB = os.path.join(os.path.dirname(__file__), "easements", "easements.gdb")


def copy_fc_to_memory(fc_path: str):
    name = os.path.basename(fc_path).split(".")[1]
    memory_name = rf"memory\{name}"
    arcpy.management.CopyFeatures(fc_path, memory_name)
    return memory_name

def copy_fc_to_temp_gdb(fc, temp_gdb, out_name=None):
    if out_name:
        new_name = out_name
        new_path = os.path.join(temp_gdb, out_name)
    elif type(fc) is arcpy._mp.Layer:
        new_name = fc.longName.replace(" ", "_").lower()
        new_path = os.path.join(temp_gdb, new_name)
    else:
        raise Exception("Expecting an arcpy Layer")
    try:
        arcpy.management.CopyFeatures(fc, new_path)
    except arcpy.ExecuteError:
        try:
            arcpy.conversion.FeatureClassToFeatureClass(fc, temp_gdb, new_name)
        except arcpy.ExecuteError:
            arcpy.CreateFeatureclass_management(temp_gdb, new_name, "POLYGON", fc)
            fc_data = make_arcpy_query(fc)
            icursor = arcpy.InsertCursor(new_path)
            for row in fc_data.values():
                icursor.insertRow(row)
    return new_path


def main(copy_data=False):
    aprx = arcpy.mp.ArcGISProject(APRX)

    m = aprx.listMaps()[0]

    gravity_mains = m.addDataFromPath(GRAVITY_MAINS if copy_data else os.path.join(TEMP_OUT_GDB, "sewer_gravity_mains"))

    wake_parcels = m.addDataFromPath(WAKE_PARCELS_URL if copy_data else os.path.join(TEMP_OUT_GDB, "wake_property"))

    franklin_parcels = m.addDataFromPath(FRANKLIN_PARCELS_URL if copy_data else os.path.join(TEMP_OUT_GDB, "franklin_parcels"))

    if copy_data:

        gravity_mains_fc = copy_fc_to_temp_gdb(gravity_mains, TEMP_OUT_GDB)

        gravity_mains = arcpy.MakeFeatureLayer_management(gravity_mains_fc)

        wake_parcels_fc = copy_fc_to_temp_gdb(wake_parcels, TEMP_OUT_GDB)
        wake_parcels = arcpy.MakeFeatureLayer_management(wake_parcels_fc)

        # durham_parcels_fc = copy_fc_to_temp_gdb(durham_parcels, TEMP_OUT_GDB, "durham_parcels")

        franklin_parcels_fc = copy_fc_to_temp_gdb(franklin_parcels, TEMP_OUT_GDB, "franklin_parcels")
        franklin_parcels = arcpy.MakeFeatureLayer_management(franklin_parcels_fc)


    gravity_mains_filter = arcpy.MakeFeatureLayer_management(gravity_mains, "gravity_mains_filter", where_clause="ACTIVEFLAG = 1 and OWNEDBY = 0")

    # gravity_mains_data = make_arcpy_query(gravity_mains_filter)

    # intersect_mains = []

    # count = 0
    # for i in range(500, 100000, 500):
    #     i += 1
    #     search_string = ", ".join([str(r["OBJECTID"]) for r in gravity_mains_data.values()][count:i])
    #     gravity_mains_filter_select = arcpy.MakeFeatureLayer_management(gravity_mains_filter, "gravity_mains_filter_select", where_clause=f"OBJECTID in ({search_string})")
    #     arcpy.SelectLayerByLocation_management(gravity_mains_filter_select, select_features=wake_parcels)
    #     intersect = [r for r in arcpy.da.SearchCursor(gravity_mains_filter_select, ["OID@"])]
    #     if intersect:
    #         intersect_string = "', '".join([str(r["OBJECTID"]) for r in intersect])
    #         intersect_selection = make_arcpy_query(gravity_mains_filter_select, where=f"OBJECTID in ('{intersect_string}')")
    #         for item in intersect_selection.values():
    #             intersect_mains.append(item)


    # with arcpy.da.SearchCursor(gravity_mains_filter, ["OID@", "SHAPE@"]) as scursor:
    #     for row in scursor:
    #         arcpy.SelectLayerByLocation_management(wake_parcels, select_features=row[1])
    #         intersect = [r for r in arcpy.da.SearchCursor(wake_parcels, ["OID@"])]
    #         if intersect:
    #             intersect_mains.append(row[0])
    #         count += 1
    #         if count %100 ==0:
    #             print(count)

    wake_parcels_select = arcpy.SelectLayerByLocation_management(wake_parcels, select_features=gravity_mains_filter)


    franklin_parcels_select = arcpy.SelectLayerByLocation_management(franklin_parcels, select_features=gravity_mains_filter)

    wake_parcels_shapes = make_arcpy_query(wake_parcels, fields="SHAPE@")

    franklin_parcels_shapes = make_arcpy_query(franklin_parcels, fields="SHAPE@")

    all_parcels = [list(s.values())[0] for s in wake_parcels_shapes.values()] + [list(s.values())[0] for s in franklin_parcels_shapes.values()]

    arcpy.SelectLayerByLocation_management(gravity_mains_filter, select_features=all_parcels)

    # arcpy.SelectLayerByLocation_management(gravity_mains_filter, select_features=durham_parcels, selection_type="ADD_TO_SELECTION")

    # arcpy.SelectLayerByLocation_management(gravity_mains_filter, select_features=franklin_parcels, selection_type="ADD_TO_SELECTION")
    clip_fc = os.path.join("memory", "mains_clip")
    arcpy.Clip_analysis(gravity_mains_filter, wake_parcels, clip_fc)
    statistics_value_table = arcpy.ValueTable(2)
    statistics_value_table.addRow("'DIAMETER' 'MEAN'")
    dissolve_fc = os.path.join("memory", "mains_dissolve")
    arcpy.Dissolve_management(clip_fc, dissolve_fc, dissolve_field="DIAMETER", statistics_fields=statistics_value_table, multi_part=False)
    intersection_points = os.path.join("memory", "intersection_points")
    intersect_value_table = arcpy.ValueTable(2)
    intersect_value_table.addRow(f"'{dissolve_fc}' ''")
    intersect_value_table.addRow(f"'{dissolve_fc}' ''")
    arcpy.Intersect_analysis(intersect_value_table, intersection_points, output_type="POINT")
    arcpy.DeleteIdentical_management(intersection_points, "SHAPE")
    split_mains = os.path.join(TEMP_OUT_GDB, "main_split")
    arcpy.SplitLineAtPoint_management(dissolve_fc, intersection_points, out_feature_class=split_mains)




    print("here")


    # memory_property_fc = copy_fc_to_memory(property_fc)

    # arcpy.management.MakeFeatureLayer(property_fc, "wake_property")

    # property_fl = "wake_property"

    # for target_fc in TARGET_FCS:
    #     count = 0
    #     fc_path = os.path.join(DB_CONNECTIONS, RPUD_DB, target_fc)
    #     fc_name = target_fc.split('.')[1]
    #     arcpy.management.MakeFeatureLayer(fc_path, f"{fc_name}_lyr")
    #     fl =  f"{fc_name}_lyr"
    #     # arcpy.SelectLayerByLocation_management(fl, overlap_type="INTERSECT_DBMS", select_features=property_fl)
    #     with arcpy.da.SearchCursor(fl, ["FACILITYID", "SHAPE@"]) as scursor:
    #         for row in scursor:
    #             arcpy.SelectLayerByLocation_management(property_fl, select_features=row[-1])
    #             intersect = [r for r in arcpy.da.SearchCursor(property_fl, "OBJECTID@")]
    #             if intersect:
    #                 print("Interesct")
    #             # count += 1
    #             # if count%100 == 0:
    #             #     print(count)
    #             # print(row)




    return



if __name__ == "__main__":

    try:
        main(False)
    except Exception as ex:
        raise ex