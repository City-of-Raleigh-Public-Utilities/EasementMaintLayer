from os.path import split
import arcpy
import os
from pathlib import Path

from arcpy.management import MakeFeatureLayer
from config import WAKE_PARCELS_URL, FRANKLIN_PARCELS_URL, GRAVITY_MAINS, SEWER_BASINS_URL, APRX, CONNECTION_FILE
from arcpy_functions import make_arcpy_query

arcpy.env.parallelProcessingFactor = "50%"
arcpy.env.overwriteOutput = True

TEMP_OUT_GDB = os.path.join(os.path.dirname(__file__), "easements", "easements.gdb")
ADD_FIELDS = [
        ["FACILITYID", "TEXT", "", 20],
        ["EASEMENT_TYPE", "TEXT", "", 20],
        ["EASEMENT_LENGTH", "DOUBLE"],
        ["SEWER_BASIN", "TEXT", "", 20],
        ["PIPE_DIAMETER", "DOUBLE"]
    ]


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

def insert_new_records(source_fc, target_fc):
    source_data = make_arcpy_query(source_fc)
    target_data = make_arcpy_query(target_fc)
    existing_facids = [r["FACILITYID"] for r in target_data.values()]
    source_data = [r for r in source_data.values() if r["FACILITYID"] not in existing_facids]
    edit = arcpy.da.Editor(CONNECTION_FILE)
   
    edit.startEditing(False, True)
    edit.startOperation()
    with arcpy.da.InsertCursor(target_fc, ["FACILITYID", "EASEMENT_TYPE", "EASEMENT_LENGTH", "SEWER_BASIN", "SHAPE@"]) as insert_cursor:
        for new_row in source_data:
            insert_cursor.insertRow([new_row["FACILITYID"], new_row["EASEMENT_TYPE"], new_row["EASEMENT_LENGTH"], new_row["SEWER_BASIN"], new_row["SHAPE@"]], )
    edit.stopOperation()
    edit.stopEditing(True)
    del edit
    
    return


def main(copy_data=False):

    # split_mains_buffer = os.path.join(TEMP_OUT_GDB, "split_mains_buffer")
    # existing_easement_fc = os.path.join(CONNECTION_FILE, "RPUD.EasementMaintenanceAreas")
    # insert_new_records(split_mains_buffer, existing_easement_fc)
    # return
    aprx = arcpy.mp.ArcGISProject(APRX)

    m = aprx.listMaps()[0]

    gravity_mains_fc  = os.path.join(CONNECTION_FILE, "RPUD.ssGravityMain") if copy_data else os.path.join(TEMP_OUT_GDB, "sewer_gravity_mains")

    wake_parcels = m.addDataFromPath(WAKE_PARCELS_URL if copy_data else os.path.join(TEMP_OUT_GDB, "wake_property"))

    franklin_parcels = m.addDataFromPath(FRANKLIN_PARCELS_URL if copy_data else os.path.join(TEMP_OUT_GDB, "franklin_parcels"))

    if copy_data:

        gravity_mains_fc = copy_fc_to_temp_gdb(gravity_mains_fc, TEMP_OUT_GDB, "sewer_gravity_mains")

        wake_parcels_fc = copy_fc_to_temp_gdb(wake_parcels, TEMP_OUT_GDB)
        wake_parcels = arcpy.MakeFeatureLayer_management(wake_parcels_fc)


        franklin_parcels_fc = copy_fc_to_temp_gdb(franklin_parcels, TEMP_OUT_GDB, "franklin_parcels")
        franklin_parcels = arcpy.MakeFeatureLayer_management(franklin_parcels_fc)


    gravity_mains_filter = arcpy.MakeFeatureLayer_management(gravity_mains_fc, "gravity_mains_filter", where_clause="ACTIVEFLAG = 1 and OWNEDBY = 0")

    arcpy.SelectLayerByLocation_management(wake_parcels, select_features=gravity_mains_filter)

    arcpy.SelectLayerByLocation_management(franklin_parcels, select_features=gravity_mains_filter)

    wake_parcels_shapes = make_arcpy_query(wake_parcels, fields="SHAPE@")

    franklin_parcels_shapes = make_arcpy_query(franklin_parcels, fields="SHAPE@")

    all_parcels = [list(s.values())[0] for s in wake_parcels_shapes.values()] + [list(s.values())[0] for s in franklin_parcels_shapes.values()]

    arcpy.SelectLayerByLocation_management(gravity_mains_filter, select_features=all_parcels)

    wake_clip_fc = os.path.join("memory", "wake_mains_clip")
    arcpy.Clip_analysis(gravity_mains_filter, wake_parcels, wake_clip_fc)

    franklin_clip_fc = os.path.join("memory", "franklin_mains_clip")
    arcpy.Clip_analysis(gravity_mains_filter, franklin_parcels, franklin_clip_fc)

    clip_fc = os.path.join("memory", "clip_fc")
    arcpy.Merge_management([wake_clip_fc, franklin_clip_fc], clip_fc)
    arcpy.Delete_management(wake_clip_fc)
    arcpy.Delete_management(franklin_clip_fc)

    statistics_value_table = arcpy.ValueTable(2)
    statistics_value_table.addRow("'DIAMETER' 'MEAN'")
    dissolve_fc = os.path.join("memory", "mains_dissolve")
    arcpy.Dissolve_management(clip_fc, dissolve_fc, dissolve_field="DIAMETER", statistics_fields=statistics_value_table, multi_part=False) # arcpy.Delete_management(clip_fc)

    intersection_points = os.path.join("memory", "intersection_points")
    intersect_value_table = arcpy.ValueTable(2)
    intersect_value_table.addRow(f"'{dissolve_fc}' ''")
    intersect_value_table.addRow(f"'{dissolve_fc}' ''")
    arcpy.Intersect_analysis(intersect_value_table, intersection_points, output_type="POINT")
    arcpy.DeleteIdentical_management(intersection_points, "SHAPE")
    split_mains = os.path.join("memory", "main_split")
    arcpy.SplitLineAtPoint_management(dissolve_fc, intersection_points, out_feature_class=split_mains) # arcpy.Delete_management(intersection_points)

    arcpy.AddFields_management(split_mains, ADD_FIELDS)

    id_counts = {"SSGMNC": 1, "SSGMC":1}
    with arcpy.da.UpdateCursor(split_mains, ["DIAMETER", "FACILITYID", "EASEMENT_TYPE", "EASEMENT_LENGTH", "SEWER_BASIN", "PIPE_DIAMETER", "SHAPE@"]) as ucursor:
        for row in ucursor:
            critical_value = "SSGMNC" if row[0] < 15 else "SSGMC"

            facility_id = f"{critical_value}{str(id_counts[critical_value]).zfill(6)}" if row[0] < 15 else f"{critical_value}{str(id_counts[critical_value]).zfill(6)}"
            easement_type = "SSGM-Non-Critical" if row[0] < 15 else "SSGM-Critical"
            easement_length = row[-1].length
            if easement_length < 10:
                ucursor.deleteRow()
                continue
            row[1] = facility_id
            row[2] = easement_type
            row[3] = easement_length
            row[5] = row[0]
            ucursor.updateRow(row)
            id_counts[critical_value] += 1
    split_mains_buffer = os.path.join("memory", "split_mains_buffer")
    split_mains_buffer = os.path.join(TEMP_OUT_GDB, "split_mains_buffer")
    arcpy.Buffer_analysis(split_mains, split_mains_buffer, 20, "FULL", "ROUND")
    arcpy.DeleteField_management(split_mains_buffer, ["DIAMETER", "MEAN_DIAMETER", "BUFF_DIST", "ORIG_FID"]) 
    arcpy.Delete_management(split_mains)

    wake_split_mains_buffer_clip = os.path.join("memory", "wake_split_mains_buffer_clip")
    arcpy.SelectLayerByLocation_management(wake_parcels, select_features=split_mains_buffer)

    arcpy.Clip_analysis(split_mains_buffer, wake_parcels, wake_split_mains_buffer_clip)

    franklin_split_mains_buffer_clip = os.path.join("memory", "franklin_split_mains_buffer_clip")

    arcpy.SelectLayerByLocation_management(franklin_parcels, select_features=split_mains_buffer)

    arcpy.Clip_analysis(split_mains_buffer, franklin_parcels, franklin_split_mains_buffer_clip)

    arcpy.Merge_management([wake_split_mains_buffer_clip, franklin_split_mains_buffer_clip], split_mains_buffer) 

    arcpy.Delete_management(wake_split_mains_buffer_clip)

    arcpy.Delete_management(franklin_split_mains_buffer_clip)

    sewer_basins = arcpy.MakeFeatureLayer_management(os.path.join(CONNECTION_FILE, "RPUD.SewerBasins"))

    update_counts = {}
    with arcpy.da.SearchCursor(sewer_basins, ["BASINS", "SHAPE@"]) as scursor:
        for row in scursor:
            within = arcpy.SelectLayerByLocation_management(split_mains_buffer, "COMPLETELY_WITHIN", row[1])
            if within:
                with arcpy.da.UpdateCursor(within, ["OID@", "SEWER_BASIN"]) as ucursor:
                    for urow in ucursor:
                        urow[1] = row[0]
                        ucursor.updateRow(urow)
                        if urow[0] in update_counts:
                            update_counts[urow[0]] += 1
                        else:
                            update_counts[urow[0]] = 1

    with arcpy.da.UpdateCursor(split_mains_buffer, ["SEWER_BASIN", "SHAPE@"], where_clause="SEWER_BASIN is NULL") as ucursor:
        for row in ucursor:
            selected_basins = arcpy.SelectLayerByLocation_management(sewer_basins, overlap_type="INTERSECT", select_features=row[1])
            selected_basins = make_arcpy_query(sewer_basins,["BASINS", "SHAPE@"])
            areas = []
            for basin in selected_basins.values():

                
                intersected = basin["SHAPE@"].intersect(row[1], 4)
                area = intersected.area
                areas.append([basin["BASINS"], area])
            for r in areas:
                if r[1] == max([a[1] for a in areas]):
                    row[0] = r[0][:20]
                    ucursor.updateRow(row)
    # arcpy.CopyFeatures_management(split_mains_buffer, )

    return



if __name__ == "__main__":

    try:
        main(False)
    except Exception as ex:
        raise ex