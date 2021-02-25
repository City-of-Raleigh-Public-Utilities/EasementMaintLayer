import arcpy
import os
from pathlib import Path




DB_CONNECTIONS = os.path.join(Path(__file__).parents[2],"SDE_Connections")
RPUD_DB = "RPUD_GISDEV.sde"
TARGET_FCS = ["RPUD.ssGravityMain"]#,"RPUD.rPressureMain","RPUD.ssForceMain","RPUD.wPressureMain"
WAKE_DB = "WAKE_PRODDB.sde"
PROPERTY_FC = "WAKE.PROPERTY_A_RECORDED"

def copy_fc_to_memory(fc_path: str):
    name = os.path.basename(fc_path).split(".")[1]
    memory_name = rf"memory\{name}"
    arcpy.management.CopyFeatures(fc_path, memory_name)
    return memory_name

def main():

    property_fc = os.path.join(DB_CONNECTIONS, WAKE_DB, PROPERTY_FC)

    # memory_property_fc = copy_fc_to_memory(property_fc)

    arcpy.management.MakeFeatureLayer(property_fc, "wake_property")

    property_fl = "wake_property"

    for target_fc in TARGET_FCS:
        count = 0
        fc_path = os.path.join(DB_CONNECTIONS, RPUD_DB, target_fc)
        fc_name = target_fc.split('.')[1]
        arcpy.management.MakeFeatureLayer(fc_path, f"{fc_name}_lyr")
        fl =  f"{fc_name}_lyr"
        # arcpy.SelectLayerByLocation_management(fl, overlap_type="INTERSECT_DBMS", select_features=property_fl)
        with arcpy.da.SearchCursor(fl, ["FACILITYID", "SHAPE@"]) as scursor:
            for row in scursor:
                arcpy.SelectLayerByLocation_management(property_fl, select_features=row[-1])
                intersect = [r for r in arcpy.da.SearchCursor(property_fl, "OBJECTID@")]
                if intersect:
                    print("Interesct")
                # count += 1
                # if count%100 == 0:
                #     print(count)
                # print(row)




    return



if __name__ == "__main__":

    try:
        main()
    except Exception as ex:
        raise ex