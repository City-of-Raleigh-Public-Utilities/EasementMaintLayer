import arcpy
from db_functions import connect_to_oracle_db
from config import GISTTRANSAUTH, CONNECTION_FILE, GRAVITY_MAINS, WAKE_PARCELS_URL, APRX, TEST_CONNECTION_FILE
from arcpy_functions import arcpy_to_df, make_arcpy_query
import time
import os
import pandas as pd
from utility_functions import read_last_run_time
from numpy import isclose
import requests
import datetime
import arcpy
import json

ADD_FIELDS = [
        ["FACILITYID", "TEXT", "", 20],
        ["EASEMENT_TYPE", "TEXT", "", 20],
        ["EASEMENT_LENGTH", "DOUBLE"],
        ["SEWER_BASIN", "TEXT", "", 20],
        ["PIPE_DIAMETER", "DOUBLE"]
    ]

TEMP_OUT_GDB = os.path.join(os.path.dirname(__file__), "easements", "easements.gdb")
arcpy.env.overwriteOutput = True
aprx = arcpy.mp.ArcGISProject(APRX)

m = aprx.listMaps()[0]
wake_parcels = m.addDataFromPath(WAKE_PARCELS_URL)
gravity_mains = m.addDataFromPath(GRAVITY_MAINS)
easements = os.path.join(TEST_CONNECTION_FILE, "RPUD.EasementMaintenanceAreas")

def main():

    sr = arcpy.SpatialReference(2264)

    table_name = "ssGravityMain"

    last_run = read_last_run_time("ssGravityMain")
    timestamp = datetime.datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000

    resp = requests.get(f"{GRAVITY_MAINS}/query", params={"where": f"EDITEDON > TIMESTAMP '{last_run}'AND ACTIVEFLAG = 1 and OWNEDBY = 0", "outFields": "*", "f": "JSON"} )
    features = [{"feature": feature["attributes"],"geometry":feature["geometry"], "SHAPE@": arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in feature["geometry"]["paths"][0]]), sr)} for feature in resp.json()["features"]]
    changed_df = pd.DataFrame([feature["feature"] for feature in features])
    changed_df["geometry"] = [feature["geometry"] for feature in features]
    changed_df["SHAPE@"] = [feature["SHAPE@"] for feature in features]
    prev_geom_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f"{table_name}_length.csv"))
    changed_mains = changed_df.dropna(subset=["FACILITYID"]).merge(prev_geom_df.dropna(subset=["FACILITYID"]), left_on="FACILITYID", right_on="FACILITYID")
    changed_mains["new_length"] = changed_mains["SHAPE@"].apply(lambda x: x.length)
    diff_geom = changed_mains[~isclose(changed_mains["new_length"], changed_mains["SHAPE_Leng"], atol=5)]
    if diff_geom.empty:
        return
    easement_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f"{table_name}Easement_lookup_intersect.csv"))

    easement_df_update = easement_df.merge(diff_geom, left_on="FACILITYID_1", right_on="FACILITYID")
    easement_df_update = easement_df_update.drop_duplicates(subset=["FACILITYID_x"])
    # easement_df_update = diff_geom.merge(easement_df, left_on="FACILITYID", right_on="FACILITYID_1")
    new_easements = []

    for i, row in easement_df_update.iterrows():
        # make sure to drop dups here
        # print(f"easement_id: {row['FACILITY_ID_x']}")
        easement_facilityid = row["FACILITYID_x"]
        related_mains = easement_df[easement_df["FACILITYID"] == row["FACILITYID_x"]]
        related_mains_features = requests.get(f"{GRAVITY_MAINS}/query", params={"where": f"""FACILITYID in ('{"', '".join(list(related_mains["FACILITYID_1"].values))}')""", "outFields": "*", "f": "JSON"}).json()
        related_mains_features = [{"feature": feature["attributes"],"geometry":feature["geometry"], "SHAPE@": arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in feature["geometry"]["paths"][0]]), sr)} for feature in related_mains_features["features"]]
        related_mains_df = pd.DataFrame([feature["feature"] for feature in related_mains_features])
        related_mains_df["geometry"] = [feature["geometry"] for feature in related_mains_features]
        related_mains_df["SHAPE@"] = [feature["SHAPE@"] for feature in related_mains_features]
        #  get intersecting parcels
        # related_mains_geom = ', '.join([json.dumps(g) for g in list(related_mains_df["geometry"].values)])
        # intersecting_parcels = []
        # arcpy.SelectLayerByLocation_management(wake_parcels, "INTERSECT", gravity_mains)
        # for geom in related_mains_df["geometry"]:

        #     intersecting_parcel = requests.get(f"{WAKE_PARCELS_URL}/query", params={"where": f"1 = 1", "outFields": "*", "geometry":json.dumps(geom), "geometryType":"esriGeometryPolyline", "spatialRel": "esriSpatialRelIntersects","f": "JSON"} ).json()["features"][0]
        #     intersecting_parcels.append(intersecting_parcel)
        #     # if intersecting_parcel not in intersecting_parcels:
        #     #     intersecting_parcels.append(intersecting_parcel)
        #     print("here")
        # for p in intersecting_parcels:
        #     p["SHAPE@"] = arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in p["geometry"]["rings"][0]]), sr)

        clip_fc = os.path.join(TEMP_OUT_GDB, "mains_clip")
        selected_mains = arcpy.MakeFeatureLayer_management(gravity_mains, where_clause=f"OBJECTID in ({', '.join([str(o) for o in related_mains_df['OBJECTID'].values])})")
        arcpy.SelectLayerByLocation_management(wake_parcels, "INTERSECT", selected_mains, search_distance=30)
        arcpy.SelectLayerByLocation_management(gravity_mains, "INTERSECT", wake_parcels)
        arcpy.Clip_analysis(gravity_mains, wake_parcels, clip_fc)
        this_easement = make_arcpy_query(easements, where=f"FACILITYID = '{easement_facilityid}'")[0]
        this_easement = [v for k,v in this_easement.items()][0]
        mains_for_analysis = []
        dissolve_fc = os.path.join(TEMP_OUT_GDB, "mains_dissolve")
        arcpy.Dissolve_management(clip_fc, dissolve_fc, "DIAMETER", multi_part=False, statistics_fields=[["FACILITYID", "FIRST"], ["FACILITYID", "LAST"]])
        

        print(mains_for_analysis)
        intersection_points = os.path.join(TEMP_OUT_GDB, "intersection_points")
        intersect_value_table = arcpy.ValueTable(2)
        intersect_value_table.addRow(f"'{dissolve_fc}' ''")
        intersect_value_table.addRow(f"'{dissolve_fc}' ''")
        arcpy.Intersect_analysis(intersect_value_table, intersection_points, output_type="POINT")
        arcpy.DeleteIdentical_management(intersection_points, "SHAPE")
        split_mains = os.path.join(TEMP_OUT_GDB, "main_split")
        arcpy.SplitLineAtPoint_management(dissolve_fc, intersection_points, out_feature_class=split_mains, search_radius=.001) # arcpy.Delete_management(intersection_points)
        split_mains_data = make_arcpy_query(split_mains)[0]
        if not split_mains_data:
            split_mains_data = make_arcpy_query(dissolve_fc)[0]

        for k, v in split_mains_data.items():
            intersect = this_easement["SHAPE@"].intersect(v["SHAPE@"], 2)

            covered = intersect.length/ v["SHAPE@"].length
            print(covered)
            # if covered > .25:
            mains_for_analysis.append([v, covered])
        matching_mains = [m for m in mains_for_analysis if m[1] > 0.5]
        if matching_mains:
            arcpy.AddFields_management(split_mains, ADD_FIELDS)
            split_mains_buffer = os.path.join(TEMP_OUT_GDB, "split_mains_buffer")
            arcpy.SelectLayerByAttribute_management(split_mains, where_clause=f"OBJECTID in ({', '.join([str(m[0]['OBJECTID']) for m in matching_mains])})")
            arcpy.Buffer_analysis(split_mains, split_mains_buffer, 20, "FULL", "ROUND")
            split_mains_buffer_clip = os.path.join(TEMP_OUT_GDB, "split_mains_buffer_clip")
            arcpy.Clip_analysis(split_mains_buffer, wake_parcels, split_mains_buffer_clip)
            split_mains_buffer_data = make_arcpy_query(split_mains_buffer_clip)[0]
            if len(matching_mains) == 1:
                top_oid = [o[0]["OBJECTID"] for o in matching_mains if o[1] ==  max([m[1] for m in matching_mains])][0]
                other_matching_easements = []
            else:
                top_oid = [o[0]["OBJECTID"] for o in matching_mains if o[0]["SHAPE@"].length ==  max([m[0]["SHAPE@"].length for m in matching_mains])][0]
                other_oids = [o[0]["OBJECTID"] for o in matching_mains if o[0]["SHAPE@"].length !=  max([m[0]["SHAPE@"].length for m in matching_mains])]
                # other_matching_oids = [o[0]["OBJECTID"] for o in matching_mains if o[0]["SHAPE@"].length !=  max([m[0]["SHAPE@"].length for m in matching_mains])][0]
                other_matching_easements = [d for d in split_mains_buffer_data.values() if d["ORIG_FID"] in other_oids]

            top_matching = [d for d in split_mains_buffer_data.values() if d["ORIG_FID"] == top_oid][0]
            top_matching_area = round(top_matching["SHAPE@"].area)
            
            print(f"current area: {round(this_easement['SHAPE@'].area)}. new area: {top_matching_area}")
            if top_matching_area == round(this_easement["SHAPE@"].area):
                print("areas match") # if the value is exactly the same, we likely do not need to do anything
            else:
                print("areas dont match")
        else:
            print("do something else...")


        # intersection_points = os.path.join(TEMP_OUT_GDB, "intersection_points")
        # intersect_value_table = arcpy.ValueTable(2)
        # intersect_value_table.addRow(f"'{dissolve_fc}' ''")
        # intersect_value_table.addRow(f"'{dissolve_fc}' ''")
        # arcpy.Intersect_analysis(intersect_value_table, intersection_points, output_type="POINT")
        # arcpy.DeleteIdentical_management(intersection_points, "SHAPE")
        # split_mains = os.path.join(TEMP_OUT_GDB, "main_split")
        # arcpy.SplitLineAtPoint_management(dissolve_fc, intersection_points, out_feature_class=split_mains, search_radius=.001) # arcpy.Delete_management(intersection_points)

    #     if arcpy.GetCount_management(dissolve_fc) > 1:
    #         print("This feature is split")
    #     # arcpy.SelectLayerByAttribute_management(gravity_mains, where_clause=f"OBJECTID in ({', '.join([str(o) for o in related_mains_df['OBJECTID'].values])})")
    #     arcpy.SelectLayerByAttribute_management(wake_parcels, where_clause=f"OBJECTID in ({', '.join([str(p['attributes']['OBJECTID']) for p in intersecting_parcels])})")
    #     # arcpy.Clip_analysis(selected_mains, wake_parcels, os.path.join(os.path.dirname(__file__), "mains_clip.shp"))
    #     #TODO: The output of the clip looked right but was in the wrong location. Need to look at spatial ref
    #     # arcpy.Dissolve_management(list(related_mains_df["SHAPE@"].values), dissolve_fc, dissolve_field="DIAMETER", multi_part=False)
    #     # arcpy.Clip_analysis(list(related_mains_df["SHAPE@"].values), [p["SHAPE@"] for p in intersecting_parcels], os.path.join(os.path.dirname(__file__), "mains_clip.shp"))
    #     if len(related_mains_df["DIAMETER"].unique()) >1:
    #         print("new diameters")
    # print("here")
        #***** this intersects with parcels**************
        # resp = requests.get(f"{WAKE_PARCELS_URL}/query", params={"where": f"1 = 1", "outFields": "*", "geometry":json.dumps(row['geometry']), "geometryType":"esriGeometryPolyline", "spatialRel": "esriSpatialRelIntersects","f": "JSON"} )
        # get all child mains here
        # child_mains = 

        # print({"main_id":row["FACILITYID_y"], "easement_id": row["FACILITY_ID_x"]})


    # gravity_mains_fc = os.path.join(CONNECTION_FILE, "RPUD.ssGravityMain")

    # mains_df = arcpy_to_df(gravity_mains_fc, where=f"""editedon > timestamp'{last_run.split(".")[0]}'""")
    
    

    # engine = connect_to_oracle_db(**GISTTRANSAUTH)
    # start = time.time()
    # # all_df = pd.read_csv(sql=f"""SELECT globalid, sde.st_length(shape) FROM SSGRAVITYMAIN_EVW""",con=engine)
    # prev_geom_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f"{table_name}_length.csv"))
    # changed_mains = pd.read_sql(sql=f"""SELECT globalid, facilityid, sde.st_length(shape) FROM {table_name}_evw where editedon > to_date('{last_run.split(".")[0]}', 'YYYY/MM/DD HH24:MI:SS') AND ACTIVEFLAG = 1 and OWNEDBY = 0""",con=engine)
    # changed_mains = changed_mains.merge(prev_geom_df, on="facilityid")
    # diff_geom = changed_mains[~isclose(changed_mains["SDE.ST_LENGTH(SHAPE)_x"], changed_mains["SDE.ST_LENGTH(SHAPE)_y"])]
    # if diff_geom.empty:
    #     return
    
    # easement_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f"{table_name}Easement_lookup.csv"))

    # # for i, row in diff_geom.iterrows():
        

    # easement_df_update = easement_df.merge(diff_geom, left_on="FACILITYID_1", right_on="facilityid")



    # gravity_mains_fc = os.path.join(CONNECTION_FILE, "RPUD.ssGravityMain")


if __name__ == "__main__":
    main()


