import arcpy
from db_functions import connect_to_oracle_db
from config import GISTTRANSAUTH, CONNECTION_FILE, GRAVITY_MAINS, WAKE_PARCELS_URL
from arcpy_functions import arcpy_to_df
import time
import os
import pandas as pd
from utility_functions import read_last_run_time
from numpy import isclose
import requests
import datetime
import arcpy
import json

def main():

    table_name = "ssGravityMain"

    last_run = read_last_run_time("ssGravityMain")
    timestamp = datetime.datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000

    resp = requests.get(f"{GRAVITY_MAINS}/query", params={"where": f"EDITEDON > TIMESTAMP '{last_run}'AND ACTIVEFLAG = 1 and OWNEDBY = 0", "outFields": "*", "f": "JSON"} )
    features = [{"feature": feature["attributes"],"geometry":feature["geometry"], "SHAPE@": arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in feature["geometry"]["paths"][0]]))} for feature in resp.json()["features"]]
    changed_df = pd.DataFrame([feature["feature"] for feature in features])
    changed_df["geometry"] = [feature["geometry"] for feature in features]
    changed_df["SHAPE@"] = [feature["SHAPE@"] for feature in features]
    prev_geom_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f"{table_name}_length.csv"))
    changed_mains = changed_df.merge(prev_geom_df, left_on="FACILITYID", right_on="facilityid")
    changed_mains["new_length"] = changed_mains["SHAPE@"].apply(lambda x: x.length)
    diff_geom = changed_mains[~isclose(changed_mains["new_length"], changed_mains["SDE.ST_LENGTH(SHAPE)"])]
    if diff_geom.empty:
        return
    easement_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f"{table_name}Easement_lookup.csv"))

    easement_df_update = easement_df.merge(changed_mains, left_on="FACILITYID_1", right_on="FACILITYID")

    for i, row in easement_df_update.iterrows():
        # make sure to drop dups here
        # print(f"easement_id: {row['FACILITY_ID_x']}")
        related_mains = easement_df[easement_df["FACILITYID"] == row["FACILITYID_x"]]
        related_mains_features = requests.get(f"{GRAVITY_MAINS}/query", params={"where": f"""FACILITYID in ('{"', '".join(list(related_mains["FACILITYID_1"].values))}')""", "outFields": "*", "f": "JSON"}).json()
        related_mains_features = [{"feature": feature["attributes"],"geometry":feature["geometry"], "SHAPE@": arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in feature["geometry"]["paths"][0]]))} for feature in related_mains_features["features"]]
        related_mains_df = pd.DataFrame([feature["feature"] for feature in related_mains_features])
        related_mains_df["geometry"] = [feature["geometry"] for feature in related_mains_features]
        related_mains_df["SHAPE@"] = [feature["SHAPE@"] for feature in related_mains_features]
        #  get intersecting parcels
        # related_mains_geom = ', '.join([json.dumps(g) for g in list(related_mains_df["geometry"].values)])
        intersecting_parcels = []
        for geom in related_mains_df["geometry"]:

            intersecting_parcel = requests.get(f"{WAKE_PARCELS_URL}/query", params={"where": f"1 = 1", "outFields": "*", "geometry":json.dumps(geom), "geometryType":"esriGeometryPolyline", "spatialRel": "esriSpatialRelIntersects","f": "JSON"} ).json()["features"][0]
            if intersecting_parcel not in intersecting_parcels:
                intersecting_parcels.append(intersecting_parcel)
            print("here")
        for p in intersecting_parcels:
            p["SHAPE@"] = arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in p["geometry"]["rings"][0]]))
        #TODO: The output of the clip looked right but was in the wrong location. Need to look at spatial ref
        arcpy.Clip_analysis(list(related_mains_df["SHAPE@"].values), [p["SHAPE@"] for p in intersecting_parcels], os.path.join(os.path.basename(__file__), "mains_clip.shp"))
        if len(related_mains_df["DIAMETER"].unique()) >1:
            print("new diameters")
    print("here")
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


