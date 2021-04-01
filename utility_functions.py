import json
import datetime
import os
import datetime

def write_run_time(table_name):
    last_run_fle = os.path.join(os.path.dirname(__file__), f"{table_name}_last_run.dat")

    with open(last_run_fle, "w") as outfile:
        json.dump({"last_run": str(datetime.datetime.now())}, outfile)
    print("done")

def read_last_run_time(table_name):
    last_run_fle = os.path.join(os.path.dirname(__file__), f"{table_name}_last_run.dat")
    with open(last_run_fle, "r") as infile:
        return json.load(infile)["last_run"]



if __name__ == "__main__":
    from arcpy_functions import arcpy_to_df
    from config import CONNECTION_FILE
    import os

    # write_run_time("ssGravityMains")
    last_run = read_last_run_time("ssGravityMains")

    gravity_mains_df = arcpy_to_df(os.path.join(CONNECTION_FILE, "RPUD.ssGravityMain"))
    gravity_mains_df.to_csv(r"C:\Users\pottera\projects\data_management\EasementMaintLayer\ssGravityMains.csv")
    gravity_mains_df.to_json(r"C:\Users\pottera\projects\data_management\EasementMaintLayer\ssGravityMains.json", orient="records", default_handler=str)