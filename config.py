import os
from pathlib import Path

CONNECTION_FILE = r"C:\Users\pottera\projects\data_management\EasementMaintLayer\gisttst.sde"
DURHAM_PARCELS_URL = "https://webgis.durhamnc.gov/server/rest/services/PublicServices/Property/MapServer/1"
FRANKLIN_PARCELS_URL = "https://arcgis5.roktech.net/arcgis/rest/services/Franklin/baseGoMaps4/MapServer/10"
WAKE_PARCELS_URL = "https://maps.raleighnc.gov/arcgis/rest/services/Property/Property/MapServer/0"
GRAVITY_MAINS = "https://maps.raleighnc.gov/arcgis/rest/services/PublicUtility/SewerCollection/MapServer/8"
SEWER_BASINS_URL = "https://maps.raleighnc.gov/arcgis/rest/services/PublicUtility/SewerCollection/MapServer/13"
APRX = os.path.join(os.path.dirname(__file__), "easements", "easements.aprx")
