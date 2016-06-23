####THE PURPOSE OF THIS SCRIPT IS TO GENERATE A PLOYGON FEATURE CLASS REPRESENTING AREAS WHERE UTILITY EASEMENTS SHOULD EXIST####
###THIS IS ACCOMPLISHED BY SELECTING CITY-OWNED PIPES INSIDE OF PARCELS AND BUFFERING THEM, THEN CLIPPING TO PARCEL BOUNDARIES###
##################THE FINAL PRODUCT FOR EACH UTILITY TYPE IS CALLED *featureclassname*EASEMENTCLIP###############################
###################################AND IS STORED IN THE PU BOUNDARIES FEATURE DATASET############################################
###############################INDIVIDUAL DRIVE MAPPING WILL HAVE TO ME MODIFIED BY USERS########################################
#################################################################################################################################

import arcpy
from arcpy import env
import os
import string
env.overwriteOutput = True

def BufferFCs():

    # Set the workspace for the BufferFCs function
    #
    env.workspace = "Database Connections\CITY_RALEIGH_TESTDB.sde"

    #########################################################################################
    ###I WILL TYPICALLY RUN THIS FOR ONE OR TWO FEATURE CLASSES AND COMMENT THE OTHERS OUT###
    fcList = ["RPUD.ssGravityMain"]#,"RPUD.rPressureMain","RPUD.ssForceMain","RPUD.wPressureMain"
    parcels = "Database Connections/WAKE_PRODDB.sde/WAKE.PROPERTY_A_RECORDED"

    #delete data first - change to suit your drive mapping if different
    delete_data = "C:\Users\stearnsc\Junk\MowingLayer.gdb"

    #Clean up existing data
    #Execute Delete
    arcpy.Delete_management(delete_data)
    print "Geodatabase deleted"

    #create new geodatabase - change to suit your drive mapping if different
    arcpy.CreateFileGDB_management("C:\Users\stearnsc\Junk", "MowingLayer.gdb")  

    #loop through feature classes
    for fc in fcList:
        fcname = str(fc)
        fcnameshort = fcname[5:]
        print"beginning operation on " + fcnameshort
        #create parcels feature layer
        print"creating parcels feature layer"
        arcpy.MakeFeatureLayer_management(parcels,"parcelsFeatLayer")
        print"parcels feature layer created"
        print"selecting parcels that intersect pipes"
        arcpy.SelectLayerByLocation_management("parcelsFeatLayer", "INTERSECT", fc, 15)
        #copy feature layer selection to file geodb
        print"copying selected parcels to file geodb"
        arcpy.CopyFeatures_management("parcelsFeatLayer", "C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "Parcels")
        print"selected pipes copied to file geodb"
        #make feature layer from City-owned pipes
        arcpy.MakeFeatureLayer_management(fc,"featLayer","OWNEDBY = 0")
        print fcnameshort + " feature layer created"
        #select pipes that intersect parcels
        print"selecting pipes that intersect parcels"
        arcpy.SelectLayerByLocation_management("featLayer", "INTERSECT", "parcelsFeatLayer")
        print"pipes intersecting parcels selected"
        #copy feature layer selection to file geodb
        print"copying selected pipes to file geodb"
        arcpy.CopyFeatures_management("featLayer", "C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "Intersect")
        print"selected pipes copied to file geodb"
        #clip pipes with parcels
        print"clipping lines with parcel layer"
        arcpy.Clip_analysis("featLayer", "parcelsFeatLayer", "C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "LineClip",0.1)
        print"lines clipped"
        #make feature layer from line clip fc
        print"making feature layer from line clip fc"
        arcpy.MakeFeatureLayer_management("C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "LineClip","LineClipFeatLayer")
        print"feature layer created"
        print"dissolving line clip feature layer"
        arcpy.Dissolve_management("LineClipFeatLayer", "C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "LineClipDissolve","","","SINGLE_PART", "DISSOLVE_LINES")
        print"line clip feature layer dissolved"
        print"making feature layer from dissolved line clip fc"
        arcpy.MakeFeatureLayer_management("C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "LineClipDissolve", "LineClipDissolveFeatLayer")
        print"feature layer created"
        #buffer pipes
        print "buffering pipes that intersect parcels"
        arcpy.Buffer_analysis("LineClipDissolveFeatLayer", "C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "EasementInt", "15 Feet", "FULL", "ROUND", "NONE", "")
        print fc + " buffered"
        print"creating buffer feature layer"
        arcpy.MakeFeatureLayer_management("C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "EasementInt","buffersFeatLayer")
        print"buffers feature layer created"
        #clip buffers with parcels
        print"clipping buffers with parcel layer"
        arcpy.Clip_analysis("buffersFeatLayer", "parcelsFeatLayer", "C:\Users\stearnsc\Junk\MowingLayer.gdb/" + fcnameshort + "EasementClip",0.1)
        print"buffers clipped"
        print"ssGravityMainEasementClip is your final product to replace features in RPUD.EasementMaintenanceAreas feature class with. In edit seession, delete features, then copy/paste from file geodb fc to SDE fc"
BufferFCs()


