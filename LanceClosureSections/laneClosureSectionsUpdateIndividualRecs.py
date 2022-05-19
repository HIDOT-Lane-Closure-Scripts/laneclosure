
# coding: utf-8


import arcpy, os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
from arcpy import env
import unicodedata
import datetime, tzlocal, time
from datetime import date , datetime, timedelta
from time import  strftime
from tzlocal import get_localzone
import math
from os import listdir
from arcgis.gis import GIS
from arcgis.geoenrichment import *
from arcgis.features._data.geodataset.geodataframe import SpatialDataFrame
from cmath import isnan
from math import trunc
#import ago

try:
    import urllib.request, urllib.error, urllib.parse  # Python 2
except ImportError:
    import urllib.request as urllib2  # Python 3
import zipfile
from zipfile import ZipFile
import json
import fileinput
from os.path import isdir, isfile, join

#%matplotlib inline
import matplotlib.pyplot as pyd
from IPython.display import display #, YouTubeVideo
from IPython.display import HTML
import pandas as pd
#from pandas import DataFrame as pdf

#import geopandas as gpd

from arcgis import geometry
from arcgis import features 
import arcgis.network as network

from arcgis.features.analyze_patterns import interpolate_points
import arcgis.geocoding as geocode
from arcgis.features.find_locations import trace_downstream
from arcgis.features.use_proximity import create_buffers
from arcgis.features import GeoAccessor as gac, GeoSeriesAccessor as gsac
from arcgis.features import SpatialDataFrame as spedf

from arcgis.features import FeatureLayer

import numpy as np

from copy import deepcopy

import logging
logger = logging.getLogger('lnclupdates')
lcnclupdatehdlr = logging.FileHandler('lnclupdates.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
lcnclupdatehdlr.setFormatter(formatter)
logger.addHandler(lcnclupdatehdlr)
logger.setLevel(logging.INFO)


def webexsearch(mgis, title, owner_value, item_type_value, max_items_value=1000,inoutside=False):
    item_match = None
    search_result = mgis.content.search(query= title + ' AND owner:' + owner_value, 
                                          item_type=item_type_value, max_items=max_items_value, outside_org=inoutside)
    if "Imagery Layer" in item_type_value:
        item_type_value = item_type_value.replace("Imagery Layer", "Image Service")
    elif "Layer" in item_type_value:
        item_type_value = item_type_value.replace("Layer", "Service")
    
    for item in search_result:
        if item.title == title:
            item_match = item
            break
    return item_match

def lyrsearch(lyrlist, lyrname):
    lyr_match = None
   
    for lyr in lyrlist:
        if lyr.properties.name == lyrname:
            lyr_match = lyr
            break
    return lyr_match

def create_section(lyr, hdrow, chdrows,rtefeat):
    try:
        object_id = 1
        pline = geometry.Polyline(rtefeat)
        feature = features.Feature(
            geometry=pline[0],
            attributes={
                'OBJECTID': object_id,
                'PARK_NAME': 'My Park',
                'TRL_NAME': 'Foobar Trail',
                'ELEV_FT': '5000'
            }
        )

        lyr.edit_features(adds=[feature])
        #_map.draw(point)

    except Exception as e:
        print("Couldn't create the feature. {}".format(str(e)))
        

def fldvartxt(fldnm,fldtyp,fldnull,fldPrc,fldScl,fldleng,fldalnm,fldreq):
    fld = arcpy.Field()
    fld.name = fldnm
    fld.type = fldtyp
    fld.isNullable = fldnull
    fld.precision = fldPrc
    fld.scale = fldScl
    fld.length = fldleng
    fld.aliasName = fldalnm
    fld.required = fldreq
    return fld

def df_colsame(df):
    """ returns an empty data frame with the same column names and dtypes as df """
    #df0 = pd.DataFrame.spatial({i[0]: pd.Series(dtype=i[1]) for i in df.dtypes.iteritems()}, columns=df.dtypes.index)
    return df

def offdirn(closide,dirn1):
    if closide == 'Right':
        offdirn1 = 'RIGHT'
    elif closide == 'Left':
        offdirn1 = 'LEFT'
        dirn1 = -1*dirn1
    elif closide == 'Center':
        offdirn1 = 'RIGHT'
        dirn1 = 0.5
    elif closide == 'Both':
        offdirn1 = 'RIGHT'
        dirn1 = 0
    elif closide == 'Directional':
        if dirn1 == -1:
            offdirn1 = 'LEFT'
        else:
            offdirn1 = 'RIGHT'
    elif closide == 'Full' or closide == 'All':
        offdirn1 = 'RIGHT'
        dirn1 = 0
    elif closide == 'Shift':
        offdirn1 = 'RIGHT'
    elif closide == 'Local':
        offdirn1 = 'RIGHT'
    else:
        offdirn1 = 'RIGHT'
        dirn1 = 0 
    return offdirn1,dirn1

def deleteupdates(prjstlyrsrc, sectfeats):
    for x in prjstlyrsrc:
        print (" layer: {} ; from item : {} ; URL : {} ; Container : {} ".format(x,x.fromitem,x.url,x.container))
        if 'Projects' in (prjstlyrsrc):
            xfeats =  x.query().features
            if len(xfeats) > 0:
                if isinstance(xfeats,(list,tuple)):
                    if "OBJECTID" in xfeats[0].attributes:
                        oids = "'" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xfeats if 'OBJECTID' in xfs.attributes ) + "'"
                        oidqry = " OBJECTID in ({}) ".format(oids)
                    elif "OID" in xfeats[0].attributes:    
                        oids = "'" + "','".join(str(xfs.attributes['OID']) for xfs in xfeats if 'OID' in xfs.attributes ) + "'"
                        oidqry = " OID in ({}) ".format(oids)
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                    
                elif isinstance(xfeats,spedf):
                    if "OBJECTID" in xfeats.columns:
                        oids = "'" + "','".join(str(f1.get_value('OBJECTID')) for f1 in xfeats ) + "'"
                        oidqry = " OBJECTID in ({}) ".format(oids)
                    elif "OID" in xfeats.columns:    
                        oids = "'" + "','".join(str(f1.get_value('OID')) for f1 in xfeats ) + "'"
                        oidqry = " OID in ({}) ".format(oids)
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                    
                if 'None' in oids:
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                else:
                    x.delete_features(where=oidqry)

def fridaywk(bdate,n1):
    wkdte = datetime.strftime(bdate,"%w") # + datetime.strftime(bdate,"%z")
    date4pm = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=16)
    fr4pm= date4pm + timedelta(days=(5-int(wkdte)+(n1-1)*7))
    return fr4pm

    # get the date and time
curDate = strftime("%Y%m%d%H%M%S") 
# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())


### Start setting variables for local operation
#outdir = r"D:\MyFiles\HWYAP\laneclosure\Sections"
#lcoutputdir =  r"C:\\users\\mmekuria\\ArcGIS\\LCForApproval"
#lcfgdboutput = "LaneClosureForApproval.gdb" #  "Lane_Closure_Feature_WebMap.gdb" #
#lcfgdbscratch =  "LaneClosureScratch.gdb"
# output file geo db 
#lcfgdboutpath = "{}\\{}".format(lcoutputdir, lcfgdboutput)

# ArcGIS user credentials to authenticate against the portal
#credentials = { 'userName' : 'dot_mmekuria', 'passWord' : 'xxxxxxxxx'}
#credentials = { 'userName' : arcpy.GetParameter(4), 'passWord' : arcpy.GetParameter(5)}
# Address of your ArcGIS portal
portal_url = r"http://histategis.maps.arcgis.com/" # r"https://www.arcgis.com/" # 

# ID or Title of the feature service to update
#featureService_ID = '9243138b20f74429b63f4bd81f59bbc9' # arcpy.GetParameter(0) #  "3fcf2749dc394f7f9ecb053771669fc4" "30614eb4dd6c4d319a05c6f82b049315" # "c507f60f298944dbbfcae3005ad56bc4"
lnclSrcFSTitle = 'LaneClosure' # arcpy.GetParameter(0) 
itypelnclsrc="Feature Service" # "Feature Layer" # "Service Definition"
lnclhdrnm = 'LaneClosure'
lnclchdnm = 'Location_repeat'

fsectWebMapTitle = 'Lane_Closure_WebMap_WFL1' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
itypeFS="Feature Service" # "Feature Layer" # "Service Definition"
wmlnclyrptsnm = 'Lane_Closure_Begin_and_End_Points'
wmlnclyrsectsnm = 'Lane_Closure_Sections'




itypelrts="Feature Service" # "Feature Layer" # "Service Definition"
servicename =  lnclSrcFSTitle # "Lane_Closure_WebMap" # "HI DOT Daily Lane Closures Sample New" # arcpy.GetParameter(1) # 
tempPath = sys.path[0]
userName = "dot_mmekuria" # credentials['userName'] # arcpy.GetParameter(2) # 
#passWord = credentials['passWord'] # arcpy.GetParameter(3) # "ChrisMaz!1"
arcpy.env.overwriteOutput = True
#print("Temp path : {}".format(tempPath))

print("Connecting to {}".format(portal_url))
#qgis = GIS(portal_url, userName, passWord)
qgis = GIS(profile="hisagolprof")
numfs = 1000 # number of items to query
#    sdItem = qgis.content.get(lcwebmapid)
ekOrg = False
# search for lane closure source data
print("Searching for lane closure source {} from {} item for user {} and Service Title {} on AGOL...".format(itypelnclsrc,portal_url,userName,lnclSrcFSTitle))
fslnclsrc = webexsearch(qgis, lnclSrcFSTitle, userName, itypelnclsrc,numfs,ekOrg)
#qgis.content.search(query="title:{} AND owner:{}".format(lnclSrcFSTitle, userName), item_type=itypelnclsrc,outside_org=False,max_items=numfs) #[0]
#print (" Content search result : {} ; ".format(fslnclsrc))
print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fslnclsrc.url,fslnclsrc.title,fslnclsrc.id))
lnclyrsrc = fslnclsrc.layers

# header layer
lnclhdrlyr = lyrsearch(lnclyrsrc, lnclhdrnm)
hdrsdf = lnclhdrlyr.query(as_df=True)
# child layer
lnclchdlyr = lyrsearch(lnclyrsrc, lnclchdnm)
# relationship between header and child layer
#relncldesc = arcpy.Describe(lnclhdrlyr)
#relncls = relncldesc.relationshipClassNames

#for rc in relncls:
#    print (" relationshp class : {} has {}  ; Title : {} ; Id : {} ".format(fs.url,fs.title,fs.id))

#route_service_url = qgis.properties.helperServices.route.url
#route_service = arcgis.network.RouteLayer(route_service_url, gis=qgis)
#route_layer = arcgis.network.RouteLayer(route_service_url, gis=qgis)

# search for lane closure sections 
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))

#fsect = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
fsectwebmap = webexsearch(qgis, fsectWebMapTitle, userName, itypeFS,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))

#print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fsectwebmap.url,fsectwebmap.title,fsectwebmap.id))
wmsectlyrs = fsectwebmap.layers
wmsectlyrpts = lyrsearch(wmsectlyrs, wmlnclyrptsnm)
wmlnclyrsects = lyrsearch(wmsectlyrs, wmlnclyrsectsnm)


sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
# get sdf to be used for new section data insert operations
fld0 = 'begDyWk'
apprtxt = ''
fldx = 'beginDate'
dateval = '11-1-2019'
#datefrom = "{}".format(datetime.strftime(datetime.date(dateval,"%m-%d-%y")))
begwkqry = "{} >= '{}'".format(fldx,dateval)  # " # "{} is Null".format( fld0,apprtxt)
begdtqry = '1=1'
lnclsectqry = wmlnclyrsects.query()
spref = lnclsectqry.spatial_reference
sprefwgs84 = {'wkid' : 4326 , 'latestWkid' : 4326 }
budist = 500
bunit = "Feet"
busr = spref
# prepare the object Id's with no route numbers (submitted without update privileges
#if len(lnclsectqry)>0:
#    norteid = "OBJECTID in ('" + "','".join(str(sfs.attributes['OBJECTID']) for sfs in lnclsectqry )  + "')"
#    norteid = "objectid in ('" + "','".join(str(lfs.attributes['objectid']) for lfs in lnclsectqry )  + "')"
    # edit the selected records
#    for sfs in  lnclsectqry:
#        appr1 = sfs.get_value('ApproverL1') #,'dot_achung')
#        email1 = sfs.get_value('l1email') # ,'albert.chung@hawaii.gov')

# query the lane closure survey entries that have been updated by users and delete them from processed feature class
lnclsectsdf = lnclsectqry.sdf
scols = lnclsectsdf.columns
for idx,srow in lnclsectsdf.iterrows(index=True):
    dfupfeat = deepcopy(srow) # lnclsectsdf.iloc[idx]
    dfupfeat.loc['beginDate'] = (srow.beginDate - timedelta(hours=10)) 
    dfupfeat.loc['enDate'] = (srow.enDate - timedelta(hours=10)) 
    dfupfeat.loc['begDyWk']= (datetime.strftime(srow.beginDate,"%A")) # srow.begDyWk = datetime.strftime(srow.beginDate,"%A") 
    dfupfeat.loc['enDyWk'] = (datetime.strftime(srow.enDate,"%A")) # srow.enDyWk =  datetime.strftime(srow.enDate,"%A")
    dfupfeat.loc['todayis'] = (datetime.today()) #srow.todayis = datetime.today()
    dfupfeat.loc['.Friday0'] = (fridaywk(srow.todayis,0)) #srow.Friday0 = fridaywk(srow.todayis,0) 
    dfupfeat.loc['.Friday1'] = (fridaywk(srow.todayis,1)) #srow.Friday1 = fridaywk(srow.todayis,1)
    dfupfeat.loc['.Friday1'] = (fridaywk(srow.todayis,2)) #srow.Friday2 = fridaywk(srow.todayis,2)
    insecgeo = srow['SHAPE']
    insecgeom = arcgis.geometry.Geometry(insecgeo)
    insecgeomwgds84 = insecgeom.project_as(sprefwgs84)
    insgeompaths = insecgeomwgds84['paths']
    insgeomcoords = insgeompaths[0]
    begcoord = insgeomcoords[0]
    endcoord = insgeomcoords[len(insgeomcoords)-1]
    try:
        intsfrom = arcgis.geocoding.reverse_geocode(begcoord,distance=500,return_intersection=True)
        intsto = arcgis.geocoding.reverse_geocode(endcoord,distance=500,return_intersection=True)
        intsfromtxt = intsfrom['address']['ShortLabel']
        intstotxt = intsto['address']['ShortLabel']
    except Exception as e:
        print ('Layer {} Update with day of week and friday data.  Error message :{}'.format(
                wmlnclyrptsnm,e ))
        logger.error('Layer {} Update with day of week and friday data failed.  Error Message  :{}'.format(
                wmlnclyrptsnm,e ))
        intsfromtxt = "Beginning at Mile Post {}".format(srow.BMP)
        intstotxt = "Ending at Mile Post {}".format(srow.EMP)
    
    dfupfeat.loc['IntersFrom'] = intsfromtxt  # arcgis.geocoding.reverse_geocode(insecgeo[0],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)
    dfupfeat.loc['IntersTo'] = intstotxt #arcgis.geocoding.reverse_geocode(insecgeo[len(insecgeo)-1],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)

    lnclupdatefeatset = dfupfeat.spatial.to_featureset()
    try:
        resultupd1 = wmlnclyrsects.edit_features(updates=lnclupdatefeatset) #calculate(where=begwkqry,calc_expression={"field" : fld1 , "value" : val1})
        print('Layer {} Update with day of week and friday data results :{}'.format(wmlnclyrptsnm,resultupd1))
    
        logger.info('Layer {} Update with day of week and friday data results :{}'.format(wmlnclyrptsnm,resultupd1 ))
    
    except Exception as e:
        print ('Layer {} Update with day of week and friday data.  Error message :{}'.format(
                wmlnclyrptsnm,e ))
        logger.error('Layer {} Update with day of week and friday data failed.  Error Message  :{}'.format(
                wmlnclyrptsnm,e ))
            
          
tend = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
logger.info (" End lane closure date update {} section features ended at {}. ".format (len(lnclsectqry),tend))

print (" End lane closure date update {} section features ended at {}. ".format (len(lnclsectqry),tend))
lcnclupdatehdlr.close()

