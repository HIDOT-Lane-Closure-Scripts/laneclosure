
# coding: utf-8


import arcpy, os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , timedelta
from time import  strftime
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
#socrata module
from sodapy import Socrata

from socrata.authorization import Authorization
from socrata import Socrata
import os
import sys



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
        dirn1 = -1*dirn
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


    # get the date and time
curDate = strftime("%Y%m%d%H%M%S") 
# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.date.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())

# Socrata access 

sochost = "highways.hidot.hawaii.gov"
socapptoken = "Di04VXcc3fJZKgDmE6veI5gCM"
socusername = "blm0hujxao81ilb72860v5llc" #"tgoodman970@gmail.com"
socpwd = "4z6b7bojkoy7y8nvykcy3g2vr8rc0ys6wfj328uafu2rmroi6n" #"123Password!"

#auth = Authorization(
#  'highways.hidot.hawaii.gov',
#  os.environ['MY_SOCRATA_USERNAME'],
#  os.environ['MY_SOCRATA_PASSWORD']
#)

auth = Authorization( sochost,socapptoken, socusername, socpwd )

laneclosocrata = Socrata(auth)

#laneclosocrata = Socrata(sochost,socapptoken,socusername,socpwd)
lctags = ["laneclosure","weekly","HIDOT"]
lcat = "Lane Closure"

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.



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

fsectViewTitle = 'Lane_Closure_Approval_View1' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
itypeFL= "Feature Layer" # "Feature Service" # "Service Definition"
wmlnclyrptsnmfl = 'Lane_Closure_Begin_and_End_Points'
wmlnclyrsectsnmfl = 'Lane_Closure_Sections'

hirtsTitle = 'HIDOTLRS' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
itypelrts="Feature Service" # "Feature Layer" # "Service Definition"
wmlrtsnm = 'HIDOTLRS'
rteFCSelNm = 'rtesel'
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
#print("Searching for lane closure source {} from {} item for user {} and Service Title {} on AGOL...".format(itypelnclsrc,portal_url,userName,lnclSrcFSTitle))
#fslnclsrc = webexsearch(qgis, lnclSrcFSTitle, userName, itypelnclsrc,numfs,ekOrg)
#qgis.content.search(query="title:{} AND owner:{}".format(lnclSrcFSTitle, userName), item_type=itypelnclsrc,outside_org=False,max_items=numfs) #[0]
#print (" Content search result : {} ; ".format(fslnclsrc))
#print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fslnclsrc.url,fslnclsrc.title,fslnclsrc.id))
#lnclyrsrc = fslnclsrc.layers

# header layer
#lnclhdrlyr = lyrsearch(lnclyrsrc, lnclhdrnm)
#hdrsdf = lnclhdrlyr.query(as_df=True)
# child layer
#lnclchdlyr = lyrsearch(lnclyrsrc, lnclchdnm)
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
#fsectwebmap = webexsearch(qgis, fsectWebMapTitle, userName, itypeFS,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))

#print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fsectwebmap.url,fsectwebmap.title,fsectwebmap.id))
#wmsectlyrs = fsectwebmap.layers
#wmsectlyrpts = lyrsearch(wmsectlyrs, wmlnclyrptsnm)
#wmlnclyrsects = lyrsearch(wmsectlyrs, wmlnclyrsectsnm)

#sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
# get sdf to be used for new section data insert operations
#sectqry = wmlnclyrsects.query()
#sectfset = sectqry.features
# select sections with where clause string

fsectview = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
#fsectviewmap = webexsearch(qgis, fsectViewTitle, userName, itypeFL,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsectview))
#fsect = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
fsectwebmap = webexsearch(qgis, fsectWebMapTitle, userName, itypeFS,numfs,ekOrg)
#print (" Feature Service URL : {} ; Title : {} ; id : {}".format(fsectviewmap.url,fsectview.title,fsectview.id))
print (" Feature Service : {} ".format(fsectview))  #  fsectviewwmap.url,fsectview.title,fsectview.id))
wmsectlyrsfl = fsectwebmap.layers
wmsectlyrptsfl = lyrsearch(wmsectlyrsfl, wmlnclyrptsnmfl)
wmlnclyrsectsfl = lyrsearch(wmsectlyrsfl, wmlnclyrsectsnmfl)

sectfldsallfl = [fd.name for fd in wmlnclyrsectsfl.properties.fields]
# get sdf to be used for new section data insert operations
sectqryfl = wmlnclyrsectsfl.query()
sectflset = sectqryfl.features

sectfldsall = [fd.name for fd in wmlnclyrsectsfl.properties.fields]
# get sdf to be used for new section data insert operations
sectqry = wmlnclyrsectsfl.query()
sectfset = sectqry.features
# delete all sections without a route number - an artifact of false submissions on dashboard
qry = "Route is null"
sectqry = wmlnclyrsectsfl.query(where=qry)
# prepare the object Id's with no route numbers (submitted without update privileges
if len(sectqry)>0:
    norteid = "objectid in ('" + "','".join(str(sfs.attributes['objectid']) for sfs in sectqry )  + "')"
    resultdel = wmlnclyrsectsfl.delete_features(where=norteid)

# select sections data entered today
qry = "ApprLevel2 != '{}' and beginDate >= '{}' ".format( 'Yes',date.today())
expfilenm = "lanecl2"
sectqry = wmlnclyrsectsfl.query()
#sectsdf = wmlnclyrsects.query(as_df=True)
sectsdf = sectqry.sdf
htmlcols = chgflds = ['OBJECTID','globalid','Route', 'CreationDate', 'Creator', 'Editor','EditDate', 'BMP',  'EMP', 'beginDate','enDate']
sectsdfidx = sectsdf.set_index(['OBJECTID'])
lnclcsvfile = "D:/MyFiles/HWYAP/laneclosure/{}.csv".format(expfilenm)
#sectsdfidx['Remarks']=Remarks.decode('utf8')
sectsdfidx['l2head']='george.abcede@hawaii.gov'
sectsdfidx['Remarks']=sectsdfidx['Remarks'].astype(str).str.decode('utf8')
#sectsdfidx['LaneCLine']= sectsdfidx['SHAPE'].iloc[0]
sectsdfidx['LaneCLineJ']= sectsdfidx['SHAPE'].to_json()
#sectline1 = sectsdfidx['LaneCLine'].head(1)
sectlinej1 = sectsdfidx['LaneCLineJ'].head(1)

lnclcsv = sectsdfidx.to_csv(lnclcsvfile) #,encoding='utf-8')

sectfeat = sectqry.features
lnclflds = sectqry.fields
sectsdfhdr = deepcopy(sectsdf.head(1))
# create an empty dataset with all the field characteristics
sectsdfptype = deepcopy(sectsdf.head(0))
# search for Route dataset  
lncolumns = sectsdfidx.columns

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
#laneclosocdata = laneclosocrata.create("LaneClosureWeekly",description="List of Lane Closures for the week",columns=lnclflds,rowidentifier="OBJECTID",tags=lctags,category=lcat) # upsert("dzia-izdx", limit=2000)
#lnclsocid = laneclosocdata.id
# publish lane closure data
#republc = laneclosocrata.publish(lnclsocid)
#laneclosocrata.set_permission(lnclsocid, "public")

# Convert to pandas DataFrame
#lnclosurejson = sectsdfidx.to_json(orient="columns") # # pd.DataFrame.to_json(self, path_or_buf, orient, date_format, double_precision, force_ascii, date_unit, default_handler, lines, compression, index) #from_records(projsocdata)

#laneclosocrata.upsert(lnclsocid, lnclosurejson)

#lnclsocdata = open(lnclcsvfile) #,encoding='utf-8')

lnclsocdataid = 'a392-u7px'
content_type = "csv"


(ok, view) = laneclosocrata.views.lookup(lnclsocdataid)
assert ok, view

with open(lnclcsvfile, 'rb') as my_file:
    #(ok, job) = laneclosocrata.replace(lnclsocdataid,my_file) #, content_type)
    (ok, job) = laneclosocrata.using_config('apprlvl2_10-16-2019_562f', view).csv(my_file)
  # These next 3 lines are optional - once the job is started from the previous line, the
  # script can exit; these next lines just block until the job completes
    assert ok, job
    (ok, job) = job.wait_for_finish(progress = lambda job: print('Job progress:', job.attributes['status']))
    sys.exit(0 if job.attributes['status'] == 'successful' else 1)
laneclosocrata.close()


#fld0 = "Creator"
#val0 = "dot_jyago" # "dot_dtanabe" #  "dot_mdelosreyes" #"dot_rtamai" # 'dot_kmurata'
#qryStr0 = "{} like '{}' ".format(fld0,val0)
#lnclqry = lnclhdrlyr.query(where=qryStr0)
# prepare to overwrite the feature layer definition


print (" End lane closure processing of {} features with {} ".format (len(sectflset),sectfldsallfl))
