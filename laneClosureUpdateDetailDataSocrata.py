#@PydevCodeAnalysisIgnore

# coding: utf-8


import arcpy, os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , timedelta, datetime
import math
from os import listdir
from arcgis.gis import GIS
from arcgis.geoenrichment import *
from arcgis.features._data.geodataset.geodataframe import SpatialDataFrame
from cmath import isnan
from math import trunc
from pandas.core.dtypes.missing import isnull
from builtins import isinstance
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

#from pyproj import Proj, transform

import numpy as np

from copy import deepcopy
#socrata module
from sodapy import Socrata as sodasoc

from socrata.authorization import Authorization
from socrata import Socrata
import os
import sys

#from socrata.authorization import Authorization
#from socrata import Socrata

import logging

#logpath = r'\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs'
#rptpath =  r"\\HWYPB\NETSHARE\HWYA\HWY-AP\laneclosure\reports"
logpath = r'D:\MyFiles\HWYAP\LaneClosure\logs'
rptpath = r'D:\MyFiles\HWYAP\LaneClosure\reports'
logfilenm = r'socratransdetail.log'
logfile = os.path.join(logpath,logfilenm)

logger = logging.getLogger('socratranslog')
lnclhdlr = logging.FileHandler(logfile)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
lnclhdlr.setFormatter(formatter)
logger.addHandler(lnclhdlr)
logger.setLevel(logging.INFO)


#usname = "tgoodman970@gmail.com"
#pwd = "123Password!"
#url = 'highways.hidot.hawaii.gov'
app_token = 'Di04VXcc3fJZKgDmE6veI5gCM'

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


def replace(client,fourby, df):
    data = df.to_dict(orient='records', into=NativeDict)
    # pprint(data)

    # set up client for api calls
    #client = Socrata(url, app_token, username=user, password=password)
    # upsert new row using client
    try:
        res = client.replace(fourby, data)
    except Exception as e:
        print('whoops')
        res = e

    logResult(fourby, res)
    print(res)
    client.close()

    return res


def logResult(fourby, res):
    logStr = fourby + ": " + str(res) + "\n"
    logFile = open(logpath, 'a')
    logFile.write(logStr)
    logFile.close()
    
class NativeDict(dict):
    """
        Helper class to ensure that only native types are in the dicts produced by
        :func:`to_dict() <pandas.DataFrame.to_dict>`
    """
    def __init__(self, *args, **kwargs):
        super().__init__(((k, self.convert_if_needed(v)) for row in args for k, v in row), **kwargs)

    @staticmethod
    def convert_if_needed(value):
        """
            Converts `value` to native python type.
        """
        if isnull(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if hasattr(value, 'dtype'):
            mapper = {'i': int, 'u': int, 'f': float}
            _type = mapper.get(value.dtype.kind, lambda x: x)
            return _type(value)
        if value == 'NaT':
            return None
        if isinstance(value, numpy.datetime64):
            return value
        return value


# function to return whether the closure date range is a weekend or weekday
def wkend(b,e):
    if b==0 and e <=1: 
        return 1 
    elif b>=1 and b<=5 and e>=1 and e<=5: 
        return 0 
    elif b>=5 and (e==6 or e==0): 
        return 1 
    else: 
        return 0

def beginwk(bdate):
    wkdte = datetime.strftime(bdate,"%w")
    if (wkdte==0):
        bw = bdate + timedelta(days=wkdte)
    else:  # wkdte>=1:
        bw = bdate + timedelta(days=(7-wkdte))
    return bw

def beginthiswk(bdate):
    wkdte = datetime.strftime(bdate,"%w")
    if (wkdte==0):
        bw = bdate - timedelta(days=wkdte)
    else:  # wkdte>=1:
        bw = bdate - timedelta(days=(8-int(wkdte)))
    return bw    

# Given anydate and n1 as 0 or 1 or 2 , etc  it computes Last Friday, First Friday and Second Friday, etc at 4PM
def fridaywk(bdate,n1):
    wkdte = datetime.strftime(bdate,"%w") # + datetime.strftime(bdate,"%z")
    date4pm = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=16)
    fr4pm= date4pm + timedelta(days=(5-int(wkdte)+(n1-1)*7))
    return fr4pm

def lanestypeside(clside,cltype,nlanes,clfact):
    if clfact.upper()=="SHOULDER":
        lts = "Shoulder closed" 
    else:       
        if clside.upper()=="BOTH":
            lts = cltype + " closure " + clside.lower() + " lanes"
        elif clside.upper() in ["RIGHT","LEFT" ,"CENTER"]:
            if nlanes>1:
                lts = str(nlanes) + " " + clside.lower() + " lanes closed"
            else:
                lts = clside.capitalize() + " lane closed"
        elif clside.upper()=="DIRECTIONAL":
            lts = "Lanes closed in one direction"
        elif clside.upper()=="SHIFT":
            lts="Lanes shifted"
        elif clside.upper()=="FULL":
            lts = "Full lane closure"
        elif clside.upper()=="LOCAL" :
            lts = "{} {} closure , {} access only. ".format(nlanes, clfact , clside )     
        else:
            lts = "{} {} {}(s) closure".format(nlanes, clside , clfact )     
    return lts        
    #[ClosureSide]="Right",IIf([NumLanes]>1,[NumLanes] & " right lanes closed","Right lane closed"),[ClosureSide]="Left",IIf([NumLanes]>1,[NumLanes] & " left lanes closed","Left lane closed"),[ClosureSide]="Center",IIf([NumLanes]>1,[NumLanes] & " center lanes closed","Center lane closed"),[ClosureSide]="Full","Full lane closure",[ClosureSide]="Directional","Lanes closed in one direction",[ClosureSide]="Shift","Lanes shifted")
    #[ClosureSide]="Right",IIf([NumLanes]>1,[NumLanes] & " right lanes closed","Right lane closed"),[ClosureSide]="Left",IIf([NumLanes]>1,[NumLanes] & " left lanes closed","Left lane closed"),[ClosureSide]="Center",IIf([NumLanes]>1,[NumLanes] & " center lanes closed","Center lane closed"),[ClosureSide]="Full","Full lane closure",[ClosureSide]="Directional","Lanes closed in one direction",[ClosureSide]="Shift","Lanes shifted")

#OnRoad: IIf([Route]="H-1" Or [Route]="H-2" Or [Route]="H-3",[Route],IIf([Route]="H-201",[RoadName],IIf(Left([Route],2)="H-",Left([Route],3) & " off ramp",[RoadName])))
def routeinfo(rteid,rtename):
    if rteid.upper() in ["H-1", "H-2", "H-3"]:
        rtext = rteid.upper() 
    elif rteid.upper() in ["H-201"]:
        rtext = rtename 
    elif rteid[1:2] =="H-":
        rtext = "Off Ramp" 
    else:
        rtext = rtename 
    return rtext        

#DirectionWords: IIf([direct] Is Null,""," in " & [direct] & " direction")
def dirinfo(dirn):
    if len(dirn)==0:
        dirtext = ""
    else:
        dirtext = dirn + " direction " 
    return dirtext

#BeginDateName,EndDateName:  The month and the day portion of the begin or end date. (ex. November 23)
def dtemon(dte):
    #dtext = datetime.strftime(dte-timedelta(hours=10),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=10),"%d")))
    dtext = datetime.strftime(dte-timedelta(hours=0),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=0),"%d")))
    return dtext

# BeginDay, EndDay: Weekday Name of the begin date (Monday, Tuesday, Wednesday, etc.)
def daytext(dte):
    dtext = datetime.strftime(dte-timedelta(hours=0),"%A") 
    return dtext

#BeginTime, EndTime: The time the lane closure begins.  12 hour format with A.M. or P.M. at the end
def hrtext(dte):
    hrtext = datetime.strftime(dte-timedelta(hours=0),"%I:%M %p") 
    return hrtext

#Fill DirpRemarks field with long remarks data if it is blank
def remarkstext(remarks,dirptext):
    if dirptext != None:
        if len(dirptext)>100:
            dirptext = remarks[0:99]
    return dirptext


""" # FullSentence: [LanesTypeSide] & " on " & [OnRoad] & [DirectionWords] & IIf([BeginDateName]=[EndDateName]," on " 
& [BeginDay] & ", " & [BeginDateName]," from " & [BeginDay] & ", " & [BeginDateName] & " to " & [EndDay] & ", " 
& [EndDateName]) & ", " & [BeginTime] & " to " & [EndTime] & " " & [Special Remarks]
"""
def fulltext (lts,rtext,dirtext,begdymon,endymon,begdynm,endynm,begtm,endtm,begint,endint,rmarks):
    #fultext = lts + " on " + rtext + dirtext 
    if (begint==endint):
        begendint = "In the vicinity of {}".format(begint)
    else:
        begendint = "Between intersection of {} and {}".format(begint,endint)
            
    if begdynm == endynm:
        fultext = "{} on {}  {} on {}, {}, {} to {} , {} .  {} .".format(lts, rtext, dirtext, begdynm, begdymon,begtm, endtm,begendint,rmarks)
    else:    
        fultext = "{} on {}  {} from {}, {}, to {} , {} , {} to {}  {}.    {} . ".format(lts, rtext, dirtext, begdynm, begdymon,endynm, endymon,begtm, endtm,begendint,rmarks) 
    return fultext


def socratamultilineshp(shp):
    if shp is not None:
        mgeom = shp['paths']
        glen = len(mgeom) # rtepaths[0][len(rtepaths[0])-1] 
        smupltxt = ""
        if glen>0:
            smupline = []
            for il,linex in enumerate(mgeom,0):
                smupline.append([])
                for xy in linex:
                    xylist = ""
                    for i,x in enumerate(xy,1):
                        if i==1:
                            xylist = str(x)
                        elif i==2:
                            xylist = xylist + " " + str(x)
                    smupline[il].append(xylist)
    else:
        smupline = ""
    smupltxt = "MultiLineString {}".format(smupline)
    smupltxt = smupltxt.replace("'","").replace("[","(").replace("]",")")            
    return smupltxt

def soclinegometry(shp):
    slinetxt = ""
    if shp is not None:
        mgeom = shp['paths']
        glen = len(mgeom) # rtepaths[0][len(rtepaths[0])-1] 
        if glen>0:
            socline = []
            for il,linex in enumerate(mgeom,0):
                for xy in linex:
                    xylist = ""
                    for i,x in enumerate(xy,1):
                        if i==1:
                            xylist = str(x)
                        elif i==2:
                            xylist = xylist + " " + str(x)
                    socline.append(xylist)
    else:
        socline = ""
    slinetxt = "LineString {}".format(socline)
    slinetxt = slinetxt.replace("'","").replace("[","(").replace("]",")")            
    return slinetxt


# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())

logger.info("update of lane closure Socrata spatial table begins at {} ".format(tbeg))
print("update of lane closure Socrata spatial table begins at {} ".format(tbeg))

# Socrata access 

sochost = "highways.hidot.hawaii.gov"
socapptoken = "Di04VXcc3fJZKgDmE6veI5gCM"
socusername = "blm0hujxao81ilb72860v5llc" #"tgoodman970@gmail.com"
socpwd =  "4z6b7bojkoy7y8nvykcy3g2vr8rc0ys6wfj328uafu2rmroi6n" #"123Password!"

#auth = Authorization(
#   'highways.hidot.hawaii.gov',
#   os.environ['MY_SOCRATA_USERNAME'],
#   os.environ['MY_SOCRATA_PASSWORD']
#)

auth = Authorization( sochost,socusername, socpwd )

laneclosocrata = Socrata(auth)

laneclosodasoc= sodasoc(sochost,socapptoken,socusername,socpwd)
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
numfs = 10000 # number of items to query
#    sdItem = qgis.content.get(lcwebmapid)
ekOrg = False
# search for lane closure sections 
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))

logger.info ("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))

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
#for f1 in sectfset:
#    f1.project("wgs_84") # from 3857 to 4326)
# delete all sections without a route number - an artifact of false submissions on dashboard
#qry = "Route is null"
#sectqry = wmlnclyrsectsfl.query(where=qry)
# prepare the object Id's with no route numbers (submitted without update privileges
#if len(sectqry)>0:
#    norteid = "objectid in ('" + "','".join(str(sfs.attributes['objectid']) for sfs in sectqry )  + "')"
#    resultdel = wmlnclyrsectsfl.delete_features(where=norteid)

# select sections data entered today
#qry = "ApprLevel2 != '{}' and beginDate >= '{}' ".format( 'Yes',date.today())
qry = "" # "1=1"
expfilenm = "laneclosuredetail"
sectqry = wmlnclyrsectsfl.query()
#sectsdf = wmlnclyrsects.query(as_df=True)
sectfeat = sectqry.features
sectsdf = sectqry.sdf
sectcols = sectsdf.columns

sectsdf.drop(columns=['Clength'])
sectgac = gac(sectsdf)

sprefwgs84 = {'wkid' : 4326 , 'latestWkid' : 4326 }

sectwgs84 = sectgac.project(sprefwgs84)
prjSects = sectgac.to_featureset()

sectsdfwgs84 = prjSects.sdf

#sectsdfidx = sectsdfwgs84.set_index(['OBJECTID'])

#sectsdfidx['Remarks']=Remarks.decode('utf8')
sectsdfwgs84['CreationDate'] = [ xt - timedelta(hours=10)  for xt in sectsdf['CreationDate']]  # + timedelta(hours=-10) # [datetime.fromtimestamp(round(datetime.timestamp(xt)/1e3,0)) for xt in sectsdfwgs84['CreationDate']]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
sectsdfwgs84['beginDate'] = [ xt - timedelta(hours=10) for xt in sectsdf['beginDate']]  # + timedelta(hours=-10) # [datetime.fromtimestamp(round(datetime.timestamp(xt)/1e3,0)) for xt in sectsdfwgs84['beginDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
sectsdfwgs84['enDate']= [ xt - timedelta(hours=10) for xt in sectsdf['enDate']]  # + timedelta(hours=-10)# [datetime.fromtimestamp(round(datetime.timestamp(xt)/1e3,0)) for xt in sectsdfwgs84['enDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
sectsdfwgs84['begWkDy'] = [date.strftime(xt- timedelta(hours=0),"%w") for xt in sectsdfwgs84['beginDate']]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
sectsdfwgs84['endWkDy'] = [date.strftime(xt- timedelta(hours=0),"%w") for xt in sectsdfwgs84['enDate']]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
sectsdfwgs84['wkEnd'] = sectsdfwgs84.apply(lambda x : wkend(int(x['begWkDy']),int(x['endWkDy'])),axis = 1)
#sectsdfwgs84['wkDy'] = [ lambda xt: wkend(date.strftime(xt['beginDate'],"%w"),date.strftime(xt['enDate'],"%w")) for xt in sectsdf]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
#sectsdfwgs84['l2hemail']='george.abcede@hawaii.gov'
#sectsdfwgs84['Remarks']=sectsdfwgs84['Remarks'].astype(str).str.decode('utf8')
#sectsdfwgs84['LaneCLineJ']=  ["""{  "type" : "LineString" ,   "coordinates" : """ + str( lshp['paths'][0]) + "}" for lshp in sectsdfwgs84['SHAPE'] ] 
sectsdfwgs84['DirPRemarks'] = sectsdfwgs84.apply(lambda x : remarkstext(x['Remarks'],x['DirPRemarks']) ,axis = 1)
sectsdfwgs84['LaneCLineS']= sectsdfwgs84.apply(lambda x : soclinegometry(x['SHAPE']),axis=1 ) # Convert to shape paths to a single line geometry  [ str(lshp) for lshp in prtesdfxy['SHAPE']] 
#sectsdfwgs84['LaneCLineS']=  ["LineString ( " + (re.sub("]",",",str(lshp['paths'][0]).replace(",","").replace("[",""))).replace(",,","") + ")" for lshp in sectsdfwgs84['SHAPE'] ]   
sectsdfwgs84['SHAPE']=  [str( lshp) for lshp in sectsdfwgs84['SHAPE'] ] 
#print (sectsdfwgs84.dtypes )

#sectsdf['LaneCLineJ']= sectsdf['SHAPE'].iloc[0].to_json()
#sectline1 = sectsdfidx['LaneCLine'].head(1)
#sectlinej1 = sectsdfidx['LaneCLineJ'].head(1)
lncldetailfilenm = r"{}detail.csv".format(wmlnclyrsectsnm)
lncldetailfile = os.path.join(rptpath,lncldetailfilenm)
try:
    lnclcsv = sectsdfwgs84.to_csv(lncldetailfile,encoding='utf-8',index=False)
except Exception as e:
    print (" Error message : {} \n exporting to csv : {} failed ".format(str(e),lncldetailfile,tbeg))
    logger.error(" Error message : {} \n exporting to csv : {} failed at {} ".format(str(e),lncldetailfile,tbeg))


# search for Route dataset  
lncolumns = sectsdfwgs84.columns

# use new version of socrata modules to connect 

#(ok, view) = laneclosocrata.views.lookup(lncldetailid)
#assert ok, view

#(ok, job) = laneclosocrata.using_config('lanecl2v2_12-24-2019_15d0', view).df(sectsdfwgs84,)
#assert ok, job
  # These next 3 lines are optional - once the job is started from the previous line, the
  # script can exit; these next lines just block until the job completes
#assert ok, job
#(ok, job) = job.wait_for_finish(progress = lambda job: print('Job progress:', job.attributes['status']))
#sys.exit(0 if job.attributes['status'] == 'successful' else 1)


lncldetailid = "88gn-aaku" # "a392-u7px"
content_type = "csv"


try:

    (detailvw) = laneclosocrata.views.lookup(lncldetailid)
    #assert ok, view
    socratrans = 'Lane_Closure_Sectionsdetail_02-27-2020_94c1'
    with open(lncldetailfile, 'rb') as detailfile:
        (revdetail, outdetail) = laneclosocrata.using_config(socratrans, detailvw).csv(detailfile)
        #assert ok, job
        # These next 3 lines are optional - once the job is started from the previous line, the
        # script can exit; these next lines just block until the job completes
        (jobdetail) = outdetail.wait_for_finish(progress = lambda outdetail: print('Job progress: {} at {}'.format(outdetail.attributes['status'], datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p"))))
        #applydetail = revdetail.apply(output_schema=outdetail) 
        #applydetail.wait_for_finish() # progress = lambda outdetail: print('Job progress: {} at {}'.format(outdetail.attributes['status'], datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p"))))
        tend=datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
        if jobdetail.attributes['status'] == 'successful':
            logger.info (" {} \n End lane closure Detail {} processed {} columns.\n Detail Table id {} processed {} features.  Update results {} ; time {} ".format (jobdetail.attributes['status'],lncldetailid,len(sectfeat),lncldetailid,len(sectsdfwgs84),jobdetail.attributes['log'],tend))
            print (" {} \n End lane closure Detail  {} processed {} columns.\n Detail Table id {} processed {} features. Update results {} ; time {}".format (jobdetail.attributes['status'],lncldetailid,len(sectfeat),lncldetailid,len(sectsdfwgs84),jobdetail.attributes['log'],tend))
        else:    
            logger.info (" {} end Lane closure Detail {} with {} columns , \n Detail Table id {} processed {} features. Update results {} ; time {}".format (jobdetail.attributes['status'],lncldetailid,len(sectfeat),lncldetailid,len(sectsdfwgs84),jobdetail.attributes['log'],tend))
            print (" {} end Lane closure Detail {} with {} columns  .\n Detail Table id {} processed {} features. Update results {} ; time {}".format (jobdetail.attributes['status'],lncldetailid,len(sectfeat),lncldetailid,len(sectsdfwgs84),jobdetail.attributes['log'],tend))
    lnclhdlr.close()
    
    sys.exit(0 if outdetail.attributes['status'] == 'successful' else 1)


except Exception as e:
    print (" Error message : {} \n detail data upsert failed at {} ".format(str(e),tbeg))
    logger.error(" Error message : {} \n detail data upsert failed at {} ".format(str(e),tbeg))
    lnclhdlr.close()

    sys.exit(1)

