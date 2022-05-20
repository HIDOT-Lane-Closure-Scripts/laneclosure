
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
from numpy.f2py.auxfuncs import isstring
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
#from sodapy import Socrata

from socrata.authorization import Authorization
from socrata import Socrata
import os
import sys

#from socrata.authorization import Authorization
#from socrata import Socrata

import logging

logfilenm = r'socratransmerged.log'
#logpath = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs"
#rptpath = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\reports"
logpath = r'D:\MyFiles\HWYAP\LaneClosure\logs'
rptpath = r'D:\MyFiles\HWYAP\LaneClosure\reports'
logfile = os.path.join(logpath,logfilenm) # r"conflaneclosections.log"

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
    logFile = open(logfilenm, 'a')
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
        return value


# function to return whether the closure date range is a weekend or weekday
def wkend(b,e):
    if np.isnan(b):
        return 0
    if np.isnan(e):
        return 0
    if b==0 and e <=1: 
        return 1 
    elif b>=1 and e<=5: 
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

def fridaywk(bdate,n1,h1=16):
    wkdte = datetime.strftime(bdate,"%w") # + datetime.strftime(bdate,"%z")
    date4pm = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=h1)
    fr4pm= date4pm + timedelta(days=(5-int(wkdte)+(n1-1)*7))
    return fr4pm

def lanestypeside(clside,cltype,nlanes,clfact):
    if clfact.upper()=="SHOULDER":
        lts = "Shoulder closed" 
    else:       
        if clside.upper()=="BOTH":
            lts = cltype + " closure " + clside.lower() + " lanes"
        elif clside.upper() in ["RIGHT","LEFT" ,"CENTER"]:
            if nlanes == None :
                nlanes=0
            elif isinstance(nlanes,str):
                if len(nlanes)==0:
                    nlanes=0
                else:    
                    nlanes = eval(nlanes)    
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
    if len(rtext)==0:  # assign default rtext 
        rtext = rteid.upper()    
    return rtext        


#DirectionWords: IIf([direct] Is Null,""," in " & [direct] & " direction")
def dirinfo(dirn):
    if len(dirn)==0:
        dirtext = ""
    else:
        dirtext = dirn + " direction " 
    return dirtext

#BeginDateName,EndDateName:  The month and the day portion of the begin or end date. (ex. November 23)
def dtemon(dte,h1=0):
    try:
        dtext = datetime.strftime(dte-timedelta(hours=int(h1)),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=int(h1)),"%d")))
    except Exception as e:
        print (" Error message : {} \n date conversion : {} failed ".format(str(e),dte,h1))
        logger.error(" Error message : {} \n date conversion  : {} failed at {} ".format(str(e),dte,h1))
        dtext = dte
    return dtext

# BeginDay, EndDay: Weekday Name of the begin date (Monday, Tuesday, Wednesday, etc.)
def daytext(dte,h1=0):
    try:
        dtext = datetime.strftime(dte-timedelta(hours=int(h1)),"%A") 
    except Exception as e:
        print (" Error message : {} \n date conversion : {} failed ".format(str(e),dte,h1))
        logger.error(" Error message : {} \n date conversion  : {} failed at {} ".format(str(e),dte,h1))
        dtext = dte
    return dtext

#BeginTime, EndTime: The time the lane closure begins.  12 hour format with A.M. or P.M. at the end
def hrtext(dte,h1=0):
    try:
        hrtext = datetime.strftime(dte-timedelta(hours=int(h1)),"%I:%M %p") 
    except Exception as e:
        print (" Error message : {} \n date conversion : {} failed ".format(str(e),dte,h1))
        logger.error(" Error message : {} \n date conversion  : {} failed at {} ".format(str(e),dte,h1))
        hrtext = dte
    return hrtext

#Fill DirpRemarks field with long remarks data if it is blank
def remarkstextold(remarks,dirptext):
    if dirptext != None:
        if len(dirptext)>100:
            dirptext = remarks[0:99]
    return dirptext

def remarkstext(remarks,dirptext):
    if isinstance(dirptext,str):
        if len(dirptext)>100:
                dirptext = dirptext[0:99]
    else:
            if isinstance(remarks,str):
                dirptext = remarks[0:99]
            else:
                dirptext = "No remarks"
    return dirptext

def routenames(rdname,rte):
    if rdname == None:
        rdname = rte
    elif len(rdname)==0:
        rdname = rte
    return rdname

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

def recs2htmlbody(featsdf,htmltd,urlinkx):
    # merge relationship records from the child table to the current header record
    htmlby = "" #htmltxtd.format(q1="\"",urlink=urlinkx,q2="\"",island=sdfrec.Island,route=sdfrec.Route,begindate=sdfrec.beginDate,endate=sdfrec.enDate,intersfrom=sdfrec.IntersFrom,intersto=sdfrec.IntersTo) + "\n"
    for ir,sdfrec in enumerate(featsdf.itertuples()):   # two points are given                                                                                                                                                                                                                                                                                                                                                                                       
    # add two sets of columns from the related data to the header data frame                                                                                                                                                                                                                                                                                                                                                                                             
        htmlby = htmlby + htmltd.format(q1="\"",urldet=urlinkx,fldid="OBJECTID",fldval=sdfrec.OBJECTID,q2="\"",island=sdfrec.Island,route=sdfrec.Route,roadname=sdfrec.RoadName,intersfrom=sdfrec.IntersFrom,intersto=sdfrec.IntersTo,begindate=sdfrec.beginDate,endate=sdfrec.enDate) + "\n"
    
    return htmlby

def dateonly(bdate,h1=0):
    datemid = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=h1)
    return datemid

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

logger.info("CSV based update of lane closure Socrata merged table begins at {} ".format(tbeg))
print("CSV based update of lane closure Socrata merged table begins at {} ".format(tbeg))

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

socrauth = Authorization( sochost,socusername, socpwd )

laneclosocrata = Socrata(socrauth)

#laneclosocrata = Socrata(sochost,socapptoken,socusername,socpwd)
#laneclosocrata = Socrata(sochost,socapptoken,socusername,socpwd)
lctags = ["laneclosure","weekly","HIDOT"]
lcat = "Lane Closure"

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.

# This will make our initial revision, on a view that does exist
#merevision = laneclosocrata.new({'name': 'Lane Closure Merged Data'})
#assert ok
# This will make our initial revision, on a view that does exist
#derevision = laneclosocrata.new({'name': 'laneclosurespatial'})
#assert ok


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

lb = '<br>'
tinyurlink = "http://arcg.is/1X95b8"  
urlink = "<a href='http://arcg.is/1X95b8'> LINK</a>"
urlong = "http://histategis.maps.arcgis.com/apps/opsdashboard/index.html#/fbe5a65b146a42fd8d3b9f6f815d835a"

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
print(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")
logger.info(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")


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


qry = "1=1" #"beginDate >= '{}' ".format( datetime.strftime(date.today(),"%m-%d-%Y"))
#qry = "1=1"
expfilenm = "laneclosuremerged"
sectqry = wmlnclyrsectsfl.query(qry)
#sectsdf = wmlnclyrsects.query(as_df=True)
sectsdf = sectqry.sdf
sectcols = sectsdf.columns

sectsdf.drop(columns=['Clength'])
sectgac = gac(sectsdf)

sprefwgs84 = {'wkid' : 4326 , 'latestWkid' : 4326 }

sectwgs84 = sectgac.project(sprefwgs84)
prjSects = sectgac.to_featureset()
utcoff = 20
sectsdfwgs84 = prjSects.sdf

#sectsdfidx = sectsdfwgs84.set_index(['OBJECTID'])

#sectsdfidx['Remarks']=Remarks.decode('utf8')
try:
    sectsdfwgs84['CreationDate'] = sectsdfwgs84['CreationDate'].apply(lambda x : x - timedelta(hours=utcoff)) #[ xt - timedelta(hours=utcoff)  for xt in sectsdf['CreationDate']]  # + timedelta(hours=-utcoff) # [datetime.fromtimestamp(round(datetime.timestamp(xt)/1e3,0)) for xt in sectsdfwgs84['CreationDate']]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
    sectsdfwgs84['beginDate'] = sectsdfwgs84['beginDate'].apply(lambda x : x if isnull(x) else (x-timedelta(hours=utcoff)) ) #[ xt - timedelta(hours=utcoff) for xt in sectsdf['beginDate']]  # + timedelta(hours=-utcoff) # [datetime.fromtimestamp(round(datetime.timestamp(xt)/1e3,0)) for xt in sectsdfwgs84['beginDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
    sectsdfwgs84['enDate']= sectsdfwgs84['enDate'].apply(lambda x : x if isnull(x) else (x-timedelta(hours=utcoff)) ) # [ xt - timedelta(hours=utcoff) for xt in sectsdf['enDate']]  # + timedelta(hours=-10)# [datetime.fromtimestamp(round(datetime.timestamp(xt)/1e3,0)) for xt in sectsdfwgs84['enDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
    sectsdfwgs84['begWkDy'] = sectsdfwgs84['beginDate'].apply(lambda x : date.strftime(pd.to_datetime(x),'%w')) # pd.to_datetime(sectsdfwgs84['beginDate']).dt.strftime('%w') #[date.strftime(xt- timedelta(hours=utcoff),"%w") for xt in sectsdfwgs84['beginDate']]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
    sectsdfwgs84['begWkDy'] = sectsdfwgs84['begWkDy'].fillna('0')
    #sectsdfwgs84['begWkDy'] = pd.to_numeric(sectsdfwgs84['begWkDy'], errors='coerce')
    sectsdfwgs84['begWkDy'] = sectsdfwgs84['begWkDy'].astype(int)
    sectsdfwgs84['endWkDy'] = sectsdfwgs84['enDate'].apply(lambda x : date.strftime(pd.to_datetime(x),'%w')) #[date.strftime(xt- timedelta(hours=utcoff),"%w") for xt in sectsdfwgs84['enDate']]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
    #sectsdfwgs84['endWkDy'] = pd.to_numeric(sectsdfwgs84['endWkDy'], errors='coerce')
    sectsdfwgs84['endWkDy'] = sectsdfwgs84['endWkDy'].fillna('0')
    sectsdfwgs84['endWkDy'] = sectsdfwgs84['endWkDy'].astype(int)
    sectsdfwgs84['wkEnd'] = sectsdfwgs84.apply(lambda x : wkend(int(x['begWkDy']),int(x['endWkDy'])),axis = 1)
except Exception as e:
    print (" Error message : {} \n date conversion : {} failed ".format(str(e),sectsdfwgs84,tbeg))
    logger.error(" Error message : {} \n date conversion  : {} failed at {} ".format(str(e),sectsdfwgs84,tbeg))

#sectsdfwgs84['wkDy'] = [ lambda xt: wkend(date.strftime(xt['beginDate'],"%w"),date.strftime(xt['enDate'],"%w")) for xt in sectsdf]  #[(int(xt/1e3)) for xt in sectsdf['CreationDate']] # [ datetime.fromtimestamp(xt) for xt in sectsdf['CreationDate']]
#sectsdfwgs84['l2hemail']='george.abcede@hawaii.gov'
#sectsdfwgs84['Remarks']=sectsdfwgs84['Remarks'].astype(str).str.decode('utf8')
#sectsdfwgs84['LaneCLineJ']=  ["""{  "type" : "LineString" ,   "coordinates" : """ + str( lshp['paths'][0]) + "}" for lshp in sectsdfwgs84['SHAPE'] ] 
sectsdfwgs84['DirPRemarks'] = sectsdfwgs84.apply(lambda x : remarkstext(x['Remarks'],x['DirPRemarks']) ,axis = 1)
sectsdfwgs84['LaneCLineS']=  sectsdfwgs84.apply(lambda x : soclinegometry(x['SHAPE']),axis=1 )
sectsdfwgs84['SHAPE']= sectsdfwgs84['SHAPE'].apply(lambda x : str(x))   # [str( lshp) for lshp in sectsdfwgs84['SHAPE'] ] 
sectsdfwgs84['RoadName']=  sectsdfwgs84.apply(lambda x : routenames(x['RoadName'],x['Route']),axis=1 ) # set route name to route if blank
sectsdfwgs84 = sectsdfwgs84.fillna("0")

#sectsdf['LaneCLineJ']= sectsdf['SHAPE'].iloc[0].to_json()
#sectline1 = sectsdfidx['LaneCLine'].head(1)
#sectlinej1 = sectsdfidx['LaneCLineJ'].head(1)
#lnclcsvfile = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\{}detail.csv".format(wmlnclyrsectsnm)



sectfeat = sectqry.features
lnclflds = sectqry.fields
#sectsdfhdr = deepcopy(sectsdf.head(1))
# create an empty dataset with all the field characteristics
#sectsdfptype = deepcopy(sectsdf.head(0))
# query columns 
lncolumns = sectsdfwgs84.columns

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



#replace the summary data also
grpbycols = ['wkEnd',  'Route', 'RoadName','RteDirn','BMP','EMP','Active', 'ApprLevel1', 'ApprLevel2', 
             'ApproverL1', 'ApproverL2', 'ApproverL3',  'ClosHours', 'ClosReason', 'ClosType', 'CloseFact', 
             'ClosureSide', 'Creator', 'CreationDate', 'DEPHWY', 'DIRPInfo', 'DistEngr',   'Island', 'LocMode', 'NumLanes', 
             'ProjectId',  'direct', 'l1email', 'l2email', 'l3email','parentglobalid']

"""'LanesTypeSide','OnRoad','DirectionWords','BegMonDate','EndMonDate','BegDayName','EnDayName','BegTime','EndTime','FullText'"""


sectsdfwgs84grpgidf = sectsdfwgs84.groupby(grpbycols,as_index=False).agg({'OBJECTID': 'min', 'beginDate': 'min', 'enDate': 'max','begWkDy':'min','endWkDy':'max','CreationDate_1':'max','LaneCLineS' : 'max',
                                                                            'Creator_1':'last', 'EditDate':'max', 'EditDate_1':'max','Editor': 'last', 'Clength' : 'first','Shape__Length' : 'first',
                                                                          'SHAPE' : 'first','todayis' : 'first','begDyWk' : 'first','enDyWk' : 'first','Friday0' : 'min','Friday1' : 'max','Friday2' : 'max',
                                                                          'IntersFrom' : 'max', 'IntersTo' : 'max','intsfroml' : 'max', 'intstol' : 'max','DirPRemarks' : 'max','Remarks' : 'max'}) 
#lnclhtml = sectsdfwgs84grpgidf.to_html(border=True, table_id="Lane Closure Summary")
#text_file = open(os.path.join(rptpath,"{}2.html".format(wmlnclyrsectsnm)), "w")
#text_file.write(lnclhtml)
#text_file.close()

sectsdfwgs84grpgidf['LanesTypeSide'] = sectsdfwgs84grpgidf.apply(lambda x : lanestypeside(x['ClosureSide'],x['ClosType'],x['NumLanes'],x['CloseFact']),axis = 1)
sectsdfwgs84grpgidf['OnRoad'] = sectsdfwgs84grpgidf.apply(lambda x : routeinfo(x['Route'],x['RoadName']),axis = 1)
sectsdfwgs84grpgidf['DirectionWords'] = sectsdfwgs84grpgidf['direct'].apply(lambda x : dirinfo(x))
sectsdfwgs84grpgidf['BegMonDate'] = sectsdfwgs84grpgidf['beginDate'].apply(lambda x : x if isnull(x) else dtemon(x))
sectsdfwgs84grpgidf['EndMonDate'] = sectsdfwgs84grpgidf['enDate'].apply(lambda x : x if isnull(x) else  dtemon(x))
sectsdfwgs84grpgidf['BegDayName'] = sectsdfwgs84grpgidf['beginDate'].apply(lambda x : x if isnull(x) else daytext(x))
sectsdfwgs84grpgidf['EnDayName'] = sectsdfwgs84grpgidf['enDate'].apply(lambda x : x if isnull(x) else daytext(x))
sectsdfwgs84grpgidf['BegTime'] = sectsdfwgs84grpgidf['beginDate'].apply(lambda x : x if isnull(x) else hrtext(x))
sectsdfwgs84grpgidf['EndTime'] = sectsdfwgs84grpgidf['enDate'].apply(lambda x : x if isnull(x) else hrtext(x))
sectsdfwgs84grpgidf['FullText'] = sectsdfwgs84grpgidf.apply(lambda x : fulltext (x['LanesTypeSide'],x['OnRoad'],x['DirectionWords'],x['BegMonDate'],x['EndMonDate'],x['BegDayName'],x['EnDayName'],x['BegTime'],x['EndTime'],x['IntersFrom'],x['IntersTo'],x['DirPRemarks']),axis = 1)


#lnclmergedfilenm = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\{}merged.csv".format(wmlnclyrsectsnm)
lnclmergedfilenm = r"{}merged.csv".format(wmlnclyrsectsnm)
lnclmergedfile = os.path.join(rptpath,lnclmergedfilenm)
print (" finished Dataframe transformation columns : {} ".format(sectsdfwgs84grpgidf.columns) )
try:
    lnclcsv = sectsdfwgs84grpgidf.to_csv(lnclmergedfile,encoding='utf-8')
except Exception as e:
    print (" Error message : {} \n exporting to csv : {} failed ".format(str(e),lnclmergedfile,tbeg))
    logger.error(" Error message : {} \n exporting to csv : {} failed at {} ".format(str(e),lnclmergedfile,tbeg))

lncolumns = sectsdfwgs84grpgidf.columns


#laneclosocrata = Socrata(sochost,socapptoken,socusername,socpwd)
lctags = ["laneclosure","weekly","HIDOT","Merged"]
lcat = "New Lane Closure Merged Data"
descriptext = "Lane Closure Spatial data from ArcGIS aggregated by variables such as Route, Direction, Mile Post, \
Closure Begin and End Dates, Active, Weekday and Weekend formatted into Socrata in LineString format and \
replaced hourly via a script. The base data is generated from entries by HiDOT personnel into a survey123 \
dataset. This data is primarily used for visualizations and reporting purposes only."

lnclmergedsocid = "n248-5efu"
print (" Begin upload to : {} ".format(lnclmergedsocid) )


"""
view = socrata.views.lookup('u4yv-z7mw') # Lane-Closure-Merged-Data

with open('Lane_Closure_Sectionsmerged.csv', 'rb') as my_file:
  (revision, job) = socrata.using_config('Lane_Closure_Sectionsmerged_02-27-2020_5a9e', view).csv(my_file)
  # These next 2 lines are optional - once the job is started from the previous line, the
  # script can exit; these next lines just block until the job completes
  job = job.wait_for_finish(progress = lambda job: print('Job progress:', job.attributes['status']))
  sys.exit(0 if job.attributes['status'] == 'successful' else 1)
"""

content_type = "dataframe" # "csv"

#try:
if 1==1:
    uploadf = upload.df(sectsdfwgs84grpgidf)
    (revmergeset, mergeset_schema) = Socrata(socrauth).create(
        name = lcat,
        description = descriptext
    ).df(sectsdfwgs84grpgidf)


##    # Find the view you want to update (here we just take it from the most recently-uploaded dataset).
##    mergevw = laneclosocrata.views.lookup(lnclmergedsocid)
##    # Create a revision on this view. You can also choose create_update_revision() to append data.
##    revmergeset = mergevw.revisions.create_replace_revision(metadata = {'name': lcat, 'description': descriptext})
##    # Create an upload to this revision based on our file
##    mergedata = 'Lane_Closure_merged_Data_Replace'
##    uplmergeset = revmergeset.create_upload(mergedata)
##    # Actually put data into the upload with a source.
##    srcmergeset = uplmergeset.df(sectsdfwgs84grpgidf)
##    # Identify the input and output schemas.
##    mergeset_schema = srcmergeset.get_latest_input_schema().get_latest_output_schema()# Apply the revision based on the output schema.

    jobrevdf = revmergeset.apply(output_schema=mergeset_schema)
    # So maybe we just want to wait here, printing the progress, until the job is done
    jobrevdf.wait_for_finish(progress = lambda jobrevdf: print('Job progress:', jobrevdf.attributes['status']))

    tend=datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
    if jobrevdf.attributes['status'] == 'successful':
        logger.info (" {} End of lane closure Socrata  id {} processed {} features , Update Result {} .\n Grouped Table id {} processed {} columns. {} ".format (jobrevdf.attributes['status'],lnclmergedsocid,len(sectfeat),ok6,lnclmergedsocid,len(sectsdfwgs84grpgidf,tend)))
        print (" {} End of lane closure Socrata  id {} processed {} features , Update Result {} .\n Grouped Table id {} processed {} columns. {} ".format (jobrevdf.attributes['status'],lnclmergedsocid,len(sectfeat),ok6,lnclmergedsocid,len(sectsdfwgs84grpgidf),tend))
    else:    
        logger.info (" {} end of Lane closure Socrata id {} with {} features , Update Result {} .\n Grouped Table id {} processed {} columns. Update results {} at {}".format (jobrevdf.attributes['status'],lnclmergedsocid,len(sectfeat),mergevw,lnclmergedsocid,len(sectsdfwgs84grpgidf),ok6,tend))
        print (" {} end of Lane closure Socrata id {} with {} features , Update Result {} .\n Grouped Table id {} processed {} columns. Update results {} at {} ".format (jobrevdf.attributes['status'],lnclmergedsocid,len(sectfeat),mergevw,lnclmergedsocid,len(sectsdfwgs84grpgidf),ok6,tend))
    lnclhdlr.close()
    
    #sys.exit(0 if jobrevdf.attributes['status'] == 'successful' else 1)
##except Exception as e:
##    print (" Error message : {} \n detail data upsert failed at {} ".format(str(e),tbeg))
##    logger.error(" Error message : {} \n detail data upsert failed at {} ".format(str(e),tbeg))



