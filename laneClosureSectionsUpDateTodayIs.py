
# coding: utf-8


import  os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
#import arcpy
#from arcpy import env
import unicodedata
import datetime, time #, tzlocal
from datetime import date , datetime, timedelta
from time import  strftime
#from tzlocal import get_localzone
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
#import matplotlib.pyplot as pyd
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

#logpath = r'\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs'
#rptpath = r'\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\reports'
logpath = r'D:\MyFiles\HWYAP\LaneClosure\logs'
rptpath = r'D:\MyFiles\HWYAP\LaneClosure\reports'
logfilenm = r"updatetodayis.log"
#logpath = r'I:\HI\tools\scripts\LaneClosure\logs'
#rptpath = r'I:\HI\tools\scripts\LaneClosure\reports'
logfile = os.path.join(logpath,logfilenm)
lcnclupdatehdlr = logging.FileHandler(logfile) # 'laneclosections.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
lcnclupdatehdlr.setFormatter(formatter)
logger = logging.getLogger('lnclupdatetoday')
logger.addHandler(lcnclupdatehdlr)
logger.setLevel(logging.INFO)



def webexsearch(mgis, title, owner_value, item_type_value, max_items_value=1000,inoutside=False):
    item_match = None
    search_result = mgis.content.search(query="title:{} AND owner:{}".format(title, owner_value), 
                                          item_type="'{}'".format(item_type_value), max_items=max_items_value, outside_org=inoutside)
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

def datemidnight(bdate,x=0):
    date0am = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=x)
    return date0am


# function to return whether the closure date range is a weekend or weekday
def wkend(b,e):
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

def lanestypeside(clside,cltype,nlanes):
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
    return lts        
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
    dtext = datetime.strftime(dte-timedelta(hours=10),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=10),"%d")))
    return dtext

# BeginDay, EndDay: Weekday Name of the begin date (Monday, Tuesday, Wednesday, etc.)
def daytext(dte):
    dtext = datetime.strftime(dte-timedelta(hours=10),"%A") 
    return dtext

#BeginTime, EndTime: The time the lane closure begins.  12 hour format with A.M. or P.M. at the end
def hrtext(dte):
    hrtext = datetime.strftime(dte-timedelta(hours=10),"%I:%M %p") 
    return hrtext

def calc(lyr,qry):
    fld = "todayis"
    val = datemidnight(datetime.today()) 
    fld0 = "Friday0"
    expn = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld,val) +chr(125) # 'value' : "\ “dot_achung”  
    # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
    result = lyr.calculate(where=qry,calc_expression={"field" : fld , "value" : val})
    print (" Update result {} for  {} ".format (result,expn))
    logger.info ("  Update result {} for  {} ".format (result,expn))
    if(datetime.strftime(datetime.today(),'%A')=='Saturday'): 
        fri0 = fridaywk(val,0)
        fld1 = "Friday1"
        fri1 = fridaywk(val,1)
        fld2 = "Friday2"
        fri2 = fridaywk(val,2)
        expn2 = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld0,fri0) +chr(125) # 'value' : "\ “dot_achung”  
        # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
        resultupd2 = wmlnclyrsects.calculate(where=qry,calc_expression={"field" : fld0 , "value" : fri0})
        print (" Update result {} for  {} ".format (resultupd2,expn2))
        logger.info ("  Update result {} for  {} ".format (resultupd2,expn2))
    
        expn3 = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld1,fri1) +chr(125) # 'value' : "\ “dot_achung”  
        # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
        resultupd3 = wmlnclyrsects.calculate(where=qry,calc_expression={"field" : fld1 , "value" : fri1})
        print (" Update result {} for  {} ".format (resultupd3,expn3))
        logger.info ("  Update result {} for  {} ".format (resultupd3,expn3))
    
        expn4 = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld2,fri2) +chr(125) # 'value' : "\ “dot_achung”  
        # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
        resultupd4 = wmlnclyrsects.calculate(where=qry,calc_expression={"field" : fld2 , "value" : fri2})
        print (" Update result {} for  {} ".format (resultupd4,expn4))
        logger.info ("  Update result {} for  {} ".format (resultupd4,expn4))


def calctoday(lyr,qry,fld,valdte):
    #fld = "todayis"
    #val =  datemidnight(datetime.today(),0) # "CURRENT_DATE()" #
    expn = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld,valdte) +chr(125) # 'value' : "\ “dot_achung”  
    # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
    sucs1=0
    errcnt = 0
    pres = None
    while(round(sucs1)==0):
        pres = lyr.calculate(where=qry,calc_expression={"field" : fld , "value" : valdte}) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
        if pres['success']==True:
            sucs1=1
        else:
            errcnt+=1
            terr = datetime.datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
            logger.error("Attempt {} at {} ; Result : {} ; Layer {} ; fld {} ; value {} ".format(errcnt,terr, pres,lyr.properties.name,fld,valdte))
            #if errcnt<=10:
            sleep(10)
    
    print (" Update {} result {} for  {} ".format (lyr.properties.name,pres,expn))
    logger.info ("  Update {} result {} for  {} ".format (lyr.properties.name,pres,expn))
    return pres

def calctodayx(lyr,qry,fld,valdte):
    #fld = "todayis"
    #val =  datemidnight(datetime.today(),0) # "CURRENT_DATE()" #
    oidfld = 'objectid'
    oidval = 20000
    qryStr0 = "{} <= '{}'".format(oidfld,oidval)
    expn = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld,valdte) +chr(125) # 'value' : "\ “dot_achung”  
    # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
    sucs1=0
    errcnt = 0
    pres = None
    oidval1 = 0
    lyrname = lyr.properties.name
    outstat = [{"statisticType": "max","onStatisticField": oidfld,"outStatisticFieldName": "maxoid"}]
    print (" layer  {} query {} ".format (lyrname,outstat))
    maxres = lyr.query(out_statistics=outstat).sdf
    maxoid =  maxres["maxoid"][0]
    tqry = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
    print (" Update {} query {} total records {} begins at {}".format (lyrname,outstat,maxoid,tqry))
    logger.info (" Update {} query {} ; total records {} begins at {}".format (lyrname,outstat,maxoid,tqry))
    while maxoid >= oidval1  :
        oidval2 = oidval1+10000
        qry = "{} between '{}' and '{}'".format(oidfld,oidval1,oidval2)
        sucs1 = 0
        while(round(sucs1)==0):
            pres = lyr.calculate(where=qry,calc_expression={"field" : fld , "value" : valdte}) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
            if pres['success']==True:
                sucs1=1
                print (" update {} current date {} ; response : {} ;  Layer {} ".format(qry,valdte,pres,lyrname))
                logger.info(" update {} current date {} ; response : {} ;  Layer {} ".format(qry,valdte,pres,lyrname))
            else:
                errcnt+=1
                terr = datetime.datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
                logger.error("Attempt {} at {} ; Result : {} ; Layer {} ; fld {} ; value {} ".format(errcnt,terr, pres,lyrname,fld,valdte))
                #if errcnt<=10:
                sleep(10)
    
        oidval1 = oidval2
    tqry = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
    print (" Update {} result {} for  {} records completed at {} ".format (lyrname,pres,maxoid,tqry))
    logger.info ("  Update {} result {} for  {} records completed at {} ".format (lyrname,pres,maxoid,tqry))
    return pres


""" # FullSentence: [LanesTypeSide] & " on " & [OnRoad] & [DirectionWords] & IIf([BeginDateName]=[EndDateName]," on " 
& [BeginDay] & ", " & [BeginDateName]," from " & [BeginDay] & ", " & [BeginDateName] & " to " & [EndDay] & ", " 
& [EndDateName]) & ", " & [BeginTime] & " to " & [EndTime] & " " & [Special Remarks]
"""
def fulltext (lts,rtext,dirtext,begdymon,endymon,begdynm,endynm,begtm,endtm,begint,endint,rmarks):
    #fultext = lts + " on " + rtext + dirtext 
    if (begint==endint):
        begendint = "In the vicinity of {}".format(begint)
    else:
        begendint = "Between intersections of {} and {}".format(begint,endint)
            
    if begdynm == endynm:
        fultext = "{} on {}  {} on {}, {}, {} to {} ,  {} . {} .".format(lts, rtext, dirtext, begdynm, begdymon,begendint,rmarks)
    else:    
        fultext = "{} on {}  {} from {}, {}, to {} , {} , {} to {} , {} , Additional Remarks : {}.".format(lts, rtext, dirtext, begdynm, begdymon,endynm, endymon,begtm, endtm,begendint,rmarks) 
    return fultext

    # get the date and time
curDate = strftime("%Y%m%d%H%M%S") 
# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())
logger.info("lane closure today field update begins at {} ".format(tbeg))
print("lane closure today field update begins at {} ".format(tbeg))


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
#arcpy.env.overwriteOutput = True
#print("Temp path : {}".format(tempPath))

print("Connecting to {}".format(portal_url))
#qgis = GIS(portal_url, userName, passWord)
qgis = GIS(profile="hisagolprof")
print(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")

numfs = 1000 # number of items to query
#    sdItem = qgis.content.get(lcwebmapid)
ekOrg = False
# search for lane closure source data
print("Searching for lane closure source {} from {} item for user {} and Service Title {} on AGOL...".format(itypelnclsrc,portal_url,userName,lnclSrcFSTitle))
fslnclsrc = webexsearch(qgis, lnclSrcFSTitle, userName, itypelnclsrc,numfs,ekOrg)
#qgis.content.search(query="title:{} AND owner:{}".format(lnclSrcFSTitle, userName), item_type=itypelnclsrc,outside_org=False,max_items=numfs) #[0]
#print (" Content search result : {} ; ".format(fslnclsrc))
if (fslnclsrc is None):
    print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,lnclSrcFSTitle))
    logger.error ("{} with title {} is not found at {} for user {} ...".format(itypeFS,lnclSrcFSTitle,portal_url,userName))
    sys.exit()
else:

    print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fslnclsrc.url,fslnclsrc.title,fslnclsrc.id))
    lnclyrsrc = fslnclsrc.layers
    
    # header layer
    lnclhdrlyr = lyrsearch(lnclyrsrc, lnclhdrnm)
    hdrsdf = lnclhdrlyr.query(as_df=True)
    # child layer
    lnclchdlyr = lyrsearch(lnclyrsrc, lnclchdnm)


# search for lane closure sections 
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))

#fsect = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
fsectwebmap = webexsearch(qgis, fsectWebMapTitle, userName, itypeFS,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))

if (fsectwebmap is None):
    print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))
    logger.error ("{} with title {} is not found at {} for user {} ...".format(itypeFS,fsectWebMapTitle,portal_url,userName))
    sys.exit()
else:
    wmsectlyrs = fsectwebmap.layers
    wmsectlyrpts = lyrsearch(wmsectlyrs, wmlnclyrptsnm)
    wmlnclyrsects = lyrsearch(wmsectlyrs, wmlnclyrsectsnm)

lyrname = wmlnclyrsects.properties.name
sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
# get sdf to be used for new section data insert operations
utcoffset=10 
fld0 = 'begDyWk'
apprtxt = ''
fldx = 'beginDate'
dateval = '11-1-2019'
#datefrom = "{}".format(datetime.strftime(datetime.date(dateval,"%m-%d-%y")))
begwkqry = "{} >= '{}'".format(fldx,dateval)  # " # "{} is Null".format( fld0,apprtxt)
begdtqry = '1=1'
fldx = "todayis"
valx = datemidnight(datetime.today(),utcoffset) 
valtstamp = datetime.strftime(datetime.today()+timedelta(hours=0),"%m-%d-%Y") # "CURRENT_DATE" # datetime.fromtimestamp((datemidnight(datetime.today()).timestamp())) 
qryStr0 = "{} <> '{}'".format(fldx,valtstamp)
sprefwgs84 = {'wkid' : 4326 , 'latestWkid' : 4326 }
# prepare the object Id's with no route numbers (submitted without update privileges
#if len(lnclsectqry)>0:
#    norteid = "OBJECTID in ('" + "','".join(str(sfs.attributes['OBJECTID']) for sfs in lnclsectqry )  + "')"
#    norteid = "objectid in ('" + "','".join(str(lfs.attributes['objectid']) for lfs in lnclsectqry )  + "')"
    # edit the selected records
#    for sfs in  lnclsectqry:
#        appr1 = sfs.get_value('ApproverL1') #,'dot_achung')
#        email1 = sfs.get_value('l1email') # ,'albert.chung@hawaii.gov')

try:
#qryStr0 = "{}={}".format(1,1)
    # break into two sections to avoid timeout errors
    oidfld = 'objectid'
    oidval = 10000
    qryStr0 = "{} <= '{}'".format(oidfld,oidval)
    resp = calctodayx(wmlnclyrsects,qryStr0,fldx,valx)
    print (" update {} current date {} ; response : {} ; calculation : {} ; Layer {} at {} ".format(qryStr0,valtstamp,resp,fsectWebMapTitle,lyrname,tbeg))
    logger.info(" update {} current date {} response : {} ; calculation for {} ; Layer {} at {} ".format(qryStr0,valtstamp, resp,fsectWebMapTitle,lyrname,tbeg))
##    qryStr0 = "{} > '{}'".format(oidfld,oidval)
##    resp = calctoday(wmlnclyrsects,qryStr0,fldx,valx)
##    print (" update {} current date {} response : {} ; calculation : {} ; Layer {}  at {} ".format(qryStr0,valtstamp,resp,fsectWebMapTitle,lyrname,tbeg))
##    logger.info(" update {} current date {} response : {} ; calculation for {} ; Layer {}  at {} ".format(qryStr0,valtstamp, resp,fsectWebMapTitle,lyrname,tbeg))
except Exception as e:
    print (" Attempted {} current date of {} Error message : {} ; date update calculation : {} {} records failed at {}".format(qryStr0,valx,str(e),fsectWebMapTitle,lyrname,tbeg))
    logger.error(" Attempted {} current date of {} Error message : {} date update calculation for {} {} records failed at {} ".format(qryStr0,valx, str(e),fsectWebMapTitle,lyrname,tbeg))


tend = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
logger.info (" End lane closure today's date update {} {} features ended at {}. ".format (fsectWebMapTitle,lyrname,tend))

print (" End lane closure today's date update {} {} features ended at {}. ".format (fsectWebMapTitle,lyrname,tend))
lcnclupdatehdlr.close()

