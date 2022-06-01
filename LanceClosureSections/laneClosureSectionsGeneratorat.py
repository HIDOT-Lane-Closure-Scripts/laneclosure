
# coding: utf-8

import arcgis
from arcgis.gis import GIS
import smtplib, ssl
import logging, re, os
#logpath = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs"
#rptpath = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\reports"
logpath = r'D:\MyFiles\HWYAP\LaneClosure\logs'
rptpath = r'D:\MyFiles\HWYAP\LaneClosure\reports'

logger = logging.getLogger('laneclosections')
logfilenm = r"laneclosections.log"
logfile = os.path.join(logpath,logfilenm) # r"conflaneclosections.log"
lnclhdlr = logging.FileHandler(logfile) # 'laneclosections.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
lnclhdlr.setFormatter(formatter)
logger.addHandler(lnclhdlr)
logger.setLevel(logging.INFO)
#username='dot_mmekuria', password='********'
# ArcGIS user credentials to authenticate against the portal
credentials = { 'userName' : 'dot_mmekuria', 'passWord' : '*********'}
userName = credentials['userName'] # arcpy.GetParameter(2) # 
passWord =  credentials['passWord'] #  # arcpy.GetParameter(3) # "*********"
#credentials = { 'userName' : arcpy.GetParameter(4), 'passWord' : arcpy.GetParameter(5)}
# Address of your ArcGIS portal
portal_url = r"http://histategis.maps.arcgis.com/"
print("Connecting to {}".format(portal_url))
logger.info("Connecting to {}".format(portal_url))
#qgis = GIS(portal_url, userName, passWord)
qgis = GIS(profile="hisagolprof")
numfs = 1000 # number of items to query
print(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")
logger.info(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")

import arcpy, shutil, sys
import xml.dom.minidom as DOM
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , timedelta, datetime
from time import sleep
import math,random
from os import listdir
from arcgis.features._data.geodataset.geodataframe import SpatialDataFrame
from cmath import isnan
from math import trunc
from _server_admin.geometry import Point
from pandas.core.computation.ops import isnumeric
# email to hidotlaneclsoures@hawaii.gov through outlook # Python 3

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


def webexsearch(mgis, title, owner_value, item_type_value, max_items_value=1000,inoutside=False):
    item_match = None
    search_result = mgis.content.search(query= "title:{} AND owner:{}".format(title,owner_value), 
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

# Given anydate and n1 as 0 or 1 or 2 , etc  it computes Last Friday, First Friday and Second Friday, etc at 4PM
def fridaywk(bdate,n1):
    wkdte = datetime.strftime(bdate,"%w") # + datetime.strftime(bdate,"%z")
    date4pm = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=16)
    fr4pm= date4pm + timedelta(days=(5-int(wkdte)+(n1-1)*7))
    return fr4pm


def intextold(intxt,rte,rtename):
    intshortlbl = intxt['address']['ShortLabel']
    intsplitxt = intshortlbl.split(sep='&', maxsplit=1)
    txtret=intsplitxt[1]  # default to the second intersection unless the second one has the route
    for txt in intsplitxt:
        if rtename not in txt or rte not in txt:
            txtret = txt
    return txtret          

def intextshortlabel(intxt,rte,rtename,fromtxt="Nothing"):
    intshortlbl = intxt['address']['ShortLabel']
    rtext = re.sub("-","",rte)
    if rtename is None:
        rtenametxt = 'Nothing'
    else:    
        rtenametxt = re.sub("-","",rtename)
    intsplitxt = intshortlbl.split(sep='&') #, maxsplit=1)
    intsplitxt = [t1.strip() for t1 in intsplitxt ]
    if len(intsplitxt)==2: 
        txtret=intsplitxt[1]  # default to the second intersection unless the second one has the route
    elif len(intsplitxt)==3:
        txtret=intsplitxt[2]  # default to the second intersection unless the second one has the route
    else:
        txtret=intsplitxt[0]  # default to the second intersection unless the second one has the route
            
    rtenmsplit = [ t2.strip() for t2 in rtenametxt.split(sep=" ")]
    if len(rtenmsplit)>2:
        rtenmsplit = "{} {}".format(rtenmsplit[0].capitalize(),rtenmsplit[1].capitalize())
    else:
        rtenmsplit = "{}".format(rtenmsplit[0].capitalize())
               
    for txt in intsplitxt:
        txtsep = txt.split(sep=" ")
        if len(txtsep)<=2 :
            if (txt[0:2]).isnumeric():
                txt = "Exit " + txt.upper()
        if rtenmsplit not in txt and rtext not in txt and fromtxt!=txt:
            txtret = txt
        else:
            txtret = txt
    return txtret          

def intext(intxt,rte,rtename,fromtxt="Nothing"):
    txtmatchaddress = intxt['address']['Match_addr']  # ['ShortLabel']
    intshortlbl = txtmatchaddress.split(sep=',')[0]
    rtext = re.sub("-","",rte)
    if rtename is None:
        rtenametxt = 'Nothing'
    else:    
        rtenametxt = re.sub("-","",rtename)
    intsplitxt = intshortlbl.split(sep='&') #, maxsplit=1)
    intsplitxt = [t1.strip() for t1 in intsplitxt ]
    if len(intsplitxt)==2: 
        txtret=intsplitxt[1]  # default to the second intersection unless the second one has the route
    elif len(intsplitxt)==3:
        txtret=intsplitxt[2]  # default to the second intersection unless the second one has the route
    else:
        txtret=intsplitxt[0]  # default to the second intersection unless the second one has the route
            
    rtenmsplit = [ t2.strip() for t2 in rtenametxt.split(sep=" ")]
    if len(rtenmsplit)>2:
        rtenmsplit = "{} {}".format(rtenmsplit[0].capitalize(),rtenmsplit[1].capitalize())
    else:
        rtenmsplit = "{}".format(rtenmsplit[0].capitalize())
               
    for txt in intsplitxt:
        txtsep = txt.split(sep=" ")
        if len(txtsep)<=2 :
            if (txt[0:2]).isnumeric():
                txt = "Exit " + txt.upper()
        if rtenmsplit not in txt and rtext not in txt and fromtxt!=txt:
            txtret = txt
        else:
            txtret = txt
    return txtret          

def datemidnight(bdate):
    date0am = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=0)
    return date0am

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
                                                                                                                                                                                                                                                                                                                                                                                                                                                        
def midnextnight(bdate,n1):
    datenextam = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(day=n1)
    return datenextam

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


def rtempt(lyrts,rtefc,lrte,bmpvalx,offs=0):
    if arcpy.Exists(mptbl):    
        if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
            arcpy.DeleteRows_management(mptbl)
    bmpval = bmpvalx
    rteFCSel = "RteSelected"
    rtevenTbl = "RteLinEvents"
    eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
    arcpy.env.overwriteOutput = True
    if (len(rtefc)>0):
        flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
        lrterows = arcpy.da.SearchCursor(lrte,flds)
        mpinscur.insertRow((rteid.upper(), bmpval,offs))
        dirnlbl = 'LEFT'
        arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
        # get the geoemtry from the result layer and append to the section feature class
        if arcpy.Exists(eveLinlyr):    
            cntLyr = arcpy.GetCount_management(eveLinlyr)
        if cntLyr.outputCount > 0:
            #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
            insecgeo = None
            # dynamic segementaiton result layer fields used to create the closure segment  
            lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'Shape@JSON']
            evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
            for srow in evelincur:
                #insecgeo = srow.getValue("SHAPE@")
                #print("id : {} , Rte : {} , BMP {} , EMP : {} , Geom : {} ".format(srow[0],srow[1],srow[2],srow[3],srow[4]))
                rtenum = srow[1]
                insecgeo = arcgis.geometry.Geometry(srow[4])
                if insecgeo == None:
                    print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                    logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                    insecgeo = geomrte.project_as(sprefwgs84)
                else:
                    print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                    logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                insecgeo = insecgeo.project_as(sprefwgs84)
            del evelincur        
        del rteFCSel,lrte,rtevenTbl  
    else:
        rteidx = "460"  # Molokaii route 0 to 15.55 mileage
        print('Route {} not found using {} create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,bmpval,empval,offs ))
        logger.info('Route {} not found using {} to create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,bmpval,empval,offs ))
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteidx,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
        ftlnclrte = featlnclrte.features
        if (len(ftlnclrte)>0):
            rtegeo = ftlnclrte[0].geometry
            geomrte = arcgis.geometry.Geometry(rtegeo)
            insecgeo = geomrte.project_as(sprefwgs84)
        else:
            insecgeo=None    
    return insecgeo



def rtesectmpo(lyrts,rteid,bmpvalx,empvalx,offs):
    if arcpy.Exists(mptbl):    
        if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
            arcpy.DeleteRows_management(mptbl)
    bmpval = bmpvalx
    empval = empvalx
    rteFCSel = "RteSelected"
    rtevenTbl = "RteLinEvents"
    eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
    arcpy.env.overwriteOutput = True
    featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)<=0):
        if rteid == "5600":
            rteid="560"
        else:
            rteid="560"    
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)>0):
        rteFCSel = featlnclrte.save('in_memory','rtesel')
        ftlnclrte = featlnclrte.features
        rtegeo = ftlnclrte[0].geometry
        geomrte = arcgis.geometry.Geometry(rtegeo,sr=sprefwebaux)
        rtepaths = rtegeo['paths']
        rtept1 = rtepaths[0][0] # geomrte.first_point
        rtept2 = rtepaths[0][len(rtepaths[0])-1] #geomrte.last_point
        bmprte = round(rtept1[2],3)
        emprte = round(rtept2[2],3)
        if (empval<bmpval):
            inpval = empval
            empval=bmpval
            bmpval = inpval
        elif (round(empval,3)==0 and bmpval<=0):
            empval=bmpval + 0.01
                
        if (bmpval<bmprte):
            bmpval=bmprte
        if (bmpval>emprte):
            bmpval=bmprte
        if (empval>emprte):
            empval=emprte
    
        #rteFCSel = featlnclrte.save(lcfgdboutpath,'rtesel')
        arcpy.env.outputMFlag = "Disabled"
        lrte = os.path.join('in_memory','rteselyr')
        arcpy.CreateRoutes_lr(rteFCSel,RteFld.name, lrte, "TWO_FIELDS", bmpFld.name, empFld.name)
        flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
        lrterows = arcpy.da.SearchCursor(lrte,flds)
        
        if (abs(empval-bmpval)<0.01):
            bmpval=max(bmpval,empval)-0.005
            empval=bmpval+0.01
        mpinscur.insertRow((rteid.upper(), bmpval,empval,offs))
        dirnlbl = 'LEFT'
        arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
        # get the geoemtry from the result layer and append to the section feature class
        if arcpy.Exists(eveLinlyr):    
            cntLyr = arcpy.GetCount_management(eveLinlyr)
        if cntLyr.outputCount > 0:
            #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
            insecgeo = None
            # dynamic segementaiton result layer fields used to create the closure segment  
            lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'EMP', 'Shape@JSON']
            evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
            for srow in evelincur:
                #insecgeo = srow.getValue("SHAPE@")
                #print("id : {} , Rte : {} , BMP {} , EMP : {} , Geom : {} ".format(srow[0],srow[1],srow[2],srow[3],srow[4]))
                rtenum = srow[1]
                insecgeo = arcgis.geometry.Geometry(srow[4])
                if insecgeo == None:
                    print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                    logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                    insecgeo = geomrte.project_as(sprefwgs84)
                else:
                    print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                    logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                insecgeo = insecgeo.project_as(sprefwgs84)
            del evelincur        
        del rteFCSel,lrte,rtevenTbl  
    else:
        rteidx = "460"  # Molokaii route 0 to 15.55 mileage
        print('Route {} not found using {} create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
        logger.info('Route {} not found using {} to create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteidx,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
        ftlnclrte = featlnclrte.features
        if (len(ftlnclrte)>0):
            rtegeo = ftlnclrte[0].geometry
            geomrte = arcgis.geometry.Geometry(rtegeo)
            insecgeo = geomrte.project_as(sprefwgs84)
        else:
            insecgeo=None    
    return insecgeo



def three2twod(shp):
    if shp is not None:
        mgeom = shp['paths']
        glen = len(mgeom) # rtepaths[0][len(rtepaths[0])-1] 
        smupltxt = ""
        if glen>0:
            smupline = []
            for il,linex in enumerate(mgeom,0):
                for xy in linex:
                    xylist = []
                    for i,x in enumerate(xy,1):
                        if i==1:
                            xylist.append(x)
                        elif i==2:
                            xylist.append(x)
                    smupline.append(xylist)
        smuplinef = [smupline]
    else:
        smuplinef = []
    return smuplinef

def rtesectmp(lyrts,rteid,bmpvalx,empvalx,offs):
    if arcpy.Exists(mptbl):    
        if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
            arcpy.DeleteRows_management(mptbl)
    bmpval = bmpvalx
    empval = empvalx
    rteFCSel = "RteSelected"
    rtevenTbl = "RteLinEvents"
    eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
    arcpy.env.overwriteOutput = True
    rtegeo = None
    lyrname = lyrts.properties.name
    featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)<=0):
        if rteid == "5600":
            rteid="560"
        else:
            rteid="560"    
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)>0):
        rteFCSel = featlnclrte.save('in_memory','rtesel')
        ftlnclrte = featlnclrte.features
        rtegeo = ftlnclrte[0].geometry
        geomrte = deepcopy(rtegeo) # arcgis.geometry.Geometry(rtegeo,sr=sprefwebaux)
        rtepaths = rtegeo['paths']
        rtept1 = rtepaths[0][0] # geomrte.first_point
        rtept2 = rtepaths[0][len(rtepaths[0])-1] #geomrte.last_point
        bmprte = rtept1[2]
        emprte = rtept2[2]
        if bmprte==None and emprte==None:
            insecgeo = three2twod(rtegeo)
            #insecgeo = Geometry({ "paths" : insecgeo , "spatialReference" : sprefwebaux })
            insecgeo =arcgis.geometry.Geometry({ "paths" : insecgeo , "spatialReference" : sprefwebaux }) #(insecgeo,sr=sprefwebaux)
            lengeo = insecgeo.length
            insecgeo = insecgeo.project_as(sprefwgs84 ) #sprefwebaux) #insecgeo.project_as(sprefwgs84)
            print('Blank value found for  Route {} ; Rte pre-geometry {} ; projected {} ; new {}  ; length : {} ; layer {} , rte {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format
                  (rteid,rtegeo,geomrte,insecgeo,lengeo,lyrname,rteid,bmpvalx,empvalx,bmprte,emprte,offs ))
            logger.error('Blank value found for Route {} ; Rte pre-geometry {} ; projected {} ; new {}  ; length : {} ; layer {} , rte {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format
                  (rteid,rtegeo,geomrte,insecgeo,lengeo,lyrname,rteid,bmpvalx,empvalx,bmprte,emprte,offs ))
        else:
            if bmprte==None:
                bmprte = emprte - 0.01
            if emprte==None:
                emprte = bmprte + 0.01
                
            bmprte = round(bmprte,3)
            emprte = round(emprte,3)
            if (empval<bmpval):
                inpval = empval
                empval=bmpval
                bmpval = inpval
            elif (round(empval,3)==0 and bmpval<=0):
                empval=bmpval + 0.01
                    
            if (bmpval<bmprte):
                bmpval=bmprte
            if (bmpval>emprte):
                bmpval=bmprte
            if (empval>emprte):
                empval=emprte
    
            #rteFCSel = featlnclrte.save(lcfgdboutpath,'rtesel')
            arcpy.env.outputMFlag = "Disabled"
            lrte = os.path.join('in_memory','rteselyr')
            arcpy.CreateRoutes_lr(rteFCSel,RteFld.name, lrte, "TWO_FIELDS", bmpFld.name, empFld.name)
            flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
            lrterows = arcpy.da.SearchCursor(lrte,flds)
            
            if (abs(empval-bmpval)<0.01):
                bmpval=max(bmpval,empval)-0.005
                empval=bmpval+0.01
            mpinscur.insertRow((rteid.upper(), bmpval,empval,offs))
            dirnlbl = 'LEFT'
            arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
            # get the geoemtry from the result layer and append to the section feature class
            if arcpy.Exists(eveLinlyr):    
                cntLyr = arcpy.GetCount_management(eveLinlyr)
            if cntLyr.outputCount > 0:
                #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
                insecgeo = None
                # dynamic segementaiton result layer fields used to create the closure segment  
                lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'EMP', 'Shape@JSON']
                evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
                for srow in evelincur:
                    #insecgeo = srow.getValue("SHAPE@")
                    #print("id : {} , Rte : {} , BMP {} , EMP : {} , Geom : {} ".format(srow[0],srow[1],srow[2],srow[3],srow[4]))
                    rtenum = srow[1]
                    insecgeo = arcgis.geometry.Geometry(srow[4])
                    if insecgeo == None:
                        print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrname,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                        logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrname,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                        insecgeo = geomrte.project_as(sprefwgs84)
                    else:
                        #print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrname,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                        #logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrname,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
                        insecgeo = insecgeo.project_as(sprefwgs84)
                del evelincur,lrte        
        del rteFCSel,rtevenTbl  
    else:
        rteidx = "460"  # Molokaii route 0 to 15.55 mileage
        print('Route {} not found using {} to create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrname,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
        logger.info('Route {} not found using {} to create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrname,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteidx,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
        ftlnclrte = featlnclrte.features
        if (len(ftlnclrte)>0):
            rtegeo = ftlnclrte[0].geometry
            geomrte = arcgis.geometry.Geometry(rtegeo)
            insecgeo = geomrte.project_as(sprefwgs84)
        else:
            insecgeo=rtegeo    
    print('Layer {} ; Route {} created section geometry {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrname,rteid,insecgeo,bmpvalx,empvalx,bmpval,empval,offs ))
    return insecgeo



def mergeometry(geomfeat):
    mgeom = geomfeat.geometry
    if len(geomfeat)>0:
        rtegeo = geomfeat.geometry
        geomrte = arcgis.geometry.Geometry(rtegeo,sr=sprefwebaux)
        rtepaths = rtegeo['paths']
        rtept1 = rtepaths[0][0] # geomrte.first_point
        glen = len(rtepaths) # rtepaths[0][len(rtepaths[0])-1] 
        rtept2 = rtepaths[glen-1][len(rtepaths[glen-1])-1] #geomrte.last_point
        mgeom =[ [ x for sublist in rtepaths for x in sublist] ]
    return mgeom

def assyncadds(lyr1,fset):
    sucs1=0
    pres = None
    t1 = 0
    while(sucs1<=0 and t1<10):
        pres = lyr1.edit_features(adds=fset) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
        if pres['addResults'][0]['success']==True:
            sucs1=1
        else:
            t1 += 1
            sleep(7)
    return pres

def assyncaddspt(lyr1,fset):
    sucs1=0
    pres = None
    t1 = 0
    while(sucs1<=0 and t1<5):
        pres = lyr1.edit_features(adds=fset) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
        if pres['addResults'][0]['success']==True:
            sucs1=1
        else:
            t1 += 1
            sleep(5)
    return pres


def qryhistdate(bdate,d1=0):
    dateqry = datetime.strftime((bdate-timedelta(days=d1)),"%m-%d-%Y")
    return dateqry
    
    # get the date and time

def setdates(sdfset,begdte,endte):
    sdfset['beginDate']= sdfsectapp['beginDate'].apply(lambda x : begdti) 
    sdfset['enDate'] = sdfsectapp['enDate'].apply(lambda x : endti) 
    sdfset['begDyWk'] = sdfsectapp['beginDate'].apply(lambda x :  datetime.strftime(x,'%A')) 
    sdfset['enDyWk'] = sdfsectapp['enDate'].apply(lambda x :  datetime.strftime(x,'%A'))
    # sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()
    return sdfset
        


curDate = datetime.strftime(datetime.today(),"%Y%m%d%H%M%S") 
# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())

x = random.randrange(0,1000,1)               
sprefwgs84 = {'wkid' : 4326 , 'latestWkid' : 4326 }
sprefwebaux = {'wkid' : 102100 , 'latestWkid' : 3857 }


### Start setting variables for local operation
#outdir = r"D:\MyFiles\HWYAP\laneclosure\Sections"
#lcoutputdir =  r"C:\\users\\mmekuria\\ArcGIS\\LCForApproval"
#lcfgdboutput = "LaneClosureForApproval.gdb" #  "Lane_Closure_Feature_WebMap.gdb" #
#lcfgdbscratch =  "LaneClosureScratch.gdb"
# output file geo db 
#lcfgdboutpath = "{}\\{}".format(lcoutputdir, lcfgdboutput)


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

midptfstitle = 'Lane Closure Mid Point Features'
midptlyrnm = 'Lane Closure Mid Point Features'

hirtsTitle = 'HIDOTRoutes' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
itypelrts="Feature Service" # "Feature Layer" # "Service Definition"
wmlrtsnm = 'HIDOTRoutes'
rteFCSelNm = 'rtesel'
servicename =  lnclSrcFSTitle # "Lane_Closure_WebMap" # "HI DOT Daily Lane Closures Sample New" # arcpy.GetParameter(1) # 
tempPath = sys.path[0]
arcpy.env.overwriteOutput = True

logger.info("\n{} Lane Closure Sections Geometry Processing begins at {} ".format(lnclSrcFSTitle,tbeg))                                                                                                                                                                                                                                                                                                                                                                                   
print("\n{} Lane Closure Sections Geometry Processing begins at {} ".format(lnclSrcFSTitle,tbeg))                                                                                                                                                                                                                                                                                                                                                                                   

#print("Temp path : {}".format(tempPath))
# ArcGIS user credentials to authenticate against the portal
credentials = { 'userName' : 'dot_mmekuria', 'passWord' : '********'}
userName = credentials['userName'] # arcpy.GetParameter(2) # 
passWord = credentials['passWord'] # "ChrisMaz" #  # arcpy.GetParameter(3) # "ChrisMaz"
#credentials = { 'userName' : arcpy.GetParameter(4), 'passWord' : arcpy.GetParameter(5)}
# Address of your ArcGIS portal
#portal_url = r"http://histategis.maps.arcgis.com/"
#print("Connecting to {}".format(portal_url))
#qgis = GIS(portal_url, userName, passWord)
#qgis = GIS(profile="hisagolprof")
numfs = 1000 # number of items to query
#    sdItem = qgis.content.get(lcwebmapid)
ekOrg = False
# search for lane closure source data
print("Searching for lane closure source {} from {} item for Service Title {} on AGOL...".format(itypelnclsrc,portal_url,lnclSrcFSTitle))
fslnclsrc = webexsearch(qgis, lnclSrcFSTitle, userName, itypelnclsrc,numfs,ekOrg)
#qgis.content.search(query="title:{} AND owner:{}".format(lnclSrcFSTitle, userName), item_type=itypelnclsrc,outside_org=False,max_items=numfs) #[0]
#print (" Content search result : {} ; ".format(fslnclsrc))
print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fslnclsrc.url,fslnclsrc.title,fslnclsrc.id))
lnclyrsrc = fslnclsrc.layers

# header layer
lnclhdrlyr = lyrsearch(lnclyrsrc, lnclhdrnm)
hdrsdf = lnclhdrlyr.query(as_df=True)
lnclhdrfldsall = [fd.name for fd in lnclhdrlyr.properties.fields]

# child layer
lnclchdlyr = lyrsearch(lnclyrsrc, lnclchdnm)
lnclchdrfldsall = [fd.name for fd in lnclchdlyr.properties.fields]
# relationship between header and child layer
#relncldesc = arcpy.Describe(lnclhdrlyr)
#relncls = relncldesc.relationshipClassNames

#for rc in relncls:
#    print (" relationshp class : {} has {}  ; Title : {} ; Id : {} ".format(fs.url,fs.title,fs.id))

route_service_url = qgis.properties.helperServices.route.url
route_service = arcgis.network.RouteLayer(route_service_url, gis=qgis)
route_layer = arcgis.network.RouteLayer(route_service_url, gis=qgis)

# query date for historical information
N1 = 7  # query data created in the last x days
qrydate = qryhistdate(datetime.today(),N1)
N2=14
#'msgbox tdate
datesectqry = "CreationDate>= '{}'".format(qrydate)
dateditqry = "EditDate>= '{}'".format(qryhistdate(datetime.today(),N2))
# search for lane closure sections 
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))

#fsect = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
fsectwebmap = webexsearch(qgis, fsectWebMapTitle, userName, itypeFS,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))
if len(fsectwebmap) > 0:
    print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fsectwebmap.url,fsectwebmap.title,fsectwebmap.id))
    logger.info (" Feature {} ; Id : {} ; URL : {} ".format(fsectwebmap.title,fsectwebmap.id,fsectwebmap.url))
    wmsectlyrs = fsectwebmap.layers
    wmsectlyrpts = lyrsearch(wmsectlyrs, wmlnclyrptsnm)
    wmlnclyrsects = lyrsearch(wmsectlyrs, wmlnclyrsectsnm)
    #fsect = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
    
    sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
    # get sdf to be used for new section data insert operations

    sectqry = wmlnclyrsects.query(where=datesectqry ) #"beginDate>='{}'".format(qrydate))
    if len(sectqry)>0:
        sectfset = sectqry.features
        sectsdf = sectqry.sdf
        sectpgid = "('" + "','".join(str(lfs.attributes['parentglobalid']) for lfs in sectfset)  + "')"
    else:
        print (" {}  with title {} created by : {} has no features in {} .\n continue session".format(itypeFS,fsectWebMapTitle, userName,wmlnclyrsectsnm))
        logger.error(" {}  with title {} created by : {} has no features in {} .\n continue session".format(itypeFS,fsectWebMapTitle, userName,wmlnclyrsectsnm))
#        sys.exit(1)
    # delete all sections without a route number
    norteqry = wmlnclyrsects.query(where="Route is null")
    # prepare the object Id's with no route numbers (submitted without update privileges
    if len(norteqry)>0:
        norteid = "OBJECTID in ('" + "','".join(str(sfs.attributes['OBJECTID']) for sfs in norteqry )  + "')"
        resultdel = wmlnclyrsects.delete_features(where=norteid)
    N1 = 14  # query data created in the last x days
    qrydate = qryhistdate(datetime.today(),N1)

    ptqry = wmsectlyrpts.query(where=datesectqry ) #"beginDate>='{}'".format(qrydate))
    ptsdf = wmsectlyrpts.query(as_df=True)
    ptsfldsall = []
    if len(ptqry)>0:
        ptsfset = ptqry.features
        ptsfldsall = [fd.name for fd in wmsectlyrpts.properties.fields]
        ptsdf = ptqry.sdf
        ptcols = ptsdf.columns
        sdfptsapp = deepcopy(ptsdf.head(1))
        # create an empty dataset with all the field characteristics
        #ptslnclsdf = deepcopy(ptsdf.head(0)) #df_colsame(sdfptsapp)


    
    flmidpts = webexsearch(qgis, midptfstitle, userName, itypeFS,numfs,ekOrg)
    if len(flmidpts)>0:
        print (" Feature {} ; Id : {} ; URL : {} ".format(flmidpts.title,flmidpts.id,flmidpts.url))
        logger.info (" Feature {} ; Id : {} ; URL : {} ".format(flmidpts.title,flmidpts.id,flmidpts.url))
        midptlyrs = flmidpts.layers
        midptlyr = lyrsearch(midptlyrs, midptlyrnm)
        midptqry = midptlyr.query(where=datesectqry ) #"beginDate>='{}'".format(qrydate))
        if len(midptqry)>0:
            midptsfset = ptqry.features
            midptsfldsall = [fd.name for fd in midptlyr.properties.fields]
            midptsdf = midptqry.sdf
            midptcols = midptsdf.columns
            midptsdfapp = deepcopy(midptsdf.head(1))
            # create an empty dataset with all the field characteristics
            #midptslnclsdf = deepcopy(midptsdf.head(0)) #df_colsame(sdfptsapp)


else:
    print (" {}  with title {} created by : {} is not accessible.\n end session".format(itypeFS,fsectWebMapTitle, userName))
    logger.error(" {}  with title {} created by : {} is not accessible.\n end session".format(itypeFS,fsectWebMapTitle, userName))
    sys.exit(1)
    
    #sectsdf = wmlnclyrsects.query(as_df=True)
    # get sdf to be used for new point data insert operations
# search for Route dataset  
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypelrts,portal_url,userName,hirtsTitle))

numfs = 4000 # number of items
fswbrts = webexsearch(qgis, hirtsTitle, userName, itypelrts,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))
if len(fswbrts) > 0:
    
    print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fswbrts.url,fswbrts.title,fswbrts.id))
    logger.info (" Feature URL: {} ; Title : {} ; Id : {} ".format(fswbrts.url,fswbrts.title,fswbrts.id))
    wmrtslyrs = fswbrts.layers
    wmlyrts = lyrsearch(wmrtslyrs, wmlrtsnm)

else:
    print (" {}  with title {} created by : {} is not accessible.\n end session".format(itypeFS,fsectWebMapTitle, userName))
    logger.error(" {}  with title {} created by : {} is not accessible.\n end session ".format(itypelrts,hirtsTitle, userName))
    sys.exit(1)

# search radius
searchRadius = 1000
intersearch = 2500
minclength = 0.01
# layer names for the linear reference results
rteFCSel = "RteSelected"
rtevenTbl = "RteLinEvents"
eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
eveLRSFC = "RteLinEvtFC"
outFeatseed = "EvTbl"
lrsGeoPTbl = """LRS_{}{}""".format(outFeatseed,x) # DynaSeg result feature table created from LRS points location along routes 
outfeatbl = """Rt{}""".format(outFeatseed) 
# linear reference link properties 
eveProPts = "Route POINT BMP EMP"
eveProLines = "Route LINE BMP EMP"

# linear reference fields 
OidFld = fldvartxt("ObjectID","LONG",False,28,"","","OID",True) 
# create the bmp and direction field for the merged result table 
RteFld = fldvartxt("Route","TEXT",False,"","",60,"ROUTE",True) 
fldrte = RteFld.name
# create the bmp and direction field for the merged result table 
bmpFld = fldvartxt("BMP","DOUBLE",False,18,11,"","BMP",True) 

# create the emp and direction field for the result table 
#empFld = arcpy.Field()
empFld = fldvartxt("EMP","DOUBLE",True,18,11,"","EMP",False) 
ofFld = fldvartxt("Offset","DOUBLE",True,18,11,"","Offset",False) 


mptbl = str(arcpy.CreateTable_management("in_memory","{}{}".format(rtevenTbl,x)).getOutput(0))


# add BMP , EMP and RteDirn fields to the linear reference lane closure table
#arcpy.AddField_management(mptbl, OidFld.name, OidFld.type, OidFld.precision, OidFld.scale)
arcpy.AddField_management(mptbl, RteFld.name, RteFld.type, RteFld.precision, RteFld.scale)
arcpy.AddField_management(mptbl, bmpFld.name, bmpFld.type, bmpFld.precision, bmpFld.scale)
arcpy.AddField_management(mptbl, empFld.name, empFld.type, empFld.precision, empFld.scale)
arcpy.AddField_management(mptbl, ofFld.name, ofFld.type, ofFld.precision, ofFld.scale)


# create the milepost insert cursor fields  
mpflds = [RteFld.name,bmpFld.name,empFld.name,ofFld.name]

# query the lane closure survey entries that have been updated by users and delete them from processed feature class
#print (" Section parent global id list : {} ".format(featlisthdrgid))
lnclupdatelistqry = "EditDate > CreationDate and {}".format(datesectqry)  # and EditDate>= '{}'".format((datetime.today()-timedelta(days=1)).strftime("%m/%d/%y"))
# lnclupdatelistqry = "EditDate > CreateDate and EditDate>= '{}'".format((datetime.today()-timedelta(days=1)).strftime("%m/%d/%y"))
# lane closure header features without generated sections 
lnclupdatefeats =  lnclhdrlyr.query(where=lnclupdatelistqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
if len(lnclupdatefeats)>0:
    # get location detail related survey repeat records for the parent record object id    
    featuplnclsdf = lnclupdatefeats.sdf #[l1w for l1 in featsectgid  ]
    featuplnclsdf['EdiTime'] = pd.to_datetime(featuplnclsdf.EditDate)
    lfeats = lnclupdatefeats.features
    #oidqry = "objectId in ('" + "','".join(str(lfs.attributes['objectid']) for lfs in lfeats if 'objectid' in lfs.attributes and isinstance(lfeats,(list,tuple)) ) + "')"
    # prepare the global Id's that have been updated using the comparison creation and update dates 
    pgidqry = "parentglobalid in ('" + "','".join(str(lfs.attributes['globalid']) for lfs in lfeats if 'globalid' in lfs.attributes and isinstance(lfeats,(list,tuple)) ) + "')"
    # query the section features from the section layer the records that need to be removed
    featsectschg = wmlnclyrsects.query(where=pgidqry,out_fields=sectfldsall) #.sdf # + ,out_fields="globalid")
    xsectchgsdf = featsectschg.sdf
    xftsectschg = featsectschg.features
    scols = xsectchgsdf.columns
    xsectchgsdf['EdiTime'] = pd.to_datetime(xsectchgsdf.CreationDate_1)
    # create the MilePost Insert cursor 
    for srow in xsectchgsdf.itertuples():
        xsedte = srow.EdiTime # srow['EditDate']
        xsedts = datetime.timestamp(xsedte) # + timedelta(int(utcofs)))
        xsparguid = srow.parentglobalid # srow['parentglobalid']
        xsoid =  srow.OBJECTID # srow['OBJECTID']
        xlfeatsdf = featuplnclsdf[featuplnclsdf['globalid'].isin([xsparguid])]
        xlrte = featuplnclsdf[featuplnclsdf['Route'].isin(['30'])]
        lcols = xlfeatsdf.columns
        rte = srow.Route
        
        #if ( xlrte['Route'] == srow.Route ):
        #    print(' Deleted section {} ; Route : {}; pgid : {} ;  Creator : {} ; Created : {} ; edited sect : {} ; \n '.format(
        #            srow.OBJECTID,srow.Route,srow.parentglobalid , 
        #            srow.Creator, srow.CreationDate,srow.EdiTime))
        #print(' Sect OID {} ; pgid : {} ; Creator : {} ; EditSectime : {} ;  Rte : {}  '.format(srow.OBJECTID,srow.parentglobalid, srow.Creator, srow.EditDate, srow.Route))
        for lrow in xlfeatsdf.itertuples():
            xledte =  lrow.EdiTime #lrow['EditDate']
            utcofs = lrow.utcoff
            if utcofs == None:
                utcofs = -10 
            if isnan(utcofs):
                utcofs = -10 
            xledts = datetime.timestamp(xledte) # - timedelta(hours=int(utcofs)))
            #xledts = datetime.timestamp(xledte)
            xlguid = lrow.globalid # getattr(lrow,'globalid') # lrow['globalid']
            xloid =  lrow.objectid #lrow['objectid']
            #print(' OID Sect {} ; pgid : {} ;  LnClgid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(srow.OBJECTID,srow.parentglobalid, lrow.globalid, srow.Creator, srow.EditDate, lrow.EditDate,lrow.Route))
            #print(' GID Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
            if round(xsedts,2) < round(xledts,2):   
                oidsectschg = "OBJECTID in ('" + str(xsoid) + "') and DIRPInfo not like 'Yes'"
                resultqry = wmlnclyrsects.query(where=oidsectschg)
                if len(resultqry) > 0:
                    sectdelfeats = resultqry.features
                    pgidelsectqry = "parentglobalid in ('" + "','".join(str(lfs.attributes['globalid']) for lfs in sectdelfeats if 'globalid' in lfs.attributes and isinstance(sectdelfeats,(list,tuple)) ) + "')"
                    resultdel = wmlnclyrsects.delete_features(where=oidsectschg)
                    if len(resultdel['deleteResults'])>0:
                        print(' Deleted section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited sect : {} ; edited Survey : {} ; deleted : {} \n '.format(
                            srow.OBJECTID,lrow.Route,srow.parentglobalid , lrow.globalid,
                             srow.Creator, srow.CreationDate,srow.EdiTime, lrow.EdiTime,resultdel))
                        logger.info(' Deleted section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited sect : {} ; edited Survey : {} ; deleted : {} \n '.format(
                                srow.OBJECTID,lrow.Route,srow.parentglobalid , lrow.globalid,
                                 srow.Creator, srow.CreationDate,srow.EdiTime, lrow.EdiTime,resultdel))
    
                        ##for xfs in xftsectschg:
                        ##    for lfs in lfeats:
                        ##        #print(' Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
                        ##        if xfs.attributes['parentglobalid'] == lfs.attributes['globalid'] and xfs.attributes['EditDate']< lfs.attributes['EditDate']:
                        ##            print(' Matching Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
                        
                        
                        # query the point features, from the beg and end point feature layer, the records that need to be removed
                        featptschg = wmsectlyrpts.query(where=pgidelsectqry,out_fields=ptsfldsall) #wmsectlyrpts.query(where=pgidqry,out_fields=ptsfldsall)
                        xftptschg = featptschg.features 
                        ##if len(xftptschg) > 0:
                        ##    oidptschg = "OBJECTID in ('" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xftptschg if 'OBJECTID' in xfs.attributes and isinstance(xftptschg,(list,tuple)) ) + "')"
                        ##    wmsectlyrpts.delete_features(where=oidptschg)
                        for xfs in xftptschg:
                            for lfs in lfeats:
                                if xfs.attributes['parentglobalid'] == lfs.attributes['globalid'] and xfs.attributes['EditDate']< lfs.attributes['EditDate']:
                                    oidptschg = "OBJECTID in ('" + str(xfs.attributes['OBJECTID']) + "')"
                                    #oidptschg = "OBJECTID in ('" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xftsectschg if 'OBJECTID' in xfs.attributes and isinstance(xftsectschg,(list,tuple)) ) + "')"
                                    resultqry = wmsectlyrpts.query(where=oidptschg)
                                    if len(resultqry) > 0:
                                        resultdel = wmsectlyrpts.delete_features(where=oidptschg)
                                        if len(resultdel['deleteResults'])>0:
                                            print('Deleted Beg & End Points {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; deleted : {} '.format(
                                                oidptschg,datetime.fromtimestamp(lfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),
                                                datetime.fromtimestamp(xfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),xfs.attributes['Route'], 
                                                xfs.attributes['BMP'], xfs.attributes['EMP'], xfs.attributes['RteDirn'], xfs.attributes['ClosureSide'],resultdel ))
                                            logger.info('Deleted Beg & End Points {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; deleted : {} '.format(
                                                oidptschg,datetime.fromtimestamp(lfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),
                                                datetime.fromtimestamp(xfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),xfs.attributes['Route'], 
                                                xfs.attributes['BMP'], xfs.attributes['EMP'], xfs.attributes['RteDirn'], xfs.attributes['ClosureSide'],resultdel ))
                        
                            #datetime.fromtimestamp(enDte/1e3).strftime("%m-%d-%y")
                        
                        
                        # query the mid point features, from the beg and end point feature layer, the records that need to be removed
                        ftmidptschg = midptlyr.query(where=pgidelsectqry,out_fields=('*'))
                        xftmidptschg = ftmidptschg.features 
                        ##if len(xftptschg) > 0:
                        ##    oidptschg = "OBJECTID in ('" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xftptschg if 'OBJECTID' in xfs.attributes and isinstance(xftptschg,(list,tuple)) ) + "')"
                        ##    wmsectlyrpts.delete_features(where=oidptschg)
                        for xfs in xftmidptschg:
                            for lfs in lfeats:
                                if xfs.attributes['parentglobalid'] == lfs.attributes['globalid'] and xfs.attributes['EditDate']< lfs.attributes['EditDate']:
                                    oidptschg = "OBJECTID in ('" + str(xfs.attributes['OBJECTID']) + "')"
                                    #oidptschg = "OBJECTID in ('" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xftsectschg if 'OBJECTID' in xfs.attributes and isinstance(xftsectschg,(list,tuple)) ) + "')"
                                    resultqry = midptlyr.query(where=oidptschg)
                                    if len(resultqry) > 0:
                                        resultdel = midptlyr.delete_features(where=oidptschg)
                                        if len(resultdel['deleteResults'])>0:
                                            print('Deleted Mid Point {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; deleted : {} '.format(
                                                oidptschg,datetime.fromtimestamp(lfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),
                                                datetime.fromtimestamp(xfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),xfs.attributes['Route'], 
                                                xfs.attributes['BMP'], xfs.attributes['EMP'], xfs.attributes['RteDirn'], xfs.attributes['ClosureSide'],resultdel ))
                                            logger.info('Deleted Mid Point {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; deleted : {} '.format(
                                                oidptschg,datetime.fromtimestamp(lfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),
                                                datetime.fromtimestamp(xfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),xfs.attributes['Route'], 
                                                xfs.attributes['BMP'], xfs.attributes['EMP'], xfs.attributes['RteDirn'], xfs.attributes['ClosureSide'],resultdel ))
                        
# update the lane closure feature creation date to match the edit date so they will be the same
lnclupdatefeats =  lnclhdrlyr.query(where="{}".format(datesectqry),out_fields=('*'))
#flnclcredit = ["OID : {} Creator : {}  date : {}  Editor : {}  date : {} , "
#.format(fd.attributes['objectid'],fd.attributes['Creator'],fd.attributes['CreationDate'],fd.attributes['Editor'],
#fd.attributes['EditDate']) for fd in lnclupdatefeats]
mpdata = []
ptcoords = []
lnclchgisdf = lnclupdatefeats.sdf
lnclchgisdf.set_index('objectid',inplace=True)

#for feat in lnclupdatefeats:
#    feat.set_value ('CreationDate',feat.get_value('EditDate'))
#lnclchgfeats = arcgis.features.FeatureSet.from_dataframe(lnclchgisdf)

#flnclcredit = ["OID : {} Creator : {}  date : {}  Editor : {}  date : {} , ".format(fd.attributes['objectid'],fd.attributes['Creator'],fd.attributes['CreationDate'],fd.attributes['Editor'],fd.attributes['EditDate']) for fd in lnclchgfeats]

#lnclchgisdf.loc[lnclchgisdf['CreationDate']<lnclchgisdf['EditDate'],'CreationDate'] = lnclchgisdf.loc[0,'EditDate']

#updict = lnclhdrlyr.edit_features(None,lnclchgfeats)

# First get the id's of the sections already processed
#feat_set = wblnclyrsects.query(where="Route is not null").sdf # + ,out_fields="globalid") #
sectflds = ['parentglobalid','route','BMP','EMP']
#qrydate = curdate+timedelta(hours=int(datetime.strftime(datetime.today(),"%H")))
#qrydate = qryhistdate(datetime.today(),15)
featsects = wmlnclyrsects.query(where="{}".format(datesectqry)) #"'3-1-2000',out_fields=sectflds) #.sdf # + ,out_fields="globalid")
featsectdf = deepcopy(featsects.sdf) #[l1 for l1 in featsectgid  ]
#print (" section list : {} ".format(featsectlist))
#featlisthdrgid1 = "','".join(x[1:(len(x)-1)] for x in featsectdf.parentglobalid if ('{' and '}') in x )
featlisthdrgid2 = "','".join(x.upper() for x in featsectdf.parentglobalid if x.upper() != 'DIRP') #if ('{' and '}') not in x )
featlisthdrgid = "'" + featlisthdrgid2 + "'" # + "','" + featlisthdrgid2 
#print (" Section parent global id list : {} ".format(featlisthdrgid))
featlisthdrgidqry = " globalid not in ({}) and {}".format(featlisthdrgid,datesectqry)
# lane closure header features without generated sections 
lnclhdrnewfeats =  lnclhdrlyr.query(where=featlisthdrgidqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
if len(lnclhdrnewfeats)>0:
    # get location detail related survey repeat records for the parent record object id    
    lnclhdroids = "','".join(str(x) for x in hdrsdf.objectid )
    hdridsqry = "{}{}{} ".format("\'",lnclhdroids,"\'")
    print(" Layer {} has {} features to be processed . \n ".format(lnclhdrnm, len(lnclhdrnewfeats)))
    logger.info(" Layer {} has {} features to be processed . \n ".format(lnclhdrnm, len(lnclhdrnewfeats)))
    #repts = lnclhdrlyr.query_related_records(hdridsqry,"1",return_geometry=True)
    #reptdata = repts['relatedRecordGroups']
    #["{}".format(x) for x in [ m['relatedRecords'] for m in reptdata]]
    
#     # delete all point layer data where the parent global id's are not in the section data 
#     pgidfeatsqry = "parentglobalid not in ({}) ".format(sectpgid) #,datesectqry)
#     nptsfeats =  wmsectlyrpts.query(where=pgidfeatsqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
#     if len(nptsfeats)>0:
#         resultdel = wmsectlyrpts.delete_features(where=pgidfeatsqry)
#         print(' layer {} has {} features deleted {} . \n '.format(wmlnclyrptsnm, len(nptsfeats),resultdel))
#         logger.info(' layer {} has {} features deleted {} . \n '.format(wmlnclyrptsnm, len(nptsfeats),resultdel))
#     midptfeats =  midptlyr.query(where=pgidfeatsqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
#     if len(midptfeats)>0:
#         resultdel = midptlyr.delete_features(where=pgidfeatsqry)
#         print(' layer {} has {} features deleted {} . \n '.format(midptlyrnm, len(midptfeats),resultdel))
#         logger.info(' layer {} has {} features deleted {} . \n '.format(midptlyrnm, len(midptfeats),resultdel))
    
    # collect lane closure Rxfs.attributes['EditDate']< lfs.attributes['EditDate']:epeat Point features without generated sections 
    ptcols = ptsdf.columns
    if len(ptcols)>0:
        lnclbeptspgid = "','".join(x for x in ptsdf.parentglobalid  )
        lnclbeptspgid = "{}{}{}".format("\'",lnclbeptspgid,"\'")
        lnclbeptsgid = "','".join(x for x in ptsdf.globalid )
        lnclbeptsgid = "{}{}{}".format("\'",lnclbeptsgid,"\'")
        lnclbeptspgidqry = " parentglobalid not in ({}) ".format(lnclbeptspgid)
    else:
        lnclbeptspgid = "{}{}{}".format("\'","","\'")    
        lnclbeptspgidqry = " parentglobalid like '%'"
    #print (" Section parent global id list : {} ".format(featlisthdrgid))
    # lane closure child features without generated sections 
    lnclbepts =  lnclchdlyr.query(where=(lnclbeptspgidqry + ' and ' + dateditqry) ,out_fields=('*')) #.sdf # + ,out_fields="globalid")
    # feature class parameters  
    has_m = "DISABLED"
    has_z = "DISABLED"
    geotype = "POINT"
    spref = lnclhdrnewfeats.spatial_reference
    
    # geopoint featureclass to store the lane closure locations 
    geoPtFC = arcpy.CreateFeatureclass_management("in_memory", lrsGeoPTbl, geotype,spatial_reference=arcpy.SpatialReference(spref['wkid']))#,'',has_m, has_z,spref) #.getOutput(0))
    
    # add BMP , EMP and RteDirn fields to the linear reference lane closure table
    arcpy.AddField_management(geoPtFC, RteFld.name, RteFld.type, RteFld.precision, RteFld.scale)
    arcpy.AddField_management(geoPtFC, bmpFld.name, bmpFld.type, bmpFld.precision, bmpFld.scale)
    arcpy.AddField_management(geoPtFC, empFld.name, empFld.type, empFld.precision, empFld.scale)
    
    geoPtFCnm = "{}".format(geoPtFC)
    
    ptflds = [RteFld.name,"SHAPE@XY"]
    
    
    # get the list of parent features
    lnclhdrfsel = lnclhdrnewfeats.features
    lnclhdrgisdf = lnclhdrnewfeats.sdf
    # Header fields ['ApprLevel1', 'ApprLevel2','ApproverL1','l1email', 'ApproverL2','l2email', 'Clength', 'ClosHours', 'ClosReason','ClosType', 'CloseFact', 'ClosureSide',
    # 'CreationDate', 'Creator','DEPHWY', 'DIRPInfo', 'DistEngr', 'EditDate', 'Editor', 'Island', 'LocMode', 'NumLanes',
    #'ProjectId', 'Remarks', 'RoadName', 'Route', 'SHAPE', 'beginDate', 'enDate', 'globalid', 'objectid' ]
    
    chgflds = {"CreationDate" : "CreateDate" ,'ClosureSide' : 'ClosedSide'}
    lnclhdrgisdf.rename(columns=chgflds, inplace=True)
    #[(f.attributes['globalid'] + ' | ' + f.attributes['Route']) for f in lnclhdrnewfeats.features]
    fsnewhdrgid = [f.attributes['globalid'] for f in lnclhdrfsel]
    
    
    featlistchdgid = "','".join(x for x in fsnewhdrgid) 
    print (" new features for repeat list selection : {}{}{} ".format(" '",featlistchdgid,"' "))
    
    # setup the query to bring in the related child records
    featlistchdgidqry = " parentglobalid in ({}{}{}) ".format(" '",featlistchdgid,"' ")
    # lane closure detail features without generated sections 
    lnclchdnewfeats =  lnclchdlyr.query(where=featlistchdgidqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
    # get the list of child features
    lnclchdfsel = lnclchdnewfeats.features
    
    lnclistchdgisdf = lnclchdnewfeats.sdf
    # shorten the column names that are greater than 10 characters
    chgflds = {"parentglobalid" : "paglobalid", "CreationDate" : "CreateDate" }
    lnclistchdgisdf.rename(columns=chgflds,inplace=True)
    
    #[arcpy.AlterField_management(lnclchdnewfeats, f, chgflds[f]) for f in chgflds]
    midptcoords = [] 
    
    #lnclistchdfc = lnclchdnewfeats.save("in-memory","newchdgidmemfc",encoding="utf-8") # (could not save field names longer than 10char
    
    numlanes = 0
    fhdrteid = ''
    #fchdrmem = arcpy.CreateFeatureclass_management("in_memory", "fchdr", "Point",spatial_reference=arcpy.SpatialReference(spref['wkid']))
    # loop over the features that are not in the sections layer 
    for fhdr in lnclhdrnewfeats:
        midptcoords.clear()
        try: 
            if 'mptbl' in locals():
                if arcpy.Exists(mptbl):
                    if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                        arcpy.DeleteRows_management(mptbl) 
                mpinscur = arcpy.da.InsertCursor(mptbl, mpflds)
            if 'geoPtFC' in locals():
                if arcpy.Exists(geoPtFC):
                    if int(arcpy.GetCount_management(geoPtFC).getOutput(0)) > 0:
                        arcpy.DeleteRows_management(geoPtFC) 
                ptinscur = arcpy.da.InsertCursor(geoPtFC,ptflds)
            objid = fhdr.attributes['objectid']
            fhdrgid = fhdr.attributes['globalid']
            loctype = fhdr.attributes['LocMode']
            clostype = fhdr.attributes['ClosType']
            closedfact = fhdr.attributes['CloseFact']
            cloSide = fhdr.attributes['ClosureSide']
            closHrs = fhdr.attributes['ClosHours']
            begDte = fhdr.attributes['beginDate']
            enDte = fhdr.attributes['enDate']
            begdt = datetime.fromtimestamp(begDte/1e3)
            endt = datetime.fromtimestamp(enDte/1e3)
            begdtl = datetime.fromtimestamp(begDte/1e3) #, tzlocal.get_localzone()) erring out at tzlocal
            endtl = datetime.fromtimestamp(enDte/1e3) #, tzlocal.get_localzone()) erring out at tzlocal
            lanes = fhdr.attributes['NumLanes']
            approverl1 = fhdr.attributes['ApproverL1']
            apprlevel1 = fhdr.attributes['ApprLevel1']
            approverl2 = fhdr.attributes['ApproverL2']
            apprlevel2 = fhdr.attributes['ApprLevel2']
            dirpinfo = fhdr.attributes['DIRPInfo']
            if lanes is None:
                lanes = 1
            if closedfact == 'Shoulder':     
                lanes = 0 #fhdr.attributes['NumLanes']
            numlanes = lanes
            remarks = fhdr.attributes['Remarks']
            dirpremarks = fhdr.attributes['DirPRemarks']
            rdname = fhdr.attributes['RoadName']
            creator = fhdr.attributes['Creator']
            creatime = fhdr.attributes['CreationDate']
            createdatetime = datetime.fromtimestamp(creatime/1e3) # , tzlocal.get_localzone())
            rteid = fhdr.attributes[RteFld.name]
            fshdr = arcgis.features.FeatureSet([fhdr])
            lnclhdrgisdf = fshdr.sdf
            
            try:
                # get location detail related survey repeat records for the parent record object id    
                lnclchdrelrecs = lnclhdrlyr.query_related_records(fhdr.attributes['objectid'],"1",return_geometry=True)
                chdrecflds = lnclchdrelrecs['fields']
                sprelrecs = lnclchdrelrecs['spatialReference']
                chdrelrecgrp = lnclchdrelrecs['relatedRecordGroups']
                chdrelrecs = chdrelrecgrp[0]['relatedRecords']
                
                # query the lane closure data that does not have a section
    #            lnclchdnewfc = lnclchdlyr.query(where = " parentglobalid in ({}{}{}) ".format(" '",fhdrgid,"' "),out_fields='*') #.sdf # + ,out_fields="globalid")
                
                # add the extra columns to the header data frame
                chdf = pd.DataFrame(chdrelrecs)
                for ir,chdrec in enumerate(chdrelrecs):   # two points are given
                    # add the two set of data to the data frame
                    if ir == 0:
                        lnclhdrgisdf.at[0,'BMP'] = chdrec['attributes']['MileMarker']
                        lnclhdrgisdf.at[0,'BPoint'] = str(chdrec['geometry'])
                        #lnclhdrgisdf.at[0,'BPoint'] = chdrec['geometry']
                        pt1geom = deepcopy(chdrec['geometry'])
                        pt1geom.update({'spatialReference' : sprefwgs84 }) 
                        lnclhdrgisdf['BPoint'] = [pt1geom]
                    if ir==1:    
                        lnclhdrgisdf.at[0,'EMP'] = chdrec['attributes']['MileMarker']
                        lnclhdrgisdf.at[0,'EPoint'] = str(chdrec['geometry'])
                        #lnclhdrgisdf.at[0,'EPoint'] = chdrec['geometry']
                        pt2geom = deepcopy(chdrec['geometry'])
                        pt2geom.update({'spatialReference' : sprefwgs84 }) 
                        lnclhdrgisdf['EPoint'] = [pt2geom]
                        
                #featlnclchd = lnclchdnewfc.features
                
                #lnclchdgisdf = lnclchdnewfc.sdf
                #lnclchdgisdf.spatial.to_featureclass('in_memory/testfc',overwrite=True)
                #chgflds = {"parentglobalid" : "paglobalid", "CreationDate" : "CreateDate" }
                #lnclchdgisdf.rename(columns=chgflds,inplace=True)
                chdcols = lnclhdrgisdf.columns
                # merge the header data into the related dataset for further processing
                #lnclfpts = lnclchdgisdf.merge(lnclhdrgisdf,how='inner',left_on='paglobalid',right_on='globalid',suffixes=('_c', '_h'))
                #lnclfptscols = lnclfpts.columns
                # query the route feature for this closure
    #            if fhdrteid != fhdr.attributes[RteFld.name]:
                fhdrteid = rteid #fhdr.attributes[RteFld.name]
                featlnclrte = wmlyrts.query(where = "Route in  ({}{}{}) ".format(" '",fhdrteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
                if 'rteFCSel' in locals():
                    del rteFCSel            
                rteFCSel = featlnclrte.save('in_memory','rtefcsel{}'.format(x))
                if (len(featlnclrte)>0):
                    ftlnclrte = featlnclrte.features
                    rtegeo = ftlnclrte[0].geometry
                    rtegeo['spatialReference'] = featlnclrte.spatial_reference
                    geomrte = arcgis.geometry.Geometry(rtegeo)
                    rteleng = geomrte.get_length(method='PLANAR',units='METERS')
                    rtepaths = rtegeo['paths']
                    # eliminate the gaps merge the paths 
                    rtept1 = rtepaths[0][0] # geomrte.first_point
                    glen = len(rtepaths) # rtepaths[0][len(rtepaths[0])-1] #geomrte.last_point
                    rtept2 = rtepaths[glen-1][len(rtepaths[glen-1])-1]
                    rtegeox = [[ x for sublist in rtepaths for x in sublist]]
                    rtegeo['paths'] =  rtegeox
                    featlnclrte.features[0].geometry['paths'] = rtegeox
                    rteFCSel = featlnclrte.save('in_memory','rtefcsel{}'.format(x))
    # new addition 6/8/2020
                    if (rtept1[2] == None):
                        rtept1[2] = 0
                    if (rtept2[2] == None):    
                        rtept2[2] = round(rteleng*3.2808333/5280.0,3)
                    
                    bmprte = round(rtept1[2],3)
                    emprte = round(rtept2[2],3)
                    #rteFCSel = featlnclrte.save(lcfgdboutpath,'rtesel')
                    if 'lrte' in locals():
                        del lrte            
                    lrte = os.path.join('in_memory','rteselyr{}'.format(x))
                    arcpy.CreateRoutes_lr(rteFCSel,RteFld.name, lrte, "TWO_FIELDS", bmpFld.name, empFld.name)
                #lrterows = arcpy.da.SearchCursor(lrte, [f.name for f in arcpy.ListFields(lrte)]) # all fields in rte
                #rtelyr = "rteselyr" #os.path.join('in_memory','rteselyr')
                flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
                lrterows = arcpy.da.SearchCursor(lrte,flds)
                #[print('{}, {}, {}'.format(row[0], row[2],row[1])) for row in lrterows]
                # get each point feature geometry
                rteMP = []
                stops = ""
                #print (" a {}  ".format(" test "))
                dirn = 1
                # delete previous route and mile post record if it exists
                if arcpy.Exists(mptbl): 
                    if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                        arcpy.DeleteRows_management(mptbl)
                    
                if loctype == 'MilePost':
                    if 'BMP' in lnclhdrgisdf.columns:
                        bmpval =  float(lnclhdrgisdf['BMP'].values[0])
                    else:
                        bmpval = bmprte    
                    if 'EMP' in lnclhdrgisdf.columns:
                        empval = float(lnclhdrgisdf['EMP'].values[0])
                    elif 'BMP' in lnclhdrgisdf.columns:
                        empval = bmpval+minclength
                    else:
                        empval = emprte    
                               
                        
                    if bmpval > empval:
                        empx = empval 
                        empval = bmpval
                        bmpval = empx
                        dirn = -1
                    if bmpval==empval:  # set the emp value +0.01 or max at emprte  
                        empval = min(bmpval+0.01,emprte)
                    if bmpval==empval:  # set the bmp as emp-0.01 or min at bmprte
                        bmpval = max(empval-0.01,bmprte)
        
                    dirnlbl,dirn1 = offdirn(cloSide,dirn)
                            
                    #rtemprow = [(fhdrteid, bmpval, empval)]
                    #mpinscur.insertRow(rtemprow)
                    offset = 0 
                    if numlanes >= 1:
                        #for k in range(numlanes):
                        offset = dirn1*12 #(6+12*(k-1))
                    elif closedfact == 'Shoulder':     
                        if dirnlbl == 'LEFT': 
                            offset = dirn1*-18
                        else:
                            offset = dirn1 * 18    
                    else:    
                        offset =  dirn1*6

                    mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                        #mpinscur.insertRow((fhdrteid, bmpval,empval))
                    # create the section from the mile post data
                    arcpy.MakeRouteEventLayer_lr (lrte,fldrte, mptbl, eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl) # , "#", "ERROR_FIELD")
                    
                elif loctype == 'MaPoints':
                    ptcoords.clear()
                    # lnclfpts1.SHAPE_x & lnclfpts1.SHAPE_xe
                    # create a feature class with the two data points to generate the section
                    if 'BPoint' in lnclhdrgisdf.columns:
                        pt1 =  lnclhdrgisdf.at[0,'BPoint'] # Point (lnclhdrgisdf['BPoint'], "'spatialReference' : {}".format( sprelrecs))
                        ptcoords.append(pt1)
                        inrow = [rteid,(pt1['x'],pt1['y'])]
                        ptinscur.insertRow(inrow)            
                    if 'EPoint' in lnclhdrgisdf.columns:
                        pt2 =  lnclhdrgisdf.at[0,'EPoint'] # Point (lnclhdrgisdf['EPoint'],"'spatialReference' : {}".format( sprelrecs))
                        ptcoords.append(pt2)
                        inrow = [rteid,(pt2['x'],pt2['y'])]              
                        ptinscur.insertRow(inrow)            
                    
                    # use the geopoint data to section the route
                    if 'gevTbl' in locals():
                        del gevTbl 
                    x = random.randrange(0,1000,1)               
                    gevTbl = os.path.join("in_memory","lnclptbl{}".format(x))
                    if arcpy.Exists(gevTbl):
                        if int(arcpy.GetCount_management(gevTbl).getOutput(0)) > 0:
                            arcpy.DeleteRows_management(gevTbl)
                    arcpy.LocateFeaturesAlongRoutes_lr(geoPtFC,lrte,fldrte,searchRadius,gevTbl,eveProPts)
                    # use LRS routines to generate section
        
                    if 'mevRows' in locals():
                        del mevRows 
                    mevflds = [f.name for f in arcpy.ListFields(gevTbl)]    
                    mevRows = arcpy.da.SearchCursor(gevTbl,mevflds )
                    mpdata.clear()                                                                                                                                                                                                                                                                                                                                                                                                                            

                    mp0 = -999
                    offset = 0
                    for mprow in mevRows:
                        if mp0!=mprow[1]:
                            mpdata.append(mprow[1])
                            mp0 = mprow[1]
                        
                    dirn = 1 # assume positive direction
                    if len(mpdata)>1:                  
                        if mpdata[1] > mpdata[0]:
                            bmpval = mpdata[0]
                            empval = mpdata[1]
                        elif mpdata[1] < mpdata[0]:
                            bmpval = mpdata[1]
                            empval = mpdata[0]
                            dirn = -1
                        else: # same point entered twice increase the mileage by 0.005 to draw line
                            bmpval = round(mpdata[0],3)
                            empval = bmpval+minclength
                    elif len(mpdata)==1: # if one location is entered then use that location only
                        bmpval = round(mpdata[0],3)
                        empval = bmpval +minclength
                    elif len(mpdata)==0: # if no matching route location is found (outside the 1000 units search location)  place the loction at the beginnning of the point
                        bmpval = round(bmprte,3)
                        empval = bmpval+minclength
                        mpdata.append(bmpval)
                        mpdata.append(empval)
                    if round(abs(empval-bmpval),2)<minclength:
                        if len(mpdata)>1:
                            bmpval = min(mpdata[1],mpdata[0])
                            empval =  bmpval + minclength
                        elif len(mpdata)==1:
                            bmpval = mpdata[0]
                            empval =  bmpval + minclength
                        elif len(mpdata)==0:
                            bmpval = bmprte
                            empval = emprte

                    if math.trunc(bmpval*1000) == math.trunc(empval*1000):
                        empval = round(bmpval,4) + minclength
                    elif math.trunc(bmpval*1000) > math.trunc(empval*1000):
                        bmpval = round(empval,4) - minclength
                        dirn = -1
                                                    
                    # cases when route mileages are outside the range of the route BMP & EMP     
                    if empval >= emprte:  # case when both emp and bmp are higher than the emp of the route
                        empval = emprte
                        if (empval-bmpval)<minclength: 
                            bmpval=empval - minclength 
                    if bmpval < bmprte:
                        bmpval = bmprte
                        if (empval-bmpval)<minclength:    
                            empval = bmpval + minclength
                        
                        
                    dirnlbl,dirn1 = offdirn(cloSide,dirn)
                    if arcpy.Exists(mptbl):    
                        if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                            arcpy.DeleteRows_management(mptbl)
                    #mpRows = arcpy.da.SearchCursor(mptbl, mpflds)
        
                    if len(mpdata)>= 1:
                    #arcpy.LocateFeaturesAlongRoutes_lr(featlnclchd,featlnclrte,"Route",500,"LRSPts","Route LINE BMP EMP")        
                        if numlanes > 1:
                            #for k in range(numlanes):
                            offset = dirn1*12 # (6+12*(numlanes-1))
                            mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                        elif closedfact == 'Shoulder':     
                            if dirnlbl == 'LEFT': 
                                offset = dirn1*-18
                            else:
                                offset = dirn1 * 18    
                            mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                        else:                
                            offset = dirn1*6*1
                            mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
        
                        arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
            ##        elif len(mpdata) == 1:
            ##            arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProPts, eveLinlyr)
                    
                # get the geoemtry from the result layer and append to the section feature class
                if arcpy.Exists(eveLinlyr):    
                    cntLyr = arcpy.GetCount_management(eveLinlyr)
                if cntLyr.outputCount > 0:
                    #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
                    
                    # dynamic segementaiton result layer fields used to create the closure segment  
                    lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'EMP', 'Shape@JSON']
                    evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
                    for srow in evelincur:
                        #insecgeo = srow.getValue("SHAPE@")
                        rtenum = srow[1]
                        insecgeo = srow[4]
                        #print("Route : {} ; Geo : {}".format(rtenum, insecgeo))
                        # construct the feature to insert using the merged dataframe and the generated LR shape
                        #section fields ['OBJECTID', 'Route', 'NumLanes', 'Creator', 'RoadName', 'ClosReason', 'EditDate', 'ProjectId',
                        #'Editor', 'Clength', 'enDate', 'ClosHours', 'ClosType', 'Remarks', 'DistEngr', 'beginDate', 'CloseFact',
                        #'globalid', 'CreationDate', 'DEPHWY', 'DIRPInfo', 'LocMode', 'ApprLevel1', 'ClosureSide', 'Island',
                        #'ApprLevel2', 'BMP', 'EMP', 'RteDirn', 'parentglobalid', 'Shape__Length']
                        # dataframe fields ['CreateDate_c', 'Creator_c', 'EditDate_c', 'Editor_c', 'InteRoad', 'MileMarker', 'SHAPE_c',
                        # 'clos_pt_id', 'globalid_c', 'objectid_c', 'paglobalid', 'ApprLevel1', 'ApprLevel2', 'Clength', 'ClosHours',
                        # 'ClosReason', 'ClosType', 'CloseFact', 'ClosedSide', 'CreateDate_h', 'Creator_h', 'DEPHWY', 'DIRPInfo',
                        # 'DistEngr', 'EditDate_h', 'Editor_h', 'Island', 'LocMode', 'NumLanes', 'ProjectId', 'Remarks', 'RoadName',
                        # 'Route', 'SHAPE_h', 'beginDate', 'enDate', 'globalid_h', 'objectid_h','globalid_ce', 'SHAPE_ce', 'MileMarkere']
                        sdfsectapp = deepcopy(lnclhdrgisdf)
                        #[ sdfsectapp[f1] for f1 in sdfsectapp]
                        
                        # update the values for the sdf to match the new section using the merged dataset
                        # sync dataframe merged header/detail laneclosure merge column names to match the section data
    #                    chgflds = {"paglobalid" : "parentglobalid" , "CreateDate_h" : "CreationDate",'Creator_h' : 'Creator',
    #                               'EditDate_h' : 'EditDate', 'Editor_h' : 'Editor', 'globalid_h' : 'globalid',
    #                               'MileMarker' : 'BMP', 'MileMarkere' : 'EMP'}
    #                    lnclfpts2 = deepcopy(lnclfpts1)
    #                    lnclfpts2.rename(columns=chgflds,inplace=True)
    #                    sdfsectapp.loc[0,'beginDate']= begdt # begdt + timedelta(days=dt)
    #                    sdfsectapp.loc[0,'enDate'] = endt #endt - timedelta(int((endt-begdt).days) - dt)
                        sdfsectapp.at[0,'todayis'] = datetime.today() 
                        sdfsectapp.at[0,'Friday0'] = fridaywk(datetime.today(),0) 
                        sdfsectapp.at[0,'Friday1'] = fridaywk(datetime.today(),1) 
                        sdfsectapp.at[0,'Friday2'] = fridaywk(datetime.today(),2)
    #                    sdfsectapp.loc[0,'Remarks'] = remarks                    #print (" b {}  ".format(" test "))
    #                    sdfsectapp.loc[0,'RoadName'] = rdname                    #print (" b {}  ".format(" test "))
                        # update the parentglobalid field to upper-case and inside braces
                        sdfsectapp.at[0,'parentglobalid'] = "{}".format((sdfsectapp.loc[0,'globalid']))
                            #print (" d {}  ".format(" test "))
                        #update columns with matching field names
                        sdfsectapp.at[0,'BMP'] = bmpval
                        sdfsectapp.at[0,'EMP'] = empval
                        sdfsectapp.at[0,'RteDirn'] = dirn
                        #createdate = datetime.fromtimestamp(sdfsectapp.loc[0,'CreationDate'].timestamp() , tzlocal.get_localzone())
                        #sdfsectapp.loc[0,'CreationDate'] = datetime.fromtimestamp(sdfsectapp.loc[0,'CreationDate'].timestamp() , tzlocal.get_localzone())
                        #sdfsectapp.loc[0,'EditDate'] = datetime.fromtimestamp(sdfsectapp.loc[0,'EditDate'].timestamp() , tzlocal.get_localzone())
                        
                        #print (" f {}  ".format(" test "))
                        sdfsectapp.at[0,'ApprLevel1'] = 'Pending'
                        sdfsectapp.at[0,'ApprLevel2'] = 'Pending'
                        sdfsectapp.at[0,'ApproverL1'] = approverl1
                        sdfsectapp.at[0,'ApproverL2'] = approverl2
                        sdfsectapp.at[0,'Active'] = '1'
                        sdfsectapp.at[0,'stconflict'] = 0
                        try:
                            if dirn in (0,1,-1):
                                # update the geometry
                                if insecgeo == None:
                                    print('Not able to create spatial data for entry by User {} ; entered on {} ; Route {} ; BMP : {} ; EMP : {} ; Direction : {}; {} side ; Location : {} ; Period : {} ; Begin {} & End ;  No section is created.'.format(creator,createdatetime,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],loctype,closHrs,begdt,endt ))
                                    logger.info('Not able to create spatial data for entry by User {} ; entered on {} ; Route {} ; BMP : {} ; EMP : {} ; Direction : {}; {} side ; Location : {} ; Period : {} ; Begin {} & End ;  No section is created.'.format(creator,createdatetime,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],loctype,closHrs,begdt,endt ))
                                    sdfsectapp.loc[0,'SHAPE'] = [geomrte.project_as(sprefwebaux)]
                                    insecgeom = arcgis.geometry.Geometry(geomrte)
                                    insgeompaths = geomrte['paths']
                                    insgeomcoords = insgeompaths[0]
                                    begcoord = insgeomcoords[0]
                                    begcoord = insgeomcoords[0]
                                    endcoord = insgeomcoords[len(insgeomcoords)-1]
                                    if len(insgeomcoords)>2:
                                        midptcoord = insgeomcoords[math.ceil(len(insgeomcoords)/2)]
                                    elif len(insgeomcoords)==2:
                                        midptcoord = [(insgeomcoords[0][0] + insgeomcoords[1][0])/2 , (insgeomcoords[0][1] + insgeomcoords[1][1])/2]    
                                    else:
                                        midptcoord = begcoord    
                                else:
                                    sdfsectapp.loc[0,'SHAPE'] = insecgeo
                                    insgeomjson = json.loads(insecgeo)
                                    insecgeom = arcgis.geometry.Geometry(insgeomjson)
                                    insecgeomwgds84 = insecgeom.project_as(spref)
                                    insgeompaths = insecgeomwgds84['paths']
                                    insgeomcoords = insgeompaths[0]
                                    begcoord = insgeomcoords[0]
                                    endcoord = insgeomcoords[len(insgeomcoords)-1]
                                    if len(insgeomcoords)>2:
                                        midptcoord = insgeomcoords[math.ceil(len(insgeomcoords)/2)]
                                    elif len(insgeomcoords)==2:
                                        midptcoord = [(insgeomcoords[0][0] + insgeomcoords[1][0])/2 , (insgeomcoords[0][1] + insgeomcoords[1][1])/2]    
                                    else:
                                        midptcoord = begcoord    
                                midptcoord = {'x' : midptcoord[0] ,'y' : midptcoord[1]}
                                    #geomlen = insecgeom.get_length(method='GEODESIC',units='METERS')
                                intersearch = rteleng/2
                                try:
                                    intsfrom = arcgis.geocoding.reverse_geocode(begcoord,distance=intersearch,return_intersection=True) #,featureTypes=StreetInt)
                                    print (" Geocoded : {} ; point : {} ; on : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has geocoded begin intersection. ".format
                                                (intsfrom,begcoord,creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    logger.info(" Geocoded : {} ; point : {}on : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has geocoded begin intersection. ".format
                                                (intsfrom,begcoord,creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                   
                                    intsfromtxt = intsfrom['address']['Address'] # ['ShortLabel']
                                    intsfroml = intsfrom['address']['Match_addr']  # ['LongLabel']
                                    intstxtfr = intext(intsfrom,rteid,rdname) 
                                except Exception as e:
                                    print (" Error message : {} \n Begin Geocoding error : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has failed to geocode begin intersection. ".format
                                                (str(e),creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    logger.error(" Error message : {} \n Begin Geocoding error : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {}  - {} has failed to geocode begin intersection. ".format
                                                (str(e),creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    intstxtfr = "MilePost {}".format(round(bmpval,3))
                                    intsfroml = "{} - {} Beginning at Mile Post {}".format(rteid,rdname,round(bmpval,3))
                            
                                try:
                                    intsto = arcgis.geocoding.reverse_geocode(endcoord,distance=intersearch,return_intersection=True) #,featureTypes=StreetInt)
                                    print (" Geocoded : {} ; point : {} ; on : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has geocoded end intersection. ".format
                                                (intsto,endcoord,creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    logger.info(" Geocoded : {} ; point : {} ; on : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has geocoded end intersection. ".format
                                                (intsto,endcoord,creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    intstol = intsto['address']['Match_addr'] #['LongLabel']
                                    intstotxt = intsto['address']['Address'] #['ShortLabel']
                                    intstxto = intext(intsto,rteid,rdname,intstxtfr) 
                                except Exception as e:
                                    print (" Error message : {} \n End Geocoding error : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has failed to geocode end intersection. ".format
                                                (str(e),creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    logger.error(" Error message : {} \n End Geocoding error : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has failed to geocode end intersection. ".format
                                                (str(e),creator,createdatetime,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    intstotxt = "MilePost {}".format(round(empval,3))
                                    intstol = "{} - {} Ending at Mile Post {}".format(rteid,rdname,round(empval,3))
                                try:
                                    sdfsectapp['IntersFrom']= sdfsectapp.apply(lambda x : intstxtfr,axis=1) # begdt + timedelta(days=dt)
                                    sdfsectapp['IntersTo'] = sdfsectapp.apply(lambda x : intstotxt,axis=1) #endt - timedelta(int((endt-begdt).days) - dt)
                                    sdfsectapp['intsfroml'] = sdfsectapp.apply(lambda x :  intsfroml,axis=1) # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
                                    sdfsectapp['intstol'] = sdfsectapp.apply(lambda x :  intstol,axis=1) # sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()
                                    sdfsectapp['Clength'] = sdfsectapp.apply(lambda x : round(insecgeom.length,2),axis=1) # intsto['address']['ShortLabel'] #arcgis.geocoding.reverse_geocode(insecgeo[len(insecgeo)-1],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)

##                                    sdfsectapp.loc[0,'IntersFrom']=intstxtfr  #intsfrom['address']['ShortLabel'] # arcgis.geocoding.reverse_geocode(insecgeo[0],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)
##                                    sdfsectapp.loc[0,'IntersTo'] = intstotxt # intsto['address']['ShortLabel'] #arcgis.geocoding.reverse_geocode(insecgeo[len(insecgeo)-1],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)
##                                    sdfsectapp.loc[0,'intsfroml'] =intsfroml  #intsfrom['address']['ShortLabel'] # arcgis.geocoding.reverse_geocode(insecgeo[0],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)
##                                    sdfsectapp.loc[0,'intstol'] = intstol # intsto['address']['ShortLabel'] #arcgis.geocoding.reverse_geocode(insecgeo[len(insecgeo)-1],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)
##                                    sdfsectapp.loc[0,'Clength'] = round(insecgeom.length,2) # intsto['address']['ShortLabel'] #arcgis.geocoding.reverse_geocode(insecgeo[len(insecgeo)-1],distance=200,out_sr=4326,return_intersection=True,for_Storage=True)
                                except Exception as e:
                                    print (" Error message : {} \n intersection assignment error from short : {} ; long : {} ; to short :  {} ; long :  {}  ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has failed to geocode end intersection. ".format
                                                (str(e),intstxtfr,intstotxt,intsfroml,intstol,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                    logger.error(" Error message : {} \n intersection assignment error from short : {} ; long : {} ; to short :  {} ; long :  {}  ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {};  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} - {} has failed to geocode end intersection. ".format
                                                (str(e),intstxtfr,intstotxt,intsfroml,intstol,fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begdt,endt,loctype,dirpremarks,remarks))
                                newlnclsdf = sdfsectapp.spatial.to_featureset().sdf
                                sdfsectapp = newlnclsdf.dropna(how='all')
                                sdfptsapp = deepcopy(sdfsectapp)
                                midptsdfapp = deepcopy(sdfsectapp)
                                print("{} features \n original : {} \n copy : {}".format(len(sdfptsapp),sdfsectapp,sdfptsapp))
                                #sdfsectapp.set_geometry(insecgeo)
                                if closHrs is None:
                                    for dt in range(0, int((endt-begdt).days)+1,1):
                                        begdti = begdt+ timedelta(days=dt)
                                        endti =  endt- timedelta(int((endt-begdt).days) - dt)
                                        sdfsectapp['beginDate']= sdfsectapp['beginDate'].apply(lambda x : begdti) # begdt + timedelta(days=dt)
                                        sdfsectapp['enDate'] = sdfsectapp['enDate'].apply(lambda x : endti) #endt - timedelta(int((endt-begdt).days) - dt)
                                        sdfsectapp['begDyWk'] = sdfsectapp['beginDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
                                        sdfsectapp.at['enDyWk'] = sdfsectapp['enDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()

##                                        sdfsectapp.loc[0,'beginDate']= begdti # begdt + timedelta(days=dt)
##                                        sdfsectapp.loc[0,'enDate'] = endti #endt - timedelta(int((endt-begdt).days) - dt)
##                                        sdfsectapp.loc[0,'begDyWk'] = sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
##                                        sdfsectapp.loc[0,'enDyWk'] = sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()
                                        newlnclsdf = sdfsectapp.spatial.to_featureset().sdf
                                        newlnclsdna = newlnclsdf.dropna(how='all')
                                        newlnclfs = newlnclsdna.spatial.to_featureset()
                                        print("{} feature \n {}".format(len(newlnclfs),newlnclfs.features))
                                        lresult = assyncadds(wmlnclyrsects,newlnclfs[0]) #  wmlnclyrsects.edit_features(newlnclfs)
                                        logger.info("{} ; Entry by {} on {} Section Closure of Rte {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created".format(lresult,creator,createdatetime,sdfsectapp.loc[0,'Route'], round(sdfsectapp.loc[0,'BMP'],3), round(sdfsectapp.loc[0,'EMP'],3), sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                                else:   # closHrs: is not None?  #('Other' in sdfsectapp.loc[0,'ClosHours']):

                                    if '24Hrs' in closHrs:                                                                                                                                                                                                                                                                                                                                                                                                  
                                        for dt in range(0, int((endt-begdt).days+1),1):
                                            if dt == 0: # first day
                                                begdti = begdt
                                                # check if the end time is more than 24 hours
                                                
                                                if datemidnight(endt)==datemidnight(begdt):   #same day entry
                                                    endti =  endt
                                                else:
                                                    endti = datemidnight(begdt)+ timedelta(hours=24)
                                                                                                                                                                                                                                                                                                                                                                                                                       
                                            elif dt <= int((endt-begdt).days) and endti < endt: # intermediate days
                                                begdti = endti                                                                                                                                                                                                                                                                                                                                                                                  
                                                endti =  begdti+ timedelta(days=1)                                                                                                                                                                                                                                                                                                                                                               
                                            elif dt == (int((endt-begdt).days)) :
                                                begdti =  endti
                                                if endti > endt: # last day
                                                    endti =  endt
                                                else:    
                                                    endti =  begdti+ timedelta(days=1)
                                                
                                            sdfsectapp['beginDate']= sdfsectapp['beginDate'].apply(lambda x : begdti) # begdt + timedelta(days=dt)
                                            sdfsectapp['enDate'] = sdfsectapp['enDate'].apply(lambda x : endti) #endt - timedelta(int((endt-begdt).days) - dt)
                                            sdfsectapp['begDyWk'] = sdfsectapp['beginDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
                                            sdfsectapp['enDyWk'] = sdfsectapp['enDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()
                                           
                                            newlnclsdf = sdfsectapp.spatial.to_featureset().sdf
                                            newlnclsdna = newlnclsdf.dropna(how='all')
                                            newlnclfs = newlnclsdna.spatial.to_featureset()
                                            print("{} feature \n {}".format(len(newlnclfs),newlnclfs.features))
                                            if (endti<=endt):        
                                                lresult = assyncadds(wmlnclyrsects,newlnclfs) # wmlnclyrsects.edit_features(newlnclfs)
                                                logger.info("{} ; Entry by {} on {} Section {} Closure of Route {} ; BMP : {} ; EMP : {} ; Direction : {}; {} side ; beg {} & end {} created".format(lresult,creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'], round(sdfsectapp.loc[0,'BMP'],3), round(sdfsectapp.loc[0,'EMP'],3), sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                                        
                                    elif 'Daily' in closHrs or 'Other' in closHrs:  #('Other' in sdfsectapp.loc[0,'ClosHours']):
                                        for dt in range(0, int((endt-begdt).days+1),1):
                                            begdti = begdt+ timedelta(days=dt)
                                            endti =  endt- timedelta(int((endt-begdt).days) - dt)
                                            sdfsectapp['beginDate']= sdfsectapp['beginDate'].apply(lambda x : begdti) # begdt + timedelta(days=dt)
                                            sdfsectapp['enDate'] = sdfsectapp['enDate'].apply(lambda x : endti) #endt - timedelta(int((endt-begdt).days) - dt)
                                            sdfsectapp['begDyWk'] = sdfsectapp['beginDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
                                            sdfsectapp['enDyWk'] = sdfsectapp['enDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()

##                                            sdfsectapp.loc[0,'beginDate']=begdti #+ timedelta(days=dt)
##                                            sdfsectapp.loc[0,'enDate'] = endti #- timedelta(int((endt-begdt).days) - dt)
##                                            sdfsectapp.loc[0,'begDyWk'] = datetime.strftime(sdfsectapp.loc[0,'beginDate'],'%A') # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
##                                            sdfsectapp.loc[0,'enDyWk'] = datetime.strftime(sdfsectapp.loc[0,'enDate'],'%A') #  sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()
                                            newlnclfs = sdfsectapp.spatial.to_featureset()
                                            newlnclsdf = newlnclfs.sdf
                                            newlnclsdna = newlnclsdf.dropna(how='all')
                                            newlnclfs = newlnclsdna.spatial.to_featureset()
                                            print("{} feature \n {}".format(len(newlnclfs),newlnclfs.features))
                                            lresult = assyncadds(wmlnclyrsects,newlnclfs) # wmlnclyrsects.edit_features(newlnclfs)
                                            logger.info("{} ; Entry by {} on {} Section {} Closure of Rte {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created".format(lresult,creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'],  round(sdfsectapp.loc[0,'BMP'],3),  round(sdfsectapp.loc[0,'EMP'],3), sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                                    elif 'Overnight' in closHrs:  #('Other' in sdfsectapp.loc[0,'ClosHours']):
                                        for dt in range(0, int((endt-begdt).days+1),1):
                                            begdti = begdt+ timedelta(days=dt)
                                            endti =  endt- timedelta(int((endt-begdt).days) - dt)
                                            sdfsectapp['beginDate']= sdfsectapp['beginDate'].apply(lambda x : begdti) # begdt + timedelta(days=dt)
                                            sdfsectapp['enDate'] = sdfsectapp['enDate'].apply(lambda x : endti) #endt - timedelta(int((endt-begdt).days) - dt)
                                            sdfsectapp['begDyWk'] = sdfsectapp['beginDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
                                            sdfsectapp['enDyWk'] = sdfsectapp['enDate'].apply(lambda x :  datetime.strftime(x,'%A')) # sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()

##                                            sdfsectapp.loc[0,'beginDate']= begdti # + timedelta(days=dt)
##                                            sdfsectapp.loc[0,'enDate'] = endti # - timedelta(int((endt-begdt).days) - dt)
##                                            sdfsectapp.loc[0,'begDyWk'] = datetime.strftime(sdfsectapp.loc[0,'beginDate'],'%A') # sdfsectapp.loc[0,'beginDate'].day_name() # sdfsectapp.loc[0,'beginDate'].day_name()
##                                            sdfsectapp.loc[0,'enDyWk'] = datetime.strftime(sdfsectapp.loc[0,'enDate'],'%A') #  sdfsectapp.loc[0,'enDate'].day_name() #sdfsectapp.loc[0,'enDate'].day_name()
                                            newlnclfs = sdfsectapp.spatial.to_featureset()
                                            newlnclsdf = newlnclfs.sdf
                                            newlnclsdna = newlnclsdf.dropna(how='all')
                                            newlnclfs = newlnclsdna.spatial.to_featureset()
                                            print("{} feature \n {}".format(len(newlnclfs),newlnclfs.features))
                                            lresult = assyncadds(wmlnclyrsects,newlnclfs) # wmlnclyrsects.edit_features(newlnclfs)
                                            logger.info("{} ; Entry by {} on {} Section {} Closure of Rt {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created".format(lresult,creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'],  round(sdfsectapp.loc[0,'BMP'],3),  round(sdfsectapp.loc[0,'EMP'],3), sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                                print("{} ; Entry by {} on {} Section {} Closure of Rt {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created".format(lresult,creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'], round(sdfsectapp.loc[0,'BMP'],3),  round(sdfsectapp.loc[0,'EMP'],3), sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdt,endt ))
                                    #print('{} Closure Rte {} ; BMP : {} ; EMP : {} ; Direction : {} ; {}  side ; pgid {} section created'.format(closHrs,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'], sdfsectapp.loc[0,'parentglobalid'] ))
                                # insert the point into the point table
                                # insert the closure entry points into the points layer with the same credentials
                                geoPt = {}
                                #for f in lnclchdgisdf.itertuples():
                                try:
                                    ptgid = sdfptsapp.at[0,'parentglobalid']
                                    if loctype == 'MilePost':
                                        ptcoords.clear()
                                        if 'BPoint' in lnclhdrgisdf.columns:
                                            lnclhdrgisdf.at[0,'BPoint'] = Point(begcoord[0] , begcoord[1] )
                                            pt1 = {'x' : begcoord[0] ,'y' :  begcoord[1]  }
                                            ptcoords.append(pt1)
                                        if 'EPoint' in lnclhdrgisdf.columns:
                                            lnclhdrgisdf.at[0,'EPoint'] = Point( endcoord[0] ,endcoord[1]  )
                                            pt2 = {'x' : endcoord[0] ,'y' :  endcoord[1]  }
                                            ptcoords.append(pt2)
        #                                    pt2 =  lnclhdrgisdf.at[0,'EPoint'] # Point (lnclhdrgisdf['EPoint'],"'spatialReference' : {}".format( sprelrecs))
    #                                if ptgid not in lnclbeptspgid:
                                    geopt = ''  # point has already been entered
                                    # insert a mid point geometry location for zoomed-out display
                                    midptcoords.append(midptcoord)
                                    for px,pt in enumerate(midptcoords,1):
                                        geomchd= {'x' : pt['x'] , 'y' : pt['y']  , 'spatialReference' : sprefwgs84}
                                        ptgeom = arcgis.geometry.Geometry(geomchd)
                                        ptgeomwebaux = ptgeom.project_as(sprefwebaux)
                                        if geoPt!=geomchd:  # check if coordinates are the same and if so skip
                                            try:
                                                #midptsdfapp = midptsdfapp.set_geometry(col="SHAPE", sr=sprefwebaux)
                                                midptsdfappgac = gac(midptsdfapp)
                                                midptsdfappgac.set_geometry(col='SHAPE', sr=sprefwebaux)
                                                midptsdfappgac.project(sprefwgs84)
                                            except Exception as e:
                                                print (" Error message : {} ;  Mid Point {} columns; First Point {} ; New Point : {} ".format(e,midptsdfapp.columns, geoPt, geomchd ))
                                                logger.error (" Error message : {} ;  Mid Point {} columns; First Point {} ; New Point : {} ".format(e,midptsdfapp.columns, geoPt, geomchd ))
                                            midptsdfapp.at[0,'SHAPE']=ptgeomwebaux
                                            midptsdfapp.at[0,'BMP'] = bmpval #fchd.get_value('MileMarker')
                                            midptsdfapp.at[0,'EMP'] = empval # fchd.get_value('MileMarker')
                                            midptsdfapp.at[0,'Clength'] = -2.0    

##                                            midptsdfapp.iat[0,midptsdfapp.columns.get_loc('SHAPE')]=ptgeomwebaux
##                                            midptsdfapp.iat[0,midptsdfapp.columns.get_loc('BMP')] = bmpval #fchd.get_value('MileMarker')
##                                            midptsdfapp.iat[0,midptsdfapp.columns.get_loc('EMP')] = empval # fchd.get_value('MileMarker')
##                                            midptsdfapp.iat[0,midptsdfapp.columns.get_loc('Clength')] = -2.0    
                                        geoPt = geomchd
                                            #sdfptsapp.iat[0,sdfptsapp.columns.get_loc('InteRoad')] = fchd.get_value('InteRoad')
                                            #print("{}".format(fchd.get_value('SHAPE')))
                                        try:
                                            lnclmidpts = midptsdfapp.spatial.to_featureset()
                                            newmidclfs = midptsdfapp.spatial.to_featureset()
                                            newmidclsdf = newmidclfs.sdf
                                            newmidclsdna = newmidclsdf.dropna(how='all')
                                            lnclmidpts = newmidclsdna.spatial.to_featureset()
                                            lresult = assyncaddspt(midptlyr,lnclmidpts) # midptlyr.edit_features(adds=lnclmidpts)
                                            print('{} ; Mid Point creation for Route {} ; BMP : {} ; EMP : {} ; Direction : {} , pgid {}  '.format(lresult,sdfptsapp.loc[0,'Route'], sdfptsapp.loc[0,'BMP'], sdfptsapp.loc[0,'EMP'], sdfptsapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'parentglobalid'] ))
                                            logger.info('{} ; Mid Point creation for Route {} ; BMP : {} ; EMP : {} ; Direction : {} , pgid {}  '.format(lresult,sdfptsapp.loc[0,'Route'], sdfptsapp.loc[0,'BMP'], sdfptsapp.loc[0,'EMP'], sdfptsapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'parentglobalid'] ))
                                            
                                        except Exception as e:
                                            print (" Error message : {} \n Mid Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} has failed to generate points.  ".format
                                                   (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),ptgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,geomchd,remarks))
                                            logger.error(" Error message : {} \n Mid Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} has failed to generate points  ".format
                                                   (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),ptgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,geomchd,remarks))
                                    
                                    for px,pt in enumerate(ptcoords,1):
                                        geomchd= {'x' : pt['x'] , 'y' : pt['y']  , 'spatialReference' : sprefwgs84}
                                        ptgeom = arcgis.geometry.Geometry(geomchd)
                                        ptgeomwebaux = ptgeom.project_as(sprefwebaux)
                                        if geoPt!=geomchd:  # check if coordinates are the same and if so skip
                                            try:
                                                sdfptsappgac = gac(sdfptsapp)
                                                sdfptsappgac.set_geometry(col='SHAPE', sr=sprefwebaux)
                                                sdfptsappgac.project(sprefwgs84)
                                            except Exception as e:
                                                print (" Error message : {} ;  px Point {} columns; First Point {} ; New Point : {} ".format(e,sdfptsapp.columns, pt, ptgeom ))
                                                logger.error (" Error message : {} ;  px Point {} columns; First Point {} ; New Point : {} ".format(e,sdfptsapp.columns, pt, ptgeom ))
                                            sdfptsapp.at[0,'SHAPE']=ptgeomwebaux
                                            sdfptsapp.at[0,'BMP'] = bmpval #fchd.get_value('MileMarker')
                                            sdfptsapp.at[0,'EMP'] = empval # fchd.get_value('MileMarker')
                                            sdfptsapp.at[0,'Clength'] = -px    
                                        geoPt = geomchd
                                            #sdfptsapp.iat[0,sdfptsapp.columns.get_loc('InteRoad')] = fchd.get_value('InteRoad')
                                            #print("{}".format(fchd.get_value('SHAPE')))
                                        try:
                                            newptsclfs = sdfptsapp.spatial.to_featureset()
                                            newptsclsdf = newptsclfs.sdf
                                            newptsclsdna = newptsclsdf.dropna(how='all')
                                            chdlnclpts = newptsclsdna.spatial.to_featureset()
                                            lresult = assyncaddspt(wmsectlyrpts,chdlnclpts) # wmsectlyrpts.edit_features(adds=chdlnclpts)
                                                
                                            print('{} ; Point for Route {} ; BMP : {} ; EMP : {} ; Direction : {} , pgid {}  created'.format(lresult,sdfptsapp.loc[0,'Route'], sdfptsapp.loc[0,'BMP'], sdfptsapp.loc[0,'EMP'], sdfptsapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'parentglobalid'] ))
                                            logger.info('{} ; Point for Route {} ; BMP : {} ; EMP : {} ; Direction : {} , pgid {}  created'.format(lresult,sdfptsapp.loc[0,'Route'], sdfptsapp.loc[0,'BMP'], sdfptsapp.loc[0,'EMP'], sdfptsapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'parentglobalid'] ))
                                                
                                        except Exception as e:
                                            print (" Error message : {} \n Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} has failed to generate points.  ".format
                                                   (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),ptgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,geomchd,remarks))
                                            logger.error(" Error message : {} \n Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} has failed to generate points  ".format
                                                   (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),ptgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,geomchd,remarks))
                                except Exception as e:
                                    print (" Error message : {} \n Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} has coordinate anomalies.".format
                                           (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,loctype,remarks))
                                    logger.error(" Error message : {} \n Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} has coordinate anomalies.".format
                                           (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,loctype,remarks))
                            #chdlnclpts = ptslnclsdf.spatial.to_featureset()
                            #wmsectlyrpts.edit_features(chdlnclpts)
                            if arcpy.Exists(sdfsectapp):
                                    del sdfsectapp
                            if arcpy.Exists(midptsdfapp):
                                    del midptsdfapp

                        except Exception as e:
                            print (" Error message : {} \n Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} - {} has coordinate anomalies.".format
                                   (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,loctype,dirpremarks,remarks))
                            logger.error(" Error message : {} \n Point entered by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) from {} to {} ; Beg Date : {} ; end date : {}  Point at : {} with remarks {} - {} has coordinate anomalies.".format
                                   (str(e),creator,datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),fhdrgid,objid,rteid,rdname,round(bmpval,3),round(empval,3),begDte,enDte,loctype,dirpremarks,remarks))
                                                    
                                
                    del evelincur,lrterows,sdfptsapp    
                del ptinscur, mpinscur

            except Exception as e:
                print (" Survey Created by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} has failed to generate sections ".format
                        (creator,createdatetime,fhdrgid,objid,rteid,rdname,begdt,endt,loctype,remarks))
                logger.error(" Error message : {} \n for survey Created by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} has failed to generate sections. ".format
                            (str(e),creator,createdatetime,fhdrgid,objid,rteid,rdname,begdt,endt,loctype,remarks))
        except Exception as e:
            print (" Survey id  : {} failed to generate sections ".format(fhdr))
            logger.error(" Error message for Survey id  : {}  failed to generate sections ".format(fhdr))
            
          
    

tend = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
print (" End lane closure {} processing of {} section features at {}. \n ".format (lnclSrcFSTitle,len(lnclhdrnewfeats),tend))
logger.info (" End lane closure {} processing of {} section features at {}. \n".format (lnclSrcFSTitle,len(lnclhdrnewfeats),tend))
lnclhdlr.close()
