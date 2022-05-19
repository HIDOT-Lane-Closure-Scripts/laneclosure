
# coding: utf-8

# generate conflict dataset for use by conflict map
import arcpy, os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , datetime, timedelta
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
import sqlite3, sqlalchemy
from copy import deepcopy

import logging

logger = logging.getLogger('lnconfile')
#rptpath = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\reports"
#logpath = r'\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs'
logfilenm = r"lanecloseconflicts.log"
#logpath = r'I:\HI\tools\scripts\LaneClosure\logs'
#rptpath = r'I:\HI\tools\scripts\LaneClosure\reports'
logpath = r'D:\MyFiles\HWYAP\LaneClosure\logs'
rptpath = r'D:\MyFiles\HWYAP\LaneClosure\reports'
logfile = os.path.join(logpath,logfilenm)
lnconfhdlr = logging.FileHandler(logfile) # 'laneclosections.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
lnconfhdlr.setFormatter(formatter)
logger.addHandler(lnconfhdlr)
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


    # get the date and time
curDate = strftime("%Y%m%d%H%M%S") 
# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())

logger.info("Spatial Conflict Processing begins at {} ".format(tbeg))
print("Spatial Conflict Processing begins at {} ".format(tbeg))

# connect to SQLIte Database to get the user name and email data for lane closure submissions
#dbname = r'D:\MyFiles\HWYAP\laneclosure\docs\laneclosure.sqlite'
#conndb = sqlite3.connect(dbname) 
tblname = "laneclosuremailist"
sqlqry = "SELECT * FROM laneclosuremailist"
#dbeng = sqlalchemy.create_engine("sqlite:///{}".format(dbname),execution_options={"sqlite_raw_colnames":True})
#userdf = pd.read_sql_table(tblname, dbeng) # , schema, index_col, coerce_float, parse_dates, columns, chunksize)
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

print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fsectwebmap.url,fsectwebmap.title,fsectwebmap.id))
wmsectlyrs = fsectwebmap.layers
wmsectlyrpts = lyrsearch(wmsectlyrs, wmlnclyrptsnm)
wmlnclyrsects = lyrsearch(wmsectlyrs, wmlnclyrsectsnm)

# conflict table data
lnclconflict_properties = {'title': 'Lane Closure Spatio-Temporal Conflicting Features',
                    'description':'Lane Closure Spatio-Temporal Conflicting Features created from sections dataframe ',
                    'tags': 'arcgis python api,pandas,dataframe,lane closure,spatio-temporal conflict'}
conflictitle = "Conflicting Lane Closures" # lnclconflict_properties['title'] # a8aa67
#servid = "5e2b34f30ab84b39a5d91f944581ef20"
conflayer = "Conflicting Lane Closures Layer"
servtype = 'Feature Service' # "featureCollection" #

#conflictsect = qgis.content.search(query="5e2b34f30ab84b39a5d91f944581ef20") 

conflictsect =webexsearch(qgis, conflictitle, userName, servtype,numfs,ekOrg)

#lnclconfsdict = dict(lnclconfs.properties)
#lnclconfsjson = json.dumps({"featureCollection": {"layers": [sectsonflictdict]}})
conflyrs = conflictsect.layers
lnclosureconflict =  lyrsearch(conflyrs, conflayer)
# xc = 100
# conflncldelqry = lnclosureconflict.query(where="(OBJECTIDM < ({}))".format(xc))
# if len(conflncldelqry)>0:
#     # delete data to be updated from the existing conflict layer based on query
#     oids = "'" + "','".join(str(xfs.attributes['ObjectIdM']) for xfs in  conflncldelqry.features  if 'ObjectIdM' in xfs.attributes ) + "'"
#     conflncldelqry = "(ObjectIdM in ({}))".format(oids) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)
#     qrylnconf = lnclosureconflict.delete_features(where=conflncldelqry)

# get sdf to be used for new section data insert operations
sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
# get sdf to be used for new section data insert operations

dt0 = 3
dt1 = 1
td = datetime.combine(date.today(),datetime.min.time())  # today's date
dts = datetime.timestamp(td) # today's date as time stamp
begdts = datetime.fromtimestamp(int(round(dts,0))) # today's date as integer time stamp
begdt = begdts + timedelta(hours=(datetime.today().hour-dt0))  # today's datetime minus dt0 hours # /1e3 , tzlocal.get_localzone()),
#begdt = begdts - timedelta(days=(dt0))  # /1e3 , tzlocal.get_localzone()),
dt2 = 14
endt = begdt + timedelta(days=dt2)  # end time from today's datetime and dt2 days later # /1e3 , tzlocal.get_localzone()),
#lncldtes = "(beginDate>= '{}')".format(begdt) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)
#lncldtes = "CreationDate>= '{}'".format('1-1-2018') # and enDate <= '{}')".format('1-15-2020','1-31-2020')
lncldtes = "(CreationDate>= '{}')".format(begdt) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)

conflicols = ['OBJECTID','parentglobalid','Route','RoadName','RteDirn','direct','beginDate','enDate','ClosReason','LocMode','ClosType','CloseFact','ClosureSide','ClosHours','SHAPE','stconflict']
bdt = 30  # last thirty days entry 
backdt = begdt - timedelta(days=bdt)  # last thirty days entry  # /1e3 , tzlocal.get_localzone()),
sectqry = wmlnclyrsects.query(where="(CreationDate>= '{}')".format(backdt)) # assumes that entries are made no more than one month ahead ,out_fields=conflicols)
sectqrynew = wmlnclyrsects.query(where=lncldtes) # ,out_fields=conflicols)

# delete data to be updated from the existing conflict layer based on query
oids = "'" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in  sectqrynew.features  if 'OBJECTID' in xfs.attributes ) + "'"
conflncldelqry = "(OBJECTIDM in ({}))".format(oids) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)
qrylnconf = lnclosureconflict.query(conflncldelqry)

"""
if len(qrylnconf)>0:
    chkdel = lnclosureconflict.delete_features(where=conflncldelqry)
    #chkdel = lnclosureconflict.delete_features(where = "(OBJECTID in ({}))".format(("'" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in lnclosureconflict.query()) +"'")))
    print("Deleted {} features from {} using query {} : Results : {} ".format(len(qrylnconf),conflictitle,conflncldelqry,chkdel))
    logger.info("Deleted {} features from {} using query {} : Results : {} ".format(len(qrylnconf),conflictitle,conflncldelqry,chkdel))
"""
spref = sectqry.spatial_reference
budist = 15
bunit = "Meters"
busr = spref
# prepare the object Id's with no route numbers (submitted without update privileges
#if len(sectqry)>0:
#    norteid = "OBJECTID in ('" + "','".join(str(sfs.attributes['OBJECTID']) for sfs in sectqry )  + "')"
#    norteid = "objectid in ('" + "','".join(str(lfs.attributes['objectid']) for lfs in sectqry )  + "')"
    # edit the selected records
#    for sfs in  sectqry:
#        appr1 = sfs.get_value('ApproverL1') #,'dot_achung')
#        email1 = sfs.get_value('l1email') # ,'albert.chung@hawaii.gov')

# query the lane closure survey entries that have been updated by users and delete them from processed feature class
lnclsectsdf = sectqry.sdf
lnclsectsdfnew = sectqrynew.sdf
#conflictsdf = lnclsectsdf[conflicols]
utcofs = -10 #datetime.utc() # 

#confeats = conflictsdf.spatial.to_featureset()
xftsect = sectqry.features
scols = lnclsectsdf.columns
if 'OBJECTID' in scols:
    lnclsectsdf.set_index('OBJECTID',inplace=True)
if 'OBJECTID' in lnclsectsdfnew.columns:
    lnclsectsdfnew.set_index('OBJECTID',inplace=True)


for i, xrow in enumerate(lnclsectsdfnew.itertuples(index=True, name='Pandas')):
    #xsedte = mfeat.attributes['EditDate # mfeat.attributes['EdiTime # srow['EditDate']
    #if pd.isnull(xsedte):
    #    xsedte = datetime.today()
    #xsedts = datetime.timestamp(xsedte) # + datetime.timedelta(int(utcofs)))
    #xsedtsl = datetime.fromtimestamp(xsedts , tzlocal.get_localzone())
    xbegdte = xrow.beginDate +timedelta(hours=utcofs) # mfeat.attributes['beginDate'] # srow['beginDate']
    xbegdts = datetime.timestamp(xbegdte) # + datetime.timedelta(int(utcofs)))
    xbegdt = xbegdts*1e3 #datetime.timestamp(xbegdte) # + datetime.timedelta(int(utcofs)))
    xendte = xrow.enDate +timedelta(hours=utcofs) # mfeat.attributes['enDate'] # srow['enDate']
    xendts = datetime.timestamp(xendte)
    xendt = xendts*1e3 # datetime.timestamp(xendte) # + datetime.timedelta(int(utcofs)))
    xbegdtel = datetime.fromtimestamp(xbegdts, tzlocal.get_localzone())
    xendtel = datetime.fromtimestamp(xendts, tzlocal.get_localzone())
    #[" {} ".format(  x) for x in lnclsectsdf['beginDate'] if x<=datetime.utcfromtimestamp(xbegdts)]
    xsparguid = xrow.parentglobalid # mfeat.attributes['parentglobalid'] # srow['parentglobalid']
    #xsoid = xrow.OBJECTID # mfeat.attributes['OBJECTID'] # srow['OBJECTID']
    xsoid = xrow.Index # mfeat.attributes['OBJECTID'] # srow['OBJECTID']
    rteid = xrow.Route # mfeat.attributes['Route']
    rdname = xrow.RoadName # mfeat.attributes['RoadName']
    sdirn = xrow.RteDirn # mfeat.attributes['RteDirn']
    loctype = xrow.LocMode # mfeat.attributes['LocMode']
    clostype = xrow.ClosType # mfeat.attributes['ClosType']
    closedfact = xrow.CloseFact # mfeat.attributes['CloseFact']
    cloSide = xrow.ClosureSide # mfeat.attributes['ClosureSide']
    closReason = xrow.ClosReason # mfeat.attributes['ClosReason']
    closHrs = xrow.ClosHours # mfeat.attributes['ClosHours']
    creatorid = xrow.Creator # mfeat.attributes['Creator']
    createdate = xrow.CreationDate # mfeat.attributes['CreationDate']
    editorid = xrow.Editor # mfeat.attributes['Editor']
    editordate = xrow.EditDate # mfeat.attributes['EditDate']
    try:
    #    dtefilt = ((lnclsectsdf.beginDate>=xbegdt and lnclsectsdf.beginDate<=xendt) or (lnclsectsdf.enDate>=xbegdt and lnclsectsdf.enDate<=xendt))
        #xlfeatsdf = lnclsectsdf[(lnclsectsdf.beginDate>=xbegdt and lnclsectsdf.beginDate<=xendt) or (lnclsectsdf.enDate>=xbegdt and lnclsectsdf.enDate<=xendt)]
        #dteqry = "(beginDate>= '{}' and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( xbegdte,xendte,xbegdte,xendte)
        #qrysectdte = wmlnclyrsects.query(where=dteqry)
        dteqryfrm = "('{}' <= beginDate <= '{}') or ('{}' >= enDate<= '{}')".format( xbegdte,xendte,xbegdte,xendte)
        sdfconfrm = lnclsectsdf[((lnclsectsdf['beginDate']>=datetime.utcfromtimestamp(xbegdts)) & (lnclsectsdf['beginDate']<=datetime.utcfromtimestamp(xendts))) | ((lnclsectsdf['enDate']>=datetime.utcfromtimestamp(xbegdts)) & (lnclsectsdf['enDate']<=datetime.utcfromtimestamp(xendts)))]  
        # lnclsectsdf[((lnclsectsdf['beginDate']>=datetime.utcfromtimestamp(xbegdte)) & (lnclsectsdf['beginDate']<=datetime.utcfromtimestamp(xendte))) | ((lnclsectsdf['enDate']>=datetime.utcfromtimestamp(xbegdte)) & (lnclsectsdf['enDate']<=datetime.utcfromtimestamp(xendte)))] #lnclsectsdf.query(dteqryfrm)
        #sdfconfrm = lnclsectsdf[((lnclsectsdf['beginDate']>=pd.to_datetime(xbegdte)) & (lnclsectsdf['beginDate']<=pd.to_datetime(xendte))) | ((lnclsectsdf['enDate']>=pd.to_datetime(xbegdte)) & (lnclsectsdf['enDate']<=pd.to_datetime(xendte)))]
        #qrysectdtesdf = qrysectdte.sdf
        #lcols = qrysectdtesdf.columns
        #print(' Sect OID {} ; pgid : {} ; Creator : {} ; EditSectime : {} ;  Rte : {}  '.format(mfeat.attributes['OBJECTID,mfeat.attributes['parentglobalid, mfeat.attributes['Creator, mfeat.attributes['EditDate, mfeat.attributes['Route))
        # check the section for time and space conflicts
        xspolygeom = xrow.SHAPE #mfeat.attributes['SHAPE']
        xspolygon = xspolygeom.buffer(budist)
        geompart = arcgis.geometry.Geometry(xspolygeom)
        #confeats = arcgis.features.FeatureSet([mrow])
        #confsdf = confeats.sdf
    #    xspolybuf = arcgis.geometry.buffer(xspolygeom, spref, budist, bunit, out_sr=spref, buffer_sr=spref, union_results=True, geodesic=True, gis=None)
        #xslconsdf = featuplnclsdf[featuplnclsdf['globalid'].isin([xsparguid])]
        ix=0
        for ir, lrow in enumerate(sdfconfrm.itertuples(index=True, name='Pandas')):
            xlguid = lrow.globalid # getattr(lrow,'globalid') # lrow['globalid']
            xlparguid = lrow.parentglobalid
            if xlparguid != xsparguid:
                try:
                    lspolygeom = lrow.SHAPE
                    xlpolygon = lspolygeom.buffer(budist)
                    geomdis = xlpolygon.disjoint(xspolygon)
                    #xledts = datetime.datetime.timestamp(xledte)
                    #loid = lrow.OBJECTID #lrow['objectid']
                    loid = lrow.Index # OBJECTID #lrow['objectid']
                    #print(' OID Sect {} ; pgid : {} ;  LnClgid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(mfeat.attributes['OBJECTID,mfeat.attributes['parentglobalid, lrow.globalid, mfeat.attributes['Creator, mfeat.attributes['EditDate, lrow.EditDate,lrow.Route))
                    #print(' GID Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
                    if not geomdis: #and ldirn == sdirn : 
                        ix=+1  
                        lbegdte = lrow.beginDate +timedelta(hours=utcofs) # srow['beginDate']
                        lbegdts = datetime.timestamp(lbegdte) 
                        lbegdt = lbegdts*1e3  # + datetime.timedelta(int(utcofs)))
                        lendte = lrow.enDate +timedelta(hours=utcofs) # srow['enDate']
                        lendts = datetime.timestamp(lendte)
                        lendt = lendts*1e3  # + datetime.timedelta(int(utcofs)))
                        lbegdtel = datetime.fromtimestamp(lbegdts, tzlocal.get_localzone())
                        lendtel = datetime.fromtimestamp(lendts, tzlocal.get_localzone())
                        if (xbegdt <= lbegdt <= xendt) or (xbegdt <= lendt <= xendt):
                        # query if there is already a reciprocal relationship with the same date time attributes
                        #qrydupconf = "parentglobalid like '{}' and parentglobalidC like '{}' and (beginDate= '{}' and enDate= '{}' and beginDateC= '{}' and enDateC= '{}')".format(consdf1.at[0,'parentglobalidC'],consdf1.at[0,'parentglobalid'],lbegdte,lendte,xbegdte,xendte)
                            qrydupconf = "(ObjectidC = {} and ObjectidM = {}) or (ObjectidC = {} and ObjectidM = {})".format(loid,xsoid,xsoid,loid)
                            qdupliconflict= lnclosureconflict.query(where=qrydupconf)
                            if (len(qdupliconflict.features)==0):
                                lrte = lrow.Route
                                lpgid = lrow.parentglobalid
                                ldirn = lrow.RteDirn
                                lcreatorid = lrow.Creator
                                lcreatedate = lrow.CreationDate +timedelta(hours=utcofs)
                                leditorid = lrow.Editor
                                leditordate = lrow.EditDate +timedelta(hours=utcofs)
                                lsdf = deepcopy(lnclsectsdf.filter(like=str(loid),axis=0))
                                lsdf.at[loid,'ObjectidM']=xsoid
                                lsdf.at[loid,'ObjectidC']=loid
                                lsdf.at[loid,'stconflict']=1
                                lsdf.at[loid,'Primacy']=(ix)
                                lfeats = lsdf.spatial.to_featureset()
                                resultl = lnclosureconflict.edit_features(adds=lfeats)
                                if (ix ==1):
                                    xsdf = deepcopy(lnclsectsdf.filter(like=str(xsoid),axis = 0))    
                                    xsdf.at[xsoid,'ObjectidM']=xsoid
                                    xsdf.at[xsoid,'ObjectidC']=loid
                                    xsdf.at[xsoid,'stconflict']=1
                                    xsdf.at[xsoid,'Primacy']=0
                                    xfeats = xsdf.spatial.to_featureset()
                                    resultx = lnclosureconflict.edit_features(adds=xfeats)
                                print("Overlap Detected oid {} Rte {} ;  begin {} ; end {} ; BMP : {} ; EMP : {} ; with Oid {} ; rte2 {} ; begin {} ; end {} ;   Dirn1 : {} ; Dirn2 {} ; {} side ; detected :res 1  {} ".format(
                                    xsoid,xrow.Route,xbegdte,xendte,xrow.BMP, xrow.EMP,loid, lrte, lbegdte,lendte,sdirn,ldirn, xrow.ClosureSide,resultl ))
                                logger.info("Overlap Detected oid {} Rte {} ;  begin {} ; end {} ; BMP : {} ; EMP : {} ; with Oid {} ; rte2 {} ; begin {} ; end {} ;   Dirn1 : {} ; Dirn2 {} ; {} side ; detected :res 1  {} ".format(
                                    xsoid,xrow.Route,xbegdte,xendte,xrow.BMP, xrow.EMP,loid, lrte, lbegdte,lendte,sdirn,ldirn, xrow.ClosureSide,resultl ))
                            #qryStr1 = "{} like '{}' ".format(fld1,appr1 ) # 'dot_kmurata')
                            val0 = '1' # "arthur.sickles@hawaii.gov" # "Mike.Medeiros@hawaii.gov" #   'kevin.a.murata@hawaii.gov'
                            fld0= "stconflict"
                            oids = "({},{})".format(xsoid,loid) # "arthur.sickles@hawaii.gov" # "Mike.Medeiros@hawaii.gov" #   'kevin.a.murata@hawaii.gov'
                            fld1= "OBJECTID"
                            expn = chr(123) + "\"field\" : \"{}\" , \"value\" : \"{}\"".format(fld0,val0) + chr(125) # 'value' : "\ ?dot_achung?  
                            # (where=qryStr0,calc_expression={"field" : "l2email" , "value" : "Mike.Medeiros@hawaii.gov"}) 
                            qryStr = "({} not like '{}') and ({} in {}) ".format(fld0,val0,fld1,oids ) # 'dot_kmurata')
                            resultupdate = wmlnclyrsects.calculate(where=qryStr,calc_expression={"field" : fld0 , "value" : val0})
                            print (" Update {} ; Expression  {} ; result {} ".format (qryStr,expn,resultupdate))
                            # query if there is already a reciprocal relationship with the same date time attributes
                            #qrydupconf = "parentglobalid like '{}' and parentglobalidC like '{}' and (beginDate= '{}' and enDate= '{}' and beginDateC= '{}' and enDateC= '{}')".format(consdf1.at[0,'parentglobalidC'],consdf1.at[0,'parentglobalid'],lbegdte,lendte,xbegdte,xendte)
                               # create a file for this conflict detail to email users 
                                
                except Exception as e:
                    print (" Error message : {} \n for survey  gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  sections failed to update. ".format
                            (str(e),xsparguid,xsoid,rteid,rdname,xbegdte,xendte,loctype))
                    logger.error(" Error message : {} \n for survey gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;   sections failed to update. ".format
                            (str(e),xsparguid,xsoid,rteid,rdname,xbegdte,xendte,loctype))
            
    except Exception as e:
        print (" Error message : {} \n for survey  gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  sections qry {} failed to update. ".format
                (str(e),xsparguid,xsoid,rteid,rdname,xbegdte,xendte,loctype,dteqryfrm))
        logger.error(" Error message : {} \n for survey gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ; sections qry {} failed to update. ".format
                (str(e),xsparguid,xsoid,rteid,rdname,xbegdte,xendte,loctype,dteqryfrm))
          
tend = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
logger.info (" End lane closure conflict processing of {} section features at {}. ".format (len(xftsect),tend))

print (" End lane closure conflict processing of {} section features ended at {}. ".format (len(xftsect),tend))
lnconfhdlr.close()
