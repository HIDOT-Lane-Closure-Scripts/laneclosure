
# coding: utf-8


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
import sqlite3
from copy import deepcopy

import logging

logger = logging.getLogger('lnconfile')
logfilenm = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs\lanecloseconflicts.log"
logfilenm = r"\\HWYPB\NETSHARE\HWYA\HWY-AP\LaneClosure\logs\lanecloseconflicts.log"
lnconfhdlr = logging.FileHandler(logfilenm) # 'laneclosections.log')
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
tbeg = date.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())

logger.info("Spatial Conflict Processing begins at {} ".format(tbeg))

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

sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]

# conflict table data
lnclconflict_properties = {'title': 'Lane Closure Spatio-Temporal Conflicting Features',
                    'description':'Lane Closure Spatio-Temporal Conflicting Features created from sections dataframe ',
                    'tags': 'arcgis python api,pandas,dataframe,lane closure,spatio-temporal conflict'}
conflictitle = "Lane_Closure_Conflicts" # lnclconflict_properties['title'] # a8aa67
#servid = "5e2b34f30ab84b39a5d91f944581ef20"
conflayer = "Lane_Closure_Conflicts_Layer"
servtype = 'Feature Service' # "featureCollection" #

#conflictsect = qgis.content.search(query="5e2b34f30ab84b39a5d91f944581ef20") 

conflictsect =webexsearch(qgis, conflictitle, userName, servtype,numfs,ekOrg)

#lnclconfsdict = dict(lnclconfs.properties)
#lnclconfsjson = json.dumps({"featureCollection": {"layers": [sectsonflictdict]}})
conflyrs = conflictsect.layers
lnclosureconflict =  lyrsearch(conflyrs, conflayer)

# get sdf to be used for new section data insert operations
sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
# get sdf to be used for new section data insert operations

dt1 = 0
td = datetime.combine(date.today(),datetime.min.time())
dts = datetime.timestamp(td) 
begdts = datetime.fromtimestamp(int(round(dts,0)))
begdt = begdts - timedelta(days=dt1)  # /1e3 , tzlocal.get_localzone()),
dt2 = 7
endt = begdt + timedelta(days=dt2)  # /1e3 , tzlocal.get_localzone()),
#lncldtes = "(beginDate>= '{}')".format(begdt) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)
#lncldtes = "CreationDate>= '{}'".format('1-1-2018') # and enDate <= '{}')".format('1-15-2020','1-31-2020')
lncldtes = "(CreationDate>= '{}')".format(begdt) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)

conflicols = ['OBJECTID','parentglobalid','Route','RoadName','RteDirn','direct','beginDate','enDate','ClosReason','LocMode','ClosType','CloseFact','ClosureSide','ClosHours','SHAPE','stconflict']

sectqry = wmlnclyrsects.query(where=lncldtes) # ,out_fields=conflicols)

# delete data to be updated from the existing conflict layer based on query
oids = "'" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in  sectqry.features  if 'OBJECTID' in xfs.attributes ) + "'"
conflncldelqry = "(OBJECTIDM in ({}))".format(oids) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)
qrylnconf = lnclosureconflict.query(conflncldelqry)
if len(qrylnconf)>0:
    chkdel = lnclosureconflict.delete_features(conflncldelqry)
    print("Deleted {} features from {} using query {} : Results : {} ".format(len(qrylnconf),conflictitle,conflncldelqry,chkdel))
    logger.info("Deleted {} features from {} using query {} : Results : {} ".format(len(qrylnconf),conflictitle,conflncldelqry,chkdel))

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
conflictsdf = lnclsectsdf[conflicols]
confeats = conflictsdf.spatial.to_featureset()
xftsect = sectqry.features
scols = lnclsectsdf.columns


for mfeat in xftsect:
    #xsedte = mfeat.attributes['EditDate # mfeat.attributes['EdiTime # srow['EditDate']
    #if pd.isnull(xsedte):
    #    xsedte = datetime.today()
    #xsedts = datetime.timestamp(xsedte) # + datetime.timedelta(int(utcofs)))
    #xsedtsl = datetime.fromtimestamp(xsedts , tzlocal.get_localzone())
    xbegdt = mfeat.attributes['beginDate'] # srow['beginDate']
    xbegdte = datetime.fromtimestamp(xbegdt/1e3) # + datetime.timedelta(int(utcofs)))
    xbegdts = datetime.timestamp(xbegdte) # + datetime.timedelta(int(utcofs)))
    xendt = mfeat.attributes['enDate'] # srow['enDate']
    xendte = datetime.fromtimestamp(xendt/1e3)
    xendts = datetime.timestamp(xendte) # + datetime.timedelta(int(utcofs)))
    xbegdtel = datetime.fromtimestamp(xbegdts, tzlocal.get_localzone())
    xendtel = datetime.fromtimestamp(xendts, tzlocal.get_localzone())
    xsparguid = mfeat.attributes['parentglobalid'] # srow['parentglobalid']
    xsoid =  mfeat.attributes['OBJECTID'] # srow['OBJECTID']
    rteid = mfeat.attributes['Route']
    rdname = mfeat.attributes['RoadName']
    sdirn = mfeat.attributes['RteDirn']
    loctype = mfeat.attributes['LocMode']
    clostype = mfeat.attributes['ClosType']
    closedfact = mfeat.attributes['CloseFact']
    cloSide = mfeat.attributes['ClosureSide']
    closReason = mfeat.attributes['ClosReason']
    closHrs = mfeat.attributes['ClosHours']
    creatorid = mfeat.attributes['Creator']
    createdate = mfeat.attributes['CreationDate']
    editorid = mfeat.attributes['Editor']
    editordate = mfeat.attributes['EditDate']
    try:
    #    dtefilt = ((lnclsectsdf.beginDate>=xbegdt and lnclsectsdf.beginDate<=xendt) or (lnclsectsdf.enDate>=xbegdt and lnclsectsdf.enDate<=xendt))
        #xlfeatsdf = lnclsectsdf[(lnclsectsdf.beginDate>=xbegdt and lnclsectsdf.beginDate<=xendt) or (lnclsectsdf.enDate>=xbegdt and lnclsectsdf.enDate<=xendt)]
        dteqry = "(beginDate>= '{}' and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( xbegdte,xendte,xbegdte,xendte)
        qrysectdte = wmlnclyrsects.query(where=dteqry)
        qrysectdtesdf = qrysectdte.sdf
        lcols = qrysectdtesdf.columns
        #print(' Sect OID {} ; pgid : {} ; Creator : {} ; EditSectime : {} ;  Rte : {}  '.format(mfeat.attributes['OBJECTID,mfeat.attributes['parentglobalid, mfeat.attributes['Creator, mfeat.attributes['EditDate, mfeat.attributes['Route))
        # check the section for time and space conflicts
        xspolygeom = mfeat.attributes['SHAPE']
        xspolygon = xspolygeom.buffer(budist)
        geompart = arcgis.geometry.Geometry(xspolygeom)
        #confeats = arcgis.features.FeatureSet([mrow])
        #confsdf = confeats.sdf
    #    xspolybuf = arcgis.geometry.buffer(xspolygeom, spref, budist, bunit, out_sr=spref, buffer_sr=spref, union_results=True, geodesic=True, gis=None)
        #xslconsdf = featuplnclsdf[featuplnclsdf['globalid'].isin([xsparguid])]
        for lrow in qrysectdte:
            xlguid = lrow.attributes['globalid'] # getattr(lrow,'globalid') # lrow['globalid']
            xlparguid = lrow.attributes['parentglobalid']
            if xlparguid != xsparguid:
                try:
                    lspolygeom = lrow.attributes['SHAPE']
                    xlpolygon = lspolygeom.buffer(budist)
                    utcofs = -10 
                    geomdis = xlpolygon.disjoint(xspolygon)
                    #xledts = datetime.datetime.timestamp(xledte)
                    xloid = lrow.attributes['OBJECTID'] #lrow['objectid']
                    lrte = lrow.attributes['Route']
                    lpgid = lrow.attributes['parentglobalid']
                    ldirn = lrow.attributes['RteDirn']
                    #print(' OID Sect {} ; pgid : {} ;  LnClgid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(mfeat.attributes['OBJECTID,mfeat.attributes['parentglobalid, lrow.globalid, mfeat.attributes['Creator, mfeat.attributes['EditDate, lrow.EditDate,lrow.Route))
                    #print(' GID Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
                    if not geomdis: #and ldirn == sdirn :   
                        lbegdts = lrow.attributes['beginDate'] # srow['beginDate']
                        lbegdte = datetime.fromtimestamp(lbegdts/1e3) # + datetime.timedelta(int(utcofs)))
                        lendts = lrow.attributes['enDate'] # srow['enDate']
                        lendte = datetime.fromtimestamp(lendts/1e3)
                        lbegdtel = datetime.fromtimestamp(lbegdts/1e3, tzlocal.get_localzone())
                        lendtel = datetime.fromtimestamp(lendts/1e3, tzlocal.get_localzone())
                        if (lbegdts>=xbegdt and lbegdts<= xendt) or (lendts>=  xbegdt and lendts<= xendt):
                            lbmp = lrow.attributes['BMP']
                            lemp = lrow.attributes['EMP']
                            if lrow.attributes['stconflict'] != 1:
                                lrow.attributes['stconflict'] = 1
                                resultupd1= wmlnclyrsects.edit_features(updates=[lrow])
                            oidconflicts = "OBJECTID in ('" + str(xsoid) + "')"
                            sfeat = wmlnclyrsects.query(where=oidconflicts)
                            
                            feats = sfeat.features
                            feat0 = feats[0]
                            if feat0.get_value('stconflict') != 1:
                                updfeatval = feat0.set_value('stconflict',1)
                                #featdict = feat0.as_dict() 
                                #fset = sfeat.from_dict(featdict)
                                resultupd2 = wmlnclyrsects.edit_features(updates=[feat0])
                                
                                print("Overlap Detected oid {} Rte {} ;  begin {} ; end {} ; BMP : {} ; EMP : {} ; with Oid {} ; rte2 {} ; begin {} ; end {} ; BMP : {} ; EMP : {} ; Dirn1 : {} ; Dirn2 {} ; {} side ; detected :res 1  {} ; res2 {}".format(
                                    xsoid,mfeat.attributes['Route'],xbegdte,xendte,mfeat.attributes['BMP'], mfeat.attributes['EMP'],xloid, lrte, lbegdte,lendte,lbmp,lemp,sdirn,ldirn, mfeat.attributes['ClosureSide'],resultupd1,resultupd2 ))
            #                    print('Overlap section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited : {} ; detected : {} \n '.format(
            #                            mfeat.attributes['OBJECTID,lrte,mfeat.attributes['parentglobalid , lpgid,mfeat.attributes['Creator, mfeat.attributes['EditDate, xledte,resultupd2))
                                logger.info("Overlap Detected oid {} Rte {} ;  begin {} ; end {} ; BMP : {} ; EMP : {} ; with Oid {} ; rte2 {} ; begin {} ; end {} ; BMP : {} ; EMP : {} ; Dirn1 : {} ; Dirn2 {} ; {} side ; detected :res 1  {} ; res2 {}".format(
                                    xsoid,mfeat.attributes['Route'],xbegdte,xendte,mfeat.attributes['BMP'], mfeat.attributes['EMP'],xloid, lrte, lbegdte,lendte,lbmp,lemp,sdirn,ldirn, mfeat.attributes['ClosureSide'],resultupd1,resultupd2 ))
         #                   logger.info('Overlap section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited : {} ; detected : {} \n '.format(
         #                           mfeat.attributes['OBJECTID,lrte,mfeat.attributes['parentglobalid , lpgid,mfeat.attributes['Creator, mfeat.attributes['EditDate, xledte,resultupd2))
                            fsconf = arcgis.features.FeatureSet([mfeat])
                            
                            consdf1 = fsconf.sdf
                            
                            consdf1.at[0,'OBJECTIDM'] = xsoid
                            consdf1.at[0,'OBJECTIDC'] = lrow.attributes['OBJECTID']
                            consdf1.at[0,'parentglobalidC'] = lrow.attributes['parentglobalid']
                            consdf1.at[0,'RouteC'] = lrow.attributes['Route']
                            consdf1.at[0,'RoadNameC'] = lrow.attributes['RoadName']
                            consdf1.at[0,'RteDirnC'] = lrow.attributes['RteDirn']
                            consdf1.at[0,'directC'] = lrow.attributes['direct']
                            consdf1.at[0,'beginDateC'] = lrow.attributes['beginDate']
                            consdf1.at[0,'enDateC'] = lrow.attributes['enDate']
                            consdf1.at[0,'LocModeC'] = lrow.attributes['LocMode']
                            consdf1.at[0,'ClosTypeC'] = lrow.attributes['ClosType']
                            consdf1.at[0,'CloseFactC'] = lrow.attributes['CloseFact']
                            consdf1.at[0,'ClosureSideC'] = lrow.attributes['ClosureSide']
                            consdf1.at[0,'ClosHoursC'] = lrow.attributes['ClosHours']
                            consdf1.at[0,'ClosReasonC'] = lrow.attributes['ClosReason']
                            consdf1.at[0,'stconflict'] = 1
                            # query if there is already a reciprocal relationship with the same date time attributes
                            #qrydupconf = "parentglobalid like '{}' and parentglobalidC like '{}' and (beginDate= '{}' and enDate= '{}' and beginDateC= '{}' and enDateC= '{}')".format(consdf1.at[0,'parentglobalidC'],consdf1.at[0,'parentglobalid'],lbegdte,lendte,xbegdte,xendte)
                            qrydupconf = "OBJECTIDM = {} and OBJECTIDC = {}".format(consdf1.at[0,'OBJECTIDC'],consdf1.at[0,'OBJECTIDM'])
                            qdupliconflict= lnclosureconflict.query(where=qrydupconf)
                            if (len(qdupliconflict)<=0):
                                confeat = consdf1.spatial.to_featureset()
                                resultupd3 = lnclosureconflict.edit_features(adds=confeat)
                                # create a file for this conflict detail to email users 
                                
                except Exception as e:
                    print (" Error message : {} \n for survey  gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  sections failed to update. ".format
                            (str(e),xsparguid,xsoid,rteid,rdname,xbegdts,xendts,loctype))
                    logger.error(" Error message : {} \n for survey gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;   sections failed to update. ".format
                            (str(e),xsparguid,xsoid,rteid,rdname,xbegdts,xendts,loctype))
            
    except Exception as e:
        print (" Error message : {} \n for survey  gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  sections qry {} failed to update. ".format
                (str(e),xsparguid,xsoid,rteid,rdname,xbegdts,xendts,loctype,dteqry))
        logger.error(" Error message : {} \n for survey gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ; sections qry {} failed to update. ".format
                (str(e),xsparguid,xsoid,rteid,rdname,xbegdts,xendts,loctype,dteqry))
          
tend = date.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
logger.info (" End lane closure conflict processing of {} section features at {}. ".format (len(xftsect),tend))

print (" End lane closure conflict processing of {} section features ended at {}. ".format (len(xftsect),tend))
lnconfhdlr.close()
