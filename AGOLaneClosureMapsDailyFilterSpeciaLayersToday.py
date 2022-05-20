from arcgis.gis import GIS

from arcgis.mapping import WebMap

import datetime, tzlocal, time, os
from datetime import date , datetime, timedelta
from time import  strftime


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

#BeginDateName,EndDateName:  The month and the day portion of the begin or end date. (ex. November 23)
def dtemon(dte):
    #dtext = datetime.strftime(dte-timedelta(hours=10),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=10),"%d")))
    dtext = datetime.strftime(dte-timedelta(hours=0),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=0),"%d")))
    return dtext

# BeginDay, EndDay: Weekday Name of the begin date (Monday, Tuesday, Wednesday, etc.)
def daytext(dte):
    dtext = datetime.strftime(dte-timedelta(hours=0),"%A") 
    return dtext

def dateonly(bdate,h1=0):
    datemid = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=h1)
    return datemid

#BeginTime, EndTime: The time the lane closure begins.  12 hour format with A.M. or P.M. at the end
def hrtext(dte):
    hrtext = datetime.strftime(dte-timedelta(hours=0),"%I:%M %p") 
    return hrtext

def strfwkdte(xt,h1=0):
    wkdte = 1
    if pd.notna(xt):
        wkdte = date.strftime(xt- timedelta(hours=h1),"%w") 
    
    return wkdte



import logging, re

import random 

logpath = r"D:\myfiles\HWYAP\laneclosure\logs"
rptpath = r"D:\myfiles\HWYAP\laneclosure\reports"

logger = logging.getLogger('Filterlaneclosections')
logfilenm = r"lanclosuredailyfilters.log"
logfile = os.path.join(logpath,logfilenm) # r"conflaneclosections.log"
fullnclhdlr = logging.FileHandler(logfile) # 'laneclosections.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
fullnclhdlr.setFormatter(formatter)
logger.addHandler(fullnclhdlr)
logger.setLevel(logging.INFO)

recdate = datetime.strftime(datetime.today(),"%m-%d-%Y %H:%M:%S")

# ArcGIS user credentials to authenticate against the portal
credentials = { 'userName' : 'dot_mmekuria', 'passWord' : 'ChrisMaz@2'}
#credentials = { 'userName' : 'dot_cdelatorre', 'passWord' : 'Bongo123'}
#credentials = { 'userName' : arcpy.GetParameter(4), 'passWord' : arcpy.GetParameter(5)}
userName = credentials['userName'] # arcpy.GetParameter(2) # "dot_mmekuria"
passWord = credentials['passWord'] # arcpy.GetParameter(3) # 

# Address of your ArcGIS portal
portal_url = r"http://histategis.maps.arcgis.com/" # r"https://www.arcgis.com/" # 
#print("Temp path : {}".format(tempPath))

print("Connecting to {}".format(portal_url))
#qgis =  GIS(profile="chrisagolprof") # GIS(portal_url, userName, passWord) #  GIS(portal_url, userName, passWord)
qgis =  GIS(portal_url, userName, passWord) # GIS(profile="hisagolprof") #
numfs = 10000 # number of items to query
print(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")
numfs = 10000 # number of items to query
itypelaneclosure=  "Web Map" #  "Feature Layer" # "Feature Service" #   "Feature Layer" # "Service Definition"
ekOrg = False
utcdiff = 10

laneclosureTitlebase = 'Lane_Closure_Approval_View_Public_Access' # 'Lane_Closure_WFL1_View_NoEd' #  'Lane_Closure_WebMap_WFL1' #  'Lane_Closure_WFL1_View_NoEd - Lane_Closure_Sections - Full Closures' #    'Lane_Closure_WFL1_View_NoEd - Lane_Closure_Sections - Shoulder Closures - Monday' 


def layerfilter(laneclosureTitle,genfilter,fullfilter,shlfilter,hartfilter,otherfilter,curdate,daynm,isunday,yesdate0,tomdate0,yesdate1,tomdate1,n1):
    # search for lane closure source data
    print("Searching for lane closure views {} from {} item for Service Title {} on AGOL...".format(itypelaneclosure,portal_url,laneclosureTitle))
    logger.info("Searching for lane closure views {} from {} item for Service Title {} on AGOL...".format(itypelaneclosure,portal_url,laneclosureTitle))
    wmapclosures =  webexsearch(qgis, laneclosureTitle, userName, itypelaneclosure,numfs,ekOrg)
    #wm = WebMap(wmapclosures.id)
    if wmapclosures != None:
        # filter lane closures layers
        lnclwm = qgis.content.get(wmapclosures.id) #flcolclosure = fcolsearch(qgis, laneclosureTitle, userName, itypelaneclosure,numfs,ekOrg) #FeatureLayerCollection.fromitem(found_item)(qgis,layer_name)
        #print (" Contents of {}  webmap : {} and layer collections {} ; Properties item Id : {} ; ".format(laneclosureTitle)) #,wm,wm.operationalLayers,wm.applicationProperties))
        print (" Contents of {}  webmap : {} ; queried by id {} ; Id {} ; ".format(laneclosureTitle,wmapclosures,lnclwm,wmapclosures.id)) #.layers)) #,wm.operationalLayers,wm.applicationProperties))
        #sdItem = qgis.content.get(lcwebmapid)
        #for key in lnclwm.keys():
        wmaplncl =WebMap(wmapclosures)
        lnclyrs = wmaplncl.layers
        print (" Contents of {}  webmap : {} ; layers {} ;  ".format(laneclosureTitle,lnclwm,list(lnclyrs[0].id))) # [0].layerDefinition)) #.layers)) #,wm.operationalLayers,wm.applicationProperties))
        for i,lyr in enumerate(lnclyrs,0):
            lyrdefex = ''
            #lyrdef = lyr['layerDefinition']
            lyrdef =  lyr.layerDefinition
    ##                    curdate = dateonly(datetime.today(),0)+timedelta(days=n1)
    ##                    daynm = datetime.strftime((curdate),"%A")
    ##                    daynum = datetime.strftime((curdate),"%w")
    ##                    yesdate = datetime.strftime(curdate-timedelta(hours=8)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M:%S")
    ##                    tomdate = datetime.strftime(curdate+timedelta(days=1)+timedelta(hours=6)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M:%S")    
            if 'definitionExpression' in lyr: #'Begin_and_End_Points' in lyr.title: 
                lyrdefex =  lyr.layerDefinition["definitionExpression"]
                lyrdefand = lyrdefex.split('AND')
                print(" Layer : {} ; num : {} ; id : {} ; title : {} ; ItemId : {} ; Lyr Def : {} ; SplAnd : {}  ".format(laneclosureTitle,i,lyr.id,lyr.title,lyr.itemId,lyrdefex,lyrdefand))
            else:
                if 'layerDefinition' in lyr:
                    lyrdefand = lyrdef
                    lyrdefex = ''
                    logger.info(" Layer : {} ; num : {} ; id : {} ; title : {} ; ItemId : {} ; Lyr {}  ".format(laneclosureTitle,i,lyr.id,lyr.title,lyr.itemId,lyrdef))
                    #print(" Layer : {} ; num : {} ; id : {} ; title : {} ; ItemId : {} ; Lyr {}  ".format(laneclosureTitle,i,lyr.id,lyr.title,lyr.itemId,lyrdef))
                else:
                    lyrdef = { }
                    lyrdefex = ''
                    lyr.update(lyrdef)
                    #print(" Layer : {} ; num : {} ; id : {} ; title : {} ; ItemId : {} ; Lyr {}  ".format(laneclosureTitle,i,lyr.id,lyr.title,lyr.itemId,lyrdef))
                    logger.info(" Layer : {} ; num : {} ; id : {} ; title : {} ; ItemId : {} ; Lyr {}  ".format(laneclosureTitle,i,lyr.id,lyr.title,lyr.itemId,lyrdef))
            #newfilter = [ x for x in lyrdefand if not ('timestamp' ) in x ]
            genfilter = ["(Active = '1')", "(DIRPInfo = 'Yes')" ]
            if 'Full Closures' in lyr.title:
                genfilter.append(fullfilter)
            elif 'Shoulder Closures' in lyr.title:   
                genfilter.append(shlfilter)
            elif 'HART Closures' in lyr.title:   
                genfilter.append(hartfilter)
            elif 'Lane_Closure_Begin_and_End_Points' in lyr.title:   
                pass
            elif 'Lane Closure Mid Point Features - Short Closures' in lyr.title:   
                pass
            else:
                genfilter.extend(otherfilter)

            print (" Contents of {}".format(genfilter) ) 
            txtfilter = " and ".join(x for x in genfilter ) 
            print (" primary filter : {}".format(txtfilter) )
            txtfildate =  "((beginDate between timestamp '{}' and timestamp '{}') or (enDate between timestamp '{}' and timestamp '{}')) and (beginDate <> enDate)  and ({})".format(yesdate0,tomdate0,yesdate1,tomdate1,txtfilter)  
            print (" with date filter : {}".format(txtfildate) )
            lyr.layerDefinition["definitionExpression"] =  txtfildate 
            logger.info(" Layer : {} ; num : {} ; id : {} ; ".format(laneclosureTitle,i,lyr.id,txtfildate))
            print(" Layer : {} ; num : {} ; id : {} ; ".format(laneclosureTitle,i,lyr.id,txtfildate))
            wmaplncl.update()
    #                testwm = WebMap(wmaplncl)
            dictwmaplncl = ("maplncl defs : {} ; Basemap : {}".format(dict(wmaplncl.definition),wmaplncl.basemap)) 
            lyrdef = lyr.layerDefinition["definitionExpression"]
    #                dictestmap = ("testwm defs : {} ; Basemap : {}".format(dict(testwm.definition),testwm.basemap)) 
            logger.info(" Layer : {} ; num : {} ; id : {} ; title : {} ; ItemId : {} ; Lyr {}  ".format(laneclosureTitle,i,lyr.id,lyr.title,lyr.itemId,lyr.layerDefinition))

    return wmapclosures

try:
    #lyrnm = clyrs[0].properties.name 
    wkend = False
    for n1 in range(0,7):
        curdate = dateonly(datetime.today(),0)+timedelta(days=n1)  # assume that the date and time is right after midnight of the new day.
        daynm = datetime.strftime((curdate),"%A")
        daynum = datetime.strftime((curdate),"%w")
        isunday = daynm == 'Sunday'
        yesdate0 = datetime.strftime(curdate-timedelta(hours=8)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M:%S")
        tomdate0 = datetime.strftime(curdate+timedelta(days=1)+timedelta(hours=6)-timedelta(minutes=5)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M:%S")
        yesdate1 = datetime.strftime(curdate-timedelta(hours=8)+timedelta(minutes=5)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M:%S")
        tomdate1 = datetime.strftime(curdate+timedelta(days=1)+timedelta(hours=6)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M:%S")
        if ('Saturday' in daynm ):
            tomdate = datetime.strftime(curdate+timedelta(days=2)+timedelta(hours=6)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M")
            daynm = 'Weekend'
##        elif ('Sunday' in daynm):
##            yesdate = datetime.strftime(curdate-timedelta(days=2)+timedelta(hours=16)+timedelta(hours=utcdiff),"%m-%d-%Y %H:%M")
##            daynm = 'Weekend'
        else:
            pass
        if not isunday:
            # set filters for all days except Sunday
            laneclosureTitle = "{}_{}".format(laneclosureTitlebase,daynm) # 'Lane_Closure_WFL1_View_NoEd' #  'Lane_Closure_WebMap_WFL1' #  'Lane_Closure_WFL1_View_NoEd - Lane_Closure_Sections - Full Closures' #    'Lane_Closure_WFL1_View_NoEd - Lane_Closure_Sections - Shoulder Closures - Monday' 
            #  'Lane_Closure_Approval_View_Public_Access_Monday' # 'HIDOTLRS' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
            genfilter =  "(Active = 1) AND (DIRPInfo = 'Yes')" 
            fullfilter = "(ClosureSide = 'Full')"
            shlfilter = "(CloseFact = 'Shoulder')"
            hartfilter = "(ClosReason = 'HART')"
            otherfilter = ["(ClosReason <> 'HART')", "(CloseFact <> 'Shoulder')" , "(ClosureSide <> 'Full')"] # ["(ClosReason <> 'Construction')", "(CloseFact <> 'Shoulder')" , "(ClosureSide <> 'Full')"]
            # search for lane closure source data
            #wm = WebMap(wmapclosures.id)
            lnclwebmap = layerfilter(laneclosureTitle,genfilter,fullfilter,shlfilter,hartfilter,otherfilter,curdate,daynm,isunday,yesdate0,tomdate0,yesdate1,tomdate1,n1)
            if n1 == 0 :
                daynm = 'Today'
                laneclosureTitle = "{}_{}".format(laneclosureTitlebase,daynm) # 'Lane_Closure_WFL1_View_NoEd' #  'Lane_Closure_WebMap_WFL1' #  'Lane_Closure_WFL1_View_NoEd - Lane_Closure_Sections - Full Closures' #    'Lane_Closure_WFL1_View_NoEd - Lane_Closure_Sections - Shoulder Closures - Monday' 
                lnclwebmap = layerfilter(laneclosureTitle,genfilter,fullfilter,shlfilter,hartfilter,otherfilter,curdate,daynm,isunday,yesdate0,tomdate0,yesdate1,tomdate1,n1)
except Exception as e:
    print("Error updating map filter data {} on {}".format(str(e),recdate))
    logger.info("Error updating map filter data {} on {}".format(str(e),recdate))
