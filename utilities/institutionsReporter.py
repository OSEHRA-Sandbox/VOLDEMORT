#
## VOLDEMORT (VDM) GOLD (FOIA) Institution Reporter
#
# (c) 2012 Caregraf, Ray Group Intl
# For license information, see LICENSE.TXT
#

"""
Produce reports on the VA institutions in FOIA. Part of generating "beyond VistA" meta data for VDM.

TODO:
- many missing a STATE. Can get off parents
- do hierarchal view by parents as well as facilities views ie/ show any hierarchies
"""

import os
import re
import urllib
import urllib2
import json
import sys
from datetime import timedelta, datetime 
import logging
import operator
from collections import OrderedDict, defaultdict
from vdm.copies.fmqlCacher import FMQLDescribeResult, FMQLCacher

"""
Report the Institution information of a system including noting the primary institution.
"""
def reportInstitutions(vistaCacher, reportBuilder, stateFilter=""):

    # First let's get the facility types
    facilityTypes = {}
    for i, fmqlResult in enumerate(vistaCacher.describeFileEntries("4_1", limit=100, cstop=10000)):
        iResult = FMQLDescribeResult(fmqlResult)
        facilityTypes[re.sub(r'\/', '_', iResult["name"])] = iResult["full_name"]
    reportBuilder.facilityTypes(facilityTypes)
            
    for i, fmqlResult in enumerate(vistaCacher.describeFileEntries("4", limit=100, cstop=10000), 1):
        iResult = FMQLDescribeResult(fmqlResult)
        if stateFilter and iResult.uriLabel("state") != stateFilter:
            continue
        visn = ""
        parent = ""
        for cnode in iResult.cnodesFD("associations"):
            # CNODE can have no data entries for VISN and PARENT FACILITY ex/ foia:4-123
            if cnode["parent_of_association"]:
                associationType = cnode.uriLabel("associations")
                if associationType == "VISN":
                    visn = cnode.uriLabel("parent_of_association")
                elif associationType == "PARENT FACILITY":
                    # Only record parent if not itself!
                    if cnode["parent_of_association"] != iResult.id:
                        # Don't want VISNs as parents. ex/ 4-340
                        parent = cnode.uriLabel("parent_of_association") if not re.match(r'VISN ', cnode.uriLabel("parent_of_association")) else ""
                elif associationType:
                    raise Exception("Unexpected association type %s" % associationType)
        taxes = iResult.cnodesFD("taxonomy_code")
        ft = iResult.uriLabel("facility_type")
        if len(taxes) > 1:
            raise Exception("Only expected one taxonomy per Institution")
        elif len(taxes):
            if taxes[0]["status"] != "ACTIVE":
                raise Exception("Only expected ACTIVE taxonomies")
            taxonomy = taxes[0].uriLabel("taxonomy_code")
        else:
            taxonomy = ""
        inactive = True if re.match('ZZ', iResult["name"]) or re.match(r'ZZ', iResult["official_va_name"]) or (iResult["inactive_facility_flag"] == "INACTIVE") else False
        reportBuilder.reportInstitution(i, iResult.id, iResult["name"], iResult["official_va_name"], iResult["npi"], iResult["station_number"], iResult.uriLabel("state"), ft, visn, parent, taxonomy, iResult["status"], iResult["agency_code"], inactive)
           
    reportBuilder.flush("foiaInstitutions" + stateFilter)
        
class InstitutionCSVMaker:
    
    def __init__(self, vistaLabel):
        self.__vistaLabel = vistaLabel
        self.data = "#,FOIAID,name,active,agency,official name,npi,station number,state,facility type,visn,parent,taxonomy\n"
        
    def facilityTypes(self, facilityTypes):
        pass
        
    def reportInstitution(self, no, id, name, officialVAName, npi, stationNumber, state, facilityType, visn, parent, taxonomy, status, agency, inactive=False):
        self.data += "%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (no, id, name, inactive, agency, officialVAName, npi, stationNumber, state, facilityType, visn, parent, taxonomy)
        
    def flush(self, where):
        fl = open(where + ".csv", "w")
        fl.write(self.data)
           
HEADMU = """
<!DOCTYPE html>
<html lang="en">
<head>
<title>VA Institutions << FOIA Reports</title>
<meta charset="utf-8" />
<link rel='stylesheet' href='http://www.caregraf.org/semanticvista/analytics/VOLDEMORT/voldemort.css' type='text/css'>
</head>
"""

TITLEMU = """
<h1>FOIA Institution File Contents</h1>
"""

TAILMU = """
<div><p>Generated using VOLDEMORT with FMQL</p></div></body></html>
"""

class InstitutionHTMLMaker:

    def __init__(self, vistaLabel):
        self.__vistaLabel = vistaLabel
        self.ftTable = defaultdict(list)
        self.inactives = []
        self.visnRef = defaultdict(int)
        self.parentRef = defaultdict(int)
        
    def facilityTypes(self, facilityTypes):
        self.__facilityTypes = facilityTypes
        self.__facilityTypes["__NOFT"] = "[Facility Type Unspecified]"
        
    def reportInstitution(self, no, id, name, officialVAName, npi, stationNumber, state, facilityType, visn, parent, taxonomy, status, agency, inactive=False):
        ft = "__NOFT" if not facilityType else facilityType
        if visn:
            self.visnRef[visn] += 1
        if parent:
            self.parentRef[parent] += 1
        data = {"no": no, "id": id, "name": name, "officialVAName": officialVAName, "npi": npi, "stationNumber": stationNumber, "state": state, "visn": visn, "parent": parent, "taxonomy": taxonomy, "inactive": inactive, "facilityType": facilityType, "agency": agency, "status": status}
        if inactive:
            self.inactives.append(data)
            return
        self.ftTable[ft].append(data)
        
    def __makeTR(self, no, data, all=True):
        """
        TODO: highlight no station number -
        - ok if status: Local or ft:VISN but otherwise should have one.
        """
        nameMU = data["name"]
        if data["name"] != data["officialVAName"]:
            nameMU += "<br/>" + data["officialVAName"]
        noMU = ""
        if data["name"] in self.visnRef:
            noMU += str(self.visnRef[data["name"]])
        if data["name"] in self.parentRef:
            noMU += "/" + str(self.parentRef[data["name"]])
        # return ''.join([`num` for num in xrange(loop_count)])
        # also look into zip statement (want a lot more of that)
        return "<tr id='%s'><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (data["name"], no, nameMU, data["id"], "<a href='#" + data["parent"] + "'>" + data["parent"] + "</a>" if data["parent"] else "", "<a href='#" + data["visn"] + "'>" + data["visn"] + "</a>" if data["visn"] else "", data["npi"], data["stationNumber"], data["state"], data["taxonomy"], data["agency"], noMU)        
        
    def flush(self, where):
        reportFile = open(where, "w")
        reportFile.write(HEADMU)
        ftsOrdered = sorted(self.ftTable.keys())
        reportFile.write(TITLEMU)
        noActive = 0
        for ft in self.ftTable:
            noActive += len(self.ftTable[ft])
        reportFile.write("<p>There are %d active and %d <a href='#inactive'>Inactive Facilities</a> - an <em>Inactive Facility</em> is one with its 'inactive facility flag' set OR a name that begins with 'ZZ'. The following lists the active facilities by facility type:</p>" % (noActive, len(self.inactives)))
        tocMU = "<ol>"
        for ft in ftsOrdered:
            tocMU += "<li><a href='#" + ft + "'>" + self.__facilityTypes[ft] + "</a> (" + ft + ") - " + str(len(self.ftTable[ft])) + "</li>"
        tocMU += "</ol><hr/>"
        ftsWithGlobalIds = []
        reportFile.write(tocMU)
        for no, ft in enumerate(ftsOrdered, 1):
            ftMU = self.__facilityTypes[ft] + " (" + ft + ")"
            reportFile.write("<h2 id='" + ft + "'>" + ftMU + "</h2>")
            # Now sort in here by ids
            # sorted(d.items(), key=lambda x: x["id"])
            fOrdered = sorted(self.ftTable[ft], key=lambda x: x["name"])
            noWithNPI = 0
            noWithTaxes = 0
            for noft, rowData in enumerate(fOrdered, 1):
                if rowData["npi"]:
                    noWithNPI += 1
                if rowData["taxonomy"]:
                    noWithTaxes += 1
            reportFile.write("<p>Of %d, %d have NPIs, %d have taxonomies</p>" % (noft, noWithNPI, noWithTaxes))
            if noWithNPI or noWithTaxes:
                ftsWithGlobalIds.append(ft)
            reportFile.write("<table><tr><th>#</th><th>Name</th><th>FOIA</th><th>Parent</th><th>VISN</th><th>NPI</th><th>Station Id</th><th>State</th><th>Taxonomy</th><th>Agency</th><th>References</th></tr>")
            for noft, rowData in enumerate(fOrdered, 1):
                reportFile.write(self.__makeTR(noft, rowData))
            reportFile.write("</table><hr/>")
        reportFile.write("<h2 id='inactive'>" + "* INACTIVE Facilities" + "</h2><table><tr><th>#</th><th>Name</th><th>FOIA</th><th>Parent</th><th>VISN</th><th>NPI</th><th>Station Id</th><th>State</th><th>Taxonomy</th><th>References</th></tr>")
        for i, data in enumerate(sorted(self.inactives, key=lambda x: x["name"]), 1):
            reportFile.write(self.__makeTR(i, data))
        reportFile.write("</table>")  
        # reportFile.write("<pre>" + str(ftsWithGlobalIds) + "</pre>")
        reportFile.write(TAILMU)          
        
def reportPackages(vistaCacher, reportBuilder):
    pass
        
# ##################### demo invoker ######################
    
def demo():

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    
    cacher = FMQLCacher("Caches")
    cacher.setVista("GOLD", "http://foiavista.caregraf.org/fmqlEP")
    reportInstitutions(cacher, InstitutionCSVMaker("GOLD"))
                
if __name__ == "__main__":
    demo()