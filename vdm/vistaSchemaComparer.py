#
## VOLDEMORT (VDM) VistA Comparer
#
# (c) 2012 Caregraf, Ray Group Intl
# For license information, see LICENSE.TXT
#

"""
VOLDEMORT Schema Comparison Report Generator

Generate Schema Comparison Reports using the VistaSchema module. Formats:
- HTML
  - references Google Table Javascript which allows re-ordering by column values
- Formatted Text
- CSV (for Excel)

For fields, the Comparer needs to distinguish the deprecated and corrupt before comparing.

Note: along with VistaSchema, this module can be part of a VOLDEMORT WSGI-based Web service. Such a service would allow analysis of any VistA with FMQL that is accessible from the service.

TODO - Changes/Additions Planned:
- refactor cnt passing out. Have report builder use Schema directly or do len(x)
- signal missing Package more clearly: beyond FOIA packages are generally missing; ditto for better handling of number ranges once accurate ones supplied
- option in Reporter to make separate reports for key areas
"""

import re
import json
import csv
import sys
import os
from datetime import datetime 
from collections import defaultdict
from vistaSchema import VistaSchema
from vdmU import HTMLREPORTHEAD, HTMLREPORTTAIL, WARNING_BLURB

__all__ = ['VistaSchemaComparer']

class VistaSchemaComparer(object):

    """
    A class to produce a report the compares a VistA against a baseline VistA. Can
    take different ReportBuilders so that the data can be represented in different
    forms (HTML, straight text; one file, many files etc.)
    
    TODO: strip out all counting here. Make ReportBuilders do it.
    """

    def __init__(self, baselineSchema, otherSchema, reportsLocation="Reports"):
        """
        @param baselineSchema: VistaSchema of baseline VistA. Usually "GOLD"
        @param otherSchema: VistaSchema of VistA being compared. Ex/ "WORLDVISTA"
        @param reportsLocation: Where to write reports. Defaults.
        """
        self.__bSchema = baselineSchema
        self.__oSchema = otherSchema
        self.__reportsLocation = reportsLocation
        if not os.path.exists(self.__reportsLocation):
            try:
                os.mkdir(self.__reportsLocation)
            except:
                raise Exception("Bad location for Comparison Reports: %s ... exiting" % reportsLocation)
        
    def compare(self, format="HTML"):
    
        if format == "HTML":
            rb = VSHTMLReportBuilder(self.__bSchema, self.__oSchema, self.__reportsLocation)
            self.__buildReport(rb) 
            reportLocation = rb.flush()
            return reportLocation
            
        raise ValueError("Unknown report format %s" % format)
        
    def __sortFiles(self, fileSet):
        return sorted(fileSet, key=lambda item: float(item))
        
    def __sortFields(self, fieldSet):
        return sorted(fieldSet, key=lambda item: float(item["number"]))
                
    def __buildReport(self, reportBuilder):
    
        baseOnlyFiles = self.__sortFiles(set(self.__bSchema.files(False)).difference(self.__oSchema.files(False)))
        otherOnlyFiles = self.__sortFiles(set(self.__oSchema.files(False)).difference(self.__bSchema.files(False)))
        bothFiles = self.__sortFiles(set(self.__bSchema.files(False)).intersection(self.__oSchema.files(False)))
                                
        # TODO: factor out this busy count gathering and put down in the Report Builder(s)
        counts = {} # we'll count as we go and then add to the report at the end. 
              
        #
        # For common files, first comparisons:
        # - common fields: renamed in other and deprecated in just one or other
        # - unique fields: deprecated or current
        # accounting for corruption. If either system's field is corrupt then no
        # comparison is made.
        # 
        reportBuilder.startInBoth()
        counts["noBNotOFields"] = 0
        counts["noONotBFields"] = 0
        counts["norenamedFields"] = 0
        realno = 0
        for no, fileId in enumerate(bothFiles, start=1):
            bsch = self.__bSchema.getSchema(fileId)
            osch = self.__oSchema.getSchema(fileId)
            if "corruption" in bsch or "corruption" in osch:
                continue
            loc = bsch["location"] if "location" in bsch else ""
            parents = bsch["parents"] if "parents" in bsch else None
            
            package = self.__bSchema.package(fileId)
            
            # Labs are special - don't count for now. Will change later.
            if re.match(r'63', fileId):
                reportBuilder.both(no, fileId, package, bsch["name"], osch["name"], loc, parents)
                continue
                
            #
            # Note: ids DON'T include multiples so counts won't either.
            #
            bFieldIds = self.__bSchema.fields(fileId) # base ids
            oFieldIds = self.__oSchema.fields(fileId) # other ids
            cFieldIds = set(bFieldIds).intersection(oFieldIds) # common ids
            bcFields = self.__bSchema.getFields(fileId, cFieldIds) # base, common
            ocFields = self.__oSchema.getFields(fileId, cFieldIds) # other, common
            baseSpecials = defaultdict(list)
            otherSpecials = defaultdict(list)
            # Common fields, catch differences
            # (a) name/different uses of fields AND (b) deprecated in one, not other
            for i in range(len(cFieldIds)):
                # won't note fields if corrupt in either
                if "corruption" in bcFields[i] or "corruption" in ocFields[i]:
                    continue
                # if deprecated in both then ignore
                if "deprecated" in bcFields[i] and "deprecated" in ocFields[i]:
                    continue
                # Dep only in base ie/ not unique but only base deps it
                if "deprecated" in bcFields[i]:
                    baseSpecials["depOnly"].append(bcFields[i])
                    continue
                # Dep only in other ie/ not unique but only other deps
                if "deprecated" in ocFields[i]:
                    otherSpecials["depOnly"].append(ocFields[i])
                    continue
                # TODO: get more refined: check inputTransform etc too
                if bcFields[i]["name"] != ocFields[i]["name"]:
                    otherSpecials["renamed"].append((ocFields[i], bcFields[i]))
                    counts["norenamedFields"] += 1 # total counts
            # Unique fields - distinguish unique but dep from unique
            if bFieldIds != oFieldIds: # there are unique fields
                bNotOFieldIds = set(bFieldIds).difference(oFieldIds) # base only ids
                if len(bNotOFieldIds):
                    counts["noBNotOFields"] += len(bNotOFieldIds) # total counts
                    # split uniques between dep and not and exclude corrupts
                    for i, field in enumerate(self.__bSchema.getFields(fileId, bNotOFieldIds)):
                        if "corruption" in field:
                            continue
                        if "deprecated" in field:
                            baseSpecials["uniqueDeps"].append(field)
                            continue
                        baseSpecials["uniques"].append(field)
                oNotBFieldIds = set(oFieldIds).difference(bFieldIds) # other only ids
                if len(oNotBFieldIds):
                    counts["noONotBFields"] += len(oNotBFieldIds) # add to total
                    # split uniques between dep and not and exclude corrupts
                    for i, field in enumerate(self.__oSchema.getFields(fileId, oNotBFieldIds)): # other unique
                        if "corruption" in field:
                            continue
                        if "deprecated" in field:
                            otherSpecials["uniqueDeps"].append(field)
                            continue
                        otherSpecials["uniques"].append(field)
                
            realno += 1
            reportBuilder.both(realno, fileId, package, bsch["name"], osch["name"], loc, parents, bsch["count"], osch["count"], len(bFieldIds), len(oFieldIds), baseSpecials, otherSpecials)

        reportBuilder.endBoth()
            
        self.__buildOneOnlyReport(reportBuilder, self.__bSchema, baseOnlyFiles, self.__bSchema.files(True), True)

        self.__buildCorruptionReport(reportBuilder, self.__bSchema, True)                            

        self.__buildOneOnlyReport(reportBuilder, self.__oSchema, otherOnlyFiles, self.__oSchema.files(True), False)
        
        self.__buildCorruptionReport(reportBuilder, self.__oSchema, False)
        
        # Number of fields in a system's top files; number in files shared by both systems
        counts["baseCountFields"] = self.__countFields(self.__bSchema, self.__bSchema.files(False))
        counts["baseBothCountFields"] = self.__countFields(self.__bSchema, bothFiles)     
        counts["otherCountFields"] = self.__countFields(self.__oSchema, self.__oSchema.files(False))
        counts["otherBothCountFields"] = self.__countFields(self.__oSchema, bothFiles)     
        
        allFiles = set(self.__bSchema.files(False)).union(self.__oSchema.files(False))

        # TODO: refactor - make reporter do this from Schema directly and some from Comparer arrays
        counts["baseDatapoints"] = self.__bSchema.datapoints()
        counts["otherDatapoints"] = self.__oSchema.datapoints()
        counts["allFiles"] = len(allFiles)
        counts["bothFiles"] = len(bothFiles)
        counts["baseFiles"] = self.__bSchema.countFiles(False)
        counts["baseTops"]= self.__bSchema.countFiles(True)
        counts["baseMultiples"] = counts["baseFiles"] - counts["baseTops"]
        counts["baseOnlyFiles"] = len(baseOnlyFiles)
        counts["basePopTops"] = self.__bSchema.countPopulatedTops()
        counts["otherFiles"] = self.__oSchema.countFiles(False)
        counts["otherTops"]= self.__oSchema.countFiles(True)
        counts["otherMultiples"] = counts["otherFiles"] - counts["otherTops"]
        counts["otherOnlyFiles"] = len(otherOnlyFiles)
        counts["otherPopTops"] = self.__oSchema.countPopulatedTops()
        
        reportBuilder.counts(counts) 
        
    def __buildCorruptionReport(self, reportBuilder, schema, base):
        """
        Corruption checking: in other reports if a file or field is corrupt in either system then no comparison is performed.
        """
        corruptFiles = {fl: schema.getSchema(fl)["corruption"] for fl in schema.filesWithAttr("corruption")}
        corruptFieldsOfFiles = {fl: schema.getFields(fl, schema.fieldsWithAttr(fl, "corruption")) for fl in schema.files() if len(schema.fieldsWithAttr(fl, "corruption"))}
        reportBuilder.startCorruption(base)
        reportBuilder.corruption(corruptFiles, corruptFieldsOfFiles)
        reportBuilder.endCorruption()
                                        
    def __buildOneOnlyReport(self, reportBuilder, schema, files, topFiles, base=True):
        reportBuilder.startOneOnly(len(files), base)
        reportBuilder.startOneOnlyGroup("topsClass1", "Top, Class 1", groupBlurb="Unique, complete, class 1 files ...")
        realNo = 0
        for no, fileId in enumerate(schema.filesWithoutAttr("class3", files), start=1):
            if fileId not in topFiles:
                continue
            sch = schema.getSchema(fileId)
            if "description" in sch:
                descr = sch["description"]["value"][:300] + " ..." if len(sch["description"]["value"]) > 300 else sch["description"]["value"]
                descr = descr.encode('ascii', 'ignore') # TODO: change report save
            else:
                descr = ""
            loc = sch["location"]
            realNo += 1
            reportBuilder.oneOnly(realNo, schema.package(fileId), fileId, sch["name"], loc, "", descr, noFields=len(schema.fields(fileId)), count=sch["count"], class3=None)
        reportBuilder.endOneOnlyGroup()
        reportBuilder.startOneOnlyGroup("topsClass3", "Top, Class 3", groupBlurb="Unique, complete, class 3 files ...")
        realNo = 0
        for no, fileId in enumerate(schema.filesWithAttr("class3", files), start=1):
            if fileId not in topFiles:
                continue
            sch = schema.getSchema(fileId)
            if "description" in sch:
                descr = sch["description"]["value"][:300] + " ..." if len(sch["description"]["value"]) > 300 else sch["description"]["value"]
                descr = descr.encode('ascii', 'ignore') # TODO: change report save
            else:
                descr = ""
            loc = sch["location"]
            realNo += 1
            reportBuilder.oneOnly(realNo, schema.package(fileId), fileId, sch["name"], loc, "", descr, noFields=len(schema.fields(fileId)), count=sch["count"], class3=sch["class3"])
        reportBuilder.endOneOnlyGroup()
        reportBuilder.startOneOnlyGroup("uniqueMultiples", "Unique Multiples in Shared Files", groupBlurb="Unique, multiples in shared files ...")
        realNo = 0
        for no, fileId in enumerate(files, start=1):
            if fileId in topFiles:
                continue
            sch = schema.getSchema(fileId)
            if "corruption" in sch:
                continue
            if "description" in sch:
                descr = sch["description"]["value"][:300] + " ..." if len(sch["description"]["value"]) > 300 else sch["description"]["value"]
                descr = descr.encode('ascii', 'ignore') # TODO: change report save
            else:
                descr = ""
            parents = sch["parents"]
            realNo += 1
            reportBuilder.oneOnly(realNo, schema.package(fileId), fileId, sch["name"], "", parents, descr, noFields=len(schema.fields(fileId)), count="-", class3=sch["class3"] if "class3" in sch else None)
        reportBuilder.endOneOnlyGroup()
        reportBuilder.endOneOnly() 
        
    def __countFields(self, schema, files):
        """ Smaller wrapper """
        return len(schema.allFieldsWithAttr(files=files))

class VSHTMLReportBuilder:
    # TODO: more direct sch access; Comparer should only pass in comparison calcs. Cut down on counts and do len(x) in here. ie/ counts if wanted should move in here.
    def __init__(self, bsch, osch, reportLocation):
        self.__bsch = bsch
        self.__osch = osch
        self.__bVistaLabel = bsch.vistaLabel
        self.__oVistaLabel = osch.vistaLabel
        self.__reportLocation = reportLocation
                       
    def startInBoth(self):
        bothStart = "<div class='report' id='both'><h2>Differences in Files common to Both</h2><p>Files common to both VistAs that have schema as opposed to content differences. Fields unique to %s (\"missing fields\") means %s has fallen behind and is missing some builds present in %s. Fields unique to %s (\"custom fields\") means it has added custom entries not found in %s. Entries labeled \"field name mismatch\" show fields with different names in each VistA. Some mismatches are superficial name variations but many represent the use of the same field for different purposes by each system. Finally, 'deprecated' fields are singled out. A deprecation by %s alone signals that %s has fallen behind.</p><p>Note that 'multiple fields' are not considered fields in this report - multiples are treated as (sub)files. And note that this list <span class='highlight'>does not highlight differences in Lab files</span> - local sites customize these extensively. There needs to be a separate 'lab differences' report.</p>" % (self.__bVistaLabel, self.__oVistaLabel, self.__bVistaLabel, self.__oVistaLabel, self.__bVistaLabel, self.__bVistaLabel, self.__oVistaLabel)
        self.__bothCompareItems = [bothStart, "<table>", self.__muTable(["Both #", "Name/ID/Locn", "# Entries", "Fields Missing", "Custom Fields"])]
                                                    
    def both(self, no, id, package, bname, oname, location, parents, bCount="-", oCount="-", noBFields=-1, noOFields=-1, baseSpecials={}, otherSpecials={}):
        
        # Don't show files with NO specials
        if not (len(baseSpecials) or len(otherSpecials)):
            return
    
        self.__bothCompareItems.append("<tr id='%s'><td>%d</td>" % (id, no))
        
        # Name difference
        if bname == oname:
            if bname[0] == "*":
                self.__bothCompareItems.append("<td class='highlight'><span class='titleInCol'>Pending Deletion</span><br/>" + bname + "<br/><br/>")
            else:
                self.__bothCompareItems.append("<td>%s<br/><br/>" % bname)                    
        else:
            self.__bothCompareItems.append("<td class='highlight'><span class='titleInCol'>File Name Mismatch</span><br/>" + bname + "<br/><br/>" + oname + "<br/><br/>")
                        
        # Top or Multiples marked up differently
        if location:
            self.__bothCompareItems.append("%s &nbsp;%s" % (id, self.__muLocation(location)))
        else:
            self.__bothCompareItems.append("%s" % (self.__muSubFileId(id, parents)))
            
        if package:
            self.__bothCompareItems.append("<br/><br/>" + package)

        self.__bothCompareItems.append("</td>")
            
        # Counts of entries
        if bCount == oCount:
            if bCount == "-":
                self.__bothCompareItems.append("<td/>")
            else:
                self.__bothCompareItems.append("<td>%s</td>" % bCount)
        else:
            self.__bothCompareItems.append("<td>%s<br/>%s</td>" % (bCount, oCount))

        if re.match(r'63', id):
            self.__bothCompareItems.append("<td colspan='2'>IGNORING - Lab files always different</td></tr>")
            return
            
        if len(baseSpecials):
            self.__bothCompareItems.append("<td>")
            if "uniques" in baseSpecials:
                self.__bothCompareItems.append("<div class='highlight'><span class='titleInCol'>%s</span><br/>%s</div>" % ("Baseline has %d unique fields out of %d" % (len(baseSpecials["uniques"]), noBFields), self.__muFields(baseSpecials["uniques"])))
            if "depOnly" in baseSpecials:
                self.__bothCompareItems.append("<br/>")
                self.__bothCompareItems.append("<div class='highlight'><span class='titleInCol'>%s</span><br/>%s</div>" % ("Baseline has %d fields only it deprecates out of %d" % (len(baseSpecials["depOnly"]), noBFields), self.__muFields(baseSpecials["depOnly"])))
            if "uniqueDeps" in baseSpecials:
                self.__bothCompareItems.append("<br/>")
                self.__bothCompareItems.append("<div><span class='titleInCol'>%s</span><br/>%s</div>" % ("Baseline has %d unique but deprecated fields out of %d" % (len(baseSpecials["uniqueDeps"]), noBFields), self.__muFields(baseSpecials["uniqueDeps"])))               
            self.__bothCompareItems.append("</td>")
        else:
            self.__bothCompareItems.append("<td/>")
            
        if not (len(otherSpecials)):
            self.__bothCompareItems.append("<td/></tr>")   
            return 
            
        self.__bothCompareItems.append("<td>")
        
        if "uniques" in otherSpecials:
            self.__bothCompareItems.append("<div><span class='titleInCol'>%s</span><br/>%s</div>" % ("Other has %d unique fields out of %d" % (len(otherSpecials["uniques"]), noOFields), self.__muFields(otherSpecials["uniques"])))
                    
        if "depOnly" in otherSpecials:
            self.__bothCompareItems.append("<br/>")
            self.__bothCompareItems.append("<div class='highlight'><span class='titleInCol'>%s</span><br/>%s</div>" % ("Other has %d fields only it deprecates out of %d" % (len(otherSpecials["depOnly"]), noOFields), self.__muFields(otherSpecials["depOnly"])))
                                
        if "uniqueDeps" in otherSpecials:
            self.__bothCompareItems.append("<br/>")
            self.__bothCompareItems.append("<div><span class='titleInCol'>%s</span><br/>%s</div>" % ("Other has %d unique but deprecated fields out of %d" % (len(otherSpecials["uniqueDeps"]), noOFields), self.__muFields(otherSpecials["uniqueDeps"])))               
                
        if "renamed" in otherSpecials:
            self.__bothCompareItems.append("<br/>")
            renamedFieldsMU = ""
            for baseField, otherField in otherSpecials["renamed"]:
                if renamedFieldsMU:
                    renamedFieldsMU += "<br/>"
                renamedFieldsMU += "%s: %s (base) -- %s (other)" % (baseField["number"], baseField["name"].lower(), otherField["name"].lower())
            self.__bothCompareItems.append("<div class='highlight'><span class='titleInCol'>%s</span><br/>%s</div>" % ("Field Name Mismatch", renamedFieldsMU))        
                          
        self.__bothCompareItems.append("</td>")
        
        self.__bothCompareItems.append("</tr>")
        
    def __muFields(self, fields):
        muFields = []
        for field in fields:
            fieldNumberMU = field["number"]
            if "class3" in field and field["class3"]:
                fieldNumberMU = self.__muClass3(field["class3"])
            fname = field["name"].lower() if "name" in field else "<CORRUPT FIELD>"
            muFields.append(fname + " (%s)" % fieldNumberMU)
        return ", ".join(muFields)
        
    def __muClass3(self, class3):
        return class3[0] + " [<strong>" + class3[1] + "</strong>]"
                
    def endBoth(self):
        self.__bothCompareItems.append("</table></div>")
        
    def startOneOnly(self, uniqueCount, base=True):
        BASEBLURB = "%d files are unique to %s. Along with missing fields, these files indicate baseline builds missing from %s. The Build reports cover builds in more detail." % (uniqueCount, self.__bVistaLabel, self.__oVistaLabel)
        OTHERBLURB = "%d files are unique to %s. Along with custom fields added to common files, these indicate the extent of custom functionality in this VistA. Descriptions should help distinguish between files once released centrally by the VA - 'Property of the US Government ...' - and files local to the VistA being compared." % (uniqueCount, self.__oVistaLabel)
        self.__oneOnlyIsBase = base
        oneOnlyStart = "<div class='report' id='%s'><h2>Files only in %s </h2><p>%s</p>" % ("baseOnly" if base else "otherOnly", self.__bVistaLabel if base else self.__oVistaLabel, BASEBLURB if base else OTHERBLURB)
        self.__oneOnlyItems = [oneOnlyStart]
        
    def startOneOnlyGroup(self, groupId, groupHeader, groupBlurb):
        self.__oneOnlyItems.append(self.__muSection(groupId + ("Base" if self.__oneOnlyIsBase else "Other"), groupHeader, groupBlurb, "h3"))
        self.__oneOnlyItems.append(self.__muTable(["#", "ID/Locn", "Name", "# Fields", "# Entries", "Description (first part)"]))
        
    def oneOnly(self, no, package, fileId, name, location, parents, descr, noFields, count, class3):
        items = [no]
        packageMU = "<br/><br/>" + package if package else ""
        if location:
            fileIdMU = fileId if not class3 else self.__muClass3(class3)
            nameMU = "<strong>" + name + "</strong><br/>PENDING DELETION" if re.match(r'\*', name) else name
            items.extend([fileIdMU + "<br/><br/>" + self.__muLocation(location), nameMU + packageMU])
        else:
            items.extend([self.__muSubFileId(fileId, parents), name + packageMU])
        items.extend([noFields, count, descr])
        self.__oneOnlyItems.append(self.__muTR(items, fileId))
                
    def endOneOnlyGroup(self):
        self.__oneOnlyItems.append("</table></div>")
        
    def endOneOnly(self):
        self.__oneOnlyItems.append("</div>")
        if self.__oneOnlyIsBase:
            self.__baseOnlyItems = self.__oneOnlyItems
        else:
            self.__otherOnlyItems = self.__oneOnlyItems
            
    def startCorruption(self, base=True):
        self.__corruptionIsBase = base
        self.__corruptionItems = ["<div class='report' id='%s'><h2>Corruption in %s</h2><p>Schema Corruption must be accounted for when comparing VistA Schemas. Review <a href='https://github.com/caregraf/FMQL/wiki/FileMan-Dictionary-Corruption-caught-by-FMQL'>FMQL's Corruption Checking</a>.</p>" % ("corruptInBase" if base else "corruptInOther", self.__bVistaLabel if base else self.__oVistaLabel)]
        
    def corruption(self, corruptFiles, corruptFieldsOfFiles):
        self.__corruptionItems.append("<p>Corrupt files ...</p>")
        self.__corruptionItems.append(self.__muTable(["#", "Corruption"]))
        corruptFilesByType = defaultdict(list)
        for fl, corrupt in corruptFiles.items():
            corruptFilesByType[re.sub(r'[^:]+: ', '', corrupt)].append(fl)
        for cft in corruptFilesByType:
            self.__corruptionItems.append(self.__muTR([cft, ", ".join(corruptFilesByType[cft])]))
        self.__corruptionItems.append("</table>")
        self.__corruptionItems.append("<p>Corrupt fields in files ...</p>")
        fls = sorted(corruptFieldsOfFiles.keys())
        self.__corruptionItems.append(self.__muTable(["#", "Field/Corruption"]))
        for fl in fls:
            mu = ""
            for entry in corruptFieldsOfFiles[fl]:
                if mu:
                    mu += ", "
                mu += entry["number"] + " (" + entry["corruption"] + ")"
            self.__corruptionItems.append(self.__muTR([fl, mu]))
        self.__corruptionItems.append("</table>")
        
    def endCorruption(self):
        self.__corruptionItems.append("</div>")
        if self.__corruptionIsBase:
            self.__baseCorruptionItems = self.__corruptionItems
        else:
            self.__otherCorruptionItems = self.__corruptionItems
        
    def counts(self, counts): 
        # TODO: will split: only diff cnts will come from builder; get others direct from sch. Too much indirection here to be maintainable.
        items = [
            ("Overall", "%d files, %d in both" % (counts["allFiles"], counts["bothFiles"])), 
            ("%s (\"Baseline\")" % self.__bVistaLabel, "%d datapoints, %d files, %d tops, %d multiples, <span class='highlight'>%d unique</span>, %d populated (%.1f%%)<br/>%d fields, %d in shared files, <span class='highlight'>%d unique</span>" % (counts["baseDatapoints"], counts["baseFiles"], counts["baseTops"], counts["baseMultiples"], counts["baseOnlyFiles"], counts["basePopTops"], round(((float(counts["basePopTops"])/float(counts["baseTops"])) * 100), 2), counts["baseCountFields"], counts["baseBothCountFields"], counts["noBNotOFields"]),
            ), 
            ("%s (\"Other\")" % self.__oVistaLabel, "%d datapoints, %d files, %d tops, %d multiples, <span class='highlight'>%d unique (%.1f%%)</span>, %d populated (%.1f%%)<br/>%d fields, %d in shared files, %d unique, %d repurposed, <span class='highlight'>%d custom (%.1f%%)</span>" % (counts["otherDatapoints"], counts["otherFiles"], counts["otherTops"], counts["otherMultiples"], counts["otherOnlyFiles"], round(((float(counts["otherOnlyFiles"])/float(counts["otherFiles"])) * 100), 2), counts["otherPopTops"], round(((float(counts["otherPopTops"])/float(counts["otherTops"])) * 100), 2), counts["otherCountFields"], counts["otherBothCountFields"], counts["noONotBFields"], counts["norenamedFields"], counts["noONotBFields"] + counts["norenamedFields"], round(((float(counts["noONotBFields"] + counts["norenamedFields"])/float(counts["otherBothCountFields"])) * 100), 2)))
        ]
        self.__countsETCMU = "<div class='report' id='counts'><h2>Schema Counts</h2><dl>" + self.__muDTD(items) + "</dl></div>"
                                
    def flush(self):
    
        reportHead = (HTMLREPORTHEAD % ("Schema Comparison Report << VOLDEMORT", " VOLDEMORT Schema Comparison Report"))
        blurb = "<p>Compare two VistA versions, %s ('Other') against %s ('Baseline'). This report shows which files and fields are shared and which are exclusive to one or other VistA. For each file, the report also gives a count of its entries as reported by its FileMan. All distinctions are not equal - highlights are in grey.</p>" % (self.__oVistaLabel, self.__bVistaLabel)
        warning = "<p><strong>Warning:</strong> %s</p>" % WARNING_BLURB if WARNING_BLURB else ""
        nav = "<p>Jump to: <a href='#counts'>Counts</a> | <a href='#both' class='highlight'>In Both</a> | <a href='#%s' class='highlight'>%s Only</a> (Class <a href='#topsClass1Other'>1</a>, <a href='#topsClass3Other'>3</a>, <a href='#uniqueMultiplesOther'>Multiples</a>) | <a href='#%s' class='highlight'>%s Only</a> (Class <a href='#topsClass1Base'>1</a>, <a href='#topsClass3Base'>3</a>, <a href='#uniqueMultiplesBase'>Multiples</a>) | Corruption (<a href='#corruptInOther'>%s </a>, <a href='#corruptInBase'>%s</a>)</p>" % ("otherOnly", self.__oVistaLabel, "baseOnly", self.__bVistaLabel, self.__oVistaLabel, self.__bVistaLabel)
        reportTail = HTMLREPORTTAIL % datetime.now().strftime("%b %d %Y %I:%M%p")
        
        reportItems = [reportHead, blurb, warning, nav, self.__countsETCMU]
        reportItems.extend(self.__bothCompareItems)
        reportItems.extend(self.__otherOnlyItems)
        reportItems.extend(self.__otherCorruptionItems)
        reportItems.extend(self.__baseOnlyItems)
        reportItems.extend(self.__baseCorruptionItems)
        reportItems.append(reportTail)
                
        reportFileName = self.__reportLocation + "/" + "schema%s_vs_%s.html" % (re.sub(r' ', '_', self.__bVistaLabel), re.sub(r' ', '_', self.__oVistaLabel))
        with open(reportFileName, "w") as reportFile:
            for reportItem in reportItems:
                reportFile.write(reportItem)
        return reportFileName
        
    def __muSection(self, id, header="", blurb="", h="h2"):
        return "<div id='%s'>" % id + "<%s>" % h + header + "</%s>" % h if header else "" + "<p>" + blurb + "</p>" if blurb else "" 
        
    def __muTable(self, colNames):
        return "<table>" + self.__muTR(colNames, td="th")

    def __muTR(self, items, id="", td="td"):
        return ("<tr id='%s'>" % id if id else "<tr>") + self.__muTD(items, td) + "</tr>"
        
    def __muTD(self, items, td="td"):
        return "".join("<%s>" % td + str(item) + "</%s>" % td for item in items)
        
    def __muDTD(self, items):
        itemsMU = ""
        for dt, dd in items:
            itemsMU += "<dt>" + dt + "</dt><dd>" + dd + "</dd>"
        return "".join(itemsMU)
        
    def __muLocation(self, location):
        if not location:
            return ""
        locationPieces = location.split("(")
        return "<span class='marray'>%s</span>%s" % (locationPieces[0], "(" + locationPieces[1] if locationPieces[1] else "")
        
    def __muSubFileId(self, fileId, parents):
        ids = parents
        ids.append(fileId)
        subFileId = ""
        indent = ""
        indentInc = "&nbsp;&nbsp;&nbsp;"
        for id in ids:
            idMU = id
            if subFileId:
                subFileId += "<br/>"
            subFileId = subFileId + indent + idMU
            indent = indent + indentInc
        return subFileId
    
class VSFormattedTextReportBuilder:
    """
    See: http://www.afpy.org/doc/python/2.7/library/textwrap.html
    
    Also: %*s and max(len(w) for w in x) ie. set width to fit biggest
    """
    def __init__(self):
        pass
        
    def counts(self):
        # %6s etc. ie. tables
        pass
        
class VSCSVReportBuilder:

    """
    CSV or consider xlrd module
    - dialect=csv.excel
    """

    def __init__(self):
        pass
    
# ######################## Module Demo ##########################

def demo():
    """
    Demo expects GOLD to be in its Cache and runs against Caregraf's web-hosted version of OpenVistA 'CGVISTA'
    
    Running this and the result:
    $ python vistaSchemaComparer.py
    GOLD: Schema Building (with caching) took 0:00:03.548585
    CGVISTA: Schema Building (with caching) took 0:00:02.408705
    Report written to Reports/schemaGOLD_vs_CGVISTA.html
    """
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from copies.fmqlCacher import FMQLCacher
    gCacher = FMQLCacher("Caches")
    gCacher.setVista("GOLD")
    oCacher = FMQLCacher("Caches")
    oCacher.setVista("CGVISTA", "http://vista.caregraf.org/fmqlEP")
    vsr = VistaSchemaComparer(VistaSchema("GOLD", gCacher), VistaSchema("CGVISTA", oCacher))
    reportLocation = vsr.compare(format="HTML")
    print "Report written to %s" % reportLocation
        
if __name__ == "__main__":
    demo()
