#
## VOLDEMORT (VDM) VistA Comparer
#
# (c) 2012 Caregraf, Ray Group Intl
# For license information, see LICENSE.TXT
#

"""
Module for retrieving, caching and analysing a VistA's schemas returned by FMQL. It reflects FMQL's "cut to the data" approach towards VistA information where data is taken directly from FileMan and finessed by interested clients.

For all files, notes:
- number of entries according to FileMan
- all fields with types, names, input transforms, ranges of enums, formula's of computed fields etc.
- parent relationships
- class of file
- packages
- version of file (if in FileMan) and package ("version", "vpackage")
- corruption in file or field meta
and overall, the total number of data points represented by this information

TODO: 
- files with no fields ex/ ARCHIVAL ACTIVITY (skipping in comparer now)
- a lot more on Class 3 (for sub files too ie/ xxx.{\d+}?). Need range from VA.
- will move away from Cache indexing every time once Cacher supports flush back. Will then just iterate over data as needed. Try first in Builds.
"""

import os
import re
import csv
import urllib
import urllib2
import json
import sys
from collections import defaultdict
from datetime import timedelta, datetime 
import logging

__all__ = ['VistaSchema']

class VistaSchema(object):
    """
    Access to the cached FMQL descriptions of a Vista's Schema
    """
    
    def __init__(self, vistaLabel, fmqlCacher):
        self.vistaLabel = vistaLabel
        self.__fmqlCacher = fmqlCacher
        self.__loadNamespaces()
        self.__loadPackages()
        self.__makeSchemas() 

    def __loadNamespaces(self):
        # TODO: look at high/low
        # TODO: fix to use pkg_resources: http://peak.telecommunity.com/DevCenter/PythonEggs#accessing-package-resources
        reader = csv.DictReader(open(os.path.join(os.path.dirname(__file__), "resources/Namespaces.csv")), delimiter='\t')
        self.namespaces = {}
        for row in reader:
            self.namespaces[row["NUMBER"]] = row["NAME"]
        self.namespaces["214"] = "MEDSPHERE"
        
    def __loadPackages(self):
        """
        TODO: Strange - COUNTY (5.1) in ONCOLOGY but this matches OSEHRA
        """
        reader = csv.DictReader(open(os.path.join(os.path.dirname(__file__), "resources/Packages.csv")))
        packageName = ""
        self.__filePackages = {}
        for row in reader:
            # Despite name either single file id or nothing
            if not row["File Numbers"]:
                continue
            packageName = row["Package Name"] if row["Package Name"] else packageName
            self.__filePackages[row["File Numbers"]] = packageName    
        
    def __str__(self):
        return "Schema of %s" % self.vistaLabel
                
    def datapoints(self):
        cnt = 0
        for fl in self.__schemas:
            flInfo = self.__schemas[fl]
            cnt += len(flInfo.keys())
            if "corruption" in flInfo:
                continue
            cnt = cnt - 1 # take away one for fields list
            # rem: this doesn't count 'multiple' fields as nix'ed. Counted as files
            cnt += sum([len(fldInfo.keys()) for fldInfo in flInfo["fields"]])
        return cnt
        
    def package(self, file):
        return self.__schemas[file]["package"] if file in self.__schemas and "package" in self.__schemas[file] else ""
        
    def files(self, topOnly=False):
        """Convenience for this frequent option - doesn't include corrupt"""
        if topOnly:
            return self.filesWithoutAttr("corruption", self.filesWithoutAttr("parent"))
        return self.filesWithoutAttr("corruption")
        
    def countFiles(self, topOnly=False):
        return len(self.files(topOnly))
        
    def filesWithAttr(self, attribute, files=None):
        """
        The meta is constructed so that key aspects come with or without attributes:
        - class3 file will have the attribute "class3"
        - corrupt file will have the attribute "corrupt"
        etc.
        
        Other attributes to use: "parent", "corruptFields"
        
        Can recurse ie/ filesWithAttr("parent", filesWithAttr("class3"))
        """
        files = self.__schemas.keys() if not files else files
        return [fl for fl in files if attribute in self.__schemas[fl]]
        
    def filesWithoutAttr(self, attribute, files=None):
        """
        Opposite of "WithAttr"
        """
        files = self.__schemas.keys() if not files else files
        return [fl for fl in files if attribute not in self.__schemas[fl]]
        
    def filesWithAssertion(self, assertion, files=None):
        """TODO: reconsider: too hard to know that assert applies to fileInfo?"""
        files = self.__schemas.keys() if not files else files
        return [fl for fl in files if assertion(self.__schemas[fl])]
        
    def countPopulatedTops(self):
        """
        How many of top level files are populated - half in FOIA/GOLD
        """
        return len([fl for fl in self.files(True) if self.__schemas[fl]["count"] not in ["-", "0"]])      
        
    def getSchema(self, file):
        return self.__schemas[file]
                                            
    def fields(self, file, includeMultiples=False, corruptOnly=False):
        """
        Return field ids. Includes corrupt fields.
        """
        if corruptOnly:
            return self.fieldsWithAttr(file, "corruption")
        sch = self.getSchema(file)
        if "corruption" in sch:
            return []
        if includeMultiples:
            return [field["number"] for field in sch["fields"]]
        return self.fieldsWithoutAttr(file, "multiple")   
        
    def fieldsWithAttr(self, file, attr=None):
        """
        See list of attributes in 'getFields'
        """
        sch = self.getSchema(file)
        if "corruption" in sch:
            return []
        if not attr:
            return [field["number"] for field in sch["fields"]]
        return [field["number"] for field in sch["fields"] if attr in field]
        
    def fieldsWithoutAttr(self, file, attr):
        sch = self.getSchema(file)
        if "corruption" in sch:
            return []
        return [field["number"] for field in sch["fields"] if attr not in field]
                
    def allFieldsWithAttr(self, attr=None, files=None): 
        # across all files, None == all
        fields = []
        files = self.__schemas.keys() if not files else files
        for fl in files:
            fields.extend(map(lambda x: fl + ":" + x, self.fieldsWithAttr(fl, attr)))
        return fields
                                
    def getFields(self, file, fieldIds):
        """
        - Return in order of field number, the same order returned by FMQL
        - If corrupt, [corruption] gives why
        - If deprecated, then [deprecated] set to True
        
        Full list of properties: index  name  deprecated  description  number  inputTransform  flags  location  computation  computation001   hidden  type  details}
        """
        sch = self.getSchema(file)
        if not len(fieldIds):
            return []
        fields = []
        for field in sch["fields"]:
            if field["number"] in fieldIds:
                fields.append(field)
        return fields
        
    def getArrays(self):
        """
        Return list of MUMPS arrays used for all known files. Each one should
        be assigned to a Package. 
        
        Note: will probably do from a fixed list AS 
        """
        pass
                
    def __makeSchemas(self):
        """
        Index schema - will force caching if not already in cache
        """
        logging.info("%s: Schema - building Schema Index ..." % self.vistaLabel)
        self.__schemas = {}
        start = datetime.now()
        for i, dtResult in enumerate(self.__fmqlCacher.describeSchemaTypes()):
            fileId = dtResult["number"] if "number" in dtResult else re.sub('\_', '.', dtResult["fmql"]["TYPE"]) # account for error
            self.__schemas[fileId] = dtResult
            if "error" in dtResult:
                dtResult["corruption"] = dtResult["error"] # just to make symmetric with fieldInfo
                del dtResult["error"]
                continue
            if re.match(r'\*', dtResult["name"]):
                dtResult["deprecated"] = True
            # Nix Multiple fields. Sub files will refer up!
            dtResult["fields"] = [field for field in dtResult["fields"] if not ("type" in field and field["type"] == "9")]
            for field in dtResult["fields"]:
                if "corruption" in field:
                    dtResult["corruptFields"] = True
                    continue
                if self.__isClass3Number(field["number"]):
                    field["class3"] = self.__vaStationId(field["number"])
                if re.match(r'\*', field["name"]):
                    field["deprecated"] = True
                if "computation" in field and field["number"] == ".001":
                    field["computation001"] = field["computation"]
                    del field["computation"] # want to differentiate
        # Note parents once all schemas gathered
        for sch in self.__schemas.values():
            if "corruption" not in sch:
                self.__noteFileDetails(sch)
        logging.info("%s: ... building (with caching) took %s" % (self.vistaLabel, datetime.now()-start))
        
    def __noteFileDetails(self, sch):
        if "parent" in sch:
            parents = []
            psch = sch
            while "parent" in psch:
                parents.insert(0, psch["parent"])
                try:
                    psch = self.__schemas[psch["parent"]]
                except:
                    sch["corruption"] = "Invalid Parent: " + psch["parent"]
                    break
            sch["parents"] = parents
            if self.__isClass3Number(parents[0]):
                sch["class3"] = self.__vaStationId(parents[0])
            sch["count"] = "-" # may revisit. For ease of iteration.
            if parents[0] in self.__filePackages:
                sch["package"] = self.__filePackages[parents[0]]
        else:
            if self.__isClass3Number(sch["number"]):
                sch["class3"] = self.__vaStationId(sch["number"])
            # Do safe counting
            sch["count"] = "-" if "count" not in sch or sch["count"] == "" or sch["count"] == "0" else sch["count"]
            if sch["number"] in self.__filePackages:
                sch["package"] = self.__filePackages[sch["number"]]
            
    def __isClass3Number(self, id):
        """
        TODO: review - may not be true that all in this range are Class 3
        """
        prefix = re.split(r'[\.\_]', id)[0]
        return True if len(prefix) == 6 and int(prefix) > 101000 else False
        
    def __vaStationId(self, id):
        # TODO: add MSC etc which is five long - 214XX 
        idInt = id.split(".")[0]
        if len(idInt) != 6:
            return None
        vaStationId = re.match(r'(\d{3})', id).group(1)
        if vaStationId in self.namespaces:
            return (id, self.namespaces[vaStationId])
        return None

# ######################## Module Demo ##########################
                       
def demo():
    """
    Simple Demo of this Module
    
    Equivalent from command line:
    $ python
    ...
    >>> from copies.fmqlCacher import FMQLCacher 
    >>> cacher = FMQLCacher("Caches")
    >>> cacher.setVista("CGVISTA") 
    >>> from vistaSchema import *
    >>> var = VistaSchema("CGVISTA", cacher)
    >>> str(vair)
    'Schema of CGVISTA'
    >>> vair.getSchema("2")
    {...
    """    
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from copies.fmqlCacher import FMQLCacher
    cacher = FMQLCacher("Caches")
    # cacher.setVista("CGVISTA", "http://vista.caregraf.org/fmqlEP") 
    cacher.setVista("GOLD")
    
    # vair = VistaSchema("CGVISTA", cacher)
    vair = VistaSchema("GOLD", cacher)
    print "Number of files %d, top only %d" % (len(vair.files()), len(vair.files(True)))
    print "Corrupt Files %d, with corrupt fields %d, deprecated %d, class 3 %d, with .001 field %d, with version set %d" % (len(vair.filesWithAttr(attribute="corruption")), len(vair.filesWithAttr("corruptFields")), len(vair.filesWithAttr("deprecated")), len(vair.filesWithAttr("class3")), len(vair.allFieldsWithAttr("computation001")), len(vair.filesWithAttr("deprecated")))
    topFiles = vair.files(True)
    print "Corrupt Top Files %d, with corrupt fields %d, deprecated %d, class 3 %d, with .001 field %d" % (len(vair.filesWithAttr("corruption", topFiles)), len(vair.filesWithAttr("corruptFields", topFiles)), len(vair.filesWithAttr("deprecated", topFiles)), len(vair.filesWithAssertion(lambda x: "class3" in x, topFiles)), len(vair.allFieldsWithAttr("computation001", topFiles))) 
    print "Total fields %d, corrupt %d, computed fields %d, with transforms %d, with simple indexes %d, class 3 fields %d, class 3 fields in non class 3 files %d" % (len(vair.allFieldsWithAttr()), len(vair.allFieldsWithAttr("corruption")), len(vair.allFieldsWithAttr("computation")), len(vair.allFieldsWithAttr("inputTransform")), len(vair.allFieldsWithAttr("index")), len(vair.allFieldsWithAttr("class3")), len(vair.allFieldsWithAttr("class3", vair.filesWithoutAttr("class3"))))
    print "Number of data points: %d" % vair.datapoints()
    print "First 10 computed fields - %s" % str(vair.allFieldsWithAttr("computation")[0:10]) 
    print "Package of 2 is: %s" % vair.package("2")

if __name__ == "__main__":
    demo()
