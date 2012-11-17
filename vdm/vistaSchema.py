#
## VOLDEMORT (VDM) VistA Comparer
#
# (c) 2012 Caregraf, Ray Group Intl
# For license information, see LICENSE.TXT
#

"""
Module for retrieving, caching and analysing a VistA's schemas returned by FMQL

TODO - Changes/Additions Planned:
- quick neaten: nix using fmqlFileId (_ for .)
- tie in Packages ie/ from Slim Package grab, do hierarchy. Make a master copy.
  - issue of common files: PATIENT/REGISTRATION
  - station number to field name for class 3: New fields within the VA should be given field numbers in the format of NNNXXX, where NNN is the 3-digit VA station identifier and XXX is a three-digit sequence number, usually 001 and going up from there for a given file.  New nodes should be added as nodes NNNXXX, same format, not in the low numerics, not in the alpha series of nodes". Ex/ acceptance (460001) ... ess people (776000) ... lblk1 choices (500003) ... or even collect (580950.1) 
- FMQL: ensure all meta - VR, INPUT TR etc. Input Tr often differs with data there ie mandatory or not ie. live vs meta data mismatch
- Frozen Files: ex/ 59.7 - "THERE SHOULD ONLY BE ONE ENTRY IN THIS FILE. Because of the nature of this file and the fact that ALL the Pharmacy packages use this file, it is VERY IMPORTANT to stress that sites DO NOT edit fields or make local field additions to the Pharmacy System file." [Look at descriptions - compare to VA list]
- CSV for field namespaces (MSC == 21400) and Station Numbers for VA private stuff
- KEY FILES (fix for now based on FOIA refs)
- any FMQLisms in Schema returned move in here
  - ie/ . not _ to match Builds file ids
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
    Access to the cached FMQL description of a Vista's Schema
    """
    
    def __init__(self, vistaLabel, fmqlCacher):
        self.vistaLabel = vistaLabel
        self.__fmqlCacher = fmqlCacher
        self.__corruptTypes = []
        self.__makeSchemas() 
        
    def __str__(self):
        return "Schema of %s" % self.vistaLabel
        
    def getNoSpecificValues(self):
        pass # TODO: ala builds etc, count specific values
        
    def listFiles(self, topOnly=False):
        if topOnly:
            topFiles = []
            for fileId in self.__schemas:
                if not self.__schemas[fileId]:
                    continue # error file
                if "parent" in self.__schemas[fileId]:
                    continue
                topFiles.append(fileId)
            return topFiles
        return self.__schemas.keys()
        
    def listCorruptFiles(self):
        return self.__corruptTypes
        
    def countFiles(self, topOnly=False):
        return len(self.listFiles(topOnly))
        
    def countPopulatedTops(self):
        """
        How many of top level files are populated - not many in FOIA/GOLD
        """
        if not self.__schemas:
            self.__makeSchemas()
        no = 0
        for fileId in self.__schemas:
            if "parent" in self.__schemas[fileId]:
                continue
            if "count" not in self.__schemas[fileId]:
                continue
            if not self.__schemas[fileId]["count"]:
                continue
            if int(self.__schemas[fileId]["count"]) > 0:
                no += 1
        return no              
        
    def getSchema(self, file):
        sch = self.__schemas[file]
        # TODO: move down to cached meta
        if "parent" in sch:
            parents = []
            psch = sch
            while "parent" in psch:
                parents.insert(0, psch["parent"])
                try:
                    psch = self.__schemas[re.sub(r'\.', '_', psch["parent"])]
                except:
                    sch["invalidParent"] = psch["parent"]
                    break # assume parent invalid!
            sch["parents"] = parents
            sch["class3"] = self.__isClass3File(parents[0])
        else:
            sch["class3"] = self.__isClass3File(file)
        return self.__schemas[file]
        
    def __isClass3File(self, file):
        filePrefix = re.split(r'[\.\_]', file)[0]
        return True if len(filePrefix) == 6 and int(filePrefix) > 101000 else False
        
    def __parent(self, file, parent):
        ids = [parent, fileId]
        while parent in self.__parents:
            ids.insert(0, self.__parents[parent][0])
            parent = self.__parents[parent][0]
        subFileId = ""
        indent = ""
        indentInc = "&nbsp;&nbsp;&nbsp;"
        for id in ids:
            idMU = id
            if subFileId:
                subFileId += "<br/>"
                """
                Check all subfiles for ...
                The VA FileMan subdictionary numbers should be assigned at the high end of the numbering sequence, following the numbering convention outlined. For example, a VA FileMan subdictionary number added to the Patient file (File 2) by station 368 should be 2.368001, a second subdictionary should be assigned 2.368002
                """
                if re.search(r'\.\d{6}$', id):
                    sid = id.split(".")[1]
                    idMU = id.split(".")[0] + "." + self.__vaStationId(sid)
            elif re.match(r'\d{6}', id): # check parent file if local site
                idMU = self.__vaStationId(id)
            subFileId = subFileId + indent + idMU
            indent = indent + indentInc
        return subFileId
        
    def getFileName(self, file):
        if file not in self.__schemas:
            return "<INVALID FILE>"
        return self.__schemas[file]["name"]
                            
    def getFieldIds(self, file, includeMultiples=False):
        """
        Return field ids of non corrupt fields
        """
        sch = self.getSchema(file)
        if includeMultiples:
            return [field["number"] for field in sch["fields"] if "name" in field]
        return [field["number"] for field in sch["fields"] if "name" in field and field["type"] != "9"]
        
    def getCorruptFields(self, file):
        sch = self.getSchema(file)
        return [field for field in sch["fields"] if "name" not in field]        
                
    def getFields(self, file, fieldIds):
        """Return in order of field number, the same order returned by FMQL"""
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
        
    def sortFiles(self, fileSet):
        """TODO: remove once move ids properly in here"""
        return sorted(fileSet, key=lambda item: float(re.sub(r'\_', ".", item)))
        
    def dotFiles(self, fileSet):
        return [float(re.sub(r'\_', ".", item)) for item in fileSet]
        
    def __makeSchemas(self):
        """
        Index schema - will force caching if not already in cache
        """
        logging.info("%s: Schema - building Schema Index ..." % self.vistaLabel)
        self.__schemas = {}
        self.__corruptSchemas = {}
        start = datetime.now()
        for i, dtResult in enumerate(self.__fmqlCacher.describeSchemaTypes()):
            if "error" in dtResult:
                self.__corruptTypes.append((dtResult["fmql"]["TYPE"], dtResult["error"]))
                continue
            fileId = dtResult["number"]
            fmqlFileId = re.sub(r'\.', '_', fileId)
            self.__schemas[fmqlFileId] = dtResult
            for field in dtResult["fields"]:
                if "name" not in field: # expect "corruption"
                    continue
                if re.match(r'\*', field["name"]):
                    field["deprecated"] = True
                # TODO: remove once off old FOIA (will fix up or nix others)
                # NOTE: goes with fmqlCacher, ignore under 1.1
                field["name"] = re.sub(r', *', ' ', re.sub(r'[\[\]\?\-\+\.\'\(\)\%\&\#\@\$\{\}]', '', re.sub("[\_\/\>\<]", " ", field["name"].upper())))
        logging.info("%s: ... building (with caching) took %s" % (self.vistaLabel, datetime.now()-start))

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
    cacher.setVista("CGVISTA", "http://vista.caregraf.org/fmqlEP") 
    vair = VistaSchema("CGVISTA", cacher)
    print "Name of file 2: %s" % vair.getSchema("442033_02")["name"]
    print vair.listFiles()
                
if __name__ == "__main__":
    demo()
