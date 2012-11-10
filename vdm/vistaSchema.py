#
## VOLDEMORT (VDM) VistA Comparer
#
# (c) 2012 Caregraf, Ray Group Intl
# For license information, see LICENSE.TXT
#

"""
Module for retrieving, caching and analysing a VistA's schemas returned by FMQL

TODO - Changes/Additions Planned:
- tie in Packages ie/ from Slim Package grab, do hierarchy. Make a master copy.
  - issue of common files: PATIENT/REGISTRATION
- FMQL: ensure all meta - VR, INPUT TR etc. Input Tr often differs with data there ie mandatory or not ie. live vs meta data mismatch
- Frozen Files: ex/ 59.7 - "THERE SHOULD ONLY BE ONE ENTRY IN THIS FILE. Because of the nature of this file and the fact that ALL the Pharmacy packages use this file, it is VERY IMPORTANT to stress that sites DO NOT edit fields or make local field additions to the Pharmacy System file." [Look at descriptions - compare to VA list]
- CSV for field namespaces (MSC == 21400) and Station Numbers for VA private stuff
- KEY FILES (fix for now based on FOIA refs)
- any FMQLisms in Schema returned move in here
  - ie/ . not _ to match Builds file ids
- leverage Packages (static: https://raw.github.com/OSEHR/VistA-FOIA/master/Packages.csv or 9_4) and Station Numbers (file 4).
  - station number to field name for class 3: New fields within the VA should be given field numbers in the format of NNNXXX, where NNN is the 3-digit VA station identifier and XXX is a three-digit sequence number, usually 001 and going up from there for a given file.  New nodes should be added as nodes NNNXXX, same format, not in the low numerics, not in the alpha series of nodes". Ex/ acceptance (460001) ... ess people (776000) ... lblk1 choices (500003) ... or even collect (580950.1) 
- for file -> package map, use locations/arrays. Note can't use Prefixes from Package
file AS they are for routines. Can QA/Compare OSEHRA's list to actual files.
- index and cross reference differences ie/ beyond just NAME differences
- see if MU led to File changes (ex/ http://vistapedia.net/index.php?title=Language_File_(.85)) and mark as such
"""

import os
import re
import csv
import urllib
import urllib2
import json
import sys
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
        self.badSelectTypes = []
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
        return self.__schemas[file]
        
    def getFileName(self, file):
        if file not in self.__schemas:
            return "<INVALID FILE>"
        return self.__schemas[file]["name"]
                            
    def getFieldIds(self, file, includeMultiples=False):
        sch = self.getSchema(file)
        if includeMultiples:
            return [field["number"] for field in sch["fields"]]
        return [field["number"] for field in sch["fields"] if field["type"] != "9"]
                
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
        schemas = {}
        start = datetime.now()
        for i, dtResult in enumerate(self.__fmqlCacher.describeSchemaTypes()):
            if "error" in dtResult:
                self.badSelectTypes.append(dtResult["error"])
                continue
            fileId = dtResult["number"]
            fmqlFileId = re.sub(r'\.', '_', fileId)
            schemas[fmqlFileId] = dtResult
        logging.info("%s: ... building (with caching) took %s" % (self.vistaLabel, datetime.now()-start))
        self.__schemas = schemas

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
    print "Name of file 2: %s" % vair.getSchema("2")["name"]
    print vair.listFiles()
                
if __name__ == "__main__":
    demo()
