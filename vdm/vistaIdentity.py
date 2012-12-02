#
## VOLDEMORT (VDM) VistA Identity Manager
#
# (c) 2012 Caregraf, Ray Group Intl
# For license information, see LICENSE.TXT
#

"""
Retrieve identifying Values from a VistA. Will appear in reports
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
from copies.fmqlCacher import FMQLDescribeResult

__all__ = ['VistaBuilds']

class VistaIdentity(object):

    def __init__(self, vistaLabel, fmqlCacher):
        self.vistaLabel = vistaLabel
        self.__fmqlCacher = fmqlCacher
        self.__identifyVistA()
        
    def identifiers(self):
        return self.__identifiers
        
    def __identifyVistA(self):
        logging.info("%s: Identity - building Identity ..." % self.vistaLabel)
        self.__identifiers = {}
        start = datetime.now()
        
        # First get Kernel System Parameters (only ever one). May sure not just 1.
        reply = self.__fmqlCacher.query("SELECT 8989_3 LIMIT 1")
        if "error" in reply:
            raise Exception("Can't get a Kernel System Parameters record")
        if len(reply["results"]) != 1:
            raise Exception("Can't get one Kernel System Parameters record")
        ksURI = reply["results"][0]["uri"]["value"]
        print ksURI
        reply = self.__fmqlCacher.query("DESCRIBE %s CSTOP 0" % ksURI)
        try: 
            result = reply["results"][0]
            self.__identifiers["domain"] = result["domain_name"]["label"]
            defaultInstitution = result["default_institution"]["value"]
        except:
            raise Exception("Can't get domain and default_institution from Kernel System Paramaters")
        reply = self.__fmqlCacher.query("DESCRIBE %s CSTOP 0" % defaultInstitution)
        try:
            result = reply["results"][0]
            for key in result:
                if key == "uri":
                    continue
                if result[key]["type"] == "uri":
                    self.__identifiers[key] = result[key]["label"]
                elif result[key]["type"] == "literal":
                    self.__identifiers[key] = result[key]["value"]
        except:
            raise Exception("Can't walk top of Default Institution")               
            
# ######################## Module Demo ##########################

import getopt, sys # for isolation testing
                       
def demo():

    opts, args = getopt.getopt(sys.argv[1:], "")

    # ex/ python vdm.vistaIdentity CGVISTA HOST PORT 'ACCESS' 'VERIFY'
    if len(args) < 5:
        print "invoke with: python vdm.vistaIdentify vistaname 'host' port 'access 'verify'" 
        return

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from copies.fmqlCacher import FMQLCacher
    cacher = FMQLCacher("Caches")
    cacher.setVista(vistaLabel=args[0], host=args[1], port=int(args[2]), access=args[3], verify=args[4]) 
    vi = VistaIdentity(args[0], cacher)
    # ex/ {u'state': u'STATE/OKLAHOMA', 'domain': u'DOMAIN/VISTA.GOLD.MEDSPHERE.COM', u'facility_type': u'FACILITY TYPE/VAO', u'station_number': u'050', u'name': u'SOFTWARE SERVICE'}
    print vi.identifiers()
    
if __name__ == "__main__":
    demo()