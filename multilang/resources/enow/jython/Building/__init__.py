import sys
import time
from enow.jython.runtimePackage.runtimeMain import runtimeMain
'''
========================================
    Class : Building
========================================
    Description : 
         The class works as an interface between the executingBolt and runtimeMain.
'''
class Building():
    main = None
    result = None
    code = None
    parameter = None
    payload = None
    previousData = None
    mapId_hashed_string = None

    def __init__(self):
        pass

    def getcode(self):
        if self.code is not None:
            return self.code
        else:
            return ""

    def getParameter(self):
        if self.parameter is not None:
            return self.parameter
        else:
            return ""

    def getPayload(self):
        if self.payload is not None:
            return self.payload
        else:
            return ""

    def getPreviousData(self):
        if self.previousData is not None:
            return self.previousData
        else:
            return ""
        
    def getPath(self):
        if self.mapId_hashed_string is not None:
            return self.mapId_hashed_string
        else:
            return ""

    def setcode(self, _code):
        self.code = _code

    def setParameter(self, _parameter):
        self.parameter = _parameter

    def setPayload(self, _payload):
        self.payload = _payload

    def setPreviousData(self, _previousData):
        self.previousData = _previousData
        
    def setPath(self, mapId_hashed_string):
        self.mapId_hashed_string = mapId_hashed_string
    '''
    ========================================
        Function : run
    ========================================
        Description : 
         The function initiates execution sequence
    '''
    def run(self):
        # checks if the class misses any member variable required for execution
        if self.parameter is not None and self.code is not None and self.payload is not None and self.mapId_hashed_string is not None:
            self.main = runtimeMain(_source=self.code,
                                    _parameter=self.parameter,
                                    _payload=self.payload,
                                    _previousData = self.previousData,
                                    _mapId_hashed_string = self.mapId_hashed_string)
            self.main.controllSource()
            self.result = self.main.run()
            return self.result
