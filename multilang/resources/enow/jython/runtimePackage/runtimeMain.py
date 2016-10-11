'''
Created on 2016. 8. 15.

@author: jeasungpark
'''
from .sourceControl import sourceControl
from .runtime import runtime

'''
Class : RemoteSubmitter
    Description : 
    Actual class controlling a pipeline for the whole program.
'''

class runtimeMain:
    def __init__(self, _source, _parameter, _payload, _previousData, _mapId_hashed_string):
        self.source = _source
        self.parameter = _parameter
        self.payload = _payload
        self.previousData = _previousData
        self.runtime = None
        self.sourceControl = None
        self.result = None
        self.mapId_hashed_string = _mapId_hashed_string
    '''
    Function : controllSource()
        Description : 
         Instantiate a sourceControl object and assign it on the sourceControl member variable.
    '''
    def controllSource(self):
        self.sourceControl = sourceControl(body=self.source, _mapId_hashed_string = self.mapId_hashed_string)
    '''
    Function : run()
        Description : 
         Instantiate a runtime object and run the source code received.
    '''    
    def run(self):
        self.runtime = runtime() 
        self.runtime.run(_args=self.parameter,
                         _payloads = self.payload,
                         _previousData = self.previousData,
                         _mapId_hashed_string = self.mapId_hashed_string)
        self.result = self.runtime.getResult()
        return self.result
        

