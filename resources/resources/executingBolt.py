import hashlib
import json
import os
import sys
from Queue import PriorityQueue
from bson.json_util import dumps
from pymongo import MongoClient

import storm

'''
========================================
    ENOW CODE EXECUTION MODULE
========================================
    Description :
         This module is for receiving 4 different components to execute python program in STORM
         The 4 components are as follow
             * CODE : Python source code stored in MongoDB
             * PARAMETER : Python program command line arguments
             * PAYLOAD : Payload come from either a device or lambda node
             * PREVIOUS DATA : Data containing results from previous execution

    Features :
        * Query MongoDB about the CODE and PARAMETER
        * Supports multi-threaded environment
        * Logs are stored in the log.txt. After executing the CODE, the program then read
        the logs and return them back to STORM.
'''
# The root directory of the current python project
fileDir = os.path.dirname(os.path.realpath('__file__'))
# Add PYTHONPATH for further execution
enow_Path = os.path.join(fileDir, 'enow/')
enow_jython_Path = os.path.join(fileDir, 'enow/jython')
enow_jython_Building_Path = os.path.join(fileDir, 'enow/jython/Building')
enow_jython_runtimePackage_Path = os.path.join(fileDir, 'enow/jython/runtimePackage')
sys.path.append(enow_Path)
sys.path.append(enow_jython_Path)
sys.path.append(enow_jython_Building_Path)
sys.path.append(enow_jython_runtimePackage_Path)
# import modules in PYTHONPATH
from enow.jython.Building import Building
# Declare class and inherit storm BasicBolt class
class ExecutingBolt(storm.Bolt):
    # static member variable used for semaphore to block the other thread from execution
    program_semaphore = 0
    # Queue for enrolling waiting threads
    program_queue = PriorityQueue()
    # initialize this instance
    def __init__(self):
        pass
    '''
    ========================================
        Function : initialize
    ========================================
        Description :
             The function declares an interface for communicating with the runtimeMain
            class and generates an instance of MongoDB to communicate with the local MongoDB
        Parameter :
            conf : system related
            context : system related
    '''
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        
        # declare an interface for setting and getting the components described above
        self.Building = Building()
        # declare an instance of MongoDB client
        try:
            self.client = MongoClient('localhost', 27017)
        except pymongo.errors.ConnectionFailure as e:
            sys.exit(1)
        self.source_db = self.client['enow']
        self.execute_collection = self.source_db['execute']
    '''
    ========================================
        Function : tupleToJson
    ========================================
        Description :
             The function receives a tuple as a parameter and returns JSON object which was
             originally in the tuple
        Parameter :
            tuple : (TYPE)TUPLE
        Return Value :
            jsonObject : (TYPE)json
    '''
    def tupleToJson(self, tuple):
        dictObject = tuple.values[0]
        jsonObject_str = json.dumps(dictObject)
        jsonObject = json.loads(jsonObject_str)
        return jsonObject
    '''
    ========================================
        Function : fileToLog
    ========================================
        Description :
             The function returns string data logged while executing the CODE
        Parameter :
            None
        Return Value :
            log_str : (TYPE)List of json string
    '''
    def fileToLog(self, l_mapId_hashed_string):
        log_str = ""
        logPath = os.path.join(fileDir, "enow/jython/pythonSrc", l_mapId_hashed_string, "log/log.txt")
        with open(logPath, 'r+') as file:
            log_str = file.readlines();
            file.seek(0)
            file.truncate()
        return log_str
    '''
    ========================================
        Function : process
    ========================================
        Description :
             The function reads data from both parameter and MongoDB
            and initiates execution sequence.
        Parameter :
            tup : (TYPE)TUPLE
        Return Value :
            None
    '''
    def process(self, tup):
        # convert tuple to json object
        jsonObject = self.tupleToJson(tup)
        # verify whether the input should be executed or not
        if jsonObject["verified"] is True and jsonObject["payload"] is not "":
            # receive data needed for execution from the converted json object
            l_payload_json = jsonObject["payload"]
            l_mapId_string = jsonObject["nodeId"]
            l_roadMapId_string = jsonObject["roadMapId"]
            # Creating a file hierarchy for local body.py source and log.txt
            l_mapId_hashed_string = str(hashlib.sha256(l_mapId_string).hexdigest())
            l_bodyRoot_string = os.path.join(fileDir, "enow/jython/pythonSrc", l_mapId_hashed_string)
            if not os.path.exists(l_bodyRoot_string):
                os.makedirs(l_bodyRoot_string)
                l_logDir_string = os.path.join(l_bodyRoot_string, "log")
                os.makedirs(l_logDir_string)
                l_logText_string = os.path.join(l_logDir_string, "log.txt")
                with open(l_logText_string, 'w+') as file:
                    pass
            # Getting previous execution informations
            l_previousData_json = jsonObject["previousData"]
            l_info_json = None
            # Make Query statement and send the query to MongoDB
            t_execute_cursor = self.execute_collection.find_one({ "roadMapId" : l_roadMapId_string })
            # Converting 'Cursor' object to json object
            t_item_bson_string = dumps(t_execute_cursor)
            t_item_json = json.loads(t_item_bson_string)
            # Getting a list of mapIds
            t_mapId_json = t_item_json["nodeIds"]
            # Search if the current mapId exists
            if l_mapId_string not in t_mapId_json:
                raise ValueError("No mapId in the given list of mapIds")
            else:
                l_info_json = t_mapId_json[l_mapId_string]
            # Concatenate parameter in a row
            rawParameter = ""
            for elem in l_info_json["parameter"]:
                rawParameter += elem
                rawParameter += " "
            # Get dumped data from json objects
            rawSource = l_info_json["code"]
            rawPayload = json.dumps(l_payload_json)
            if rawPayload == "null":
                rawPayload = "{\"payload\" : \"none\"}"
            rawPreviousData = json.dumps(l_previousData_json)
            # Replace carriage returns
            payload = rawPayload.replace("\r", "")
            source = rawSource.replace("\r", "")
            parameter = rawParameter.replace("\r", "")
            previousData = rawPreviousData.replace("\r", "")
            # Set up data for execution
            self.Building.setParameter(parameter.encode("ascii"))
            self.Building.setcode(source.encode("ascii"))
            self.Building.setPayload(payload.encode("ascii"))
            self.Building.setPreviousData(previousData.encode("ascii"))
            self.Building.setPath(l_mapId_hashed_string.encode("ascii"))
            tmp = self.Building.run()
            
            jsonObject["previousData"] = "null"
            # Verify the result whether the execution succeed or not
            if tmp == "":
                jsonObject["pyError"] = "true"
                jsonObject["log"] = self.fileToLog(l_mapId_hashed_string)
                storm.emit([jsonObject, l_roadMapId_string])
            else:
                jsonResult = json.loads(tmp, strict=False)
                jsonObject["log"] = self.fileToLog(l_mapId_hashed_string)
                jsonObject["payload"] = jsonResult
                storm.emit([jsonObject, l_roadMapId_string])
            # Handle the result and convert it to JSON object
        else:
            jsonObject["payload"] = ""
            jsonObject["log"] = ""
            jsonObject["previousData"] = "null"
            l_roadMapId_string = jsonObject["roadMapId"]
            storm.emit([jsonObject, l_roadMapId_string])

# Start the bolt when it's invoked
ExecutingBolt().run()
