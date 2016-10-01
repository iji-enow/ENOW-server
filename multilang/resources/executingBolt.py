import storm
import sys
import thread
import json
import logging
import os
from Queue import PriorityQueue
from symbol import parameters
import collections
import bson
import time
from bson.json_util import dumps
from bson.json_util import loads
from pymongo import MongoClient
fileDir = os.path.dirname(os.path.realpath('__file__'))
enow_Path = os.path.join(fileDir, 'enow/')
enow_jython_Path = os.path.join(fileDir, 'enow/jython')
enow_jython_Building_Path = os.path.join(fileDir, 'enow/jython/Building')
enow_jython_runtimePackage_Path = os.path.join(fileDir, 'enow/jython/runtimePackage')
sys.path.append(r'/Users/jeasungpark/Downloads/Eclipse.app/Contents/Eclipse/plugins/org.python.pydev_5.1.2.201606231256/pysrc')
import pydevd;
sys.path.append(enow_Path)
sys.path.append(enow_jython_Path)
sys.path.append(enow_jython_Building_Path)
sys.path.append(enow_jython_runtimePackage_Path)
from enow.jython.Building import Building
# from jython.Building import Building
# Counter is a nice way to count things,
# but it is a Python 2.7 thing


class ExecutingBolt(storm.BasicBolt):
    # Initialize this instance
    program_semaphore = 0
    program_queue = PriorityQueue()
    def __init__(self):
        pass

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self.Building = Building()
        try:
            self.client = MongoClient('localhost', 27017)
        except pymongo.errors.ConnectionFailure as e:
            sys.exit(1)

        self.source_db = self.client['enow']
        self.execute_collection = self.source_db['execute']
        # Create a new counter for this instance
        # storm.logInfo("Counter bolt instance starting...")

    def tupleToJson(self, tuple):
        dictObject = tuple.values[0]
        jsonObject_str = json.dumps(dictObject)
        jsonObject = json.loads(jsonObject_str)
        return jsonObject
    
    
    def fileToLog(self):
        log_str = ""
        logPath = os.path.join(fileDir, "enow/jython/pythonSrc/log/log.txt")
        with open(logPath, 'r+') as file:
            log_str = file.readlines();
            file.seek(0)
            file.truncate()    
        return log_str
    
    def process(self, tup):
        # Get the word from the inbound tuple
        # word = tup.values[0]
        # Increment the counter9
        # storm.logInfo("Emitting %s" %(word))
        # Emit the word and count
        #jsonObject = json.loads(word, strict=False)
        # Executtion Cycle

        jsonObject = self.tupleToJson(tup)
        
        if jsonObject["verified"] == True:
            # Get the document of which type is json
            # The document indicates that
            # the 'SOURCE' and 'PARAMETER' is the one currently executing

            # Getting the whole payload object from the tuple
            # 
            l_payload_json = jsonObject["payload"]
            l_mapId_string = jsonObject["nodeId"]
            l_roadMapId_string = jsonObject["roadMapId"]

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
            
                
            if ExecutingBolt.program_semaphore == 0:
                ExecutingBolt.program_semaphore = 1
                time.sleep(1)
                tmp = self.Building.run()
<<<<<<< HEAD
                jsonObject["log"] = self.fileToLog()
                
                if tmp == "":
                    jsonObject["pyError"] = "true"
                else:
                    jsonResult = json.loads(tmp, strict=False)
                    jsonObject["payload"] = jsonResult
                storm.emit([jsonObject])
                ExecutingBolt.program_semaphore = 0
=======
                if tmp == "":
                    jsonObject["pyError"] = "true"
                    jsonObject["log"] = self.fileToLog()
                    ExecutingBolt.program_semaphore = 0
                    storm.emit([jsonObject])
                else:
                    jsonResult = json.loads(tmp, strict=False)
                    jsonObject["log"] = self.fileToLog()
                    jsonObject["payload"] = jsonResult
                    ExecutingBolt.program_semaphore = 0
                    storm.emit([jsonObject])
>>>>>>> d6c2c3684883372ad02b066c90add8c59daefd46
            else:
                ExecutingBolt.program_queue.put(l_mapId_string)
                while True:
                    if ExecutingBolt.program_semaphore == 0:
                        if ExecutingBolt.program_queue.queue[0] == l_mapId_string:
                            ExecutingBolt.program_semaphore == 1
                            ExecutingBolt.program_queue.get()
                            tmp = self.Building.run()
<<<<<<< HEAD
                            jsonObject["log"] = self.fileToLog()
                            if tmp == "":
                                jsonObject["pyError"] = "true"
                            else:
                                jsonResult = json.loads(tmp, strict=False)
                                jsonObject["payload"] = jsonResult
                            storm.emit([jsonObject])
                            ExecutingBolt.program_semaphore == 0
                        
            
=======
                            if tmp == "":
                                jsonObject["pyError"] = "true"
                                jsonObject["log"] = self.fileToLog()
                                ExecutingBolt.program_semaphore = 0
                                storm.emit([jsonObject])
                            else:
                                jsonResult = json.loads(tmp, strict=False)
                                jsonObject["log"] = self.fileToLog()
                                jsonObject["payload"] = jsonResult
                                ExecutingBolt.program_semaphore = 0
                                storm.emit([jsonObject])
>>>>>>> d6c2c3684883372ad02b066c90add8c59daefd46
            # Handle the result and convert it to JSON object
        else:
            jsonObject["payload"] = ""
            storm.emit([jsonObject])

# Start the bolt when it's invoked
ExecutingBolt().run()
