import storm
import sys
import thread
import json
import logging
import os
from symbol import parameters
fileDir = os.path.dirname(os.path.realpath('__file__'))
enow_Path = os.path.join(fileDir, 'enow/')
enow_jython_Path = os.path.join(fileDir, 'enow/jython')
enow_jython_Building_Path = os.path.join(fileDir, 'enow/jython/Building')
enow_jython_runtimePackage_Path = os.path.join(
    fileDir, 'enow/jython/runtimePackage')

sys.path.append(enow_Path)
sys.path.append(enow_jython_Path)
sys.path.append(enow_jython_Building_Path)
sys.path.append(enow_jython_runtimePackage_Path)
from enow.jython.Building import Building
# from jython.Building import Building
# Counter is a nice way to count things,
# but it is a Python 2.7 thing


class CountBolt(storm.BasicBolt):
    # Initialize this instance

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self.Building = Building()
        # Create a new counter for this instance
        # storm.logInfo("Counter bolt instance starting...")

    def process(self, tup):
        # Get the word from the inbound tuple
        word = tup.values[0]
        # Increment the counter9
        # storm.logInfo("Emitting %s" %(word))
        # Emit the word and count

        jsonstring = r'''{
       "PARAMETER" : "-ls -t",
       "SOURCE" : "def eventHandler(event, context, callback):\n\tevent[\"identification\"] = \"modified\"\n\tprint(\"succeed\")\n\ta=10\n\tcallback[\"returned\"] = str(a)\n",
       "PAYLOAD" : {"identification" : "original"}
       }'''

        jsonObject = json.loads(word, strict=False)

        dictPayload = jsonObject["PAYLOAD"]
        rawPayload = json.dumps(dictPayload)
        rawSource = jsonObject["SOURCE"]
        rawParameter = jsonObject["PARAMETER"]

        payload = rawPayload.replace("\r", "")
        source = rawSource.replace("\r", "")
        parameter = rawParameter.replace("\r", "")

        self.Building.setParameter(parameter.decode("utf-8").encode("ascii"))
        self.Building.setcode(source.decode("utf-8").encode("ascii"))
        self.Building.setPayload(payload.decode("utf-8").encode("ascii"))
        tmp = self.Building.run()

        '''
       self.Building.setParameter("-ls -t")
       self.Building.setcode("def eventHandler(event, context, callback):\n\tevent[\"identification\"] = \"modified\"\n\tprint(\"succeed\")\n\ta=10\n\tcallback[\"returned\"] = str(a)\n")
       self.Building.setPayload("{\"identification\" : \"original\"}")
       tmp = self.Building.run()
       '''

        storm.emit([tmp])

# Start the bolt when it's invoked
CountBolt().run()
