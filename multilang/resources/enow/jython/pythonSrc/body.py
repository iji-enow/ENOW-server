import httplib
import sys
import pymongo

def eventHandler(event, context, callback):
    l_temperature_int = event["temperature"]
    l_http_object = httplib.HTTPConnection(" https://umeiqyyboh.execute-api.us-east-1.amazonaws.com/production/RequestIntent")

    if(l_temperature_int > 30):
        l_http_object.sendRequest("ON")
    else:
        sys.sleep(1)

    l_repository_object = pymongo.MongoClient('http://mongo.repository.net/division/present')

    l_detect_collection = l_repository_object["present"]

    if l_detect_collection == "person":
        alarm()
    else:
        sleep(1)

    callback["result"] = "completed"
