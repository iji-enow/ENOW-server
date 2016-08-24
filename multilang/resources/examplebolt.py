import storm
from enow.jython import Building
# Counter is a nice way to count things,
# but it is a Python 2.7 thing

class CountBolt(storm.BasicBolt):
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        Building = Building()
        # Create a new counter for this instance
        storm.logInfo("Counter bolt instance starting...")
        

    def process(self, tup):
        # Get the word from the inbound tuple
        word = tup.values[0]
        # Increment the counter9
        #storm.logInfo("Emitting %s" %(word))
        # Emit the word and count
    
    
        storm.emit([word])

# Start the bolt when it's invoked
CountBolt().run()
