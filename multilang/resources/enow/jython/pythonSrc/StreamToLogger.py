import logging
from kafka import KafkaProducer

class StreamToLogger(object):
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level,
                            line.rstrip())
            self.producer.send('logger', line)
