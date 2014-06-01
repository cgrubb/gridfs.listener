#!/usr/bin/python
'''
Created on May 31, 2014

@author: cgrubb
'''
import json
import os

import zmq
from zmq.eventloop import zmqstream, ioloop
ioloop.install()

import pymongo
import gridfs

class GridFSServer():

    def __init__(self):
        self.context = zmq.Context()
        self.listener = self.context.socket(zmq.SUB)
        self.listener.connect("tcp://192.168.0.105:8891")
        self.listener.setsockopt(zmq.SUBSCRIBE, "scan")
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind("tcp://*:8893")
        self.loop = ioloop.IOLoop.instance()
        self.stream = zmqstream.ZMQStream(self.listener)
        self.stream.on_recv(self.handle_msg)
        self.pubstream = zmqstream.ZMQStream(self.publisher)

        self.db = pymongo.MongoClient("192.168.0.104").scans
        self.fs = gridfs.GridFS(self.db)

    def handle_msg(self, message):
        try:
            print message[1]
            msg = json.loads(message[1])
            files = msg["files"]
            for f in files:
                with open(f, 'rb') as upload:
                    self.fs.put(upload,
                                filename=os.path.basename(f))
            self.pubstream.send_multipart(['grid_publish',
                                           json.dumps(msg)])
        except Exception as ex:
            print ex.message

def main():
    server = GridFSServer()
    ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
