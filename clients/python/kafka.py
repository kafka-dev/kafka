# Copyright 2010 LinkedIn
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import struct

class Message:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload

    ## Message format is 4 byte length, 2 byte topic length, N byte topic and M byte payload
    def encode(self):
        return struct.pack('>i', len(self.payload) + len(self.topic) + 2) + \
               struct.pack('>h', len(self.topic)) + self.topic + self.payload


class KafkaProducer:
    def __init__(self, topic, host, port):
        self.REQUEST_KEY = 0
        self.topic = topic
        self.connection = socket.socket()
        self.connection.connect((host, port))

    def close(self):
        self.connection.close()

    def send(self, message):
        encoded = message.encode()
        self.connection.send(struct.pack('>i', len(encoded) + 2) + struct.pack('>h', self.REQUEST_KEY) + encoded)
	