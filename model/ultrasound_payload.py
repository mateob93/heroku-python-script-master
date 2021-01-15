from enum import Enum
from model.payload import Payload


class UltrasoundPayload(Payload):

    def __init__(self, distance):
        self.distance = distance

    def to_dict(self):
        return {UltrasoundPayloadTags.distance.name: self.distance}

    def to_tuple(self):
        return self.distance


class UltrasoundPayloadTags(Enum):
    distance = 1
