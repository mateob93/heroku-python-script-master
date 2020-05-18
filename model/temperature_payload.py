from enum import Enum
from model.payload import Payload


def parse_payload(payload):
    if payload == "FAIL":
        t_measure = None
        h_measure = None
    else:
        h_position = payload.upper().index("H")
        t_position = payload.upper().index("T")
        h_measure = payload[h_position + 1:]
        t_measure = payload[t_position + 1:h_position]
    return [t_measure, h_measure]


class TemperaturePayload(Payload):

    def __init__(self, payload):
        self.temperature, self.humidity = parse_payload(payload)

    def to_dict(self):
        return {TemperaturePayloadTags.temperature.name: self.temperature,
                TemperaturePayloadTags.humidity.name: self.humidity,}

    def to_tuple(self):
        return self.temperature, self.humidity


class TemperaturePayloadTags(Enum):
    temperature = 1
    humidity = 2
