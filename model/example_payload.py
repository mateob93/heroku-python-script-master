from enum import Enum
from model.payload import Payload


def parse_payload(payload): # (1) Como dijimos, el payload es: DXXTXX, aca lo parseo a 2 variables separadas
    if payload == "FAIL":
        d_measure = None
        t_measure = None
    else:
        d_position = payload.upper().index("D")
        t_position = payload.upper().index("T")
        t_measure = payload[t_position + 1:]
        d_measure = payload[d_position + 1:t_position]
    return [d_measure, t_measure]


class ExamplePayload(Payload):  # (2) Aqui se indica la herencia de la clase Payload

    def __init__(self, payload):  # (3) En el mismo constructor se indican los atributos de la clase y se parsea el paylaod
        self.distance, self.temperature = parse_payload(payload)

    def to_dict(self):  # (4) Devuelve de la forma {tag1: atributo1, tag2: atributo2, ...}
        return {ExamplePayloadTags.distance.name: self.distance,
                ExamplePayloadTags.temperature.name: self.temperature}

    def to_tuple(self): # (5) Devuelve de la forma (atributo1, atributo2, ...)
        return (self.distance,
                self.temperature)


class ExamplePayloadTags(Enum): # (6) Los tags para usar en el diccionario y no manejar stings que llevan a errores humanos
    distance = 1
    temperature = 2
