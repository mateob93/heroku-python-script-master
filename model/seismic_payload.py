from enum import Enum
from model.payload import Payload


class SeismicPayload(Payload):

    def __init__(self, time, magnetic_field_x, magnetic_field_y, magnetic_field_z, acc):
        self.time = time
        self.magnetic_field_x = magnetic_field_x
        self.magnetic_field_y = magnetic_field_y
        self.magnetic_field_z = magnetic_field_z
        self.acc = acc

    def to_dict(self):
        return {SeismicPayloadTags.time.name: self.time,
                SeismicPayloadTags.magnetic_field_x.name: self.magnetic_field_x,
                SeismicPayloadTags.magnetic_field_y.name: self.magnetic_field_y,
                SeismicPayloadTags.magnetic_field_z.name: self.magnetic_field_z,
                SeismicPayloadTags.acc.name: self.acc}

    def to_tuple(self):
        return (self.time, self.acc, self.magnetic_field_x,
                self.magnetic_field_y, self.magnetic_field_z)


class SeismicPayloadTags(Enum):
    time = 1
    magnetic_field_x = 2
    magnetic_field_y = 3
    magnetic_field_z = 4
    acc = 5
