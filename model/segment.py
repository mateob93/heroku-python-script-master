from model import seismic_payload, temperature_payload, ultrasound_payload, example_payload
from enum import Enum

from topics import Topics


class Segment:
    def __init__(self, payload_type, segment_id, segment_date, segment_microsec, segment_rate, flag1, flag2, flag3, flag4, payload):
        self.segment_id = segment_id
        self.segment_date = segment_date
        self.segment_microsec = segment_microsec
        self.segment_rate = segment_rate
        self.flag1 = flag1
        self.flag2 = flag2
        self.flag3 = flag3
        self.flag4 = flag4
        self.payload = self.build_payload(payload, payload_type)

    @staticmethod
    def build_payload(payload, payload_type):
        # todo: acomodar parseo payload sismica
        if payload_type == Topics.SEISMIC.value:
            return seismic_payload.SeismicPayload(**payload)
        elif payload_type == Topics.T_AND_H.value:
            return temperature_payload.TemperaturePayload(payload)
        elif payload_type == Topics.ULTRASOUND.value:
            return ultrasound_payload.UltrasoundPayload(**payload)
        elif payload_type == Topics.EXAMPLE.value:
            return example_payload.ExamplePayload(payload)

    def to_dict(self):
        return {
                SegmentTags.segment_id.name: self.segment_id,
                SegmentTags.segment_date.name: self.segment_date,
                SegmentTags.segment_microsec.name: self.segment_microsec,
                SegmentTags.segment_rate.name: self.segment_rate,
                SegmentTags.flag1.name: self.flag1, SegmentTags.flag2.name: self.flag2,
                SegmentTags.flag3.name: self.flag3, SegmentTags.flag4.name: self.flag4,
                SegmentTags.payload.name: self.payload.to_dict()}

    def to_tuple(self):
        return (self.segment_id, self.segment_date, self.segment_microsec, self.segment_rate,
                self.flag1, self.flag2, self.flag3, self.flag4)


class SegmentTags(Enum):
    segment_id = 1
    segment_date = 2
    segment_microsec = 3
    segment_rate = 4
    flag1 = 5
    flag2 = 6
    flag3 = 7
    flag4 = 8
    payload = 9
