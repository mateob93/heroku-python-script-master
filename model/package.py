from enum import Enum
from model import segment


class Package:
    def __init__(self, payload_type, package_id, package_date, package_len, segments):
        self.package_id = package_id
        self.package_date = package_date
        self.package_len = package_len
        self.segments = [segment.Segment(payload_type, **seg) for seg in segments]

    def to_dict(self):
        return {PackageTags.package_id.name: self.package_id,
                PackageTags.package_date.name: self.package_date,
                PackageTags.package_len.name: self.package_len,
                PackageTags.segments.name: [seg.to_dict() for seg in self.segments]}

    def to_tuple(self):
        return self.package_date, self.package_id, self.package_len


class PackageTags(Enum):
    package_id = 1
    package_date = 2
    package_len = 3
    segments = 4
