from .Patient import *


class ClinicalTime:
    def __init__(self, year, month, day, hour=0, minute=0):
        self.start = datetime(year=year, month=month, day=day, hour=hour, minute=minute)
        self.end = datetime(year=year, month=month, day=day, hour=hour, minute=minute)

    @classmethod
    def from_decade(cls, year, mid=None, early=None, late=None):
        if early:
            start = int(year)
            end = int(year) + 4
        elif mid:
            start = int(year) + 4
            end = int(year) + 7
        elif late:
            start = int(year) + 6
            end = int(year) + 10
        else:
            start = int(year)
            end = int(year) + 10
        new_time = cls(year=start, month=0, day=0)
        new_time.end = datetime(year=end, month=0, day=0)
        return new_time


class ClinicalEvent:
    permanent_tags = []

    def __init__(self, patient: Patient, clinical_time: ClinicalTime):
        self.patient = patient
        self.clinical_time = clinical_time
        self.tags = self.permanent_tags.copy()

    def add_tag(self, tag: str):
        self.tags.append(tag)

    def add_tags(self, tags: [str]):
        self.tags.append(tags)
