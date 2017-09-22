from .Patient import  *


class ClinicalTime:
    def __init__(self):
        self.date = ''


class ClinicalEvent:
    permanent_tags = []

    def __init__(self, patient: Patient, clinical_time: ClinicalTime):
        self.patient = patient
        self.clinical_time = clinical_time
        self.tags = []

    def add_tag(self, tag: str):
        self.tags.append(tag)

    def add_tags(self, tags: [str]):
        self.tags.append(tags)
