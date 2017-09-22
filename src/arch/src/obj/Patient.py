from datetime import date


class PatientDataField:
    def __init__(self, value: object, pc_value=None):
        self.data = {
            'primary': value
        }
        if pc_value:
            self.data['pc'] = pc_value

    def update(self, value: object, category: object = None, preferred: bool = False, pc_value=None):
        if category:
            if category not in self.data.keys():
                self.data[category] = [value]
            elif value not in self.data[category]:
                self.data[category].append(value)
            return
        if preferred:
            if pc_value:
                self.data['primary'] = value
                self.data['pc'] = pc_value
            else:
                raise KeyError('must specify a pc value if preferred value passed')
            return
        self.data['pc'] = value
        self.data['primary'] = value

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.data['primary'] == other.data['primary']
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)

    def __hash__(self):
        return hash(self.data['primary'])

    def remove(self, value: object, category: object = None, preferred: bool = False, pc_value=None):
        if category:
            for key, item in self.data.items():
                if key is category:
                    self.data[category].remove(value)
                    if not self.data[category]:
                        del self.data[key]
            return
        if preferred:
            if pc_value:
                self.data['primary'] = value
                self.data['pc'] = pc_value
            else:
                raise KeyError('must specify a pc value if preferred value passed')
            return
        self.data['pc'] = value
        self.data['primary'] = value

    def get_data(self, pc: bool=False, category: object=None):
        if pc:
            return self.data['pc']
        if category:
            return self.data[category]
        return self.data['primary']

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, item):
        return self.data[item]


class PatientData:
    def __init__(self):
        self.data = {}

    def add_data(self, data_type: object, patient_data_field: PatientDataField):
        if data_type in self.data:
            raise KeyError('type for %s already exists, use replace_data instead' % str(data_type))
        self.data[data_type] = patient_data_field

    def replace_data(self, data_type: object, patient_data_field: PatientDataField):
        if patient_data_field not in self.data:
            raise KeyError('type for %s does not exist, use add_data instead' % str(data_type))
        if isinstance(patient_data_field, PatientDataField):
            raise KeyError('stored object %s is not a PatientDataField object' % str(patient_data_field))
        self.data[data_type] = patient_data_field

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, item):
        return self.data[item]


class PatientExtId(PatientData):
    def __init__(self):
        super().__init__()

    def add_ext_id(self, source: str, ext_id: PatientDataField):
        super().add_data(source, ext_id)

    def replace_ext_id(self, source: str, ext_id: PatientDataField):
        super().replace_data(source, ext_id)


class PatientDemographic(PatientDataField):
    def __init__(self, value: object, pc_value: object = None):
        super().__init__(value, pc_value)

    def add_demographic(self, value: object, category: object = None, preferred: bool = False, pc_value=None):
        super().update(value, category, preferred, pc_value)

    def remove_demographic(self, value: object, category: object = None, preferred: bool = False, pc_value=None):
        super().remove(value, category, preferred, pc_value)


class PatientDemographics(PatientData):
    def __init__(self):
        super().__init__()

    def add_demographic(self, demographic_type: object, patient_demographic: PatientDemographic):
        super().add_data(demographic_type, patient_demographic)

    def replace_demographic(self, demographic_type: object, patient_demographic: PatientDemographic):
        super().replace_data(demographic_type, patient_demographic)


class Patient:
    def __init__(self, first_name: str, last_name: str, dob: date):
        self.first_name = first_name
        self.last_name = last_name
        self.dob = dob
        self.alg_guid = ''
        self.patient_data = {}

    def add(self, data_type: object, patient_data: PatientData):
        if data_type in self.patient_data:
            raise KeyError('type for %s already exists, use update instead' % str(data_type))
        self.patient_data[data_type] = patient_data

    def update(self, data_type: object, patient_data: PatientData):
        if data_type not in self.patient_data:
            raise KeyError('type for %s does not exist, use add instead' % str(data_type))
        self.patient_data[data_type] = patient_data
