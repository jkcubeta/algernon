import collections
from dateutil import parser

import pymysql
import requests
from xml.dom import minidom


class CredibleReport:
    def __init__(self,  data):
        self.data = data

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return iter(self.data)

    @classmethod
    def from_sql(cls, sql):
        data = get_report_as_dict(0, [sql], dynamic=True)
        return cls(data)  # this is the same as calling Person(*params)

    @classmethod
    def from_report(cls, report_id: int, params: [str]):
        data = get_report_as_dict(report_id, params)
        return cls(data)  # this is the same as calling Person(*params)


def get_google_formatted_report(report_id, params, dynamic=False):
    try:
        returned_report = []
        document = get_report_by_id(
            report_id=report_id,
            params=params,
            dynamic=dynamic
        )
        if document is 0:
            return returned_report
        else:
            tables = parse_tables(document, dynamic)
            returned_report.extend(tables)
            return returned_report
    except AttributeError as err:
        print(err)
        print('report id ' + str(report_id) + ' with parameters ' + str(params) + ' returned no values ')
        return []


def get_report_as_dict(report_id, params, dynamic=False):
    dict_report = collections.OrderedDict()
    report = get_google_formatted_report(
        report_id=report_id,
        params=params,
        dynamic=dynamic
    )
    try:
        header = report.pop(0)
    except IndexError:
        return dict_report
    for row in report:
        pk_guess = row[0]
        count = 0
        report_line = collections.OrderedDict()
        for field in row:
            report_line[header[count]] = field
            count += 1
        if pk_guess in dict_report:
            current_value = dict_report[pk_guess]
            if isinstance(current_value, list):
                dict_report[pk_guess].append(report_line)
            else:
                dict_report[pk_guess] = [current_value, report_line]
        else:
            dict_report[pk_guess] = report_line
    return dict_report


def parse_header(document, dynamic=False):
    header = []
    pointer = 0
    if dynamic:
        pointer = 1
    header_row_element = document.getElementsByTagName(
        'xs:sequence'
    ).item(pointer)
    header_rows = header_row_element.getElementsByTagName(
        'xs:element'
    )
    for header_row in header_rows:
        header_entry = {
            'name': header_row.getAttribute('name'),
            'type': header_row.getAttribute('type').replace('xs:', '')
        }
        header.append(header_entry)
    return header


def parse_tables(document, dynamic=False):
    target = 'Table'
    header = []
    if dynamic:
        target = 'Table1'
    header_data = parse_header(document, dynamic)
    data_sets = document.getElementsByTagName('NewDataSet')
    for header_row in header_data:
        header.append(header_row['name'])
    table = [header]
    if len(data_sets) is 0:
        return []
    else:
        data_sets = data_sets[0]
        for data_set in data_sets.childNodes:
            if data_set.nodeType == data_set.ELEMENT_NODE:
                if data_set.localName == target:
                    row = []
                    row_dict = {}
                    for entry in data_set.childNodes:
                        if entry.nodeType == entry.ELEMENT_NODE:
                            if entry.firstChild:
                                data = entry.firstChild.data
                            else:
                                data = ''
                            header_field = entry.nodeName
                            if len(data) > 45000:
                                data = data[:45000]
                            row_dict[header_field] = data
                    for header_entry in header_data:
                        try:
                            data = row_dict[header_entry['name']]
                            data_type = header_entry['type']
                            if data_type in ('int', 'short', 'double'):
                                try:
                                    data = int(data)
                                except ValueError:
                                    data = float(data)
                            elif data_type == 'string':
                                data = data.replace('<b>', '')
                                data = data.replace('</b>', '')
                            elif data_type == 'dateTime':
                                data = parser.parse(data)
                            elif data_type == 'boolean':
                                data = data == 'true'
                            else:
                                data = data
                        except KeyError:
                            data = ''
                        row.append(data)
                    table.append(row)
        return table


def get_report_key_by_id(report_id):
    db = pymysql.connect(
        "172.31.26.23",
        user="python",
        passwd="pythonpassword",
        db="algernon_scrub")
    cursor = db.cursor()
    cursor.callproc('getReportKeyById', [report_id])
    results = cursor.fetchone()
    return results[0]


def get_report_by_id(report_id, params, dynamic=False):
    completed = False
    tries = 0
    if len(params) is 0:
        params = ['', '', '']
    if len(params) is 1:
        params.append('')
        params.append('')
    if len(params) is 2:
        params.append('')
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'

    if dynamic:
        report_id = 712
    key = get_report_key_by_id(report_id)

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': params[0],
        'custom_param2': params[1],
        'custom_param3': params[2]
    }
    while not completed and tries < 3:
        cr = requests.get(url, params=payload)
        raw_xml = cr.content
        document = minidom.parseString(raw_xml).childNodes[0]
        if len(document.childNodes) > 0:
            return document
        else:
            tries += 1
    print('report with report id ' + str(report_id) + ' and params ' + str(params) + ' could not be fetched')
    return 0
