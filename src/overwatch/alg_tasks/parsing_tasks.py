#!/usr/bin/env python3
import string

from bs4 import BeautifulSoup

from . import credible_tasks

mco_map = {
    'AMERIHEALTH DISTRICT OF COLUMBIA': '5',
    'TRUSTED HEALTH PLAN': '6',
    'MEDSTAR FAMILY CHOICE, INC': '7',
}
mco_payers = ['5', '6', '7', '11']
medicaid_payers = ['15']
medicare_payers = ['17']
local_payers = ['13', '16']


def address_insurance_record(investigation_args):
    credible_id = investigation_args['client_id']
    medicaid_id = credible_tasks.get_medicaid_number(credible_id)
    suggestions = ''
    current_insurance = credible_tasks.get_client_insurance(credible_id)
    payers = current_insurance.keys
    print(current_insurance)
    medicaid = investigation_args['is_medicaid']
    print('medicaid is: ' + str(medicaid))
    mco = investigation_args['is_mco']
    print('mco is: ' + str(mco))
    medicare = investigation_args['is_medicare']
    print('medicare is: ' + str(medicare))
    uninsured = investigation_args['is_uninsured']
    print('uninsured is: ' + str(uninsured))
    description = investigation_args['description']
    print('description is: ' + description)
    incarcerated = check_incarcerated(description)
    if mco:
        suggestions = address_mco(
            current_insurance,
            investigation_args,
            medicaid_id)
        if medicare:
            suggestions = address_medicare(
                current_insurance,
                investigation_args,
                medicaid_id)
    if medicaid and not mco and not medicare:
        suggestions = address_medicaid(
            current_insurance,
            investigation_args,
            medicaid_id)
    if uninsured:
        suggestions = address_uninsured(
            current_insurance,
            investigation_args)
    if incarcerated:
        suggestions = address_incarcerated(
            current_insurance,
            description)
    if suggestions == '':
        suggestions = 'no changes suggested'
    return suggestions


def address_medicare(current_insurance, investigation_args):
    suggestions = ''
    medicare_clear = False
    payers = current_insurance.keys()
    for payer in payers:
        if payer in medicare_payers:
            medicare_clear = True
        if payer not in medicare_payers:
            new_idea = ('client is a medicare recipient, ' +
                        'advise removing insurance for payer :' + payer)
            suggestions = (suggestions + ' | ' + new_idea)
    return suggestions


def check_incarcerated(fields):
    if 'incar' in fields or 'INCAR' in fields or 'Incar' in fields:
        return True
    else:
        return False


def address_incarcerated(current_insurance, field):
    suggestions = ''
    payers = current_insurance.keys()
    for payer in payers:
        if 'incar' in field or 'INCAR' in field or 'Incar' in field:
            suggestions = (suggestions +
                           ' | client appears to have an incarcerated insurance, ' +
                           'I suggest removing payer ' + payer)
    return suggestions


def address_uninsured(current_insurance, investigation_args):
    suggestions = ''
    payers = current_insurance.keys()
    for payer in payers:
        if payer not in local_payers:
            new_idea = credible_tasks.deactivate_insurance(
                investigation_args['client_id'],
                current_insurance[payer])
            suggestions = suggestions + ' | ' + new_idea
    return suggestions


def address_mco(current_insurance, investigation_args, medicaid_id):
    suggestions = ''
    elements = [0, 1]
    payers = current_insurance.keys()
    mco_clear = False
    medicaid_clear = False
    for payer in payers:
        if payer in mco_payers:
            mco_clear = True
        if payer in medicaid_payers:
            medicaid_clear = True
        if payer not in mco_payers and payer not in medicaid_payers:
            new_idea = credible_tasks.deactivate_insurance(
                investigation_args['client_id'],
                current_insurance[payer])
            suggestions = suggestions + ' | ' + new_idea
    if not mco_clear:
        mco_dates = parse_mco_dates(elements)
        new_idea = credible_tasks.add_insurance(
            investigation_args['client_id'],
            mco_map[investigation_args['description']],
            medicaid_id,
            mco_dates[0],
            mco_dates[1])
        suggestions = suggestions + ' | ' + new_idea
    if not medicaid_clear:
        mco_dates = parse_mco_dates(elements)
        new_idea = credible_tasks.add_insurance(
            investigation_args['client_id'],
            medicaid_payers[0],
            medicaid_id,
            mco_dates[0],
            mco_dates[1])
        suggestions = suggestions + ' | ' + new_idea
    return suggestions


def address_medicaid(current_insurance, investigation_args, medicaid_id):
    suggestions = ''
    elements = [0, 1]
    payers = current_insurance.keys()
    medicaid_clear = False
    for payer in payers:
        if payer in medicaid_payers:
            medicaid_clear = True
        if payer not in medicaid_payers:
            new_idea = credible_tasks.deactivate_insurance(
                investigation_args['client_id'],
                current_insurance[payer])
            suggestions = suggestions + ' | ' + new_idea
    if not medicaid_clear:
        medicaid_dates = parse_medicaid_dates(elements)
        new_idea = credible_tasks.add_insurance(
            investigation_args['client_id'],
            medicaid_payers[0],
            medicaid_id,
            medicaid_dates[0],
            medicaid_dates[1])
        suggestions = suggestions + ' | ' + new_idea
    return suggestions


def get_mco_name(fields):
    if check_mco(fields):
        if 'Provider:' in fields:
            marker = fields.index('Provider:')
            mco_name = fields[marker + 1]
            return mco_name


def check_mco(fields):
    if 'Service Management Type:' in fields:
        marker = fields.index('Service Management Type:')
        mco_check = fields[marker + 1]
        if mco_check == 'MCO':
            return True
        else:
            return False
    else:
        return False


def check_ltc(fields):
    program_name = get_program_name(fields)
    if program_name:
        if str("ltc") in program_name or str("LTC") in program_name:
            return True
        else:
            return False
    else:
        return False


def check_medicare(fields):
    program_name = str(get_program_name(fields))
    if program_name:
        if "medicare" in program_name or "MEDICARE" in program_name or "Medicare" in program_name:
            return True
        else:
            return False
    else:
        return False


def check_tpl(fields):
    program_name = get_program_name(fields)
    if program_name:
        if str("tpl") in program_name or str("TPL") in program_name:
            return True
        else:
            return False
    else:
        return False


def get_program_name(fields):
    if 'Plan Coverage:' in fields:
        marker = fields.index('Plan Coverage:')
        program_code = fields[marker + 1]
        return program_code
    else:
        return False


def get_program_code(fields):
    if 'Program Code:' in fields:
        marker = fields.index('Program Code:')
        program_code = fields[marker + 1]
        return program_code


def check_medicaid(fields):
    steps = 1
    if 'Plan Coverage:' in fields:
        marker = fields.index('Plan Coverage:')
        print('marker found at: ' + str(marker))
        while steps < 6:
            medicare_check = fields[marker + steps]
            print(medicare_check)
            if medicare_check == 'ACTIVE':
                return True
            steps += 1
        return False
    else:
        return False


def parse_medicaid_dates(html):
    return [0, 1]


def parse_mco_dates(html):
    return [0, 1]


def standardize(html):
    table_rows = []
    printable = set(string.printable)
    html = ''.join(filter(lambda x: x in printable, html))
    html = html.replace('<!--', '')
    html = html.replace('-!>', '')
    html = html.replace('<br>', '')
    html = html.replace('\n', '')
    html = html.replace('\t', '')
    html = html.replace('\r', '')
    html = str(html)
    html = html.strip()
    document = BeautifulSoup(html, "html.parser")
    table_data = document.find_all('td')
    for table_row in table_data:
        table_string = table_row.string
        if table_string is not None:
            table_rows.append(table_string.strip())
    return table_rows
