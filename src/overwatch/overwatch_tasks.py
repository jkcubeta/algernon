import os
import time
from inspect import signature

import enchant
from celery import chord
from celery import group
from fluent import event
from fluent import sender
from core.app import app

from .alg_tasks import alg_utils
from .alg_tasks import credible_tasks
from .alg_tasks import database_tasks
from .alg_tasks import gsuite_tasks
from .alg_tasks import mongo_tasks
from .alg_tasks import pickle_tasks
from .alg_tasks import report_tasks

UPDATE_CLIENT_BATCH_SIZE = alg_utils.get_config('tuning', 'update_client_batch_size')
PICKLE_PAUSE = int(alg_utils.get_config('tuning', 'pickle_pause'))
VC_BATCH_SIZE = alg_utils.get_config('tuning', 'vc_batch_size')
NOTE_ARBITRATION_BATCH_SIZE = alg_utils.get_config('tuning', 'note_arbitration_batch_size')
CLINICAL_HOTWORDS = alg_utils.get_config('overwatch', 'clinical_hotwords')
IGNORE_WORDS = alg_utils.get_config('overwatch', 'ignore_words')
try:
    IGNORE_WORDLIST_PATH = os.path.join(os.path.dirname(__file__), "ignore_words.txt")
except IOError:
    IGNORE_WORDLIST = open(os.path.join(os.path.dirname(__file__), "ignore_words.txt"))
    for word in IGNORE_WORDS:
        IGNORE_WORDLIST.write(word)
    IGNORE_WORDLIST_PATH = os.path.join(os.path.dirname(__file__), "ignore_words.txt")

sender.setup(
    host=alg_utils.get_config('fluent', 'host'),
    port=alg_utils.get_config('fluent', 'port'),
    tag='alg.worker.overwatch')


@app.task
def vc_single(email):
    service = gsuite_tasks.get_email_request(email)
    request = service.users().messages().list(userId=email)
    results = request.execute()
    for result in results:
        mongo_tasks.store_email(result)
    next_results = service.users().messages().list_next(request, results)
    while next_results:
        for result in next_results:
            mongo_tasks.store_email(result)
        next_results = service.users().messages().list_next(request, results)


@app.task
def update_pickle_datastore():
    data_types = ['csw', 'visits', 'clients']
    team_data = report_tasks.get_csw_teams()
    pause = 0
    for team_id in team_data:
        update_team_datastore.apply_async((team_data[team_id], data_types), countdown=pause)
        pause += PICKLE_PAUSE


@app.task
def update_team_datastore(team_data, data_types):
    team_id = team_data['team_id']
    book_tasks = []
    team_name = team_data['team_name']
    book_tasks.append(update_team_book.si(team_id, team_name))
    for data_type in data_types:
        book_tasks.append(get_datastore_report.s(team_id, data_type))
    chord(group(book_tasks))(update_datastore.s(team_name=team_name))


@app.task
def update_single_pickle_datastore(target_team_id):
    data_types = ['csw', 'visits', 'clients']
    team_data = report_tasks.get_csw_teams()
    for team_id in team_data:
        if team_id == str(target_team_id):
            book_tasks = []
            team_name = team_data[team_id]['team_name']
            book_tasks.append(update_team_book.si(team_id, team_name))
            for data_type in data_types:
                book_tasks.append(get_datastore_report.s(team_id, data_type))
            chord(group(book_tasks))(update_datastore.s(team_name=team_name))


@app.task
def get_datastore_report(team_id, data_type):
    return pickle_tasks.get_datastore_report(team_id, sheet_type=data_type)


@app.task
def update_datastore(results, team_name):
    pickle_tasks.update_data_store(results, team_name)


@app.task
def update_team_book(team_id, team_name):
    pickle_tasks.update_team_book(team_id, team_name)


@app.task
def synchronize_groups():
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started synchronize_groups'
        }
    })
    group_map = gsuite_tasks.get_group_name_map()
    credible_user_dict = report_tasks.get_credible_email_report()
    for user_email in credible_user_dict:
        if gsuite_tasks.check_user(user_email):
            user_groups = gsuite_tasks.get_emp_group(user_email)
            user_team = credible_user_dict[user_email]['team_name']
            team_email = group_map[user_team]
            if team_email not in user_groups:
                gsuite_tasks.add_google_group_user(user_email, team_email, credible_user_dict[user_email]['is_admin'])
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished synchronize_groups'
        }
    })


@app.task
def update_emp_email(emp_id, emp_email):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started update_emp_email',
            'employee id': emp_id,
            'email': str(emp_email)
        }
    })
    credible_tasks.update_employee_email(emp_id, emp_email)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished update_emp_email',
            'employee id': emp_id,
            'email': str(emp_email)
        }
    })


@app.task
def lp_emp_email():
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started lp_emp_email'
        }
    })
    credible_users_dict = report_tasks.get_credible_username_check()
    for emp_id in credible_users_dict:
        actual_email = credible_users_dict[emp_id]['actual_email']
        expected_email = credible_users_dict[emp_id]['expected_email']
        if not gsuite_tasks.check_user(actual_email):
            credible_tasks.update_employee_email(emp_id=emp_id, emp_email=expected_email)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished lp_emp_email'
        }
    })


@app.task
def generate_audit_targets(num_targets):
    dates = alg_utils.get_audit_dates()
    targets = report_tasks.get_audit_targets(dates['start_date'], dates['end_date'], num_targets)
    return {'dates': dates, 'targets': targets}


@app.task
def select_audit_targets(num_targets):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started select_audit_targets',
            'num_targets': num_targets
        }
    })
    target_package = generate_audit_targets(num_targets)
    dates = target_package['dates']
    targets = target_package['targets']
    subject = 'audit targets for ' + dates['start_date'] + ' - ' + dates['end_date']
    for team_name in targets:
        clinical_command = report_tasks.get_clinical_command(team_name)
        clinical_command_emails = []
        for emp_id in clinical_command:
            clinical_command_emails.append(clinical_command[emp_id]['email'])
        gsuite_tasks.send_email_to_multiple(clinical_command_emails, subject, str(targets[team_name]))
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished select_audit_targets',
            'num_targets': num_targets
        }
    })


@app.task
def package_arbitration(field_checks, tx_check):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started package_arbitration',
            'field_checks': field_checks,
            'tx_check': tx_check
        }
    })
    arbitration = {}
    hotword_check = field_checks[0]
    clone_check = field_checks[1]
    ghost_check = field_checks[2]
    if hotword_check or clone_check or ghost_check or tx_check:
        arbitration['approve'] = False
        arbitration['red_x_package'] = {}
    else:
        arbitration['approve'] = True
    if hotword_check:
        arbitration['red_x_package']['hotwords'] = hotword_check
    if clone_check:
        arbitration['red_x_package']['clones'] = clone_check
    if ghost_check:
        arbitration['red_x_package']['ghosts'] = ghost_check
    if tx_check:
        arbitration['red_x_package']['tx_plan'] = tx_check
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished package_arbitration',
            'arbitration': arbitration
        }
    })
    return arbitration


@app.task
def arbitrate_note(service_id, ordered_checks=None):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started arbitrate_note',
            'service_id': service_id,
            'ordered_checks': ordered_checks
        }
    })
    if ordered_checks is None:
        ordered_checks = []
    go_ahead = database_tasks.check_single_service_status(service_id)
    if go_ahead:
        database_tasks.record_arbitration_start(service_id, time.time())
        service_package = report_tasks.get_service_package(service_id)
        fields = report_tasks.get_commsupt_fields(service_id)
        header = []
        for check in ordered_checks:
            if check in locals():
                called_function = locals()[check]
                arguments = ordered_checks[check]
                sig = signature(called_function)
                if len(arguments) != len(sig.parameters) - 1:
                    raise SyntaxError('incorrect arguments passed for ' + str(called_function))
                header.append(called_function.s(*arguments))
            else:
                raise KeyError('requested check does not exist!')
        field_check = check_fields(
            service_id,
            fields=fields,
            ghost=True,
            hotword=True,
            clone=True,
            similarities=False,
            restrict_to_full_clones=True
        )
        tx_date_check = check_tx_date(service_id)
        arbitration = package_arbitration(field_check, tx_date_check)
        event.Event('event', {
            'task': 'overwatch_tasks',
            'info': {
                'message': 'finished arbitrate_note',
                'service_id': service_id,
                'arbitration': {'service_package': service_package, 'arbitration': arbitration}
            }
        })
        return {service_id: {'service_package': service_package, 'arbitration': arbitration}}


@app.task
def adjust_notes(results, approve=False):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started adjust_notes',
            'results': results
        }
    })
    checked_results = []
    if type(results) is not list:
        checked_results.append(results)
    else:
        checked_results = results
    credible_tasks.batch_adjust_notes(checked_results, approve)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished adjust_notes',
            'checked_results': checked_results
        }
    })


@app.task
def lp_unapproved_commsupt():
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started lp_unapproved_commsupt'
        }
    })
    unapproved_service_ids = report_tasks.get_unapproved_pilot_whales()
    if unapproved_service_ids:
        pending_service_ids = database_tasks.check_arbitration_status(unapproved_service_ids)
        ids_for_arbitration = [x for x in unapproved_service_ids if x not in pending_service_ids]
        batch_count = 0
        header = []
        for unapproved_service_id in ids_for_arbitration:
            if batch_count >= NOTE_ARBITRATION_BATCH_SIZE:
                chord(header)(adjust_notes.s())
                header = []
                batch_count = 0
            else:
                header.append(arbitrate_note.s(unapproved_service_id))
                batch_count += 1
        if len(header) > 0:
            chord(header)(adjust_notes.s())
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished lp_unapproved_commsupt'
        }
    })


@app.task
def lp_discharge():
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started lp_discharge'
        }
    })
    pending_discharges = report_tasks.get_pending_discharge()
    header = []
    count = 0
    batch = {}
    for client_id in pending_discharges:
        if count >= UPDATE_CLIENT_BATCH_SIZE:
            header.append(update_client_profile_batch.s(batch))
            batch = {}
            count = 0
        else:
            update_package = {'client_status': 'DISCHARGE'}
            batch[client_id] = update_package
            count += 1
    if len(batch) > 0:
        header.append(update_client_profile_batch.s(batch))
    group(header)()


@app.task
def lp_clinical_team():
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started lp_clinical_team'
        }
    })
    update_data = report_tasks.get_profile_changes()
    count = 0
    header = []
    batch = {}
    for client_id in update_data:
        if count >= UPDATE_CLIENT_BATCH_SIZE:
            header.append(update_client_profile_batch.s(batch))
            batch = {}
            count = 0
        else:
            update_package = update_data[client_id]
            batch[client_id] = update_package
            count += 1
    if len(batch) > 0:
        header.append(update_client_profile_batch.s(batch))
    group(header)()
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished lp_clinical_team'
        }
    })


@app.task
def update_client_profile_batch(name_package):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started update_client_profile_batch',
            'name_package': name_package
        }
    })
    credible_tasks.update_client_batch(name_package)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished update_client_profile_batch',
            'name_package': name_package
        }
    })


@app.task
def check_fields(service_id, hotword, clone, ghost, similarities, restrict_to_full_clones=False, fields=None):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started check_fields',
            'service_id': service_id,
            'hotword': hotword,
            'clone': clone,
            'ghost': ghost,
            'similarities': similarities,
            'restrict_to_full_clones': restrict_to_full_clones,
            'fields': fields
        }
    })
    if fields is None:
        fields = report_tasks.get_commsupt_fields(service_id)
    header = []
    if hotword:
        header.append(check_for_hotwords(fields))
    if clone:
        header.append(check_for_clones(
            fields,
            restrict_to_full_clones=restrict_to_full_clones))
    if ghost:
        header.append(check_for_ghosts(fields))
    if similarities:
        header.append(check_for_field_similarities(fields))
        event.Event('event', {
            'task': 'overwatch_tasks',
            'info': {
                'message': 'finished check_fields',
                'header': header
            }
        })
    return header


@app.task
def check_for_clones(fields, restrict_to_full_clones=False):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started check_for_clones',
            'fields': fields,
            'restrict_to_full_clones': restrict_to_full_clones
        }
    })
    clone_package = {}
    presentation = fields['presentation']
    response = fields['response']
    clientvisit_id = fields['clientvisit_id']
    clones = report_tasks.check_service_for_clones(presentation, response, clientvisit_id)
    for service_id in clones:
        match = clones[service_id]['match']
        if match not in clone_package:
            clone_package[match] = [service_id]
        else:
            clone_package[match].append(service_id)
    if restrict_to_full_clones:
        filtered_clones = {}
        for match_type in clones:
            full_match = 'presentation & response'
            if match_type == full_match:
                event.Event('event', {
                    'task': 'overwatch_tasks',
                    'info': {
                        'message': 'finished check_for_clones',
                        'match_type': clone_package[match_type]
                    }
                })
                return {full_match: clone_package[match_type]}
        return filtered_clones
    elif not restrict_to_full_clones:
        event.Event('event', {
            'task': 'overwatch_tasks',
            'info': {
                'message': 'finished check_for_clones',
                'clone_packgage': clone_package
            }
        })
        return clone_package


@app.task
def check_for_hotwords(fields):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started check_for_hotwords',
            'fields': fields
        }
    })
    tagged_hotwords = {}
    for field_name in fields:
        field = fields[field_name].lower()
        field = field.replace(',', '')
        words = field.split(' ')
        for hotword in CLINICAL_HOTWORDS:
            if hotword in words:
                if field_name not in tagged_hotwords:
                    tagged_hotwords[field_name] = [hotword]
                else:
                    tagged_hotwords[field_name].append(hotword)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished check_for_hotwords',
            'tagged_hotwords': tagged_hotwords
        }
    })
    return tagged_hotwords


@app.task
def check_for_ghosts(fields):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started check_for_ghosts',
            'fields': fields
        }
    })
    ghost_results = {}
    checker = enchant.DictWithPWL('en_US', IGNORE_WORDLIST_PATH)
    for field_name in fields:
        off_words = []
        if field_name != 'clientvisit_id':
            field = fields[field_name].lower()
            if len(field) < 11:
                ghost_results['character_results'] = {
                    field_name: str(len(field))
                }
            field = field.replace(',', ' ')
            field = field.replace('"', ' ')
            field = field.replace('\'', ' ')
            field = field.replace('/', ' ')
            field = field.replace('\n', ' ')
            field = field.replace('-', ' ')
            field = field.replace('.', ' ')
            field = field.replace('(', ' ')
            field = field.replace(')', ' ')
            field = field.replace('&', ' ')
            field = field.replace(':', ' ')
            words = field.split(' ')
            if len(words) < 6:
                ghost_results['word_results'] = {
                    field_name: str(len(words))
                }
            for word in words:
                if word and not alg_utils.is_int(word):
                    if not checker.check(word):
                        off_words.append(word)
            if len(off_words) / len(words) > 1 / 6:
                ghost_results['spelling'] = {
                    field_name: off_words
                }
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished check_for_ghosts',
            'ghost_results': ghost_results
        }
    })
    return ghost_results


@app.task
def package_all_field_similarities(results):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started package_all_field_similarities',
            'results': results
        }
    })
    returned_data = {}
    for field_name in results:
        results = results[field_name]
        if results:
            returned_data[field_name] = results
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished package_all_field_similarities',
            'returned_data': returned_data
        }
    })
    return returned_data


@app.task
def check_for_field_similarities(fields):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started check_for_field_similarities',
            'fields': fields
        }
    })
    header = []
    service_id = fields['clientvisit_id']
    del fields['clientvisit_id']
    for field_name in fields:
        field_value = fields[field_name]
        header.append(calculate_field_similarity.s(field_name, field_value, service_id))
    result = chord(header)(package_all_field_similarities.s())
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished check_for_field_similarities',
            'result': result
        }
    })
    return result


@app.task
def calculate_similarities(fields):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started calculate_similarities',
            'fields': fields
        }
    })
    service_id = fields['clientvisit_id']
    del fields['clientvisit_id']
    for field_name in fields:
        field_value = fields[field_name]
        calculate_field_similarity.delay(field_name, field_value, service_id)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished calculate_similarities',
            'fields': fields
        }
    })


@app.task
def calculate_service_similarities(service_id):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started calculate_service_similarities',
            'service_id': service_id
        }
    })
    fields = report_tasks.get_commsupt_fields(service_id)
    calculate_similarities(fields)
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished calculate_service_similarities',
            'service_id': service_id
        }
    })


@app.task
def calculate_field_similarity(field_name, field_value, service_id):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started calculate_field_similarity',
            'field_name': field_name,
            'field_value': field_value,
            'service_id': service_id
        }
    })
    field_ids = {
        'presentation': '514015',
        'interventions': '514018',
        'response': '514019'
    }
    header = []
    field_id = field_ids[field_name]
    foreign_fields = report_tasks.get_given_field(field_id, service_id)
    for foreign_service_id in foreign_fields:
        foreign_field = foreign_fields[foreign_service_id]['field']
        header.append(get_lcs_percentage.s(
            foreign_service_id,
            service_id,
            field_id,
            foreign_field,
            field_value))
    chord(header)(package_single_field_similarities.s(field_name=field_name))
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished calculate_field_similarity',
            'field_name': field_name,
            'field_value': field_value,
            'service_id': service_id
        }
    })


@app.task
def store_field_similarities(field_name, field_value, service_id):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started store_field_similarity',
            'field_name': field_name,
            'field_value': field_value,
            'service_id': service_id
        }
    })
    field_ids = {
        'presentation': '514015',
        'interventions': '514018',
        'response': '514019'
    }
    header = []
    field_id = field_ids[field_name]
    foreign_fields = report_tasks.get_given_field(field_id, service_id)
    for foreign_service_id in foreign_fields:
        foreign_field = foreign_fields[foreign_service_id]['field']
        header.append(get_lcs_percentage.delay(
            foreign_service_id,
            service_id,
            field_id,
            foreign_field,
            field_value))
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished store_field_similarity',
            'field_name': field_name,
            'field_value': field_value,
            'service_id': service_id
        }
    })


@app.task
def package_single_field_similarities(results, field_name):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started package_single_field_similarities',
            'results': results,
            'field_name': field_name
        }
    })
    returned_data = {}
    for service_id in results:
        result = results[service_id]
        if result > 0.9:
            returned_data[service_id] = result
    if returned_data:
        event.Event('event', {
            'task': 'overwatch_tasks',
            'info': {
                'message': 'finished package_single_field_similarities',
                'field_name': field_name[returned_data]
            }
        })
        return {field_name: returned_data}
    else:
        event.Event('event', {
            'task': 'overwatch_tasks',
            'info': {
                'message': 'finished package_single_field_similarities',
                'field_name': '{}'
            }
        })
        return {}


@app.task
def get_lcs_percentage(foreign_service_id, local_service_id, field_id, foreign_field, local_field):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started get_lcs_percentage',
            'foreign_service_id': foreign_service_id,
            'local_service_id': local_service_id,
            'field_id': field_id,
            'foreign_field': foreign_field,
            'local_field': local_field
        }
    })
    result = lcs(local_field, foreign_field)
    percentage_match = float(len(result)) / float(len(local_field))
    database_tasks.store_similarity(
        local_service_id=local_service_id,
        foreign_service_id=foreign_service_id,
        field_id=field_id,
        similarity=round(percentage_match, 2)
    )
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished get_lcs_percentage',
            'return': {foreign_service_id: round(percentage_match, 2)}
        }
    })
    return {foreign_service_id: round(percentage_match, 2)}


@app.task
def lcs(a, b):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started lcs',
            'a': a,
            'b': b
        }
    })
    lengths = [[0 for j in range(len(b) + 1)] for i in range(len(a) + 1)]
    # row 0 and column 0 are initialized to 0 already
    for i, x in enumerate(a):
        for j, y in enumerate(b):
            if x == y:
                lengths[i + 1][j + 1] = lengths[i][j] + 1
            else:
                lengths[i + 1][j + 1] = max(lengths[i + 1][j], lengths[i][j + 1])
    # read the substring out from the matrix
    result = ""
    x, y = len(a), len(b)
    while x != 0 and y != 0:
        if lengths[x][y] == lengths[x - 1][y]:
            x -= 1
        elif lengths[x][y] == lengths[x][y - 1]:
            y -= 1
        else:
            assert a[x - 1] == b[y - 1]
            result = a[x - 1] + result
            x -= 1
            y -= 1
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished lcs',
            'result': result
        }
    })
    return result


@app.task
def check_tx_date(service_id):
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'started check_tx_date',
            'service_id': service_id
        }
    })
    tx_date_check = {}
    tx_plan_check = report_tasks.check_tx_plan_date(service_id)
    if not tx_plan_check:
        tx_date_check['expired'] = True
    event.Event('event', {
        'task': 'overwatch_tasks',
        'info': {
            'message': 'finished check_tx_date',
            'tx_date_check': tx_date_check
        }
    })
    return tx_date_check
