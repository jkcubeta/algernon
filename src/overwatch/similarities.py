from celery import chord
from core.app import app

from .alg_tasks import database_tasks
from .alg_tasks import report_tasks


@app.task
def calculate_similarities(service_id):
    header = []
    fields = report_tasks.get_commsupt_fields(service_id)
    service_id = fields['clientvisit_id']
    del fields['clientvisit_id']
    for field_name in fields:
        field_value = fields[field_name]
        header.append(calculate_field_similarities.s(field_name, field_value, service_id))
    chord(header)(store_results.s())


@app.task
def store_results(results):
    for service_dict in results:
        database_tasks.store_similarity(
            local_service_id=service_dict['service_id'],
            foreign_service_id=service_dict['foreign_service_id'],
            field_id=service_dict['field_id'],
            similarity=service_dict['similarity']
        )


@app.task
def move_along(results):
    return results


@app.task
def calculate_field_similarities(field_name, field_value, service_id):
    header = []
    field_ids = {
        'presentation': '514015',
        'interventions': '514018',
        'response': '514019'
    }
    field_id = field_ids[field_name]
    foreign_fields = report_tasks.get_given_field(field_id, service_id)
    for foreign_service_id in foreign_fields:
        foreign_field = foreign_fields[foreign_service_id]['field']
        header.append(calculate_single_similarity.s(
            foreign_service_id,
            service_id,
            field_id,
            foreign_field,
            field_value)
        )
    chord(header)(move_along.s())


@app.task
def calculate_single_similarity(foreign_service_id, local_service_id, field_id, foreign_field, local_field):
    result = lcs(local_field, foreign_field)
    percentage_match = float(len(result)) / float(len(local_field))
    returned_data = {
        'service_id': local_service_id,
        'foreign_service_id': foreign_service_id,
        'field_id': field_id,
        'similarity': round(percentage_match, 2)
    }
    return returned_data


@app.task
def lcs(a, b):
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
    return result
