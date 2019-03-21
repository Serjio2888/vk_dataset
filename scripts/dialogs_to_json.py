import json
import pandas as pd


def _get_item_from_row(field, pd_row):
    result = pd_row[field] if not isinstance(pd_row[field], pd.core.series.Series) else pd_row[field].item()
    return result


def _get_type_author(row, girl_ids, pop_ids):
    if _get_item_from_row('from_id', row) in girl_ids:
        return 'girl'
    elif _get_item_from_row('from_id', row) in pop_ids:
        return 'pop'


def dialogs_to_json(dialogs):
    list_girl_id = set(json.load(open('../filtered_users/21.02.19-12:08_girl.json')))
    list_pop_id = set(json.load(open('../filtered_users/21.02.19-12:08_pop.json')))
    json_dialogs = []
    for dialog in dialogs:
        json_dialog = []
        for index_row, row in dialog.iterrows():
            replica = {}
            replica.update({'text': _get_item_from_row('text', row)})
            replica.update({'owner_id': _get_item_from_row('owner_id', row)})
            replica.update({'date': _get_item_from_row('date', row)})
            replica.update({'from_id': _get_item_from_row('from_id', row)})
            replica.update({'date': _get_item_from_row('date', row)})
            replica.update({'reply_to_user': _get_item_from_row('reply_to_user', row)})
            replica.update({'type_author': _get_type_author(row,
                                                            list_girl_id,
                                                            list_pop_id)})

            if


