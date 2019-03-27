import json
import pandas as pd


def _get_post(source_table, row):
    post_id = _get_item_from_row('post_id', row)
    owner_id = _get_item_from_row('owner_id', row)
    return source_table[(source_table['post_id'] == post_id)
                        & (source_table['owner_id'] == owner_id)]


def _get_item_from_row(field, pd_row):
    result = pd_row[field] if not isinstance(pd_row[field], pd.core.series.Series) else pd_row[field].item()
    return result


def _get_type_author(row, girl_ids, pop_ids):
    if _get_item_from_row('from_id', row) in girl_ids:
        return 'girl'
    elif _get_item_from_row('from_id', row) in pop_ids:
        return 'pop'
    else:
        return 'other'


def _get_idx_of_speaker(users_in_dialog, row):
    user_id = _get_item_from_row('from_id', row)
    return users_in_dialog[user_id]


def dump_dialogs(dialogs, file_girls_ids, file_pops_ids, source_post):

    list_girl_id = set(json.load(open(file_girls_ids)))  # '../filtered_users/21.02.19-12:08_girl.json')))
    list_pop_id = set(json.load(open(file_pops_ids)))  # '../filtered_users/21.02.19-12:08_pop.json')))

    json_dialogs = []
    for dialog in dialogs:
        json_dialog = {}
        users_in_dialog = set(dialog['from_id'])
        user_to_num = dict(zip(users_in_dialog, range(len(users_in_dialog))))
        filtered_dialog = []
        for index_row, row in dialog.iterrows():
            replica = {}
            replica.update({'comment_id': _get_item_from_row('comment_id', row)})
            replica.update({'text': _get_item_from_row('text', row)})
            replica.update({'date': _get_item_from_row('date', row)})
            replica.update({'mess_from_user': _get_item_from_row('from_id', row)})
            replica.update({'reply_to_user': _get_item_from_row('reply_to_user', row)})
            replica.update({'type_author': _get_type_author(row,
                                                            list_girl_id,
                                                            list_pop_id)})
            replica.update({'idx_speaker': _get_idx_of_speaker(user_to_num, row)})
            filtered_dialog.append(replica)

        current_post = _get_post(source_post, dialog.head(1))
        json_dialog.update({'post_id': _get_item_from_row('post_id', current_post)})
        json_dialog.update({'wall_owner_id': _get_item_from_row('owner_id', current_post)})
        json_dialog.update({'post_num_comments': _get_item_from_row('num_comments', current_post)})
        json_dialog.update({'post_text': _get_item_from_row('text', current_post)})
        json_dialog.update({'post_date': _get_item_from_row('date', current_post)})
        json_dialog.update({'dialog': filtered_dialog})
        json_dialog.update({'dialog_theme': "#####"})
        json_dialog.update({'dialog_eval': "#####"})

        json_dialogs.append(json_dialog)
    return json_dialogs
