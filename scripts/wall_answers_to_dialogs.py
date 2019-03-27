import pandas as pd
import json

NUM_ANSWERS_IN_POPULAR_COMMENT = 5


def delete_comment(source, comment):
    comment_id = comment['comment_id'].item()
    owner_id = comment['owner_id'].item()
    return source[(source['comment_id'] != comment_id)
                  & (source['owner_id'] == owner_id)]


def get_messages_from_users(person, other, source_answers = None, source_comments = None):
    if isinstance(source_answers, pd.DataFrame):
        answers = source_answers[(source_answers['from_id'] == int(person.item())) |\
                                 (source_answers['from_id'] == int(other.item()))]
    else:
        answers = None
    if isinstance(source_comments, pd.DataFrame):
        comments = source_comments[
            (source_comments['from_id'] == int(person.item())) |\
            (source_comments['from_id'] == int(other.item()))]
    else:
        comments = None
    return answers, comments


def get_answers_from_one_parent(answers, parents_stack):
    return answers[answers['parents_stack']==parents_stack.item()]


def _is_in_used_answers(x, used_answers):
    comment_id = x['comment_id'] if isinstance(x['comment_id'], int) else x['comment_id'].item()
    owner_id = x['owner_id'] if isinstance(x['owner_id'], int) else x['owner_id'].item()
    return (comment_id, owner_id) in used_answers


def get_unused_answer(used_answers, source_answers):
    used_filter = source_answers[['comment_id', 'owner_id']].apply(lambda x: _is_in_used_answers(x, used_answers), axis=1)
    unused = source_answers[~used_filter]
    return unused.head(1)


def get_child_answer(comments, parent_id):
    return comments[comments['reply_to_comment'] == int(parent_id)]


def add_comment_to_dialog(used_answers, dialog, comment, index_row=None):
    """
    :return: (used_answers, dialog)
    """
    comment_id = comment['comment_id'] if isinstance(comment['comment_id'], int) else comment['comment_id'].item()
    owner_id = comment['owner_id'] if isinstance(comment['owner_id'], int) else comment['owner_id'].item()
    used_answers.append((comment_id, owner_id))
    if index_row:
        dialog.append(pd.DataFrame(comment.to_dict(), index=[index_row]))
    else:
        dialog.append(comment)


def get_parent_comment(source_comments, answer):
    """
    Ищет комментарий который указан в parents_stack
    """
    parent_id = json.loads(answer['parents_stack'].item())[0]
    owner_id = answer['owner_id'].item()
    return source_comments[(source_comments['comment_id'] == parent_id) &
                           (source_comments['owner_id'] == owner_id)]


def get_parent(comments, answers, comment):
    """
    если якорь ссылается на какой-то ответ то этот ответ является первым
    иначе первым будет коммент
    """
    if comment['reply_to_comment'].item():
        parent_id = int(comment['reply_to_comment'].item())
        parent_owner = comment['owner_id'].item()
        parent_comment = answers[(answers['comment_id'] == parent_id) \
                                 & (answers['owner_id'] == parent_owner)]
        type_mes = 'answer'
        if parent_comment.empty:
            parent_comment = get_parent_comment(comments, comment)
            type_mes = 'comment'
    else:
        parent_comment = get_parent_comment(comments, comment)
        type_mes = 'comment'
    return type_mes, parent_comment


def get_number_of_partiсipants(answers, comment):
    return len(set(answers['from_id']) | set(comment['from_id']))



def get_dialogs_from_answers(source_answers, source_comments, source_posts):
    dialogs = []
    used_answers = []

    while not get_unused_answer(used_answers, source_answers).empty:
        dialog = []
        anchor = get_unused_answer(used_answers, source_answers)
        type_mes, parent = get_parent(source_comments, source_answers, anchor)
        dialog.append(parent)
        # Оставляем ответы только те, которые относятся к одному комменту
        dialog_answers = get_answers_from_one_parent(source_answers, anchor['parents_stack'])
        # Определяем количество участников в комментариях
        num_of_participants = get_number_of_partiсipants(dialog_answers, get_parent_comment(source_comments, anchor))
        # Оставляем только двух персонажей автора anchor, parent
        dialog_answers, _ = get_messages_from_users(person=anchor['from_id'],
                                                    other=parent['from_id'],
                                                    source_answers=dialog_answers)
        #если parent это answer то необходимо удалить его
        if type_mes == "answer":
            dialog_answers = delete_comment(dialog_answers, parent)
        if 2 == num_of_participants:
            #Если под комментарием только 2 персонажа
            # то можем сделать предположение что все сообщения между двумя выбранными персонами являются диалогом
            for index_row, row in dialog_answers.iterrows():
                add_comment_to_dialog(used_answers, dialog, row, index_row)
        else:
            #Если количество ответов на комментарий велико,
            # то опираемся тольк она поле reply_to_comment
            add_comment_to_dialog(used_answers, dialog, anchor)
            parent_id = int(anchor['comment_id'])
            while not get_child_answer(dialog_answers, parent_id).empty:
                child_answers = get_child_answer(dialog_answers, parent_id)
                for index_row, row in child_answers.iterrows():
                    add_comment_to_dialog(used_answers, dialog, row, index_row)
                    parent_id = int(row['comment_id'])
        dialogs.append(dialog)
    return dialogs


def _remove_unnecessary_info(table):
    table = table.reset_index(drop=True)
    table = table[['comment_id', 'date', 'from_id',\
                   'num_answer', 'owner_id', 'parents_stack',\
                   'post_id', 'text']]
    return table


def _is_last_and_first_utterance_equal(first, second):
    tail = first.tail(1)
    tail = _remove_unnecessary_info(tail)
    head = second.head(1)
    head = _remove_unnecessary_info(head)
    return (tail == head).all(axis=1).item()


def _is_persons_equal(first, second):
    f_set = set(first['from_id'])
    s_set = set(second['from_id'])
    return f_set == s_set


def post_process_dialogs_from_answers(dialogs):
    source_dialogs = list(map(pd.concat, dialogs))
    processed_dialogs = []
    acc = [source_dialogs[0]]
    for dialog in source_dialogs[1:]:
        if _is_last_and_first_utterance_equal(acc[-1], dialog) \
                and _is_persons_equal(acc[-1], dialog):
            acc.append(dialog)
        else:
            completed_dialog = pd.concat(acc)
            completed_dialog = completed_dialog.drop_duplicates()
            processed_dialogs.append(completed_dialog)
            acc = [dialog]
    return processed_dialogs

