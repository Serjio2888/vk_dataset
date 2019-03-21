import pandas as pd
import numpy as np
import sys
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


# base
def _get_dialogs_from_answers(source_answers, source_comments, source_posts):
    from json import JSONDecoder
    j = JSONDecoder()
    
    already_used_id = []
    dialogs = []

    answers = source_answers.copy()
    anchor = answers[list(map(lambda x: x != '[]', answers['parents_stack']))].head(1)
    while not anchor.empty:
        dialog = []
        already_used_id.append(int(anchor['comment_id']))

        parent_id = j.decode(answers['parents_stack'].iloc[0])[0]
        dialog.append(source_comments[source_comments['comment_id'] == parent_id].head(1))
        dialog.append(anchor)

        after_anchor = answers[answers['reply_to_comment'] == int(anchor['comment_id'])].head(1)
        while not after_anchor.empty:
            anchor = after_anchor
            already_used_id.append(anchor['comment_id'])
            dialog.append(anchor)
            after_anchor = answers[answers['reply_to_comment'] == int(anchor['comment_id'])].head(1)

        if len(pd.concat(dialog)) > 1:
            dialogs.append(pd.concat(dialog))

        answers = answers[~answers['comment_id'].isin(already_used_id)]
        anchor = answers[list(map(bool, answers['parents_stack']))].head(1)
    return dialogs


def answer_post_processing(dialogs):
    res = []
    dialog = []
    dialog.append(dialogs[0])
    for cur in dialogs[1:]:
        if set(dialog[-1]['from_id']) == set(cur['from_id'])\
               and set(dialog[-1]['post_id']) == set(cur['post_id']):
            dialog.append(cur)
        else:
            if len(dialog) > 1:
                head = dialog[0]
                tail = pd.concat(dialog[1:])
                tail = tail[tail['parents_stack'] != '[]']
                res.append(pd.concat([head, tail]))
            else:
                res.append(pd.concat(dialog))
            dialog = [cur]
    return res


def get_dialogs_from_comments(source_answers, source_comments, source_posts):
    already_used_id = []
    dialogs = []
    comments = source_comments.copy()

    comments = comments[~comments['comment_id'].isin(already_used_id)]
    anchor = comments[list(map(bool, comments['reply_to_comment']))].head(1)
    while(not anchor['reply_to_comment'].dropna().empty):
        dialog = []
        comments = comments[~comments['comment_id'].isin(already_used_id)]
        anchor = comments[list(map(bool, comments['reply_to_comment']))].head(1)
        already_used_id.append(int(anchor['comment_id']))
        first = comments[comments['comment_id'] == int(anchor['reply_to_comment'])].head(1)
        dialog.append(first)
        dialog.append(anchor)
        after_anchor = comments[comments['reply_to_comment'] == int(anchor['comment_id'])].head(1)
        filter_user = (comments['from_id'] == int(anchor['reply_to_user']))\
                      | (comments['from_id'] == int(anchor['from_id']))
        filter_post = comments['post_id'] == int(anchor['post_id'])
        while(not after_anchor.dropna().empty):
            anchor = after_anchor
            already_used_id.append(int(anchor['comment_id']))
            dialog.append(anchor)
            filter_reply = comments['reply_to_comment'] == int(anchor['comment_id'])
            after_anchor = comments[filter_reply & filter_user & filter_post].head(1)

        if len(pd.concat(dialog)) > 1:
            dialogs.append(pd.concat(dialog))
        comments = comments[~comments['comment_id'].isin(already_used_id)]
        anchor = comments[list(map(bool, comments['reply_to_comment']))].head(1)
    return dialogs


def comments_post_processing(dialogs):
    res = []
    dialog = []
    dialog.append(dialogs[0])
    for cur in dialogs[1:]:
        if set(dialog[-1]['from_id']) == set(cur['from_id'])\
               and set(dialog[-1]['post_id']) == set(cur['post_id']):
            dialog.append(cur)
        else:
            res.append(pd.concat(dialog))
            dialog = [cur]
    return res


def dialogs_to_json(dialogs, path_to_save):
    from json import JSONEncoder
    
    j = JSONEncoder()
    all_dialogs = []
    persona_id = 0
    other_id = 0
    
    for num, i in enumerate(dialogs):
        dialog = []
        for _, row in i.iterrows():
            if row['from_id'] == row['owner_id']:
                persona_type = 'persona'
                persona_id = int(row['from_id'])
            else:
                persona_type = 'other'
                other_id = int(row['from_id'])
            utterance = {
                "persona_type": persona_type,
                "num_answers": int(row['num_answer']),
                "text": row['text']
            }
            dialog.append(utterance)
        
        post = source_posts[source_posts['post_id'] == int(i['post_id'].iloc[0])].head(1)
        curr_dialog = {
            "post_id": int(post['post_id']),
            "owner_id": int(post['owner_id']),
            "dialog_id": f"140319_{str(num)}",
            "post_text": post['text'].iloc[0],
            "post_num_comments": int(post['num_comments'].iloc[0]),
            "persona_id": persona_id,
            "other_id": other_id,
            "dialog": dialog
        }
        all_dialogs.append(curr_dialog)
    with open(path_to_save, 'w') as fw:
        fw.write(j.encode(all_dialogs))


def main():

    FILE_NAME = "14.03.19-14:16"  # sys.argv[1]

    source_comments = pd.read_csv(f'~/Programs/vk_dataset/wall/girl_wall/comments/{FILE_NAME}.csv', index_col=0)
    source_answers = pd.read_csv(f'~/Programs/vk_dataset/wall/girl_wall/answers/{FILE_NAME}.csv', index_col=0)
    source_posts = pd.read_csv(f'~/Programs/vk_dataset/wall/girl_wall/posts/{FILE_NAME}.csv', index_col=0)
    source_comments.replace({pd.np.NaN: None}, inplace=True)
    source_answers.replace({pd.np.NaN: None}, inplace=True)
    source_posts.replace({pd.np.NaN: None}, inplace=True)
    source_comments.sort_values(by=['post_id', 'date'], inplace=True)
    source_answers.sort_values(by=['post_id', 'date'], inplace=True)

    answers_dialogs = get_dialogs_from_answers(source_answers, source_comments, source_posts)
    proc_answers_dialogs = answer_post_processing(answers_dialogs)
    comments_dialogs = get_dialogs_from_comments(source_answers, source_comments, source_posts)
    proc_comments_dialogs = comments_post_processing(comments_dialogs)
    concat_dialogs = proc_answers_dialogs.copy()
    concat_dialogs.extend(proc_comments_dialogs)
    dialogs_to_json(concat_dialogs, f'./result_json/{FILE_NAME}.json')

if __name__ == '__main__':
    FILE_NAME = "14.03.19-14:16"
    source_comments = pd.read_csv(f'~/Programs/vk_dataset/wall/girl_wall/comments/{FILE_NAME}.csv', index_col=0)
    source_answers = pd.read_csv(f'~/Programs/vk_dataset/wall/girl_wall/answers/{FILE_NAME}.csv', index_col=0)
    source_posts = pd.read_csv(f'~/Programs/vk_dataset/wall/girl_wall/posts/{FILE_NAME}.csv', index_col=0)
    source_answers = source_answers.loc[:, ~source_answers.columns.str.contains('^Unnamed')]
    source_comments = source_comments.loc[:, ~source_comments.columns.str.contains('^Unnamed')]
    source_posts = source_posts.loc[:, ~source_posts.columns.str.contains('^Unnamed')]
    source_comments.replace({pd.np.NaN: None}, inplace=True)
    source_answers.replace({pd.np.NaN: None}, inplace=True)
    source_posts.replace({pd.np.NaN: None}, inplace=True)
    source_comments.drop_duplicates(inplace=True)
    source_answers.drop_duplicates(inplace=True)
    source_posts.drop_duplicates(inplace=True)
    source_comments.sort_values(by=['post_id', 'owner_id', 'date'], inplace=True)
    source_answers.sort_values(by=['post_id', 'owner_id', 'date'], inplace=True)

    result = get_dialogs_from_answers(source_answers, source_comments, None)

    with open('./result.txt', 'w') as f:
        for num, dialog in enumerate(result):
            f.write(str(num) + " " + '\n')
            for _, row in pd.concat(dialog).iterrows():
                f.write(str(row['text']) + '\n')
            f.write('\n')
