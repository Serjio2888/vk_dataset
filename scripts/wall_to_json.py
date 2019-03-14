import pandas as pd
import sys


# base
def get_dialogs_from_answers(source_answers, source_comments, source_posts):
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
        filter_user = (comments['from_id'] == int(anchor['reply_to_user']))
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

    source_comments = pd.read_csv(f'./wall/girl_wall/comments/{FILE_NAME}.csv', index_col=0)
    source_answers = pd.read_csv(f'./wall/girl_wall/answers/{FILE_NAME}.csv', index_col=0)
    source_posts = pd.read_csv(f'./wall/girl_wall/posts/{FILE_NAME}.csv', index_col=0)
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

print("blyat")
if __name__ == '__main__':
    print("suka")
    main()
