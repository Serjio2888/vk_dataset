import pandas as pd
import json


def _get_item_from_row(field, pd_row):
    result = pd_row[field] if not isinstance(pd_row[field], pd.core.series.Series) else pd_row[field].item()
    return result


def _is_in(x, in_comments):
    comment_id = _get_item_from_row('comment_id', x)
    owner_id = _get_item_from_row('owner_id', x)
    return (comment_id, owner_id) in in_comments


def get_unused_comment(used_comments, from_comments):
    used_filter = from_comments[['comment_id', 'owner_id']].apply(lambda x: _is_in(x, used_comments), axis=1)
    unused = from_comments[~used_filter]
    return unused.head(1)


def get_parent_comment(child, from_comments):
    parent_id = _get_item_from_row('reply_to_comment', child)
    parent_owner = _get_item_from_row('owner_id', child)
    return from_comments[(from_comments['comment_id'] == parent_id)
                         & (from_comments['owner_id'] == parent_owner)]


def get_comments_from_one_post(comment_from_post, from_comments):
    post_id = _get_item_from_row('post_id', comment_from_post)
    owner_id = _get_item_from_row('owner_id', comment_from_post)
    return from_comments[(from_comments['post_id'] == post_id)
                         & (from_comments['owner_id'] == owner_id)]


def get_num_participants(posts_comments):
    return len(set(posts_comments['from_id']))


def get_comments_from_users(person, other, from_comments):
    return from_comments[(from_comments['from_id'] == person)
                         | (from_comments['from_id'] == other)]


def add_comment_to_dialog(used_comments, dialog, comment, index_row=None):
    comment_id = _get_item_from_row('comment_id', comment)
    owner_id = _get_item_from_row('owner_id', comment)
    used_comments.add((comment_id, owner_id))
    if index_row:
        dialog.append(pd.DataFrame(comment.to_dict(), index=[index_row]))
    else:
        dialog.append(comment)


def delete_comment(comment, from_comments):
    comment_id = _get_item_from_row('comment_id', comment)
    owner_id = _get_item_from_row('owner_id', comment)
    used_filter = from_comments.apply(lambda x: _is_in(x, [(comment_id, owner_id)]), axis=1)
    return from_comments[~used_filter]


def get_child_comments(parent_id, from_comments):
    return from_comments[from_comments['reply_to_comment'] == parent_id]


def get_dialogs_from_comments(source_answers, source_comments, source_posts):
    dialogs = []
    used_comments = []
    comments_with_reply = source_comments[source_comments['reply_to_comment'].apply(lambda x: bool(x))]

    while not get_unused_comment(used_comments, comments_with_reply).empty:
        dialog = []
        anchor = get_unused_comment(used_comments, from_comments=comments_with_reply)
        parent = get_parent_comment(child=anchor, from_comments=source_comments)
        dialog.append(parent)
        # Оставляем только те, которые относятся к одному посту
        posts_comments = get_comments_from_one_post(comment_from_post=anchor, from_comments=source_comments)
        # Определяем количество участников в комментариях к посту
        num_participants = get_num_participants(posts_comments)
        # Оставляем только 2 персонажей: anchor, parent
        dialog_comments = get_comments_from_users(person=_get_item_from_row('from_id', anchor),
                                                  other=_get_item_from_row('from_id', parent),
                                                  from_comments=posts_comments)
        #Удаляем parent из dialog_comments
        dialog_comments = delete_comment(comment=parent, from_comments=dialog_comments)
        if 2 == num_participants:
        # Если под постом только 2 персонажа
        # то можем сделать предположение что все сообщения между двумя выбранными персонами являются диалогом
            for index_row, row in dialog_comments.iterrows():
                add_comment_to_dialog(used_comments, dialog, row, index_row)
        else:
        # Иначе приходится опираться только на reply_to_comment
            add_comment_to_dialog(used_comments, dialog, anchor)
            parent_id = _get_item_from_row('comment_id', anchor)
            while not get_child_comments(parent_id, from_comments=dialog_comments).empty:
                child_comment = get_child_comments(parent_id, from_comments=dialog_comments)
                for index_row, row in child_comment.iterrows():
                    add_comment_to_dialog(used_comments, dialog, row, index_row)
                    parent_id = _get_item_from_row('comment_id', row)
        dialogs.append(dialog)
    return dialogs


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

    result = get_dialogs_from_comments(source_answers, source_comments, None)

    with open("~/Programs/vk_dataset/tmp_results/comments_dialogs.txt", 'w') as f:
        for num, dialog in enumerate(result):
            f.write(str(num) + " " + '\n')
            for _, row in pd.concat(dialog).iterrows():
                f.write(str(row['text']) + '\n')
            f.write('\n')

