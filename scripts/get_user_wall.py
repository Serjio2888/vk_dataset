import vk
import time
import pandas as pd
import sys

API_VERSION = 5.92
LAST_PREPROCESSED_USER = int(sys.argv[1])
FILE_NAME = sys.argv[2]
ACCESS_TOKEN = sys.argv[3]
SCOPE = 'users,wall'
COUNT_PER_REQUEST = 100


def parse_user_wall(response):
    res = []
    for item in response['items']:
        if item.get('deleted'):
            continue
        res.append({
            'post_id': item['id'],
            'owner_id': item['owner_id'],
            'from_id': item['from_id'],
            'text': item.get('text'),
            'reply_owner_id': item.get('reply_owner_id'),
            'reply_post_id': item.get('reply_post_id'),
            'num_comments': item.get('comments')['count'] if item.get('comments') else 0,
            'date': item.get('date')
        })
    return res


def get_user_wall(vk_api, user_id, timeout=0.5, max_count=1000):
    wall = []
    response = vk_api.wall.get(owner_id=user_id,
                               offset=0,
                               count=COUNT_PER_REQUEST,
                               v=API_VERSION)
    wall.extend(parse_user_wall(response))
    time.sleep(timeout)
    #добираем оставшиеся записи
    num_wall = response['count']
    for offset in range(COUNT_PER_REQUEST,
                        min(COUNT_PER_REQUEST*((num_wall//COUNT_PER_REQUEST)+1), max_count),
                        COUNT_PER_REQUEST):
        response = vk_api.wall.get(owner_id=user_id,
                                   offset=offset,
                                   count=COUNT_PER_REQUEST,
                                   v=API_VERSION)
        wall.extend(parse_user_wall(response))
        time.sleep(timeout)
    return wall


def parse_comment_post(response):
    res = []
    for item in response['items']:
        if item.get('deleted'):
            continue
        res.append({
            'post_id': item['post_id'],
            'owner_id': item['owner_id'],
            'comment_id': item['id'],
            'from_id': item['from_id'],
            'text': item.get('text'),
            'reply_to_user': item.get('reply_to_user'),
            'reply_to_comment': item.get('reply_to_comment'),
            'parents_stack': item.get('parents_stack'),
            'num_answer': item.get('thread')['count'] if item.get('thread') else 0,
            'date': item.get('date')
        })
    return res


def get_comment_post(vk_api, post, timeout=0.5, max_count=1000):
    if post['num_comments'] == 0:
        return []
    comments = []
    response = vk_api.wall.getComments(owner_id=post['owner_id'],
                                       post_id=post['post_id'],
                                       offset=0,
                                       count=COUNT_PER_REQUEST,
                                       v=API_VERSION)
    comments.extend(parse_comment_post(response))
    time.sleep(timeout)
    #добираем оставшиеся
    num_comments = response['count']
    for offset in range(COUNT_PER_REQUEST,
                        min(COUNT_PER_REQUEST*((num_comments//COUNT_PER_REQUEST)+1), max_count),
                        COUNT_PER_REQUEST):
        response = vk_api.wall.getComments(owner_id=post['owner_id'],
                                           post_id=post['post_id'],
                                           offset=offset,
                                           count=COUNT_PER_REQUEST,
                                           v=API_VERSION)
        comments.extend(parse_comment_post(response))
        time.sleep(timeout)
    return comments


def get_answer_comment(vk_api, comment, timeout=0.5, max_count=1000):
    if comment['num_answer'] == 0:
        return []
    answers = []
    response = vk_api.wall.getComments(owner_id=comment['owner_id'],
                                       post_id=comment['post_id'],
                                       offset=0,
                                       count=COUNT_PER_REQUEST,
                                       comment_id=comment['comment_id'],
                                       v=API_VERSION)
    answers.extend(parse_comment_post(response))
    time.sleep(timeout)
    #добираем оставшиеся
    num_answers = response['count']
    for offset in range(COUNT_PER_REQUEST,
                        min(COUNT_PER_REQUEST*((num_answers//COUNT_PER_REQUEST) + 1), max_count),
                        COUNT_PER_REQUEST):
        response = vk_api.wall.getComments(owner_id=comment['owner_id'],
                                           post_id=comment['post_id'],
                                           offset=offset,
                                           count=COUNT_PER_REQUEST,
                                           comment_id=comment['comment_id'],
                                           v=API_VERSION)
        answers.extend(parse_comment_post(response))
        time.sleep(timeout)
    return answers

from json import JSONDecoder


def main(path_to_filtered_users, last_preprocessed_user):
    all_posts = []
    all_comments = []
    all_answers = []

    # загрузка юзеров
    j = JSONDecoder()
    users = j.decode(open(path_to_filtered_users, 'r').read())
    users = list(map(int, users))
    # задание сессии
    session = vk.Session(access_token=ACCESS_TOKEN)
    vk_api = vk.API(session, scope=SCOPE)
    # поиск данных
    for num, user in enumerate(users):
        if user > last_preprocessed_user:
            if num % 100 == 0:
                print(user)
            try:
                posts = get_user_wall(vk_api=vk_api, user_id=user, timeout=0.34)
                comments = []
                for post in posts:
                    comments.extend(get_comment_post(vk_api=vk_api, post=post, timeout=0.34))
                answers = []
                for comment in comments:
                    answers.extend(get_answer_comment(vk_api=vk_api, comment=comment, timeout=0.34))
                all_posts.extend(posts)
                all_comments.extend(comments)
                all_answers.extend(answers)
            except Exception as e:
                print(e)
    return all_posts, all_comments, all_answers

if __name__ == '__main__':
    all_posts, all_comments, all_answers =\
                main(f'./filtered_users/{FILE_NAME}.json', LAST_PREPROCESSED_USER)
    pd.DataFrame(all_posts).to_csv(f'./wall/girl_wall/posts/__14__{FILE_NAME}.csv')
    pd.DataFrame(all_comments).to_csv(f'./wall/girl_wall/comments/__14__{FILE_NAME}.csv')
    pd.DataFrame(all_answers).to_csv(f'./wall/girl_wall/answers/__14__{FILE_NAME}.csv')
