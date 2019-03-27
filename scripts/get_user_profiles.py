import sys
import vk
import pandas as pd
import numpy as np
import time
from itertools import count
import user_info_constants

"""
Последнего обработанного юхер аможно посмотреть в папке users,
сортировав по имени и выбрав последний файл

токен доступа можно получить по этой ссылке
https://oauth.vk.com/authorize?client_id=6848946%20&redirect_uri=https://oauth.vk.com/blank.html&response_type=token&scope=8194
"""
LAST_PREPROCESSED_USER = 6_500_000 # int(sys.argv[1])
ACCESS_TOKEN = "b8a066f200dfdb2f5713697ffd7ddb95fa6700bd80ea887b0b7b83c6a7fa3f1bab9849282abc97db83ba7" # sys.argv[2]
API_VERSION = 5.92
SCOPE='users,wall'
USER_FIELDS = 'about, activities, bdate, books, career,\
                 city, country, education, home_town,\
                 military, movies, music, occupation,\
                 personal, relation, schools, sex, universities, status'
USER_COLUMNS = ['about', 'first_name', 'last_name',\
               'sex', 'bdate','career', 'military',\
                'home_town','relation', 'music', 'activities',\
                'movies', 'books']

def check(ddict, key):
    if key in ddict:
        return ddict[key]
    else:
        return ''

def filter_user_fields(user):
    tmp_user = {col: check(user,col) for col in USER_COLUMNS}
    #
    if 'city' in user:
        tmp_user.update({'city': check(user['city'],'title')})
    if 'country' in user:
        tmp_user.update({'country': check(user['country'],'title')})
    #
    tmp_user.update({'user_id': user['id']})
    if 'occupation' in user:
        tmp_user.update({'occupation': check(user['occupation'],'name')})
        tmp_user.update({'type_occupation': user['occupation']['type']})
    if 'military' in user:
        tmp_user.update({'military': check(user,'military')})
    
    if 'university' in user:
        tmp_user.update({'university_id': check(user, 'university'),\
                         'university_name': check(user, 'university_name'),\
                         'faculty_id': check(user, 'faculty'),\
                         'faculty_name': check(user, 'faculty_name')})
    elif 'universities' in user:
        tmp_user.update({'university_id': check(user['universities'][-1], 'id'),\
                         'university_name': check(user['universities'][-1], 'name'),\
                         'faculty_id': check(user['universities'][-1], 'faculty'),\
                         'faculty_name': check(user['universities'][-1], 'faculty_name')})
    else:
        tmp_user.update({'university_id': '',\
                         'university_name': '',\
                         'faculty_id': '',\
                         'faculty_name': ''})
    tmp_user.update({'graduation': check(user,'graduation')})
    
    #school
    if 'schools' in user:
        if user['schools']:
            tmp_user.update({'school_city': check(user['schools'][-1], 'city'),\
                             'school_name': check(user['schools'][-1], 'name'),\
                             'school_type': check(user['schools'][-1], 'type_str')})
            tmp_user.update({'school_type': check(user['schools'][-1], 'speciality')})
        else:
            tmp_user.update({'school_city': '', 'school_name': '', 'school_type': ''})
    else:
        tmp_user.update({'school_city': '', 'school_name': '', 'school_type': ''})
        
    if 'personal' in user:
        for i in ['people_main', 'political', 'life_main', 'smoking', 'alcohol']:
            if i in user['personal']:
                tmp_user.update({i: user['personal'][i]})
            else:
                tmp_user.update({i: ''})
    return tmp_user

def get_users(vk_api, user_ids):
    assert len(user_ids) <= 1000 #ограничение на запрос от вк
    list_users = vk_api.users.get(user_ids=user_ids, fields=USER_FIELDS, can_access_closed=True, v=API_VERSION)
    list_users = list(map(filter_user_fields, list_users))
    return list_users

def filter_sub(user_id, sub):
    res = []
    for s in sub['items']:
        if s['type'] != 'profile':  
            res.append({'user_id': user_id,\
                        'full_name': s['name'],\
                        'shrot_name': s['screen_name'],\
                        'group_id': s['id']})
    return res

def get_subscriptions(vk_api, user_ids):
    sub_list = []
    not_privated_user_list = []
    for i in user_ids:
        try:
            sub = vk_api.users.getSubscriptions(user_id=i, v=API_VERSION, extended=True, count=200)
            sub_list.extend(filter_sub(i, sub))
            not_privated_user_list.append(i)
        except:
            pass
    return sub_list, not_privated_user_list

def main():
    session = vk.Session(access_token=ACCESS_TOKEN)
    vk_api = vk.API(session, scope=SCOPE)
    from_id = LAST_PREPROCESSED_USER
    to_id = 10**9
    step = 999
    for num, user_bound in enumerate(range(from_id, to_id, step)):
        user_ids = np.arange(user_bound-step, user_bound)
        try:
            print(f"[{user_ids[0]}..{user_ids[-1]}]")
            time.sleep(3)
            users_sub, not_privated_user_list = get_subscriptions(vk_api, user_ids)
            pd.DataFrame(users_sub).to_csv(f"./sub/sub_{num}.csv")
            time.sleep(3)
            users_info = get_users(vk_api, not_privated_user_list)
            pd.DataFrame(users_info).to_csv(f"./users/users_{num}.csv")
        except:
            print(f"badddd {num}")

if __name__ == '__main__':
    main()