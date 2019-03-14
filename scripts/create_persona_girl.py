import pandas as pd
import users_filters as uf
import sys
from json import JSONEncoder


FILE_NAME = sys.argv[1]
subs = pd.read_csv(f'./sub/{FILE_NAME}.csv')
subs = subs.replace({pd.np.nan: None})
users = pd.read_csv(f'./users/{FILE_NAME}.csv')
users.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'], inplace=True)
users = users.replace({pd.np.nan: None})

def main():
    girl = []
    num_girl = 0
    girl_history = []
    for num, user in enumerate(users.iterrows()):
        user = user[1]
        filter_girl = []
        if num > -1:
            user_sub = subs[subs['user_id'] == user['user_id']]
            
            filter_girl.append(uf.sex_filter(user, 1))
            filter_girl.append(uf.age_filter(user, list(range(1988, 2001)), True))
            filter_girl.append(uf.top_univer_filter(user, user_sub))
            filter_girl.append(uf.travel_filter(user, user_sub, threshold=0))
            
            if all(filter_girl):
                girl.append(user['user_id'])
                num_girl += 1
            girl_history.append(filter_girl)
            if  num % 500 == 0 and num > 0:
                print(f"girl: {num_girl}")
                print(f"random_id girl: {girl[-1]}")

    j = JSONEncoder()
    with open(f'./filtered_users/girl_{FILE_NAME}.json', 'w') as fw:
        fw.write(j.encode(girl))
        
if __name__ == '__main__':
    main()