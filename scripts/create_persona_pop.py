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
    pop = []
    num_pop = 0
    pop_history = []
    for num, user in enumerate(users.iterrows()):
        user = user[1]
        filter_pop = []
        if num > -1:
            user_sub = subs[subs['user_id'] == user['user_id']]
            
            filter_pop.append(uf.age_filter(user, list(range(2003, 1950)), True))
            filter_pop.append(uf.nauchpop_filter(user, user_sub, threshold=1))
            
            if all(filter_pop):
                pop.append(user['user_id'])
                num_pop += 1
            pop_history.append(filter_pop)
            if  num % 500 == 0 and num > 0:
                print(f"pop: {num_pop}")
                print(f"pop: {pop[-1]}")
    j = JSONEncoder()
    with open(f'./filtered_users/pop_{FILE_NAME}.json', 'w') as fw:
        fw.write(j.encode(pop))

if __name__ == '__main__':
    main()