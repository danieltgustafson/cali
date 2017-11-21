import pandas as pd
from piplapis.search import SearchAPIRequest
from pandas import DataFrame
import numpy as np

#global parameters
filename = 'C:\Users\Herbie\sample_pop.csv'
# business key
# key = 'kq8birqerxjuvhuwccgm61jc'
# premium key
key = 'b0nomzyl3e8iui4ox6vn48zu'
match_min = 0.25
user_parse_list=['dob','gender','emails','names','addresses','phones','jobs']

users = pd.read_csv(filename)
social_df = DataFrame()
user_df = DataFrame()
query_df = DataFrame()
count = 0
for index, user in users.iterrows():
    request = SearchAPIRequest(
            country=u'US',
            raw_address=user['City'] + ', ' + user['State'] + ' ' + str(user['ZipCode']),
            raw_name=user['BorrowerFullName'], api_key=key, minimum_match=match_min)
    response = request.send()
    response_dict = response.to_dict()
    response_dict['cust_ID'] = user['DatasetID']
    person_list = []
    query_df = query_df.append(response_dict, ignore_index=True)
    if response_dict.has_key('possible_persons'):
        person_list = response_dict['possible_persons']
    elif response_dict.has_key('person'):
        person_list = [response_dict['person']]
    
    if person_list <> []:
        person_index = 0
        for curr_person in person_list:
            person = curr_person.copy()
            person['cust_ID'] = user['DatasetID']
            person['person_index'] = person_index
            user_df = user_df.append(person, ignore_index=True)
            if person.has_key('urls'):
                for social_site in person['urls']:
                    curr_social = social_site.copy()
                    curr_social['cust_ID'] = user['DatasetID']
                    curr_social['person_ID'] = person['@id']
                    curr_social['person_index'] = person_index
                    if person.has_key('@match'):
                        # only works with premium
                        curr_social['match'] = person['@match']
                    social_df = social_df.append(curr_social, ignore_index=True)
            person_index += 1

    if count % 10 == 0:
        print "Query " + str(count)
    count += 1
    if count >= 50:
        break


df_list={}
for column in user_parse_list:
    count=0
    df_list[column]=[]
    for i in user_df[column]:
        print column
        print count
        if pd.isnull(i)==True:
            print 'empty'
            df_list[column].append(i)

        else:
            if column=='dob':
                temp=ast.literal_eval(user_df[column].iloc[i])
                temp['range_start']=temp['date_range']['start']
                temp['range_end']=temp['date_range']['end']
                temp.pop('date_range',None)
                df_list[column].append(pd.DataFrame(temp,index=[0]))
            else:
                try:
                    df_list[column].append(pd.DataFrame(ast.literal_eval(i)))
                except:
                    df_list[column].append(pd.DataFrame(ast.literal_eval(i),index=[0]))
        
            df_list[column][count]['customer_id']=user_df['cust_ID'].iloc[count]
            df_list[column][count]['search_pointer']=user_df['@search_pointer'].iloc[count]
            df_list[column][count]['match']=user_df['@match'].iloc[count]
        count+=1
    temp_list=np.array(df_list[column])
    df_list[column]=pd.concat(temp_list[pd.isnull(temp_list)==False])

#Maybe turn the above into a function instead?
#def field_dict_to_df(df_identifiers,df_series,type='norm',premium=True):



'''
for columns in user_df_parse:
    df_list.append(pd.concat(user_df_parse[columns][pd.isnull(user_df_parse[columns])==False].apply(ast.literal_eval).\
        apply(pd.DataFrame.from_dict).tolist()))

for columns in user_df_parse:
    for i in user_df[columns]:
        if pd.isnull(i)==True:
            dob_parse.append(i)
        else:
            #get list of vals
            temp=ast.literal_eval(i)
            keys=temp.keys()
            dob_parse.append([temp['date_range']['start'],temp['date_range']['end']])
'''
social_df.to_csv('social_df.csv')
user_df.to_csv('user_df.csv')
query_df.to_csv('query_df.csv')
print 'Done'