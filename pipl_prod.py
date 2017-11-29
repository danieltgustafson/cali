import pandas as pd
from piplapis.search import SearchAPIRequest
from pandas import DataFrame
import MySQLdb
import urllib2
from sqlalchemy import create_engine
import config as cfg
import numpy as np
import sys
#global parameters
#filename = 'C:\Users\Herbie\sample_pop.csv'
mysql_eng=cfg.mysql_prod
#key_type='key'
key_type=sys.argv[1]
write_type = sys.argv[2]
#write type = replace / append
# premium key.  Business_key = key
match_min = float(sys.argv[3])
query_lim = sys.argv[4]
db_name = sys.argv[5]
key = cfg.pipl[key_type]
user_parse_list=['dob','gender','emails','names','addresses','phones','jobs','urls','user_ids']

engine = create_engine('mysql://'+mysql_eng['user']+':'+mysql_eng['pass']+'@'+mysql_eng['host']+':'+mysql_eng['port']+'/'+db_name,encoding='utf8',convert_unicode=True)
db=MySQLdb.connect(host=mysql_eng['host'],user=mysql_eng['user'],passwd=mysql_eng['pass'],db=db_name,charset='utf8',use_unicode=True)
cur = db.cursor()

#users = pd.read_csv(filename)

query="SELECT * FROM "+db_name+".user_data limit " + str(query_lim)
cur.execute(query)

names=[]
for rows in cur.fetchall():
    names.append(rows)
users=pd.DataFrame(names)
users.columns=[i[0] for i in cur.description]

def get_responses(key,key_type,users,query_lim):
    print match_min
    social_df = DataFrame()
    user_df = DataFrame()
    query_df = DataFrame()
    user_social_df = DataFrame()
    count = 0
    for index, user in users.iterrows():
        request = SearchAPIRequest(
                country=u'US',
                raw_address=user['City'] + ', ' + user['State'] + ' ' + str(user['ZipCode']),
                raw_name=user['BorrowerFullName'], 
                api_key=key, 
                minimum_match=match_min)
        response = request.send()
        try:
            response_dict = response.to_dict()
        except:
            response_dict={}
            print 'response error for user ' + str(user['customer_id'])
        response_dict['customer_id'] = user['customer_id']
        print user['customer_id']
        person_list = []
        if response_dict.has_key('possible_persons'):
            person_list = response_dict['possible_persons']
        elif response_dict.has_key('person'):
            person_list = [response_dict['person']]
        
        if person_list <> []:
            if response_dict.has_key('available_data'):
                response_dict['available_data']['customer_id']=user['customer_id']
                query_df = query_df.append(pd.DataFrame(response_dict['available_data']))
            person_index = 0
            for curr_person in person_list:
                person = curr_person.copy()
                for i in set(user_parse_list) - set(person.keys()):
                    person[i]=float('NaN')
                if key_type=='key':
                    person['@match']='None'
                person['customer_id'] = user['customer_id']
                person['key_type'] = key_type
                person['internal_id'] = str(person['customer_id'])+str(person_index)

                user_df = user_df.append(person, ignore_index=True)

                person_index += 1
        if count % 10 == 0:
            print "Query " + str(count)
        count += 1
        #if count >= query_lim:
        #    break
    user_df['key_type'],social_df['key_type'],user_social_df['key_type']=key_type,key_type,key_type
    query_df=query_df.reset_index()
    #query_df.columns=['data_type','basic_count','premium_count','customer_id']
    return social_df,user_df,query_df,user_social_df       

social_df,user_df,query_df,user_social_df=get_responses(key,key_type,users,query_lim)

df_list={}
for column in user_parse_list:
    count=0
    df_list[column]=[]
    for i in user_df[column]:
        print column
        print count
        if np.all(pd.isnull(i))==True:
            print 'empty'
            df_list[column].append(i)
        else:
            if 'date_range' in i:
                i['range_start']=i['date_range']['start']
                i['range_end']=i['date_range']['end']
                i.pop('date_range',None)
            try:
                df_list[column].append(pd.DataFrame(i))
            except:
                df_list[column].append(pd.DataFrame(i,index=[0]))


            df_list[column][count]['customer_id']=user_df['customer_id'].iloc[count]
            df_list[column][count]['@id']=user_df['@id'].iloc[count]
            df_list[column][count]['key_type']=user_df['key_type'].iloc[count]
            df_list[column][count]['match']=user_df['@match'].iloc[count]
            df_list[column][count]['internal_id']=user_df['internal_id'].iloc[count]
        count+=1
    temp_list=np.array(df_list[column])
    if pd.isnull(temp_list).all()==False:
        df_list[column]=pd.concat(temp_list[pd.isnull(temp_list)==False])
    else:
        del df_list[column]

#Maybe turn the above into a function instead?
#def field_dict_to_df(df_identifiers,df_series,type='norm',premium=True):

#Split user ids from domains in the user_ids df:
df_list['user_ids']['user_id']=df_list['user_ids']['display'].str.split('@').str[0]
df_list['user_ids']['domain']=df_list['user_ids']['display'].str.split('@').str[1]
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
#For CSV writing instead of DB
#social_df.to_csv('social_df.csv')
#user_df.to_csv('user_df.csv')
#query_df.to_csv('query_df.csv')


for i in df_list:
    if 'date_range' in df_list[i].columns:
        df_list[i]['date_range']=df_list[i]['date_range'].astype(str)
    df_list[i].to_sql(i+'_'+key_type,engine,if_exists=write_type,chunksize=100)
#social_df.to_sql('urls',engine,if_exists=write_type,chunksize=100)
if key_type=='key':
    query_df.to_sql('query',engine,if_exists=write_type,chunksize=100)


print 'Done'