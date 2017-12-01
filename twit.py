import MySQLdb
import urllib2
from TwitterAPI import TwitterAPI
import json
import pandas as pd
from sqlalchemy import create_engine
import config as cfg
import sys
import numpy as np

mysql_eng=cfg.mysql_prod
db_name = sys.argv[1]
counts=sys.argv[2]
write_type=sys.argv[3]
#if using the dev mysql switch connection params.
#mysql_eng=cfg.mysql_dev

engine = create_engine('mysql://'+mysql_eng['user']+':'+mysql_eng['pass']+'@'+mysql_eng['host']+':'+mysql_eng['port']+'/'+db_name,encoding='utf8',convert_unicode=True)
main_column_list=['input_user_name','text','lang','tweet_index']
entity_columns=['hashtags','urls','user_id','user name']

###MAX NUMBER OF TWEETS TO STORE####



db=MySQLdb.connect(host=mysql_eng['host'],user=mysql_eng['user'],passwd=mysql_eng['pass'],db=db_name,charset='utf8',use_unicode=True)
cur = db.cursor()
names=[]

cur.execute("SELECT user_id from user_ids_key where lower(domain) = 'twitter'")

for rows in cur.fetchall():
	names.append(rows[0])

dfs=[]
json_raw={}
j=0
api = TwitterAPI(consumer_key=cfg.twitter['api_key'], consumer_secret=cfg.twitter['secret'], access_token_key=cfg.twitter['access_token'], 
	access_token_secret=cfg.twitter['access_secret'])
for i in names:
	r=api.request('statuses/user_timeline', {'user_id':i,'count':counts})
	json_raw[i]=r.json()
	#dfs.append(pd.io.json.DataFrame(json_raw[i]))
	dfs.append(pd.io.json.json_normalize(json_raw[i]))
	dfs[j]['input_user_name']=i
	dfs[j]['tweet_index']=range(0,len(dfs[j]))
	j+=1
prime = pd.concat(dfs, axis=0, ignore_index=True)

'''
ent_dfs=[]
j=0
for i in prime.entities:
	if pd.isnull(i):
		ent_dfs.append(pd.DataFrame())
	else:
		ent_dfs.append(pd.DataFrame.from_dict(i,orient='index').transpose())
	ent_dfs[j]['input_user_name']=prime['input_user_name'][j]
	ent_dfs[j]['tweet_index']=prime['tweet_index'][j]
	ent_dfs[j]['tweet_id']=prime.index[j]
	j+=1
entities=pd.concat(ent_dfs,axis=0,ignore_index=True)
'''


url_dfs=[]
j=0
for i in range(len(prime)):
	#if pd.isnull(prime.entities[i]) or len(prime.entities[i]['urls'])==0:
	if np.array(pd.isnull(prime['entities.urls'][i])).all() or len(prime['entities.urls'][i])==0:
		print i, ' no url'
	else:
		#url_dfs.append(pd.io.json.DataFrame(prime.entities[i]['urls']))
		url_dfs.append(pd.io.json.json_normalize(prime['entities.urls'][i]))
		url_dfs[j]['input_user_name']=prime['input_user_name'][i]
		url_dfs[j]['tweet_id']=prime.index[i]
		j+=1
if url_dfs<>[]:
	urls=pd.concat(url_dfs,axis=0,ignore_index=True)
	urls['id']=range(0,len(urls))
	urls=urls.drop('indices',1)
	urls.to_sql('twitter_urls',engine,if_exists=write_type,chunksize=100)


hash_dfs=[]
j=0
for i in range(len(prime)):
	#if pd.isnull(prime.entities[i]) or len(prime.entities[i]['hashtags'])==0:
	if np.array(pd.isnull(prime['entities.hashtags'][i])).all() or len(prime['entities.hashtags'][i])==0:
		print i, ' no hash'
	else:
		#hash_dfs.append(pd.io.json.DataFrame(prime.entities[i]['hashtags']))
		hash_dfs.append(pd.io.json.json_normalize(prime['entities.hashtags'][i]))
		hash_dfs[j]['input_user_name']=prime['input_user_name'][i]
		hash_dfs[j]['tweet_id']=prime.index[i]
		j+=1
if hash_dfs<>[]:
	hashes=pd.concat(hash_dfs,axis=0,ignore_index=True)
	hashes['id']=range(0,len(hashes))
	hashes=hashes.drop('indices',1)
	hashes.to_sql('twitter_hashes',engine,if_exists=write_type,chunksize=100)

loc_dfs=[]
j=0
for i in range(len(prime)):
	#if pd.isnull(prime.place[i]) or len(prime['place'][i])==0:
	if np.array(pd.isnull(prime['place'][i])).all() or len(prime['place'][i])==0:
		print i, ' no place'
	else:
		#loc_dfs.append(pd.io.json.json_normalize(prime.place[i]))
		loc_dfs.append(pd.io.json.json_normalize(prime['place'][i]))
		loc_dfs[j]['input_user_name']=prime['input_user_name'][i]
		loc_dfs[j]['tweet_id']=prime.index[i]
		j+=1
if loc_dfs<>[]:
	locations=pd.concat(loc_dfs,axis=0,ignore_index=True)
	locations['id']=range(0,len(locations))
	locations.to_sql('twitter_locations',engine,if_exists=write_type,chunksize=100)
#locations=locations.drop('indices',1)

#prime[main_column_list].to_sql('prime',engine,flavor='mysql',if_exists='replace',chunksize=100)
prime[main_column_list].to_sql('twitter_prime',con=engine,if_exists=write_type,chunksize=100)





