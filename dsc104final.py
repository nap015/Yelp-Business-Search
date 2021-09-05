import pandas as pd
import numpy as np
import csv

df = pd.read_csv('yelp_business.csv')#.astype({'stars': 'string'})

print('Dataset loaded.')

print('Starting Cassandra...')

from cassandra.cluster import Cluster
cluster = Cluster(['cassandra'])
session = cluster.connect()

session.execute(
    "CREATE KEYSPACE IF NOT EXISTS ks "
    "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"
)

session.set_keyspace('ks')

session.execute("""
CREATE TABLE business(
   id text,
   name text,
   address text,
   city text,
   state text,
   stars float,
   review_count int,
   is_open int,
   categories text,
   hours text,
   PRIMARY KEY (id, name)
);
""")

print('Loading dataset to Cassandra...')

query = "INSERT INTO business (id, name, address, city, state, stars, review_count, is_open, categories, hours)  VALUES (?,?,?,?,?,?,?,?,?,?)"
prepared = session.prepare(query)

try:
    for index, item in df.iterrows():
        session.execute(prepared, (item[0], item[1], str(item[2]), item[3], item[4], item[8], item[9], item[10], item[11], item[12]))
except OperationTimedOut:
    pass


print('Cassandra table loaded.')
#print('Enter City to search for:') #prompt

state = input('Enter State to search for:')

print('Searching for restaurants in '+state+'...')

query = """
SELECT id, name, address, city, state, stars, review_count, is_open, categories, hours
FROM business
WHERE state = %s and is_open = 1
ALLOW FILTERING;
"""

#rows = session.execute(query, ('Tempe', )) # input example Tempe for city
rows = session.execute(query, (state, ))

print('Search results found. Exporting...')

header = ['id','name', 'address', 'city', 'state', 'stars', 'review_count', 'categories', 'hours']

with open('test.csv', 'w') as fp:
    writer = csv.writer(fp, delimiter=',')
    writer.writerow(header)
    for row in rows:
        writer.writerow([row.id, row.name, row.address, row.city, row.state, row.stars, row.review_count, row.categories, row.hours])

print('Results exported to test.csv, starting subquery...')
        
session.execute("""
DROP TABLE business;
""")

cdf = pd.read_csv('test.csv')

import redis
r = redis.Redis(host='my-redis', port=6379, db=0,decode_responses=True)

pipe = r.pipeline()
for i in range(len(cdf)):
    id_ = i
    pipe.hmset(id_, {'business_id': cdf['business_id'][i],
                     'name': cdf['name'][i],
                     'address': cdf['address'][i],
                     'city': cdf['city'][i], 
                     'state': cdf['state'][i],
                     'stars': float(cdf['stars'][i]),
                     'review_count': int(cdf['review_count'][i]),
                     'categories': cdf['categories'][i],
                     'hours': cdf['hours'][i]})
    pipe.zadd(cdf['city'][i], {id_:i})
pipe.execute()

city = input('Enter City to search for:')

with open('test.json', 'w') as f:
    for x in r.zrangebyscore(city, 0, len(cdf)):
        pipe.hgetall(x)
    json.dump(pipe.execute(),f)

