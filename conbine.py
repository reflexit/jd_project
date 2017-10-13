from pymongo import MongoClient

client = MongoClient()
db = client.jd
colle = db.statistic

for item in colle.find():
    print item[u'goods_name']
