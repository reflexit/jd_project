from pymongo import MongoClient
import mysql.connector

client = MongoClient()
db = client.jd
cnx = mysql.connector.connect(user='root', password='12345678', host='127.0.0.1', database='jd')
cursor = cnx.cursor()

def add_dynamic():
    dnm = list(db.dynamic.find())
    for item in dnm:
        comments_count = str(item['comment_count'])
        score = str(item['good_rate'])
        current_price = str(item['price']['present_price'])
        product_id = str(item['product_id'])
        
        # ensure there are no duplicates
        cursor.execute("select count(*) from jdtest where product_id = '%s' and current_price = '%s' and comments_count = '%s' and score = '%s'" \
                       % (product_id, current_price, comments_count, score))
        res = cursor.fetchall()
        if (res[0][0] == 0):    # record does not exist
##            print "insert into jdtest (product_id, current_price, comments_count, score) values('%s', '%s', '%s', '%s')" \
##                           % (product_id, current_price, comments_count, score)
            cursor.execute("insert into jdtest (product_id, current_price, comments_count, score) values('%s', '%s', '%s', '%s')" \
                           % (product_id, current_price, comments_count, score))

def add_static():
    pass    # TODO

def main():
    add_dynamic()
    add_static()
    
    cnx.commit()

    cursor.close()
    cnx.close()

main()
