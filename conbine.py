#coding: utf-8

from pymongo import MongoClient
import mysql.connector
import json
import sys

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
            print "insert dynamic for pid = %s" % product_id
            cursor.execute("insert into jdtest (product_id, current_price, comments_count, score) values('%s', '%s', '%s', '%s')" \
                           % (product_id, current_price, comments_count, score))

def add_static():
    stt = list(db.statistic.find())
    for item in stt:
        product_id = str(item['product_id'])
        name = safe_convert(item['goods_name'])
        params = json.dumps(item['specifications'], ensure_ascii = False, encoding = 'utf-8')
        brand = safe_convert(item['brand_name'])
        categories = safe_convert(item['classify'])
        description = safe_convert(item['introduce_info'])
        shop_name = safe_convert(item['shop_name'])
        url = str(item['website'])
        keyowrd = safe_convert(item['keyword'])
        images = str(item['image_urls'])

        # check if product_id exists
        cursor.execute("select count(*) from jdtest where product_id = '%s'" % product_id)
        res = cursor.fetchall()
        if (res[0][0] == 0):    # product_id does not exist
            print "insert static for pid = %s" % product_id
            cursor.execute("insert into jdtest (product_id, name, params, brand, categories, product_sku, product_sku_detail, shop_name, url, keyowrd, images, detail, description) \
values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                           % (product_id, name, params, brand, categories, product_id, description, shop_name, url, keyowrd, images, description, description))
        else:   # product_id exists
            print "update static for pid = %s" % product_id
            cursor.execute("update jdtest set name = '%s', params = '%s', brand = '%s', categories = '%s', product_sku = '%s', product_sku_detail = '%s', shop_name = '%s', url = '%s', keyowrd = '%s', images = '%s', detail = '%s', description = '%s' where product_id = '%s'" \
                           % (name, params, brand, categories, product_id, description, shop_name, url, keyowrd, images, description, description, product_id))
        

def safe_convert(raw):
    if (type(raw) == type(u'')):
        return raw.replace("'", "''")
    elif (type(raw) == type(None)):
        return u''
    else:
        sys.exit(1)

def main():
    add_dynamic()
    add_static()
    
    cnx.commit()

    cursor.close()
    cnx.close()

    print "Process finished successfully."

main()
