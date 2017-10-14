# coding: utf-8

import json
import re

from pymongo import MongoClient
import mysql.connector


'''
Please read this first!

This module will modify the following two tables:
1. The main table:
        Fields: as agreed
2. The comments table:
        Fields: id (int(11), AUTO_INCREMENT, PRIMARY KEY)
                product_id (longtext)
                cmts (json)

You need to provide the name of these two tables.
If the corresponding tables do not exist, the module will create them automatically.

I supply an example() function at the bottom to demonstrate how to use this module.
'''


# ***** MAIN FUNCTION *****
# Function name: combine_tables
# Purpose: inserts and updates info from 3 mongodb collections to mysql table
# Parameters:
#      db: mongodb database
#      cnx: an established mysql connection
#      mysql_table_name: name of mysql table, default "jd_info"
#      comments_table_name: name of mysql comments table, default "jd_comments"

def combine_tables(db, cnx, mysql_table_name="jd_info", comments_table_name="jd_comments"):
    cursor = cnx.cursor()   # get mysql cursor
    
    # check if target tables are present
    flag_main = False
    flag_comments = False
    cursor.execute("show tables")
    tables = cursor.fetchall()
    for table in tables:
        if table[0] == mysql_table_name:
            flag_main = True
        if table[0] == comments_table_name:
            flag_comments = True
    if not flag_main:
        cursor.execute("CREATE TABLE %s (\
id int(11) NOT NULL AUTO_INCREMENT, product_id longtext, name longtext, params longtext, current_price longtext, \
is_self_run longtext, brand longtext, categories longtext, product_sku longtext, product_sku_detail longtext, \
shop_id longtext, shop_name longtext, url longtext, keyowrd longtext, rank longtext, \
images longtext, detail longtext, description longtext, score longtext, comments_count longtext, \
comments longtext, \
date_static longtext, selections longtext, \
date_dynamic longtext, good_count longtext, general_count longtext, poor_count longtext, after_count longtext, \
general_rate longtext, poor_rate longtext, hot_comment_tag_statistics longtext, \
preferential longtext, baitiao longtext, \
__pk varchar(1024) DEFAULT NULL, __version int(11) DEFAULT NULL, \
PRIMARY KEY (id) \
) ENGINE=InnoDB AUTO_INCREMENT=4381 DEFAULT CHARSET=utf8mb4" % mysql_table_name)
    if not flag_comments:
        cursor.execute("CREATE TABLE %s (\
id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, product_id longtext, cmts json \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" % comments_table_name)
    cursor.close()
    cnx.commit()

    # insert and update info
    add_dynamic(db, cnx, mysql_table_name)
    add_static(db, cnx, mysql_table_name)
    add_comments(db, cnx, mysql_table_name, comments_table_name)
    
    print "Process finished successfully."


# Function name: add_dynamic
# Purpose: inserts and updates dynamic info in mysql table
# Parameters:
#      db: mongodb database
#      cnx: an established mysql connection
#      mysql_table_name: name of mysql table

def add_dynamic(db, cnx, mysql_table_name):
    cursor = cnx.cursor()   # get mysql cursor
    
    # find the latest dynamic entry time in mysql
    cursor.execute("select max(date_dynamic) from %s" % mysql_table_name)
    res = cursor.fetchall()
    if res[0][0] is None:
        max_date = u'1970-01-01'
    else:
        max_date = res[0][0]
    
    dnm = db.dynamic.find({"date": {"$gt": max_date}}, no_cursor_timeout=True)
    for item in dnm:
        product_id = safe_convert(item['product_id'])
        print "Insert dynamic for pid = %s" % product_id
        cursor.execute("insert into %s(date_dynamic, good_count, general_count, poor_count, after_count, \
comments_count, score, general_rate, poor_rate, hot_comment_tag_statistics, \
preferential, baitiao, current_price, product_id) \
values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                       % (mysql_table_name, safe_convert(item['date']), safe_convert(item['good_count']),
                          safe_convert(item['general_count']), safe_convert(item['poor_count']),
                          safe_convert(item['after_count']), safe_convert(item['comment_count']),
                          safe_convert(item['good_rate']), safe_convert(item['general_rate']),
                          safe_convert(item['poor_rate']), safe_convert(item['hot_comment_tag_statistics']),
                          safe_convert(item['preferential']), safe_convert(item['baitiao']),
                          safe_convert(item['price']), product_id))
    
    cursor.close()
    cnx.commit()


# Function name: add_static
# Purpose: inserts and updates static info in mysql table
# Parameters:
#      db: mongodb database
#      cnx: an established mysql connection
#      mysql_table_name: name of mysql table

def add_static(db, cnx, mysql_table_name):
    cursor = cnx.cursor()   # get mysql cursor
    
    stt = db.statistic.find(no_cursor_timeout=True)
    for item in stt:
        product_id = safe_convert(item['product_id'])
        
        # check if product_id exists
        cursor.execute("select count(*) from %s where product_id = '%s'"
                       % (mysql_table_name, product_id))
        res = cursor.fetchall()
        if res[0][0] > 0:    # product_id exists
            print "Update static for pid = %s" % product_id
            cursor.execute("update %s \
set date_static = '%s', categories = '%s', url = '%s', keyowrd = '%s', brand = '%s', \
name = '%s', selections = '%s', description = '%s', params = '%s', images = '%s', \
shop_name = '%s', product_sku = '%s', product_sku_detail = '%s', detail = '%s' \
where product_id = '%s'"
                           % (mysql_table_name,
                              safe_convert(item['date']), safe_convert(item['classify']), safe_convert(item['website']),
                              safe_convert(item['keyword']), safe_convert(item['brand_name']),
                              safe_convert(item['goods_name']), safe_convert(item['selections']),
                              safe_convert(item['introduce_info']), safe_convert(item['specifications']),
                              safe_convert(item['image_urls']), safe_convert(item['shop_name']), product_id,
                              safe_convert(item['specifications']), safe_convert(item['specifications']), product_id))
        else:   # product_id does not exist
            print "Insert static for pid = %s" % product_id
            cursor.execute("insert into %s(date_static, categories, url, keyowrd, brand, name, selections, \
description, params, images, product_id, shop_name, product_sku, product_sku_detail, detail) \
values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                           % (mysql_table_name,
                              safe_convert(item['date']), safe_convert(item['classify']), safe_convert(item['website']),
                              safe_convert(item['keyword']), safe_convert(item['brand_name']),
                              safe_convert(item['goods_name']), safe_convert(item['selections']),
                              safe_convert(item['introduce_info']), safe_convert(item['specifications']),
                              safe_convert(item['image_urls']), product_id, safe_convert(item['shop_name']),
                              product_id, safe_convert(item['specifications']), safe_convert(item['specifications'])))
        cnx.commit()

    cursor.close()
    cnx.commit()


# Function name: add_comments
# Purpose: inserts comments, and updates links to comments table
# Parameters:
#      db: mongodb database
#      cnx: an established mysql connection
#      mysql_table_name: name of mysql table
#      comments_table_name: name of mysql comments table

def add_comments(db, cnx, mysql_table_name, comments_table_name):
    cursor = cnx.cursor()   # get mysql cursor

    # FOR DEBUG ONLY!!! Otherwise, always set to 0
    limit = 0
    
    # find the latest comment time in mysql
    pat = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    max_date = u'1970-01-01 00:00:00'
    cursor.execute("select json_extract(cmts, \"$[*].creation_time\") from %s" % comments_table_name)
    row = cursor.fetchone()
    while row is not None:
        dates = re.findall(pat, row[0])
        temp = max(dates)
        if temp > max_date:
            max_date = temp
        row = cursor.fetchone()
    print "Latest time:", max_date
    
    cmt = db.comments.find({"creation_time": {"$gt": max_date}}, no_cursor_timeout=True)
    print "New documents:", cmt.count()
    cnt = 0
    for item in cmt:
        product_id = safe_convert(item['product_id'])
        comment_id = (str(item['comment_id'])).decode("utf-8")
        this_cmt = convert_to_json(item)

        # check if comments for this product already exists
        cursor.execute("select id from %s where product_id = '%s'" % (comments_table_name, product_id))
        res = cursor.fetchall()
        if len(res) == 0:   # first comment for this product
            cursor.execute("insert into %s(product_id, cmts) values('%s','[]')"
                           % (comments_table_name, product_id))
        try:
            cursor.execute("update %s set cmts = json_array_append(cmts, '$', cast('%s' as json)) "
                           "where product_id = '%s'"
                           % (comments_table_name, this_cmt, product_id))
        except Exception:
            print "Discard comment:", this_cmt

        cnt += 1
        if cnt % 1000 == 0:
            print cnt, "completed"
            cnx.commit()
            if 0 < limit <= cnt:    # FOR DEBUG ONLY!!!
                break

    # update links
    cursor.execute("select id, product_id from %s" % comments_table_name)
    rows = cursor.fetchall()
    for row in rows:
        cursor.execute("update %s set comments = '%s' where product_id = '%s'" % (mysql_table_name, row[0], row[1]))

    cursor.close()
    cnx.commit()


# Function name: safe_convert
# Purpose: replaces (') in a unicode or string with a (''), and converts a None object to a unicode
# Parameters:
#      raw: the object to convert
# Return value: converted unicode or string

def safe_convert(raw):
    # if raw is not a unicode or string, convert it first
    if isinstance(raw, (int, float)):
        return str(raw)
    elif isinstance(raw, (list, dict)):
        raw = json.dumps(raw, ensure_ascii=False, encoding='utf-8')

    if isinstance(raw, (unicode, str)):
        return raw.replace("'", "''")
    elif raw is None:
        return u''
    else:   # should never reach here
        print "ERROR converting", type(raw)
        assert False


# Function name: convert_to_json
# Purpose: converts to json format, and eliminates escape characters
# Parameters:
#      raw: dict object to convert
# Return value: converted json format

def convert_to_json(raw):
    raw.pop(u'_id', None)
    ret = json.dumps(raw, ensure_ascii=False, encoding="utf-8")
    ret = ret.replace(u"\\", u"")
    return ret


# This is the example of using this module

def example():
    client = MongoClient()
    db = client.jd
    cnx = mysql.connector.connect(user='root', password='12345678', host='127.0.0.1', database='jd')

    combine_tables(db, cnx, "jd_info", "jd_comments")    # call the main function
    
    cnx.commit()
    cnx.close()
