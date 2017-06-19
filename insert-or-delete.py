#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import StringIO
import pycurl
import urllib,urllib2,cookielib
import requests
import time
import random
import traceback
from multiprocessing.dummy import Pool as ThreadPool
from fileinput import filename
import simplejson as json

reload(sys)
sys.setdefaultencoding('utf-8')

global user_count
global list_count
global url_demond
global cookie_login


seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def writeTxt(fileName, inputText):
        
    fileName = fileName
    file_obj = open(fileName, 'a+')
    file_obj.write(inputText)
    file_obj.write('\n')
    file_obj.close()

def insert_data(args):

    index_tmp = args[0]
    cookie_login_tmp = args[1]
    
    url_tmp = 'http://%s.ipaloma.com/execute' %(url_demond)
    filename_guid_insert = ''
    number_guid = list_count
    guid_total_tmp = []
    for j in range(number_guid):
        guid_tmp = ''
        data_tmp = []
        for i in range(0, 32):
            data_tmp.append(random.choice(seed))
        guid_tmp = ''.join(data_tmp)
        filename_guid_insert = 'guid.txt'
        writeTxt(filename_guid_insert, guid_tmp)
        guid_total_tmp.append(guid_tmp)

    filename1_guid_insert = ''
    guid1_total_tmp = []    
    for j in range(number_guid):
        guid_tmp = ''
        data_tmp = []
        for i in range(0, 32):
            data_tmp.append(random.choice(seed))
        guid_tmp = ''.join(data_tmp)
        filename1_guid_insert = 'guid1.txt' 
        writeTxt(filename1_guid_insert, guid_tmp)
        guid1_total_tmp.append(guid_tmp)

    filename2_guid_insert = ''   
    guid2_total_tmp = []    
    for j in range(number_guid):
        guid_tmp = ''
        data_tmp = []
        for i in range(0, 32):
            data_tmp.append(random.choice(seed))
        guid_tmp = ''.join(data_tmp)
        filename2_guid_insert = 'guid2.txt' 
        writeTxt(filename2_guid_insert, guid_tmp)
        guid2_total_tmp.append(guid_tmp)

    filename3_guid_insert = ''
    guid3_total_tmp = []       
    for j in range(number_guid):
        guid_tmp = ''
        data_tmp = []
        for i in range(0, 32):
            data_tmp.append(random.choice(seed))
        guid_tmp = ''.join(data_tmp)
        filename3_guid_insert = 'guid3.txt'
        writeTxt(filename3_guid_insert, guid_tmp)
        guid3_total_tmp.append(guid_tmp)

    url_requests = url_tmp
    
    num_insert = 0
    for i in range(len(guid_total_tmp)):
        data_insert = {}
        insert_data = '[{"view_name":"vapp_auto_tblbilliteminventory","insert":{"expirecount":0,"archived":0,"toexpirecount":0,"itemcount":12.0,"itemunitcost":12.0,"stop":0,"billid":"%s","damagecount":0,"scrapped":0,"qualitycount":12.0,"delivercount":0,"guid":"%s","flowrootid":"%s","itemobjid":"5c05a3c86bed4a3faff7c477850868ad"}},{"view_name":"vapp_auto_tblbillpofromcustomer","update":{"billexpecteddelivertime":"2016-12-02","guid":["%s"],"executorid":"6999abfedcbafedcbafedcbafedcbaba","shippingfrom":"6999abfedcbafedcbafedcbafedcbaba","podeliverto":"cf5090cc60b04c2a9e0d356006190fe4"}},{"view_name":"vapp_auto_tblbillgift","update":{"billexpecteddelivertime":"2016-12-02","guid":["%s"],"shippingto":"6999abfedcbafedcbafedcbafedcbaba","shippingfrom":"6999abfedcbafedcbafedcbafedcbaba"}},{"view_name":"vapp_auto_tblbillreturncustomer","update":{"billexpecteddelivertime":"2016-12-02","guid":["%s"],"shippingto":"6999abfedcbafedcbafedcbafedcbaba"}}]' %(guid_total_tmp[i], guid1_total_tmp[i], guid_total_tmp[i], guid_total_tmp[i], guid2_total_tmp[i], guid3_total_tmp[i])        
        coltype_data = '{"vapp_auto_tbltagmisc":{"b64":{},"coltype":{"category":"text","archived":"bool","description":"text","stop":"bool","realvalue":"real","name":"text","intvalue":"integer","strvalue":"text","issueto":"id","blobvalue":"blob","scrapped":"bool","sticktogroup":"text","sticktoclass":"text","sticktoobject":"id","guid":"text"},"pk":"guid"},"vapp_auto_tblobject":{"b64":{},"pk":"guid","coltype":{"guid":"text","scrapped":"integer"}},"vapp_auto_tblbillpofromcustomer":{"b64":{},"coltype":{"generatebypc":"integer","serialnumber":"text","executorid":"id","parentid_class":"text","archived":"bool","contactphone":"text","contactname":"text","shippingto":"id","salecategory":"text","description":"text","stop":"bool","closetime":"datetime","flowrootid":"id","openflag":"bool","parentid":"id","scrapped":"bool","printcount":"integer","pocategory":"id","shippingfrom":"id","podeliverto":"id","issueto":"id","billcamp":"text","billexpecteddelivertime":"date","guid":"text"},"pk":"guid"},"vapp_auto_tblbillgift":{"b64":{},"coltype":{"printcount":"integer","generatebypc":"integer","archived":"bool","description":"text","billcamp":"text","serialnumber":"text","stop":"bool","closetime":"datetime","flowrootid":"id","openflag":"bool","parentid":"id","shippingto":"id","scrapped":"bool","parentid_class":"text","shippingfrom":"id","billexpecteddelivertime":"date","issueto":"id","guid":"text"},"pk":"guid"},"vapp_auto_tblbillreturncustomer":{"b64":{},"coltype":{"printcount":"integer","generatebypc":"integer","archived":"bool","description":"text","warehouseid":"id","billcamp":"text","serialnumber":"text","stop":"bool","closetime":"datetime","flowrootid":"id","openflag":"bool","parentid":"id","shippingto":"id","scrapped":"bool","parentid_class":"text","shippingfrom":"id","billexpecteddelivertime":"date","issueto":"id","guid":"text"},"pk":"guid"},"vapp_auto_tblbilliteminventory":{"b64":{},"coltype":{"ruleid_class":"text","itemoriginalcost":"currency","expirecount":"real","productiondate":"date","itemtoid_class":"text","itemtoid":"id","archived":"bool","itemtype":"text","ruleid":"id","toexpirecount":"real","itemcount":"real","itemunitcost":"currency","itemgifttype":"integer","promotionno":"text","description":"text","stop":"bool","billid":"id","damagecount":"real","flowrootid":"id","scrapped":"bool","batchinformation":"text","qualitycount":"real","itemexpecteddeliverdate":"date","delivercount":"real","issueto":"id","batchcode":"text","itemobjid":"object","guid":"text"},"pk":"guid"}}'
        
        data_insert['cookie'] = cookie_login_tmp
        data_insert['transaction_level'] = 'notransaction'
        data_insert['data'] = str(insert_data)
        data_insert['coltype'] = str(coltype_data)
        #print data_insert
        body_value = urllib.urlencode(data_insert)
        pc = pycurl.Curl()
        b = StringIO.StringIO()
        pc.setopt(pycurl.POST, 1)
        pc.setopt(pycurl.URL, url_requests)
        pc.setopt(pycurl.POSTFIELDS, body_value)
        try:
            pc.perform()
        except pycurl.error:
            pc.close()
            b.close()
            raise AbortedException, pycurl.error
        
        response_context = b.getvalue()
        response_Total_Time = pc.getinfo(pycurl.TOTAL_TIME)
        print  str(response_context)    
        print  '\ntotal_time:%s' %(response_Total_Time)
        print  '%s num_of_insert:%d' %(index_tmp, num_insert)
        print  'insert %s' %(guid_total_tmp[i])  + '\r\n'
        num_insert = num_insert + 1
        pc.close()
        b.close()
 
def delete_data(args):
    guid, guid1, guid2, guid3, cookie_login = args[0], args[1], args[2], args[3], args[4]

    url_requests = 'http://%s.ipaloma.com/execute' %(url_demond)

    
        
    data_delete = {}
    data_delete['cookie'] = cookie_login
    data_delete['transaction_level'] = 'notransaction'
    data_delete['data'] = '[{"view_name":"vapp_auto_tblbillpofromcustomer","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbillgift","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbillreturncustomer","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbillgift","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbilliteminventory","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbillgift","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbillreturncustomer","delete":{"guid":["%s"]}},{"view_name":"vapp_auto_tblbilliteminventory","delete":{"guid":["%s"]}}]' %(guid.strip(), guid.strip(), guid1.strip(), guid1.strip(), guid1.strip(), guid1.strip(),  guid2.strip(), guid3.strip())
    data_delete['coltype'] = '{"vapp_auto_tbltagmisc":{"b64":{},"coltype":{"category":"text","archived":"bool","description":"text","stop":"bool","realvalue":"real","name":"text","intvalue":"integer","strvalue":"text","issueto":"id","blobvalue":"blob","scrapped":"bool","sticktogroup":"text","sticktoclass":"text","sticktoobject":"id","guid":"text"},"pk":"guid"},"vapp_auto_tblobject":{"b64":{},"pk":"guid","coltype":{"guid":"text","scrapped":"integer"}},"vapp_auto_tblbillpofromcustomer":{"b64":{},"coltype":{"generatebypc":"integer","serialnumber":"text","executorid":"id","parentid_class":"text","archived":"bool","contactphone":"text","contactname":"text","shippingto":"id","salecategory":"text","description":"text","stop":"bool","closetime":"datetime","flowrootid":"id","openflag":"bool","parentid":"id","scrapped":"bool","printcount":"integer","pocategory":"id","shippingfrom":"id","podeliverto":"id","issueto":"id","billcamp":"text","billexpecteddelivertime":"date","guid":"text"},"pk":"guid"},"vapp_auto_tblbillgift":{"b64":{},"coltype":{"printcount":"integer","generatebypc":"integer","archived":"bool","description":"text","billcamp":"text","serialnumber":"text","stop":"bool","closetime":"datetime","flowrootid":"id","openflag":"bool","parentid":"id","shippingto":"id","scrapped":"bool","parentid_class":"text","shippingfrom":"id","billexpecteddelivertime":"date","issueto":"id","guid":"text"},"pk":"guid"},"vapp_auto_tblbillreturncustomer":{"b64":{},"coltype":{"printcount":"integer","generatebypc":"integer","archived":"bool","description":"text","warehouseid":"id","billcamp":"text","serialnumber":"text","stop":"bool","closetime":"datetime","flowrootid":"id","openflag":"bool","parentid":"id","shippingto":"id","scrapped":"bool","parentid_class":"text","shippingfrom":"id","billexpecteddelivertime":"date","issueto":"id","guid":"text"},"pk":"guid"},"vapp_auto_tblbilliteminventory":{"b64":{},"coltype":{"ruleid_class":"text","itemoriginalcost":"currency","expirecount":"real","productiondate":"date","itemtoid_class":"text","itemtoid":"id","archived":"bool","itemtype":"text","ruleid":"id","toexpirecount":"real","itemcount":"real","itemunitcost":"currency","itemgifttype":"integer","promotionno":"text","description":"text","stop":"bool","billid":"id","damagecount":"real","flowrootid":"id","scrapped":"bool","batchinformation":"text","qualitycount":"real","itemexpecteddeliverdate":"date","delivercount":"real","issueto":"id","batchcode":"text","itemobjid":"object","guid":"text"},"pk":"guid"}}'
    #print data_delete
        
    body_value = urllib.urlencode(data_delete)
    pc = pycurl.Curl()
    b = StringIO.StringIO()
    pc.setopt(pycurl.POST, 1)
    pc.setopt(pycurl.URL, url_requests)
    pc.setopt(pycurl.POSTFIELDS, body_value)
    try:
        pc.perform()
    except pycurl.error:
        pc.close()
        b.close()
        raise AbortedException, pycurl.error
        
    response_context = b.getvalue()
    response_Total_Time = pc.getinfo(pycurl.TOTAL_TIME)
    print   str(response_context)  
    print   '\ntotal_time:%s' %(response_Total_Time)
    print  'delete %s' %(guid.strip())  + '\r\n'
    pc.close()
    b.close()



def main(user_count, username, password, mode):

    url_tmp = 'http://%s.ipaloma.com/login' %(url_demond)
    data_login = {}
    data_login['username'] = username
    data_login['password'] = password
    
    url_requests = url_tmp
    body_value = urllib.urlencode(data_login)
    pc = pycurl.Curl()
    b = StringIO.StringIO()
    pc.setopt(pycurl.POST, 1)
    pc.setopt(pycurl.URL, url_requests)
    pc.setopt(pycurl.POSTFIELDS, body_value)
    pc.setopt(pycurl.WRITEFUNCTION, b.write)
    try:
        pc.perform()
    except pycurl.error:
        pc.close()
        b.close()
        raise AbortedException, pycurl.error
        
    response_context = b.getvalue()
    pc.close()
    b.close()
    
    context_tmp = json.loads(str(response_context))
    cookie_login = str(context_tmp['cookie'])
    
    if mode == 'insert':
        pool = ThreadPool(processes=user_count)
        try:
            for pool_index in range(user_count):
                args = [(str(pool_index), cookie_login)]
                pool.map(insert_data, args)
        except Exception as e:
            traceback.print_exc()
        finally:
            pool.close()
            #pool.join()
        
        #time.sleep(100)    
        print u'插入数据结束'
    else:

        file_del = open('guid.txt', 'r+')
        delete_guid = file_del.readlines()
        file_del.flush()
        file_del.close()

        file1_del = open('guid1.txt', 'r+')
        delete_guid1 = file1_del.readlines()
        file1_del.flush()
        file1_del.close()

        file2_del = open('guid2.txt', 'r+')
        delete_guid2 = file2_del.readlines()
        file2_del.flush()
        file2_del.close()

        file3_del = open('guid3.txt', 'r+')
        delete_guid3 = file3_del.readlines()
        file3_del.flush()
        file3_del.close()

        pool = ThreadPool(processes=8)
        try:
            for i in range(len(delete_guid)):
                args = [(delete_guid[i], delete_guid1[i], delete_guid2[i], delete_guid3[i], cookie_login)]
                pool.map(delete_data, args)
        except Exception as e:
            traceback.print_exc()
        finally:
            pool.close()
            #pool.join()
        os.remove('guid.txt')
        os.remove('guid1.txt')
        os.remove('guid2.txt')
        os.remove('guid3.txt')
        print u'删除数据结束'


if __name__ == "__main__":
    
    if len(sys.argv) != 7:
        print '='*88
        print '== Example:%s need 6 extra param ==' %(sys.argv[0])
        print '== The first: the name of demond, such as "navigon" =='
        print '== The secend: the number of thread =='
        print '== The third: the number of data which was inserted above in each thread =='
        print '== The fourth: the username of the demond which was input above, such as "fjcs" =='
        print '== The fifth: the password of the demond which was input above, such as "123456" =='
        print '== The sixth: "insert" or "delete" =='
        print '='*88
        exit(0)
    
    url_demond = str(sys.argv[1])
    user_count = int(sys.argv[2])
    list_count = int(sys.argv[3])
    username = str(sys.argv[4])
    password = str(sys.argv[5])
    mode = str(sys.argv[6])

    main(user_count, username, password, mode)

