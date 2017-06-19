#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import sys
import simplejson as json
import time
import datetime
from functools import partial
import traceback
from multiprocessing.dummy import Pool as ThreadPool
from ipabase import *
import requests
import paramiko
import os

ISOTIMEFORMAT='%Y-%m-%d %H:%M:%S'
reload(sys)
sys.setdefaultencoding('utf-8')

class CompareJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (bytes, bytearray)):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, datetime.time):
            return obj.strftime('%H:%M:%S')
        if isinstance(obj, Decimal):
            return round(float(obj), 5)
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, (str, unicode)):
            return re.sub(r"[\W'\\/\\r\\n]", '', obj)
        return json.JSONEncoder.default(self, obj)
#------------------------------数据比对函数-------------------------------------------------------------------
def is_valid_date(str):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str, "%Y-%m-%d")
        return True
    except:
        return False

def print_error_log(one, two, instanceclass, oid, one_obj, two_obj, key):
    final_result = 0
    if one['issuetime'] and one['lastmodifytime'] and two['issuetime'] and two['lastmodifytime']:
        if (datetime.datetime.now() - one['lastmodifytime']).seconds < 300:
            pass
        else:
            log = u'数据不一致 %s oid:%s %s source:%s type:%s, target:%s type:%s\r\nsource_issuetime:%s source_lastmodifytime:%s  target_issuetime:%s target_lastmodifytime:%s  当前时间:%s\r\n' %(
                    instanceclass, str(oid), key, one_obj, type(one_obj).__name__, two_obj, type(two_obj).__name__, one['issuetime'], one['lastmodifytime'], two['issuetime'], two['lastmodifytime'], str(datetime.datetime.now())[:19])
            print log
            writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
            final_result = 2
    else:
        log = u'数据不一致 %s id:%d %s source:%s type:%s, target:%s type:%s  当前时间:%s\r\n' %(
            instanceclass, str(oid), key, one_obj, type(one_obj).__name__, two_obj, type(two_obj).__name__, str(datetime.datetime.now())[:19])
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        final_result = 2
    return final_result


#------------------------------DRDS-GP数据比对-------------------------------------------------------------------
def compare_dict(one, two, instanceclass):
    '''
    return 0 equal
           1 columns not equal
           2 values not equal
    '''
    one_keys, two_keys = set(one.keys()), set(two.keys())
    one_keys.discard('RowNum')
    two_keys.discard('RowNum')
    row_id = one['oid'] if 'oid' in one else one['tblobject_oid']
    if len(one_keys) > len(two_keys):
        log = u'数据不一致 %s id:%d %s 字段在ADS中不存在\r\n' % (instanceclass, row_id, one_keys-two_keys)
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        return 1
    final_result = 0
    compare_time = time.time()
    try:
        key_tmp = ''
        for key in one_keys:
            key_tmp = key
            if key == 'geoloc':
                continue
            one_obj, two_obj = one[key], two[key]

            if isinstance(one_obj, datetime.datetime) or isinstance(two_obj, datetime.datetime):
                fmt_one_obj, fmt_two_obj = one_obj, two_obj
                if isinstance(one_obj, datetime.datetime):
                    fmt_one_obj = one_obj.strftime('%Y-%m-%d %H:%M:%S')[:19]
                if isinstance(two_obj, datetime.datetime):
                    fmt_two_obj = two_obj.strftime('%Y-%m-%d %H:%M:%S')[:19]
                if (one_obj is None) and (two_obj is None):
                    continue 
                elif (one_obj is None) or (two_obj is None):
                    if one['issuetime'] and one['lastmodifytime'] and two['issuetime'] and two['lastmodifytime']:
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                else:
                    if fmt_one_obj[:19] != fmt_two_obj[:19]:
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue

            if isinstance(one_obj, (str, unicode)) or isinstance(two_obj, (str, unicode)):
                fmt_one_obj, fmt_two_obj = one_obj, two_obj
                if not isinstance(one_obj, (str, unicode)):
                    fmt_one_obj = str(one_obj)
                if not isinstance(two_obj, (str, unicode)):
                    fmt_two_obj = str(one_obj)

                if str(fmt_one_obj).find('{')!=-1 and str(fmt_one_obj).find('}')!=-1 and str(fmt_one_obj).find(':')!=-1:
                    if str(fmt_two_obj).find('{')!=-1 and str(fmt_two_obj).find('}')!=-1 and str(fmt_two_obj).find(':')!=-1:
                        src_json = json.loads(str(fmt_one_obj))
                        tar_json = json.loads(str(fmt_two_obj))
                        if str(src_json).encode('utf-8') != str(tar_json).encode('utf-8'):
                            final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                    else:
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                    continue

                if one_obj == '' or one_obj == 'null':
                    if two_obj and two_obj != 'null' and two_obj != '':
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                    else:
                        pass
                    continue
                
                if re.sub(r"['\\/\\r\\n\s\ E`]", '', fmt_one_obj) != re.sub(r"['\\/\\r\\n\s\ E`]", '', fmt_two_obj):
                    if ('issuetime' in one.keys()) and ('lastmodifytime' in one.keys()) and ('issuetime' in two.keys())  and ('lastmodifytime' in two.keys()):
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue

            if isinstance(one_obj, (Decimal, float)) or isinstance(two_obj, (Decimal, float)):
                fmt_one_obj, fmt_two_obj = one_obj, two_obj
                if fmt_one_obj<0.000001 and fmt_two_obj==None:
                    continue
                try:
                    fmt_one_obj = round(float(one_obj), 1)
                except Exception as e:
                    final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                try:
                    fmt_two_obj = round(float(two_obj), 1)
                except Exception as e:
                    final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue

                if abs(fmt_one_obj-fmt_two_obj) > 1.0/100000:
                    final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue

            if isinstance(one_obj, (Decimal, int)) or isinstance(two_obj, (Decimal, int)):
                if not one_obj or one_obj==0:
                    if two_obj:
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                else:
                    if one_obj != two_obj:
                        final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue

            if one_obj != two_obj:
                final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue  

            if str(one_obj) == '':
                if two_obj:
                    final_result = print_error_log(one, two, instanceclass, str(row_id), one_obj, two_obj, key)
                continue
    except Exception as e:
        log = u'比较出现异常 %s oid:%d %s %s\r\n' % (instanceclass, row_id, key_tmp, e)
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        traceback.print_exc()
        final_result = 2
    
    return final_result

def mssql_fetch_src(db_conn, mappingitem, domain, servicename, targetdbtype=DATASOURCE_ADS, count=300):
    fetchsql, fsql, offset = None, None, 0
    oid_tmp = 0
    while True:
        fetchsql = "select %s from %s where sandboxname = '%s' and oid > %d ORDER BY oid  LIMIT %d" %(
            mappingitem['greenplum_cols'].replace('"', '`') , mappingitem['targettable'], servicename, oid_tmp, count)
        src_sql_time = time.time()
        rows = db_conn.exec_select(fetchsql, as_dict_flag=True)
        #writeTxt('time.log', '%s src_sql_time:%f\n' %(mappingitem['targettable'], time.time() - src_sql_time))
        offset = len(rows)

        #print 'offset:%d' %offset
        if not offset:
            break
        oid_tmp = rows[offset - 1]['oid']
        if rows:    
            yield { row['oid']: row for row in rows }
        else:
            raise StopIteration()
        if offset < count:
            break

def mysql_fetch_dst(db_conn, mappingitem, domain, oids, servicename, targetdbtype=DATASOURCE_ADS):
    fetchsql = None
    fetchsql = "select %s from %s.%s where sandboxname='%s' and oid in (%s)" %(
        mappingitem['greenplum_cols'], domain, mappingitem['targettable'], servicename, ",".join([str(oid) for oid in oids]))
    try:
        tar_sql_time = time.time()
        rows = db_conn.exec_select(fetchsql, as_dict_flag=True)
        #writeTxt('time.log', '%s tar_sql_time:%f\n' %(mappingitem['targettable'], time.time() - tar_sql_time))
        return { row['oid']: row for row in rows }
    except Exception as e:
        log = '异常 SQL:%s Exception:%s' % (fetchsql, e)
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        raise e

def compare_resultset(mappingitem, src_rows, tar_rows, targetdbtype=DATASOURCE_ADS):
    final_result = True
    for oid in src_rows:
        if oid not in tar_rows:
            if ('issuetime' in src_rows[oid].keys()) and ('lastmodifytime' in src_rows[oid].keys()):
                if (datetime.datetime.now() - src_rows[oid]['lastmodifytime']).seconds < 300:
                    continue
                else:
                    log = u'%s 数据源oid:%s 在目的库中不存在  当前时间:%s\r\nsource_issuetime:%s source_lastmodifytime:%s\r\n' % (
                        mappingitem['targettable'], str(oid), time.strftime(ISOTIMEFORMAT, time.localtime()), src_rows[oid]['issuetime'], src_rows[oid]['lastmodifytime'])
                    print log
                    writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
                    return False
            else:
                log = u'%s 数据源oid:%d 在目的库中不存在  当前时间:%s\r\n' % (mappingitem['targettable'], oid, str(datetime.datetime.now())[:19])
                print log
                writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
                return False
            #print str(src_rows[oid])
        else:    
            if compare_dict(src_rows[oid], tar_rows[oid], mappingitem['targettable'])!=0:
                final_result = False
    return final_result

def compare_pool(args):
    src_dbconn, tar_dbconn, mappingitem, servicename, oid_tmp, domain = args[0], args[1], args[2], args[3], args[4], args[5]
    ret = 0
    src_rows = {}
    tar_rows = {}
    count = 300
    fetchsql = ''
    try:
        fetchsql = ("select %s from %s where sandboxname = '%s' and oid > %d ORDER BY oid  LIMIT %d") %(
            mappingitem['greenplum_cols'].replace('"', '`') ,mappingitem['targettable'], servicename, oid_tmp, count)
        rows = src_dbconn.exec_select(fetchsql)
        offset = len(rows)
        if rows:
            src_rows = { row['oid']: row for row in rows }
        else:
            return ret, 0
        tar_rows = mysql_fetch_dst(tar_dbconn, mappingitem, domain, src_rows.keys(), servicename)
        if tar_rows:
            final_result = compare_resultset(mappingitem, src_rows, tar_rows)
            if final_result:
                return ret, len(rows)
            else:
                ret = 1
                return ret, len(rows)
        else:
            return ret, 0
    except Exception as e:
        log = '异常 SQL:%s Exception:%s' % (fetchsql, e)
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        raise e

def mssql2ads_compare(params):
    start_time = time.time()
    src_dburi, tar_dburi, mappingitem, domain, servicename = params[0], params[1], params[2], params[3], params[4]
    targettable = mappingitem['targettable']
    src_dbconn = IpaDBMySQLDB(None, None, DATASOURCE_DRDS)
    src_dbconn.create_db_conn(src_dburi)
    tar_dbconn = IpaDBPostgreSQL(None, None)
    tar_dbconn.create_db_conn(tar_dburi)
    src_count_sql = "select count(*) from %s where sandboxname='%s'"  %(mappingitem['targettable'], servicename)
    src_count = src_dbconn.exec_select(src_count_sql)[0]
    try:
        src_count_sql = "select count(*) from %s where sandboxname='%s'" %(mappingitem['targettable'], servicename)
        src_count = src_dbconn.exec_select(src_count_sql)[0]
        #print str(src_count)
        if src_count['count(*)'] == 0:
            log = u"%s 表中没有数据\r\n" %(mappingitem['targettable'])
            print log
            writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
            return False
        tar_count_sql = "select count(*) from %s.%s where sandboxname='%s'" %(domain, mappingitem['targettable'], servicename)
        tar_count = tar_dbconn.exec_select(tar_count_sql)[0]
        #print str(tar_count)
        if src_count['count(*)']!=tar_count['count']:
            log = u"%s 表原表、目标表个数不同，原表个数：%d，目标表个数：%d\r\n" %(mappingitem['targettable'], src_count['count(*)'], tar_count['count'])
            print log
            writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
    except Exception as e:
        log = u"%s 查询表中数据个数出现异常：%s 当前时间:%s\r\n" %(targettable, e, str(datetime.datetime.now())[:19])
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        return False
    try:
        #TODO 实现反向对比 ADS 作为源，RDS DB作为目的
        final_result = 0
        fetchsql_tmp, fsql, offset_tmp, count, oid_tmp = None, None, 0, 300, 0
        fetchsql_tmp = ("select oid from %s where sandboxname = '%s'") %(mappingitem['targettable'], servicename)
        oid_row = src_dbconn.exec_select(fetchsql_tmp, as_dict_flag=True)
        if oid_row:
            oid_tmp = oid_row[0]['oid']
            #print '%s oid:%s' %(mappingitem['targettable'], str(oid_row))
            pool_table = ThreadPool(processes=4)
            try:
                while True:
                    args_pool = [(src_dbconn, tar_dbconn, mappingitem, servicename, oid_tmp, domain)]
                    ret = pool_table.map(compare_pool, args_pool)[0]
                    final_result += ret[0]
                    offset_tmp += ret[1]
                    if offset_tmp < src_count['count(*)']:
                        oid_tmp = oid_row[offset_tmp - 1]['oid']
                        if ret[1] < count:
                            break
                    else:
                        break
                if final_result:
                    log = u"FAIL 针对 %s 数据比较 失败 用时:%f秒  当前时间:%s\r\n" % (targettable, time.time()-start_time, str(datetime.datetime.now())[:19])
                    print log
                    writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
                else:
                    log = u"SUCCESS 针对 %s 数据比较 成功 用时:%f秒  当前时间:%s\r\n" % (targettable, time.time()-start_time, str(datetime.datetime.now())[:19])
                    print log
                    writeTxt(sys_param + '.log', log)
            except Exception as e:
                log = u"FAIL 针对 %s 数据比较 异常: %s index:%d len_list:%d 用时:%f秒  当前时间:%s\r\n" % (targettable, e, offset_tmp, ret[1], time.time()-start_time, str(datetime.datetime.now())[:19])
                print log
                writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
                traceback.print_exc()
                #pool_table.close()
            finally:
                pool_table.close()
        return final_result
    except Exception as e:
        log =  u"FAIL 针对 %s 数据比较 异常: %s 用时:%f秒  当前时间:%s\r\n" % (targettable, e, time.time()-start_time, str(datetime.datetime.now())[:19])
        print log
        writeTxt(sys_param + '.log', '<font color="#FF0000">' + str(log) + '</font>')
        traceback.print_exc()
        return False
    finally:
        src_dbconn.close_db_conn()
        tar_dbconn.close_db_conn()

def writeTxt(fileName, inputText):
    fileName = fileName
    file_obj = open(fileName, 'a+')
    file_obj.write(inputText)
    
    file_obj.close()

def send_mail(flag_sendmail, sys_param):
    file_name = '%s.log' %(sys_param)
    log_file = open(file_name, 'rb')
    if not log_file:
        print u'log文件%s不存在' %(file_name)
        exit(0)
    log_data_tmp = log_file.readlines()
    log_file.flush()
    log_file.close()

    if len(log_data_tmp) == 0:
        exit(0)

    index_tmp = 0
    content = ''
    flag_sendmail = True
    for log in log_data_tmp:
        if log.find('FAIL') != -1:
            flag_sendmail = False

    status_tmp = ''
    if flag_sendmail:
        status_tmp = 'SUCCESS   '
    else:
        status_tmp = 'FAIL      '
    
    while True:
        content = ''
        if (len(log_data_tmp) - index_tmp)  < 20000:
            #print 'index_tmp:%d' %(index_tmp)
            for i in range(0, (len(log_data_tmp )- index_tmp)):
                #print index_tmp + i
                content = content + str(log_data_tmp[index_tmp + i])
            
            
            mail_url = 'http://crm.ipaloma.com/sendmail'
            mail_payload = {
                            'dstmail':'xu.xin@ipaloma.com, liu.guangtao@ipaloma.com, jin.dongxu@ipaloma.com, backend@ipaloma.com, ma.li@ipaloma.com, zhang.haifeng@ipaloma.com, wang.dawei@ipaloma.com',
                            #'dstmail':'xu.xin@ipaloma.com',
                            'subject':status_tmp + str(sys_param) + '数据比较' + str(datetime.datetime.today())[0:19],
                            'content':str(content).replace('\r\n', '<br>') + '<br>邮件发送完成<br>'
                            }
            time.sleep(10)            
            mail_headers = {
                            'content-type': "application/x-www-form-urlencoded",
                            'cache-control': "no-cache",
                            }
            time.sleep(10)                                                                                                               
            mail_response = requests.request("POST", mail_url, data=mail_payload, headers=mail_headers)

            if str(mail_response).find('200') != -1:
                #print u'邮件发送完成'
                return True
            else:
                
                return False
                
        else:
            #print 'index_tmp:%d' %(index_tmp)
            for i in range(0, 20000):
                
                content = content + str(log_data_tmp[index_tmp + i])
                
            
            
            mail_url = 'http://crm.ipaloma.com/sendmail'
            mail_payload = {
                            'dstmail':'xu.xin@ipaloma.com, liu.guangtao@ipaloma.com, jin.dongxu@ipaloma.com, backend@ipaloma.com, ma.li@ipaloma.com, zhang.haifeng@ipaloma.com, wang.dawei@ipaloma.com',
                            'subject':status_tmp + str(sys_param) + '数据比较' + str(datetime.datetime.today())[0:19],
                            'content':str(content).replace('\r\n', '<br>') + '<br>邮件发送中<br>'
                            }
            time.sleep(10)            
            mail_headers = {
                            'content-type': "application/x-www-form-urlencoded",
                            'cache-control': "no-cache",
                            }
            time.sleep(10)                                                                                                                 
            mail_response = requests.request("POST", mail_url, data=mail_payload, headers=mail_headers)

            if str(mail_response).find('200') != -1:
                #print u'邮件发送中'
                index_tmp = index_tmp + 20000
            else:
                
                send_mail()
                
    

if __name__ == '__main__':
    
    sys_param = sys.argv[1]
    table_list = []
    if len(sys.argv) > 2:
        table_list = sys.argv[2].split()
    global data_config
    global data_json
    test_filename = '%s.txt' %(str(sys_param))
    file_config = open(test_filename, 'r+')
    data_config = file_config.readlines()
    file_config.flush()
    file_config.close()
    flag_sendmail = 0
    if len(list(data_config)) > 1:
        flag_sendmail = 1
    
    if os.path.exists(sys_param + '.log') == True:
        os.remove(sys_param + '.log')
    for i in range(0, len(list(data_config))):
        data_json = json.loads(list(data_config)[i])
            
        if str(data_json['SRC']['DB_MODE']) == 'DRDS':
            src_dburi = "mssql+pymssql://%s:%s@%s:%s/%s" %(
                        data_json['SRC']['USER'],
                        data_json['SRC']['PASSWD'],
                        data_json['SRC']['DB_SERVER'],
                        data_json['SRC']['DB_PORT'],
                        data_json['SRC']['DB_NAME']
            )
            des_dburi = "postgresql://%s:%s@%s:%s/%s" % (
                        data_json['DES']['USER'],
                        data_json['DES']['PASSWD'],
                        data_json['DES']['DB_SERVER'],
                        data_json['DES']['DB_PORT'],
                        data_json['DES']['DB_NAME']
                    )
        com_start_time = time.time()   
        #"mssql+pymssql://ipaloma_tableau:ipaloma_tableau@2017@ipalomamysql01extra.mysql.rds.aliyuncs.com:3306/membership-tableau" 
        try:
            #print 'DB_NAME:%s' %(data_json['SRC']['DB_NAME'])
            print src_dburi
            src_dbconn = IpaDBMySQLDB(None, None, DATASOURCE_DRDS)
            src_dbconn.create_db_conn(src_dburi)
            servicename = src_dbconn.exec_select("select strvalue from tblglobalstatus where statusname='servicename'")[0]['strvalue']
            log = 'servicename:%s    target:%s\r\n' %(servicename, data_json['target'])
            print log
            writeTxt(sys_param + '.log', log)
            #全量查询
            if data_json['COM_MODE'] == 'com_by_tblmigratesetting':
                log = u'全量比较\r\n'  
                print log
                writeTxt(sys_param + '.log', log) 
                where_sql = " where targettable in ('%s')" % "',".join(table_list) if table_list else ""
                print where_sql
                map_sql = "select * from tblmigratesetting%s" %(where_sql)
                mapping_settings = src_dbconn.exec_select(map_sql)
                src_dbconn.close_db_conn()
                if not mapping_settings:
                    log = u'Failed to get tblmigratesetting'
                    print log
                    writeTxt(sys_param + '.log', log) 
                    sys.exit(1)
                pool = ThreadPool(processes=8)

                try:
                    domain = data_json['DES']['DOMAIN']
                    args = [ (src_dburi, des_dburi, item, domain, servicename) for item in mapping_settings ]
                    #print args
                    pool.map(mssql2ads_compare, args)
                except Exception as e:
                    traceback.print_exc()
                finally:
                    pool.close()
            
        except Exception as e:
            traceback.print_exc()
    log = '比对用时:%f秒\r\n' %(time.time() - com_start_time)
    print log
    writeTxt(sys_param + '.log', log)
    time.sleep(20)
    #发送邮件
    
    #print u'发送邮件'       
    
    for i in range (0, 3):
        ret = send_mail(flag_sendmail, sys_param)
        if ret == True:
            break
    
