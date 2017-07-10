# coding:UTF-8
from xlrd import open_workbook
from xlutils.copy import copy
import xlwt,xlrd
from xlwt import *
import urllib,urllib2,cookielib
import pycurl
import os
import StringIO
import time
import thread
import threading
import sys


xlsfile1 = r'Auto_Test.xls'
ISOTIMEFORMAT='%Y-%m-%d %X'
mylock = threading.Condition()
myfile = xlwt.Workbook(encoding='utf-8')

def set_style(name,height,bold=False):
  style = xlwt.XFStyle() # 初始化样式
 
  font = xlwt.Font() # 为样式创建字体
  font.name = name # 'Times New Roman'
  font.bold = bold
  font.color_index = 4
  font.height = height
 
  # borders= xlwt.Borders()
  # borders.left= 6
  # borders.right= 6
  # borders.top= 6
  # borders.bottom= 6
 
  style.font = font
  # style.borders = borders
 
  return style

###建立请求类，调用类中函数发送请求###
class myThread(threading.Thread):
    def __init__(self, fileName, url, data, method, sheet):
        threading.Thread.__init__(self)  
        self.t_fileName     = fileName
        self.t_url          = url
        self.t_data         = data
        self.t_method       = method
        self.t_sheet        = sheet
        
    def run(self):
        mylock.acquire()
        ###发送请求，获取返回数据###
        url_value = urllib.urlencode(self.t_data)
        #print(url_value)
        pc = pycurl.Curl()###利用pycurl发送请求至服务器，记录返回各个数据信息###
        b = StringIO.StringIO()
        pc.setopt(pycurl.WRITEFUNCTION, b.write)
        
        if(self.t_method.lower() == 'post'):
            print("send POST")
            pc.setopt(pycurl.POST, 1)
            pc.setopt(pycurl.URL, str(self.t_url))
            pc.setopt(pycurl.POSTFIELDS, url_value)
        else:
            url = self.t_url + "?" + url_value
            #print(url)
            pc.setopt(pycurl.URL, url)
        
        pc.perform()
        
        
        response_context = b.getvalue()
        response_code = pc.getinfo(pycurl.RESPONSE_CODE)
        #print(response_code)
        response_NameLookUp_time = pc.getinfo(pycurl.NAMELOOKUP_TIME)
        #print(response_NameLookUp_time)
        response_Connect_Time = pc.getinfo(pycurl.CONNECT_TIME)
        response_Pretransfer_Time = pc.getinfo(pycurl.PRETRANSFER_TIME)
        response_StartTransfer_Time = pc.getinfo(pycurl.STARTTRANSFER_TIME)
        response_Total_Time = pc.getinfo(pycurl.TOTAL_TIME)
        response_Redirect_Time = pc.getinfo(pycurl.REDIRECT_TIME)
        #print(response_Redirect_Time)
                
        pc.close()
        b.close()
        
        #mylock.acquire()
        ###将获取各数据信息写入到xls中###
        #row1 = [u"请求发送地址（URL）", u"请求发送参数（PARA）",u"请求方法", u"返回状态（Ret）", u"域名解析时间（NAMELOOKUP_TIME）", u"远程服务器连接时间（CONNECT_TIME）", u"连接上后到开始传输时的时间（PRETRANSFER_TIME）", u"接收到第一个字节的时间（STARTTRANSFER_TIME）", u"上一请求总的时间（TOTAL_TIME）", u"如果存在转向，花费的时间（REDIRECT_TIME）"]
        rown = [str(time.strftime(ISOTIMEFORMAT, time.localtime(time.time()))), str(self.t_url), str(self.t_data), str(self.t_method), str(response_code), str(response_NameLookUp_time), str(response_Connect_Time), str(response_Pretransfer_Time), str(response_StartTransfer_Time), str(response_Total_Time), str(response_Redirect_Time)]
        #writeTxt(self.t_fileName + '.txt', rown[0])
        #writeTxt(self.t_fileName + '.txt', rown[1])
        #writeTxt(self.t_fileName + '.txt', rown[2])
        #writeTxt(self.t_fileName + '.txt', rown[3])
        #writeTxt(self.t_fileName + '.txt', rown[4])
        #writeTxt(self.t_fileName + '.txt', response_context)
        #writeTxt(self.t_fileName + '.txt', rown[5])
        #writeTxt(self.t_fileName + '.txt', rown[6])
        #writeTxt(self.t_fileName + '.txt', rown[7])
        #writeTxt(self.t_fileName + '.txt', rown[8])
        #writeTxt(self.t_fileName + '.txt', rown[9])
        #writeTxt(self.t_fileName + '.txt', rown[10])
        
        
        r_xls = xlrd.open_workbook('sheet.xls', 'a+')
        sheet_tmp = r_xls.sheet_by_name(self.t_fileName)
        #print(self.t_sheet)
        if sheet_tmp:
            print("into sheet_tmp")
            print(self.t_sheet)
            rows = sheet_tmp.nrows
            print("sheet row:%d" %rows)
            w_xls = copy(r_xls)
            sheet_tmp = w_xls.get_sheet(self.t_sheet-1)
                               
            for i in range(0, len(rown)):
                sheet_tmp.write(rows, i, rown[i])
                                
            w_xls.save('sheet.xls')
            mylock.release()
            return        
        
###处理excel表格数据的函数####
def  excel_data(xlsfile, count):
    book = xlrd.open_workbook(xlsfile, 'a+')
    
    sheet_number = book.sheets()
    row0 = [u"日志时间", u"请求发送地址（URL）", u"请求发送参数（PARA）",u"请求方法", u"返回状态（Ret）", u"域名解析时间（NAMELOOKUP_TIME）", u"远程服务器连接时间（CONNECT_TIME）", u"连接上后到开始传输时的时间（PRETRANSFER_TIME）", u"接收到第一个字节的时间（STARTTRANSFER_TIME）", u"上一请求总的时间（TOTAL_TIME）", u"如果存在转向，花费的时间（REDIRECT_TIME）", u"所有请求总时间", u"所有动作平均时间"]
    if not os.path.exists('sheet.xls'): 
        w_xls = Workbook(encoding='utf-8')
        for sheet in range(1, len(book.sheets())):
            sheet = w_xls.add_sheet(book.sheet_names()[sheet], cell_overwrite_ok=True)
            #for i in range(0, len(row0)):
            #   sheet.write(0, i, row0[i])
        w_xls.save('sheet.xls')
    #return

    avg_list = []
    
    for sheet in range(1, len(book.sheets())):###每个sheet中数据处理###
        threads = []
        api_sheet = book.sheet_by_index(sheet)
        #print(sheet)    
        sheet_name = book.sheet_names()[sheet]
        nrows = api_sheet.nrows
        #print("nrows%s" %nrows)
        row_number = 0#表示第几行
        
        r_xls1 = xlrd.open_workbook('sheet.xls', 'a+')
        sheet_tmp = r_xls1.sheet_by_name(sheet_name)
        row_tmp = sheet_tmp.nrows
        w_xls1 = copy(r_xls1)
        sheet_tmp1 = w_xls1.get_sheet(sheet - 1)
        for i in range(0, len(row0)):
            sheet_tmp1.write(row_tmp, i, row0[i])
        w_xls1.save('sheet.xls')
        
        #list = 0
        
        while(row_number < nrows/3):
            url1 = book.sheet_by_index(0).cell(0, 0).value
            #print("row_number:%d" %row_number*3)
            method = book.sheet_by_index(sheet).cell(row_number*3, 1).value
            print(method)
            print("Implement: " + str(url1))
            
            ### 获取输入参数的名字###
            input_para_count = int(api_sheet.cell(row_number*3, 0).value)
            #print("input_para_count:%d" %input_para_count)
            input_para_num = 0
            input_para_nameList = []
            input_para_nameList.append(book.sheet_by_index(0).cell(1, 0))
            while(input_para_num < input_para_count-1):###循环获取 每个输入参数的名字###
                input_para_name = api_sheet.cell(row_number*3 + 1, input_para_num)
                input_para_nameList.append(input_para_name)
                input_para_num = input_para_num + 1
            
            ####获取输入参数的值#####        
        
            input_para_num2 = 0
            input_para_valueList = []
            input_para_valueList.append(book.sheet_by_index(0).cell(2, 0))
            nrows = api_sheet.nrows
            data1 = {}
            while(input_para_num2 < input_para_count-1):
                    input_para_value = api_sheet.cell(row_number*3 + 2, input_para_num2)
                    input_para_valueList.append(input_para_value)
                    input_para_num2 = input_para_num2 + 1

            #fileName = sheet_name + str(row_number) + ".txt"
            
            j = 0
            while(j < input_para_count):###循环将获取的数据组成url可识别的形式###
                data1[input_para_nameList[j].value] = input_para_valueList[j].value
                #print(j)
                j = j + 1
                
            thread = myThread(sheet_name, url1, data1, method, sheet)
            threads.append(thread)
            
                       
            row_number = row_number + 1
        
        for t in threads:
            t.start()
            
        while(True):
            flag = 0
            for t in threads:
                if t.isAlive():
                  flag = flag + 1
            time.sleep(3)
            if not flag:
                break;  
        
        r_xls_tmp = xlrd.open_workbook('sheet.xls')
        sheet_r = r_xls_tmp.sheet_by_index(sheet - 1)
        rows_r = sheet_r.nrows
        total_time_each = []
        for i in range(0, row_number):
            total_time_each.append(float(sheet_r.cell(rows_r - 1 - i, 9).value))
        print(total_time_each)
        total_time = 0
        for i in range(0, len(total_time_each)):
            total_time = total_time + total_time_each[i]
        w_xls_tmp = copy(r_xls_tmp)
        sheet_w = w_xls_tmp.get_sheet(sheet - 1)
        sheet_w.write(rows_r - 1, 11, total_time)
        #sheet_w.write(rows_r - 1, 12, total_time/13)
        w_xls_tmp.save('sheet.xls')
        
        avg_list.append(total_time)
        print(avg_list)
        
        print("total")
    
    return avg_list 
    
    
def writeTxt(fileName, inputText):
        
    fileName = fileName
    file_obj = open(fileName, 'a+')
    file_obj.write(inputText)
    file_obj.write('\n\n\n')
    file_obj.close()
   
if __name__ == "__main__":
    #global count_request 
    count_request = raw_input("input number:")
    print('request number:%d' %int(count_request))
    avg = []
    for i in range(0, int(count_request)):
        ret = excel_data(xlsfile1, count_request)
        avg.append(ret)
        print(avg)
    
    xls_r = xlrd.open_workbook('sheet.xls', 'a+')
    #sheet_number = xls_r.sheets()
    xls_w = copy(xls_r)
    for sheet in range(0, len(xls_r.sheets())):
        avg_total = 0
        avg_each = 0
        for i in range(0, int(count_request)):
            avg_total = avg_total + avg[i][sheet]
        avg_each = avg_total/int(count_request)
        #print(avg_each)
        sheet_r = xls_r.sheet_by_index(sheet)
        
        sheet_w = xls_w.get_sheet(sheet)
        sheet_w.write(1, 12, avg_each)
    
    xls_w.save('sheet.xls')
            
    print("test end")