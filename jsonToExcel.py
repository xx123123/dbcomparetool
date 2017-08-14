#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import json
import types
import xlrd,xlwt

reload(sys)
sys.setdefaultencoding('utf8')

def WriteToExcel(dataToExcel, sheet, row, col):
	rowTmp = row
	colTmp = col
	if type(dataToExcel) == types.DictType:
		for keyFromDict in dataToExcel:			
			sheet.write(rowTmp, colTmp, '"' + str(keyFromDict) + '":')			
			if type(dataToExcel[keyFromDict]) == types.DictType:
				if len(dataToExcel[keyFromDict]) != 0:
					rowTmp += 1
					rowTmp = WriteToExcel(dataToExcel[keyFromDict], sheet, rowTmp, colTmp + 1)
			elif type(dataToExcel[keyFromDict]) == types.ListType:
				if len(dataToExcel[keyFromDict]) != 0:
					if type(dataToExcel[keyFromDict][0]) == types.DictType or type(dataToExcel[keyFromDict]) == types.ListType:
						rowTmp += 1
						rowTmp = WriteToExcel(dataToExcel[keyFromDict], sheet, rowTmp, colTmp + 1)
			else:
				#print str(dataToExcel[keyFromDict])
				#print 'rowTmp--->' + str(rowTmp)
				sheet.write(rowTmp, colTmp + 1, '"' + str(dataToExcel[keyFromDict]) + '"')
				rowTmp += 1

	if type(dataToExcel) == types.ListType:
		#print str(dataToExcel)
		for keyFromList in dataToExcel:
			#print 'keyFromList--->' + str(keyFromList)
			rowTmp = WriteToExcel(keyFromList, sheet, rowTmp, colTmp)
			
	return rowTmp

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print '输入参数错误'
		sys.exit(0)

	fileName = str(sys.argv[1])

	with open(fileName + '.txt', 'r') as fileData:
		dataJson = fileData.read()

	dataToExcel = json.loads(dataJson)
	print str(dataToExcel) 
	workBook = xlwt.Workbook()
	sheet = workBook.add_sheet('sheet1')
	ret = WriteToExcel(dataToExcel, sheet, 0, 0)
	if ret:
		workBook.save(fileName + '.xls')