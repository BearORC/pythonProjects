# -*- coding: utf-8 -*-
import os
import cx_Oracle
import mysql.connector
import re
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def main():
    config = {'host': '45.4.8.10',
              'user': 'root',
              'password': 'kdc',
              'port': 3306,
              'database': 'KDMAAA',
              'charset': 'utf8'
              }
    cnx = mysql.connector.connect(**config)
    ptcursor = cnx.cursor()

    #取出数据源
    kdmIdList = getoradata()
    print("========开始=============")
    for row in kdmIdList:
        if row is not None:
            kdmId = row[3].split(',')[0]
            chnId = row[3].split(',')[1]
            jd = row[4]
            wd = row[5]
            #print("{},{},{},{},{},{}".format(row[0],row[1],row[2],row[3],row[4],row[5]))
            uuidlist = getuuid( ptcursor, kdmId)
            if uuidlist is not None:
                uuid = uuidlist[0]
                deviceName = uuidlist[1]
                updateextdata(ptcursor, uuid, chnId, deviceName, jd, wd)
                #getmysqldata(kdmId,chnId,jd,wd)
    #updatekdmidtooracledb(ptcursor)
    print("========结束=============")
    cnx.commit()
    ptcursor.close()
    cnx.close()

def getoradata():
    #Oracle连接参数
    username="ezview"
    userpwd="ezview"
    host="45.4.0.66"
    port=1521
    dbname="orcl1"
    #连接数据库
    dsn=cx_Oracle.makedsn(host,port,dbname)
    connection=cx_Oracle.connect(username,userpwd,dsn)
    oracursor=connection.cursor()
    #查询语句
    sql="select xh,sbbh,sbmc,sbms,jd,wd from b_sssb_sbxx where sbdl = 3"
    #执行语句
    oracursor.execute(sql)
    #获取所有结果
    result=oracursor.fetchall()
    #关闭游标和链接
    oracursor.close()
    connection.close()
    #返回查询结果
    return result

def updatekdmidtooracledb(ptcursor):
    #Oracle连接参数
    username="ezview"
    userpwd="ezview"
    host="45.4.0.66"
    port=1521
    dbname="orcl1"
    #连接数据库
    dsn=cx_Oracle.makedsn(host,port,dbname)
    connection=cx_Oracle.connect(username,userpwd,dsn)
    oracursor=connection.cursor()
    #查询语句
    sql="select uuid from B_SSSB_SBXX_YZT"
    #执行语句
    oracursor.execute(sql)
    #获取所有结果
    result=oracursor.fetchall()
    for row in result:
        rs = getkdmid(ptcursor,row[0])
        if rs is not None:
            uuid = str(row[0])
            m = re.search("\d{32}",rs[0])
            kdmid = str(m.group(0))
            update_string = "update B_SSSB_SBXX_YZT set KDMID = {} where UUID = '{}'".format(kdmid,uuid)
            oracursor.execute(update_string)
        connection.commit()
    #关闭游标和链接
    oracursor.close()
    connection.close()
    #返回查询结果

'''
def getmysqldata(kdmId, chnId, jd, wd):
    config = {'host': '45.4.8.10',
              'user': 'root',
              'password': 'kdc',
              'port': 3306,
              'database': 'KDMAAA',
              'charset': 'utf8'
              }
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    #通过kdmId找uuid
    sql_queryDevice = "select devId,devName,devData from tblDevice_test WHERE devData like CONCAT('%',%s, '%')"

    cursor.execute(sql_queryDevice,(kdmId,))
    uuidlist = cursor.fetchall();
    if len(uuidlist)==1:
        uuid=''
        dname=''
        extData=makeextdata( jd, wd)
        for (devId,devName,devData)in uuidlist:
            print("devID:{}".format(devId))
            uuid = devId
            dname = devName
        #通过uuid找经纬度数据
        sql_queryDeviceCapIndexData = 'select deviceId,deviceCapIndexName,deviceCapIndexExtData from tblDeviceCapIndexData_test where deviceId =  %s'
        cursor.execute(sql_queryDeviceCapIndexData,(uuid,))
        capIndexDatalist = cursor.fetchall();
        #判断是否有经纬度数据，1有0没有
        #有经纬度数据执行更新
        if len(capIndexDatalist)==1:
            #update script
            for (deviceId,deviceCapIndexName,deviceCapIndexExtData)in capIndexDatalist:
                update_capIndexData = ("update tblDeviceCapIndexData_test "
                                        "set deviceCapIndexExtData = %s"
                                        "where deviceId = %s")
                data_for_update = (extData, deviceId)
                cursor.execute(update_capIndexData, data_for_update)
                print('2.0pt:{},{}'.format(deviceCapIndexName, deviceCapIndexExtData))
        #没有经纬度数据执行插入
        if len(capIndexDatalist)==0:
            #insert script
            add_capIndexData = ("INSERT INTO tblDeviceCapIndexData_test "
                            "(deviceId, deviceCapId, deviceCapIndex, deviceCapIndexName, deviceCapIndexExtData) "
                            "VALUES (%s, %s, %s, %s, %s)")
            data_for_insert = (uuid, 1, chnId, dname, extData)
            cursor.execute(add_capIndexData, data_for_insert)
            #cursor.execute(sql_insertDeviceCapIndexData, (uuid,))
    print("----------------------------------------------------")
    cnx.commit()
    cursor.close()
    cnx.close()
'''
def getuuid( cursor, kdmId):
    #通过kdmId找uuid
    sql_queryDevice = "select devId,devName,devData from tblDevice_test WHERE devData like CONCAT('%',%s, '%')"
    cursor.execute(sql_queryDevice,(kdmId,))
    uuidlist = cursor.fetchall();
    uuid=''
    if len(uuidlist)==1:
        for (devId,devName,devData)in uuidlist:
            return devId,devName
def getkdmid( cursor, uuid):
    #通过kdmId找uuid
    sql_queryDevice = "select devData from tblDevice_test WHERE devId = %s"
    cursor.execute(sql_queryDevice,(uuid,))
    uuidlist = cursor.fetchall();
    if len(uuidlist)==1:
        for (devData)in uuidlist:
            return devData

def updateextdata( cursor, uuid, chnId, dname, jd, wd):

    #通过入参生成经纬度xml
    extData = makeextdata(jd, wd)
    # 通过uuid判断是否有经纬度数据
    sql_queryDeviceCapIndexData = 'select deviceId,deviceCapIndexName,deviceCapIndexExtData from tblDeviceCapIndexData_test where deviceId =  %s'
    cursor.execute(sql_queryDeviceCapIndexData, (uuid,))
    capIndexDatalist = cursor.fetchall();
    # 判断是否有经纬度数据，1有0没有
    # 有经纬度数据执行更新
    if len(capIndexDatalist) == 1:
        # update script
        for (deviceId, deviceCapIndexName, deviceCapIndexExtData) in capIndexDatalist:
            update_capIndexData = ("update tblDeviceCapIndexData_test "
                                   "set deviceCapIndexExtData = %s"
                                   "where deviceId = %s")
            data_for_update = (extData, deviceId)
            cursor.execute(update_capIndexData, data_for_update)
    # 没有经纬度数据执行插入
    if len(capIndexDatalist) == 0:
        # insert script
        add_capIndexData = ("INSERT INTO tblDeviceCapIndexData_test "
                            "(deviceId, deviceCapId, deviceCapIndex, deviceCapIndexName, deviceCapIndexExtData) "
                            "VALUES (%s, %s, %s, %s, %s)")
        data_for_insert = (uuid, 1, chnId, dname, extData)
        cursor.execute(add_capIndexData, data_for_insert)
        # cursor.execute(sql_insertDeviceCapIndexData, (uuid,))


def makeextdata(jd, wd):
    extData = '<TDeviceCapExtData><extData size="2"><item index="0"><key>Lat</key><value>{}</value></item><item index="1"><key>Lon</key><value>{}</value></item></extData></TDeviceCapExtData>'.format( wd, jd)
    return extData

if __name__ == '__main__':
    main()