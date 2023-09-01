import xlrd2
import json
from pymongo import MongoClient
from import_excel_complex import excel_to_mongodb_complex as import_target
from import_excel_complex2 import excel_to_mongodb_complex2 as import_target2

def excel_to_mongodb(host,port,collection,username,password,list,name):
    # 连接数据库
    client = MongoClient(host=host, port=port,username=username,password=password)
    db = client.PharmRG
    collection = collection

    # 读取Excel文件
    data = xlrd2.open_workbook(name)
    table = data.sheets()[0]

    # 读取excel第一行数据作为存入mongodb的字段名
    row_stag = table.row_values(0)
    n_rows = table.nrows
    return_data = {}

    save_size = 0
    save_collection = []
    for i in range(1, n_rows):
        # 将字段名和excel数据存储为字典形式，并转换为json格式
        return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
        # 通过编解码还原数据
        return_data[i] = json.loads(return_data[i])
        # 转换表头为所需字段名
        # del return_data[i]['序号']
        for j in list:
             return_data[i][j] = return_data[i].pop(j)
        # return_data[i]['TARGETID'] = return_data[i].pop('TARGETID')
        # return_data[i]['UNIPROID'] = return_data[i].pop('UNIPROID')
        # return_data[i]['TARGNAME'] = return_data[i].pop('TARGNAME')
        # return_data[i]['TARGTYPE'] = return_data[i].pop('TARGTYPE')

        save_collection.insert(save_size, return_data[i])
        if save_size >= 1000:
            db[collection].insert_many(save_collection)
            save_size = 0
            save_collection.clear()
        else:
            save_size += 1

    db[collection].insert_many(save_collection)
def search_one(host,port,db,collection,username,password,fliter):
    """

    :param host:
    :param port:
    :param db:
    :param collection:
    :param fliter:
    :return:
    """
    client = MongoClient(host=host, port=port,username=username,password=password)
    db = client[db]
    collection = db[collection]
    result = collection.find_one(fliter)
    print(result)
    return result
def search_many(host,port,db,collection,username,password,fliter):
    """

    :param host:
    :param port:
    :param db:
    :param collection:
    :param fliter:
    :return:
    """
    client = MongoClient(host=host, port=port,username=username,password=password)
    db = client[db]
    collection = db[collection]
    result = collection.find(fliter)
    for i in result:
        print(i)
    return result

if __name__ == '__main__':
    #变量初始化
        #本地变量
    host="localhost"
    port=27017
    db="PharmRG"
    username=''
    password=''
        #远程连接
    # host = "117.73.10.251"
    # port = 27017
    # db = "PharmRG"
    # username = 'readwrite'
    # password = 'readwrite'

    #search功能变量
    search_collection="TTD_uniport_id_all"
    fliter={"TARGTYPE":"Clinical Trial target"}

    #import功能变量
    import_collection = "TTD_Drug_synonyms"
    import_list=[]
    import_url='D:\桌面\学校\PharmRG科研\TTD_data\处理表格\P1-04-Drug_synonyms.xlsx'

    #import_excel变量
    signal='TTDDRUID'
    list1=['DRUGNAME']
    list2=['INDICATI']

    # import_excel变量
    signal2 = 'KEY'
    list3 = ['TTDDRUID','DRUGNAME']
    list4 = ['SYNONYMS']
    #方法操作区
    # search_one(host,port,db,search_collection,username,password,fliter)
    # search_many(host,port,db,search_collection,username,password,fliter)
    # excel_to_mongodb(host,port,import_collection,username,password,import_list,import_url)
    # import_target(host,port,import_collection,username,password,signal,list1,list2,import_url)
    import_target2(host,port,import_collection,username,password,signal2,list3,list4,import_url)

