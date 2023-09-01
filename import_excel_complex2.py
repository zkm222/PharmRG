import xlrd2
import json
from pymongo import MongoClient
def excel_to_mongodb_complex2(host,port,collection,username,password,signal,list1,list2,name):
    """

    :param host:
    :param port:
    :param collection: 需要存放的集合
    :param username:
    :param password:
    :param signal: 唯一标识
    :param list1: 只有一个的数据
    :param list2: 需要用list存放的数据
    :param name:  文件名
    :return:
    """

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
    # 原始数据

    k=0
    now='qwq'
    # 获取现在数据的唯一标识

    save_collection=[]
    # 缓冲list
    return_data2={}
    # 上传数据
    for i in range(1, n_rows):
        # 将字段名和excel数据存储为字典形式，并转换为json格式
        return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
        # 通过编解码还原数据
        return_data[i] = json.loads(return_data[i])
        return_data2[k+1]={}
        # 转换表头为所需字段名
        # del return_data[i]['序号']

        # print(return_data[i])
        # 输出样例：{'TARGETID': 'T78590', 'KEY': 'DRUGINFO', 'VALUE': 'D0OB7J', 'DRUGNAME': 'NPC-15669', 'Highest Clinical Status': 'Discontinued in Phase 1'}

        # 对于唯一标识的数据进行归纳
        if return_data[i][signal]=='':
            k+=1
            # return_data2[k]['TARGETID']=now
            for j in list1:
                return_data2[k][j] =''
            for j in list2:
                return_data2[k][j]=[]
            # print(now,k)
        for j in list1:
            if j==return_data[i]['KEY']:
                return_data2[k][j]=return_data[i].pop('VALUE')
        for j in list2:
            if j==return_data[i]['KEY']:
                #对于特殊数据的处理
                # if j=='DRUGINFO':
                if j=='INDICATI':
                    # return_data2[k][j].append(return_data[i].pop('VALUE')+' '+return_data[i].pop('DRUGNAME')+' '+return_data[i].pop('Highest Clinical Status'))
                    return_data2[k][j].append(return_data[i].pop('VALUE')+' '+return_data[i].pop('ADD'))
                else:
                    return_data2[k][j].append(return_data[i].pop('VALUE'))
    for t in range(1,k):
        # db[collection].insert_one(return_data2[t])
        save_collection.insert(t, return_data2[t])
        if t % 1000==0:
            db[collection].insert_many(save_collection)
            save_collection.clear()
    db[collection].insert_many(save_collection)
