from pymongo import MongoClient

client = MongoClient(host='localhost', port=27017)
# 连接数据库
db = client.PharmRG
# 连接到users集合
# uid = db["uniport_id"]

print(db.list_collection_names())  # 获取数据库中所有集合名称
# print(uid.count())  # 统计users集合的文档数

# 连接需要操作的集合
collection = db["drop_all"]
data = {"name": "Test", "address": "Highway 22"}
query = {"name":"Test"}
new_values={"$set":{"address":"Shanghai 11"}}
# 操作数据，insert,delete,remove,update,find

# result = collection.insert_one(data)
# result = collection.find_one(query)
# result = collection.delete_one(query)
# result = collection.update_one(query,new_values)
# print(result.modified_count)

def connect(host,port,db_name,username,password):
    client=MongoClient(host,port)
    db=client[db_name]
    if username and password:
        db.authenticate(username,password)
    return client
def insertOne(host,port,db_name,username,password,collection_name,data):
    # 连接数据库
    client=connect(host, port, db_name, username, password)
    # 选择需要操作的集合
    collection=client[db_name][collection_name]
    # 插入一条数据
    result=collection.insert_one(data)
    # 返回插入数据
    print(result.inserted_id)
    return result.__inserted_id

def insertMany(host,port,db_name,username,password,collection_name,data):
    # 连接数据库
    client=connect(host, port, db_name, username, password)
    # 选择需要操作的集合
    collection=client[db_name][collection_name]
    # 插入多条数据
    result=collection.insert_many(data)
    # 返回插入数据
    print(result.inserted_ids)
    return result.__inserted_ids

def deleteOne(host,port,db_name,username,password,collection_name,filter):
    # 连接数据库
    client = connect(host, port, db_name, username, password)
    # 选择需要操作的集合
    collection = client[db_name][collection_name]
    # 删除一条数据
    result = collection.delete_one(data)
    # 返回插入数据
    return result

def removeAll(host,port,db_name,username,password,collection_name):
    # 连接数据库
    client = connect(host, port, db_name, username, password)
    # 选择需要操作的集合
    collection = client[db_name][collection_name]
    # 删除集合
    result = collection.remove()

def findOne(host,port,db_name,username,password,collection_name,filter):
    # 连接数据库
    client = connect(host, port, db_name, username, password)
    # 选择需要操作的集合
    collection = client[db_name][collection_name]
    #查找数据
    result=collection.find_one(filter)
    print(result)
    return result

def updateOne(host,port,db_name,username,password,collection_name,filter,update):
    client = connect(host, port, db_name, username, password)
    # 选择需要操作的集合
    collection = client[db_name][collection_name]
    #
    result = collection.update_one(filter,{"$set":update})
#qwq
#qwqwqwqw
