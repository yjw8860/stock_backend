from pymongo import MongoClient


client = MongoClient('localhost', 27017)

# # 'test_database'라는 이름의 데이터베이스 선택 (없다면 자동 생성)
# db = client['test_database']
#
# # 'test_collection'라는 이름의 컬렉션 선택 (없다면 자동 생성)
# collection = db['test_collection']
#
# # 데이터 삽입
# doc = {'name':'John', 'age':30, 'job':'developer'}
# collection.insert_one(doc)
#
# # 데이터 조회
# for data in collection.find():
#     print(data)
client.drop_database('test_database')