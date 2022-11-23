import csv
import os
from pymongo import MongoClient

header = []
inputLists = []


def addDB():
    client = MongoClient(host='localhost', port=27017)
    db = client['jungook']
    collection = db['addresses']
    
    result = collection.insert_many(inputLists)

    
def makeAddress(zipcode, sido, sigungu, eupmyeon, gil, mainNewNum, subNewNum, dong, mainPastNum, subPastNum, building):
    newAddress = sido
    pastAddress = sido
    
    if(sido != sigungu):
        newAddress += " "+sigungu
        pastAddress += " "+sigungu
        
    if(eupmyeon != ""):
        newAddress += " "+eupmyeon
        pastAddress += " "+eupmyeon

    newAddress += " "+gil
    pastAddress += " "+dong
    
    newAddress += " "+mainNewNum
    pastAddress += " "+mainPastNum
    
    if(subNewNum != "0"):
        newAddress+="-"+subNewNum
    if(subPastNum != "0"):
        pastAddress+="-"+subPastNum
        
    newAddress += " ("
    newAddress += dong
    
    if(building != ""):
        newAddress+=", "+building
        pastAddress += " ("+building+")"
    newAddress+=")"
    
    return newAddress, pastAddress
    

def parsing(line):
    global header
    global inputLists
    
    addressDict = {key: value for key, value in zip(header, line)}
    zipcode = addressDict["우편번호"]
    sido = addressDict["시도"]
    sigungu = addressDict["시군구"]
    eupmyeondong = addressDict["읍면"]
    if(eupmyeondong == ""):
        eupmyeondong = addressDict["법정동명"]
        
    eupmyeon = addressDict["읍면"]
    roadname = addressDict["도로명"][:addressDict["도로명"].find("로")+1]
    fullRoadName = addressDict["도로명"]
    mainNewNum = addressDict["건물번호본번"]
    subNewNum = addressDict["건물번호부번"]
    
    dong = addressDict["법정동명"]
    if dong == "":
        dong = addressDict["리명"]
    
    lee = addressDict["리명"]
    mainPastNum = addressDict["지번본번"]
    subPastNum = addressDict["지번부번"]
    building = addressDict["시군구용건물명"]
    newAddress, pastAddress = makeAddress(zipcode, sido, sigungu, eupmyeon, fullRoadName, mainNewNum, subNewNum, dong, mainPastNum, subPastNum, building)
    
    
    inputLists.append({
        "sido": sido,
        "sigungu": sigungu,
        "eupmyeondong": eupmyeondong,
        "lee": lee,
        "roadname": roadname,
        "fullRoadName": fullRoadName,
        "building": building,
        "newAddress": newAddress,
        "pastAddress": pastAddress
        })
    
def readFolder(): 
    global header
    global inputLists
    
    folderPath="./zipcode"
    allList = os.listdir(folderPath)
    fileList = [file for file in allList if file.endswith(".txt")]
    for file in fileList:
        inputLists=[]
        with open(folderPath+"/"+file, encoding="utf-8-sig") as fileData:
            csvReader = list(csv.reader(fileData, delimiter='|'))
            header = csvReader[0]
            for line in csvReader[1:]:
                parsing(line)
        print(file)
        addDB()

    '''
    행정동 과 읍면 일치
    읍면이 없는 경우
    시군구 - 읍면 - 도로명 - 건물 본번 - 건물 부번?
    
    '''
if __name__ == '__main__':
    readFolder()

    print("end")
    
