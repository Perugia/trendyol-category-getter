from asyncio import tasks
import redis
import sqlite3
import asyncio
import json
import xml.etree.ElementTree as ET
import platform
import aiohttp
import time
from aiohttp.client import ClientSession
from bs4 import BeautifulSoup

import category
import user_agents

red = redis.Redis(host='localhost', port=6379, db=0)
con = sqlite3.connect('trendyol.db')
cur = con.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS categories
(id integer primary key autoincrement, name tinytext, tylink tinytext, tyid tinytext, parent_category_id tinytext, last_category bit)''')

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def download_link(url:str,id:str,session:ClientSession,page_sources:dict):
    errorCounter = 0
    while True:
        if errorCounter > 20:
            break
        header = user_agents.get_new_header()
        async with session.get(url, headers=header) as response:
            result = await response.text()
            page_sources[id] = result
            if response.status != 200:
                errorCounter += 1
                continue

        break

async def download_all(urls:dict,page_sources:dict):
    my_conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        tasks = []
        for id,url in urls.items():
            task = asyncio.ensure_future(download_link(url=url,id=id,session=session,page_sources=page_sources))
            tasks.append(task)
        await asyncio.gather(*tasks,return_exceptions=True) # the await must be nest inside of the session

def getCategories(parentCategoryDict,allCategoriesDict):
    url = "https://public.trendyol.com/discovery-web-searchgw-service/v2/api/aggregations"
    page_sources = {}
    url_list = {}
    for i in parentCategoryDict:
        url_list[parentCategoryDict[i].tyid]= url + parentCategoryDict[i].tylink

    start = time.time()

    asyncio.run(download_all(url_list,page_sources))
    end = time.time()
    print(f'download {len(url_list)} links in {end - start} seconds')

    for id,source in page_sources.items():
        ownCategoriesDict = {}
        lastCategory = False
        soup = BeautifulSoup(source, 'html.parser')
        try:
            aggregationsJson = json.loads(str(soup))
        except:
            print(str(soup))

        if aggregationsJson["result"] != None:
            for aggregations in aggregationsJson["result"]["aggregations"]:
                if aggregations["group"] == "CATEGORY":
                    try:
                        if len(aggregations["values"]) == 1 and aggregations["values"][0]["id"] == aggregationsJson["result"]["selectedFilters"][0]["id"]:
                                allCategoriesDict[id].setLastCategory()
                                lastCategory = True
                                print("-------------------LAST CATEGORY-----------------------",id)
                        else:
                            for cat in aggregations["values"]:
                                if ("-x-c" not in cat["url"]):
                                    continue
                                exist = red.hsetnx('categories',cat["id"],'1')
                                if exist == 1:
                                    allCategoriesDict[cat["id"]] = (category.Category(cat["text"],cat["id"],cat["url"],id))
                                    ownCategoriesDict[cat["id"]] = (category.Category(cat["text"],cat["id"],cat["url"],id))
                                    
                                else:
                                    print(cat["text"], "Already added!")

                    except Exception:
                        print("------------------------------------",id,Exception)
        if lastCategory == False:
            getCategories(ownCategoriesDict,allCategoriesDict)

def getNonExistCategories(CategoriesDict):
    url = "https://trendyol.com"
    page_sources = {}
    url_list = {}
    for i in CategoriesDict:
        url_list[CategoriesDict[i].tyid]= url + CategoriesDict[i].tylink

    asyncio.run(download_all(url_list,page_sources))
    end = time.time()
    print(f'download {len(url_list)} links in {end - start} seconds')

    for id,source in page_sources.items():
        lastCategory = False
        soup = BeautifulSoup(source, 'html.parser')

        categoriesTags = (soup.find("div",{"data-partial-fragment-name":"MarketingSearch"})).find_all("a")

        if len(categoriesTags) == 0:
            print("last Category")
        else:
            CategoriesDict[id].setName(categoriesTags[-1].getText())
            CategoriesDict[id].setParentId(((categoriesTags[-2]["href"]).split("-x-c"))[-1])

            print(CategoriesDict[id].name,CategoriesDict[id].tyid,CategoriesDict[id].tylink,CategoriesDict[id].parent_category_id)
    
    getCategories(CategoriesDict,CategoriesDict)






#---------------------------------------------------------------------------------

allCategoriesDict = {}

url = "https://public.trendyol.com/discovery-web-searchgw-service/v2/api/aggregations/"
mainCategoryIndexes = [{"name":"Aksesuar","id":"27","link":"/aksesuar-x-c27"},
{"name":"Ayakkabı","id":"114","link":"/ayakkabi-x-c114"},
{"name":"Giyim","id":"82","link":"/giyim-x-c82"},
{"name":"Ev & Mobilya","id":"145704","link":"/ev--mobilya-x-c145704"},
{"name":"Kozmetik & Kişisel Bakım","id":"89","link":"/kozmetik-x-c89"},
{"name":"Elektronik","id":"104024","link":"/elektronik-x-c104024"},
{"name":"Süpermarket","id":"103799","link":"/supermarket-x-c103799"},
{"name":"Anne & Bebek & Çocuk","id":"144835","link":"/anne--bebek--cocuk-x-c144835"},
{"name":"Spor & Outdoor","id":"104593","link":"/spor-outdoor-x-c104593"},
{"name":"Dijital Ürünler","id":"144649","link":"/digital-goods-x-c144649"},
{"name":"Bahçe & Yapı Market","id":"103719","link":"/bahce-yapi-market-x-c103719"},
{"name":"Hamile Giyim","id":"104625","link":"/hamile-giyim-x-c104625"},
{"name":"Su","id":"104006","link":"/su-x-c104006"},
{"name":"Müzik","id":"1357","link":"/muzik-x-c1357"},]

ownCategoriesDict = {}

for i in mainCategoryIndexes:

    exist = red.hsetnx('categories',i["id"],'1')
    if exist == 1:
        ownCategoriesDict[i["id"]] = (category.Category(i["name"],i["id"],i["link"]))
        allCategoriesDict[i["id"]] = (category.Category(i["name"],i["id"],i["link"]))
    else:
        print(i["name"], "Already added!")


getCategories(ownCategoriesDict,allCategoriesDict)

for i in allCategoriesDict:
    cur.execute("INSERT INTO categories(name, tylink, tyid, parent_category_id, last_category) VALUES (?, ?, ?, ?, ?)",
        (allCategoriesDict[i].name,allCategoriesDict[i].tylink,allCategoriesDict[i].tyid,allCategoriesDict[i].parent_category_id,allCategoriesDict[i].last_category))



# #---------------------------------------------------------------------------------

page_sources = {}
url_list = {"1":"https://www.trendyol.com"}
allCategoriesDict2 = {}

start = time.time()
asyncio.run(download_all(url_list,page_sources))
end = time.time()
print(f'download {len(url_list)} links in {end - start} seconds')

for id,source in page_sources.items():
    soup = BeautifulSoup(source, 'html.parser')

    scripts = soup.find_all("script",type="application/javascript")
    for sc in scripts:
        
        if "window.__NAVIGATION_APP_INITIAL_STATE_V2__" in str(sc.getText()):
            script = sc
            replaceText = "window.__NAVIGATION_APP_INITIAL_STATE_V2__="
            productListJson = (((str(sc.getText())).replace(replaceText,"")).lstrip()).rstrip()
            productListJson = productListJson[:-1]

            allCategoriesJson = json.loads(productListJson)
            
for item in allCategoriesJson["items"]:
    for column in item["Children"]:
        for child in column["Children"]:
            if ("-x-c" not in child["Url"]):
                continue
        
            id = (((child["Url"].split("-c"))[-1]).split("?"))[0]
            exist = red.hsetnx('categories',id,'1')
            if exist == 1:
                    allCategoriesDict2[id] = (category.Category(child["Name"],id,child["Url"]))
            else:
                print(child["Name"], "Already added!")
                
getCategories(allCategoriesDict2,allCategoriesDict2)

for i in allCategoriesDict2:
    cur.execute("INSERT INTO categories(name, tylink, tyid, parent_category_id, last_category) VALUES (?, ?, ?, ?, ?)",
        (allCategoriesDict2[i].name,allCategoriesDict2[i].tylink,allCategoriesDict2[i].tyid,allCategoriesDict2[i].parent_category_id,allCategoriesDict2[i].last_category))




#---------------------------------------------------------------------------------

page_sources = {}
url_list = {"1":"https://www.trendyol.com"}
allCategoriesDict2 = {}

start = time.time()
asyncio.run(download_all(url_list,page_sources))
end = time.time()
print(f'download {len(url_list)} links in {end - start} seconds')

for id,source in page_sources.items():
    soup = BeautifulSoup(source, 'html.parser')

    scripts = soup.find_all("script",type="application/javascript")
    for sc in scripts:
        
        if "window.__NAVIGATION_APP_INITIAL_STATE_V2__" in str(sc.getText()):
            script = sc
            replaceText = "window.__NAVIGATION_APP_INITIAL_STATE_V2__="
            productListJson = (((str(sc.getText())).replace(replaceText,"")).lstrip()).rstrip()
            productListJson = productListJson[:-1]

            allCategoriesJson = json.loads(productListJson)
            
for item in allCategoriesJson["items"]:
    for column in item["Children"]:
        for child in column["Children"]:
            for children in child["Children"]:
                if ("-x-c" not in children["Url"]):
                    continue
        
                id = (((children["Url"].split("-c"))[-1]).split("?"))[0]
                exist = red.hsetnx('categories',id,'1')
                if exist == 1:
                        allCategoriesDict2[id] = (category.Category(children["Name"],id,children["Url"]))
                else:
                    print(children["Name"], "Already added!")


getCategories(allCategoriesDict2,allCategoriesDict2)

for i in allCategoriesDict2:
    cur.execute("INSERT INTO categories(name, tylink, tyid, parent_category_id, last_category) VALUES (?, ?, ?, ?, ?)",
        (allCategoriesDict2[i].name,allCategoriesDict2[i].tylink,allCategoriesDict2[i].tyid,allCategoriesDict2[i].parent_category_id,allCategoriesDict2[i].last_category))


#------------------------------------------------------------------------------------------------

page_sources = {}
url_list = {"1":"https://www.trendyol.com/sitemap_categories.xml"}

CategoriesDict = {}

start = time.time()
asyncio.run(download_all(url_list,page_sources))
end = time.time()
print(f'download {len(url_list)} links in {end - start} seconds')

for id,source in page_sources.items():
    root = ET.fromstring(source)

    for child in root:
        for loc in child:
            link = ((loc.text).split(".com"))[1]
            linkId = link.split("-x-c")[-1]

            exist = red.hexists('categories',linkId)
            if exist == False:
                CategoriesDict[linkId] = category.Category(tyid=linkId,tylink=link)
                red.hsetnx('categories',linkId,'1')


getNonExistCategories(CategoriesDict)

for i in CategoriesDict:
    cur.execute("INSERT INTO categories(name, tylink, tyid, parent_category_id, last_category) VALUES (?, ?, ?, ?, ?)",
        (CategoriesDict[i].name,CategoriesDict[i].tylink,CategoriesDict[i].tyid,CategoriesDict[i].parent_category_id,CategoriesDict[i].last_category))

con.commit()
con.close()