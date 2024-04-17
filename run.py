from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

import requests
import time
import sys
import pymongo
import os
import re

class TwitterParser():
    def __init__(self):
        self.options = Options()
        self.options.headless = True
        self.options.add_argument("--window-size=1024,1200")
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.implicitly_wait(30)
        
        self.twitterUser = { 'login':    os.environ["TWITTER_USERNAME"], 'password': os.environ["TWITTER_PASSWORD"]}
        self.twitter_url = f'https://twitter.com/{os.environ["TWITTER_ACCOUNT"]}?lang=en'
        self.discord_hook_url = os.environ["DISCORD_HOOK_LINK"]

        self.locators = {}
        # self.locators['LOGIN_XPATH'] = (By.ID, 'layers')
        self.locators['MAIN_XPATH'] = (By.XPATH, '//div[@data-testid="primaryColumn"]')
        self.locators['POST_XPATH'] = (By.XPATH, '//article')
        self.locators['LINK_XPATH'] = (By.XPATH,'//time/parent::*')
        self.locators['LINK_DATE_XPATH'] = (By.XPATH,'./time')
        self.locators['LOGIN_BUTTON_XPATH'] = (By.XPATH,'//a[contains(@href, "/login")]/parent::*')
        self.locators['LOGIN_USERNAME_INPUT_XPATH'] = (By.XPATH, '//input[@autocomplete="username"]')
        self.locators['LOGIN_PASSWORD_INPUT_XPATH'] = (By.XPATH, '//input[@autocomplete="current-password"]')
        self.locators['LOGIN_NEXT_BUTTON_XPATH'] = (By.XPATH, '//div[@role="button"][.="Next"]')
        self.locators['LOGIN_BUTTON_FINAL_XPATH'] = (By.XPATH,'//div[@role="button"][.="Log in"]')
        

        self.page = {}
        self.getUrl()
        # self.page['main']  = None
        # self.page['posts'] = None
        self.page['links'] = []
        self.output = []

    def getUrl(self):
        self.driver.get(self.twitter_url)
        time.sleep(10)
    
    def login(self):
        self.driver.find_element(*self.locators['LOGIN_BUTTON_XPATH']).click()
        time.sleep(30)
        loginInput = self.driver.find_element(*self.locators['LOGIN_USERNAME_INPUT_XPATH'])
        loginInput.send_keys(self.twitterUser['login'])
        time.sleep(10)
        self.driver.find_element(*self.locators['LOGIN_NEXT_BUTTON_XPATH']).click()
        time.sleep(10)
        passwordInput = self.driver.find_element(*self.locators['LOGIN_PASSWORD_INPUT_XPATH'])
        passwordInput.send_keys(self.twitterUser['password'])
        time.sleep(10)
        self.driver.find_element(*self.locators['LOGIN_BUTTON_FINAL_XPATH']).click()
        time.sleep(30)

    def pageDown(self, iterationCallback = None ):
        self.driver.find_element(By.TAG_NAME, "html").send_keys(Keys.PAGE_DOWN)
        if iterationCallback:
            iterationCallback()

    def pageDownWithWait(self, waitSeconds: int = 3, iterationCallback = None ):
        self.pageDown()
        time.sleep(waitSeconds)
        if iterationCallback:
            iterationCallback()

    def pageDownByCount(self, count: int = 1, waitSeconds: int = 3, iterationCallback = None ):
        for x in range(count):
            self.pageDownWithWait(waitSeconds=waitSeconds)
            if iterationCallback:
                iterationCallback()

    def pageDownToEnd(self, waitSeconds: int = 3, iterationCallback = None ):
        pageHeight = self.driver.execute_script("return document.body.scrollHeight")
        totalScrolledHeight = self.driver.execute_script("return window.pageYOffset + window.innerHeight")
        print(f'PageH: {pageHeight}, TotalH: {totalScrolledHeight},PageH-1 >= TotalH: {(pageHeight-1)>=totalScrolledHeight}')
        while((pageHeight-1)>=totalScrolledHeight):
            # if self.driver.execute_script('return window.innerHeight + window.pageYOffset >= document.body.offsetHeight'):
            #     break
            self.pageDownWithWait(waitSeconds=waitSeconds)
            totalScrolledHeight = self.driver.execute_script("return window.pageYOffset + window.innerHeight")
            pageHeight = self.driver.execute_script("return document.body.scrollHeight")
            if iterationCallback:
                iterationCallback()
        else:
            pass

    def getLinks(self):
        self.page['links'] = self.driver.find_elements(*self.locators['LINK_XPATH'])
    

    def extractLinks_url(self):
        self.getLinks()
        print('Iterate links...')
        for link in self.page['links']:    
            href = link.get_attribute('href').rsplit('/', 1)[-1]
            date = link.find_element(*self.locators['LINK_DATE_XPATH']).get_attribute('datetime')
            
            data = { 'accountName': os.environ["TWITTER_ACCOUNT"], 'url': href, 'datetime': date }

            if data not in self.output:
                print(data)
                self.output.append(data)
    
    def getUrlsArray(self):
        arr = []
        for link in parser.output:
            arr.append(link['url'])
        return arr
    
    def sendMessageToDiscord(self, link: str):
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "content": f'https://fxtwitter.com/{os.environ["TWITTER_ACCOUNT"]}/status/{link}',
        }
        requests.post(self.discord_hook_url, json=data, headers=headers)

class LinksDBModel():
    def __init__(self, url: str, datetime: str):
        self.url = url
        self.datetime = datetime

    def __eq__(self, other):
        return self.url == other.url
    
class MongoDB:
    def __init__(self):
        self.host = os.environ["MONGO_HOST"]
        self.port = int(os.environ["MONGO_PORT"])
        self.dbName = os.environ["MONDO_DBNAME"]

    def insert_many(self, collection: str, data: dict ):
        pymongo.MongoClient(host=self.host, port=self.port)[self.dbName][collection].insert_many(data)

    def insert_one(self, collection: str, data: dict ):
        pymongo.MongoClient(host=self.host, port=self.port)[self.dbName][collection].insert_one(data)    

    def update_many(self, collection: str, who: dict, data: dict):
        pymongo.MongoClient(host=self.host, port=self.port)[self.dbName][collection].update_many(who, data)
    
    def find(self, collection: str, who: dict):
        return pymongo.MongoClient(host=self.host, port=self.port)[self.dbName][collection].find(who)
    
    def find_one(self, collection: str, who: dict):
        return pymongo.MongoClient(host=self.host, port=self.port)[self.dbName][collection].find_one(who)
    
    def aggregate(self, collection: str, pipeline: list):
        return pymongo.MongoClient(host=self.host, port=self.port)[self.dbName][collection].aggregate(pipeline)


if __name__ == '__main__':
    os.environ
    parser = TwitterParser()
    mongo = MongoDB()
    dbData = mongo.find_one( "links",{ "accountName": os.environ["TWITTER_ACCOUNT"]} )
    if os.environ["TWITTER_USERNAME"]:
        parser.login()
    if not dbData:
        parser.pageDownToEnd(iterationCallback=parser.extractLinks_url)
        mongo.insert_many("links", parser.output)
        parser.output = []
    
    while True:
        parser.output = []
        parser.getUrl()
        parser.pageDownWithWait(iterationCallback=parser.extractLinks_url)
        time.sleep(int(os.environ["REPEAT_INTERVAL_SEC"]) or 86400)
        foundedUrls = parser.getUrlsArray()

        pipeline = [ 
            {"$match": { "accountName": os.environ["TWITTER_ACCOUNT"], "url": { "$in" : foundedUrls } } }, 
            {"$group": { 
                "_id": None, 
                "urls": {"$push": "$url"} 
            }} 
        ] 
        existedUrls = list(mongo.aggregate("links", pipeline ))[0]
        
        if len(foundedUrls) > len(existedUrls['urls']):
            for url in foundedUrls:
                if url not in existedUrls['urls']:
                    parser.sendMessageToDiscord(url)
                    res = next((link for link in parser.output if link['url'] == url), None)
                    if res:
                        mongo.insert_one('links', res)

