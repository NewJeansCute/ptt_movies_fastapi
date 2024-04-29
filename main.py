import time
import random
import pymongo
from queue import Queue
from loguru import logger
from threading import Thread
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["ptt"]
movies_by_threads = db["movies_by_threads"]

logger.remove()
logger.add("./ptt_movies_{time:YYYY-MM-DD}.log", encoding="utf-8")


class Push:
    def __init__(self, push_tag: str, push_userid: str, push_content: str, push_time: str):
        self.push_tag = push_tag
        self.push_userid = push_userid
        self.push_content = push_content
        self.push_time = datetime.strptime(f"2024/{push_time}", "%Y/%m/%d %H:%M")


class Article:
    def __init__(self, author: str, title: str, article_time: str, content: str, pushes: list[Push]):
        self.author = author
        self.title = title
        self.article_time = datetime.strptime(article_time, "%a %b %d %H:%M:%S %Y")
        self.content = content
        self.pushes = pushes


article_queue: Queue[Article.__dict__] = Queue()


class Crawler:
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(10)

        self.crawler_thread = Thread(target=self.run)
        self.crawler_thread.daemon = True
        self.crawler_thread.start()
        logger.info("Crawler thread started")

    def scrape(self, href: str, title: str):
        self.driver.get(href)
        soup = BeautifulSoup(self.driver.page_source, "lxml")

        author_span = soup.find("span", class_="article-meta-tag", string="作者")
        article_time_span = soup.find("span", class_="article-meta-tag", string="時間")

        if author_span and article_time_span:
            author = author_span.next_sibling.text.strip()
            article_time = article_time_span.next_sibling.text.strip()

        else:
            # 文章格式不正確
            author = ""
            article_time = ""
            logger.error(f"Got an article of invalid format. Title: {title}")

        main_content = soup.find("div", id="main-content").text
        article = main_content.split("\n--\n")[0]
        lines = article.split("\n")
        if "標題" not in lines[0]:
            content = "\n".join(lines[:])

        else:
            content = "\n".join(lines[1:])

        # 回文
        push_objs: list[Push.__dict__] = []
        url_span = soup.select("span.f2")[-1]
        pushes = url_span.find_all_next("div", class_="push")

        for push in pushes:
            spans = push.contents

            try:
                push_tag = spans[0].text.strip()
                push_userid = spans[1].text.strip()
                push_content = spans[2].text.strip()
                push_time = spans[3].text.strip()

            except IndexError:
                # 少數文章由於回文太多，會出現結構不同的提示訊息，直接跳過該訊息
                continue

            push_objs.append(
                Push(
                    push_tag=push_tag,
                    push_userid=push_userid,
                    push_content=push_content,
                    push_time=push_time
                ).__dict__
            )

        article_queue.put_nowait(
            Article(
                author=author,
                title=title,
                article_time=article_time,
                content=content,
                pushes=push_objs
            ).__dict__
        )
        logger.success("Added a new article into article_queue.")

    def run(self):
        try:
            # 抓取文章連結
            # 最新頁
            self.driver.get(f"https://www.ptt.cc/bbs/movie/index.html")
            current_url = self.driver.current_url
            logger.info(f"Started crawling {current_url}.")

            for a in self.driver.find_elements(By.CSS_SELECTOR, ".title a"):
                href = a.get_attribute("href")
                title = a.text.strip()

                # 抓取文章資訊
                self.scrape(href, title)
                # 調整發送頻率，避免短時間內大量send request
                # 延遲some milliseconds
                sleep_time = random.uniform(1, 20) / 1000
                time.sleep(sleep_time)

                # 返回文章列表
                self.driver.back()

            # 往後1000頁
            for page in range(1000):
                self.driver.find_element(By.XPATH, "//a[@class='btn wide' and contains(text(), '‹ 上頁')]").click()
                current_url = self.driver.current_url
                logger.info(f"Started crawling {current_url}.")

                for a in self.driver.find_elements(By.CSS_SELECTOR, ".title a"):
                    href = a.get_attribute("href")
                    title = a.text.strip()

                    # 抓取文章資訊
                    self.scrape(href, title)
                    # 調整發送頻率，避免短時間內大量send request
                    # 延遲some milliseconds
                    sleep_time = random.uniform(1, 20) / 1000
                    time.sleep(sleep_time)

                    # 返回文章列表
                    self.driver.back()

        finally:
            self.driver.quit()


class Saver:
    def __init__(self):
        self.saver_thread = Thread(target=self.run)
        self.saver_thread.daemon = True
        self.saver_thread.start()
        logger.info("Saver thread started")

    def run(self):
        while True:
            if article_queue.empty():
                continue

            else:
                article = article_queue.get_nowait()
                movies_by_threads.update_one(
                    # 檢查重複
                    # 同一個作者不會同一時間發超過一篇文
                    {"author": article["author"], "article_time": article["article_time"]},
                    {"$set": article},
                    upsert=True
                )
                logger.success("Added a new article into MongoDB.")


class Main:
    def get_list(self):
        article_list = list(
            movies_by_threads.aggregate(
                [
                    {"$sample": {"size": 15}},
                    {"$project": {"_id": 0, "title": 1}}
                ]
            )
        )

        print("title")

        for article in article_list:
            print(article['title'])

    def get_article(self):

        def print_article(article):
            for k, v in article.items():
                if k == "pushes":
                    print("pushes\n")

                    for i in v:
                        push_time = datetime.strftime(i['push_time'], '%m-%d %H:%M')
                        push_string = f"{i['push_tag']} {i['push_userid']} {i['push_content']} {push_time}"
                        print(push_string, "\n")

                else:
                    print(k, "\n")
                    print(v, "\n")

        while True:
            title = input("Enter the title or 'exit' to switch to other actions: ").strip()
            article = list(
                movies_by_threads.aggregate(
                    [
                        {"$match": {"title": title}},
                        {"$replaceRoot": {
                            "newRoot": {"title": "$title", "content": "$content", "pushes": "$pushes"}
                        }}
                    ]
                )
            )

            if title == "exit":
                break

            else:
                if article:
                    print_article(article[0])

                else:
                    print("Article not found. Try again or enter 'exit' to switch to other actions.")
                    logger.error(f"Article not found. Input title is {title}.")

    def menu(self):
        while True:
            try:
                print("\n1: Get the list of 15 articles. 2: Get the specified article. 3: Exit.")
                action = int(input("Enter an action: "))

                if action == 1:
                    self.get_list()

                elif action == 2:
                    self.get_article()

                elif action == 3:
                    print("Bye.")
                    break

                else:
                    print("Only accept 1, 2, or 3. Try again.")
                    logger.error("Menu got an integer input that's not in the range.")

            except ValueError:
                print("Only accept integers. Try again.")
                logger.error("Menu got an input that's not integer.")


crawler = Crawler()
saver = Saver()
main = Main()
main.menu()