from types import new_class
from xml.dom.minidom import Element
import requests
import json
from bs4 import BeautifulSoup
import sys
from import_content_articles_csv_to_postgres import ImportArticlesContentCsvToPostgresDB
def write_back_json(CFG,Query_Count):
        CFG['Query-Index'] = Query_Count
        with open('./standardNewsConfig.json', 'w') as jsonFile:
            json.dump(CFG, jsonFile)                                
def main():
    #set up db connection
    data_transformer = ImportArticlesContentCsvToPostgresDB()
    data_transformer.init_db_connection()
    data_transformer.create_new_table_for_article_and_content('standard_news_article_table','standard_news_content_table')
    src = "The Standard"
    CFG = json.load(open('./standardNewsConfig.json'))
    QueryList=CFG['Query-List']
    while(True):
        Query_Count = CFG['Query-Index']
        keyword = QueryList[Query_Count]
        # Set up the URL and search query
        url = "https://www.thestandard.com.hk/ajax_search.php"

        # Set up the headers and payload for the POST request
        headers = {
        'authority': 'www.thestandard.com.hk',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': 'cid=wKiUfmRA7td+ugLNA+whAg==; _ga=GA1.3.1767633029.1681981876; PHPSESSID=65l51akp9rvdbm9sokf3d6s7f6; __gads=ID=e6435f406db6653f:T=1681984373:S=ALNI_MZ-eg4RBUkPLiUyFftR8KgCilk3hQ; _gid=GA1.3.1182872611.1682302846; __gpi=UID=00000bfa593a2d93:T=1681984373:RT=1682393598:S=ALNI_MaCScSMgbutsY_LhKXpRrBAFkDJ5A; _gat_UA-41819048-6=1; PHPSESSID=2du69pitvnr7rjaab0a77eie16; cid=wKiUfmRHSqxmaSubA475Ag==',
        'origin': 'https://www.thestandard.com.hk',
        'referer': 'https://www.thestandard.com.hk/search/hsbc',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
        }

        payload = {"sf[]": 1,
        "ds": 0,
        "af": 0,
        "rf": "04/27/2022",
        "rt": "04/27/2023",
        "p": 10,
        "sa[]": 11,
        "sa[]": 17,
        "sa[]": 4,
        "sa[]": 2,
        "sa[]": 3,
        "sa[]": 6,
        "sa[]": 8,
        "sa[]": 21,
        "ft[]": "fc_1",
        "ft[]": "sid_12",
        "ft[]": "fc_7",
        "ft[]": "fc_13",
        "ft[]": "fc_4",
        "ft[]": "fc_14",
        "ft[]": "fc_12",
        "ft[]": "fc_16",
        "ft[]": "fc_2",
        "ft[]": "fc_6",
        "ft[]": "fc_11",
        "ft[]": "fc_3",
        "ft[]": "sid_5",
        "ft[]": "sid_7",
        "kw": keyword,
        "adv": 1
        }
        # Send a POST request to the URL with the headers and payload
        response = requests.post(url, headers=headers, data=payload)

        # Parse the JSON content of the response
        data = response.json()
        for element in data["htmlData"]:
            content=[]
            url = element["pageUrl"]
            newsCategory = element['category']
            release_date = element['datetime']
            title = element['headline']
            response = requests.get(url)
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            print(soup)
            news_paragraphs = soup.find("div", {"class": "content"}).find_all("p")
            for news_paragraph in news_paragraphs:
                if "</a>" not in (str(news_paragraph)):
                    content.append(str(news_paragraph).strip("<p>").strip("</p>").strip("\r"))
            newsResult = {
                'keyword': str(keyword),
                'news_category': str(newsCategory),
                'title': str(title),
                'url': str(url),
                'release_date': str(release_date),
                'src': str(src),
                'content': content,
            }
            data_transformer.inject_single_article_and_content_into_db(newsResult['keyword'], newsResult['news_category'],
                                                                        newsResult['release_date'], newsResult['title'],
                                                                        newsResult['url'], newsResult['src'],
                                                                        newsResult['content'])
        Query_Count=Query_Count+1
        if(Query_Count==938):
            Query_Count=0
        write_back_json(CFG,Query_Count)

if __name__ == "__main__":
    sys.exit(main())