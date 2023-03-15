import json
import urllib.request
from datetime import datetime, timedelta

key = '9c91bc6d7cca946fffc04ee94bc2e24c'


class Api_GNews():

    def __init__(self,api_key = '9c91bc6d7cca946fffc04ee94bc2e24c'):
        self.api_key = api_key
        print(f'Api_Gnews object was initialized')


    def fetch_articles(self, keyword='Tesla'):
        
        from_time = int((datetime.now() - timedelta(hours=1)).timestamp())
        url = f"https://gnews.io/api/v4/search?q={keyword}&lang=en&mindate={from_time}&country=*"
        url += f"&max=10&in=title&apikey={self.api_key}"
        
        with urllib.request.urlopen(url) as response:
            status_code = response.getcode()
            if status_code == 200:
                fetched_data = json.loads(response.read().decode("utf-8"))
                print("Data fetched successfully!")
                return fetched_data
            else:
                error_message = fetched_data.get("message")
                print(f"Error fetching data. Status code: {status_code}. Error message is {error_message}")


    def prepare_news_data_for_upload(self, data):

        data_dict = {
                "title": [],
                "description": [],
                "content": [],
                "url": [],
                "image": [],
                "publishedat": [],
                "source": []
                }        

        for article in data['articles']:

            data_dict['title'].append(article['title'])
            data_dict['description'].append(article['description'])
            data_dict['content'].append(article['description'])
            data_dict['url'].append(article['url'])
            data_dict['image'].append(article['image'])
            data_dict['publishedat'].append(article['publishedAt'])
            data_dict['source'].append(article['source']['name'])

        print('Converted data to dictionary, ready for upload!')
        
        return data_dict