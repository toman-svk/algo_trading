import http.client, urllib.parse
import json
from datetime import datetime

class Api_Mediastack():

    def __init__(self, api_key = 'da3b3c58cd1f40e5938bc7388e91840f'):
        self.api_key = api_key
        print(f'Api_Mediastack object was initialized')

    def fetch_articles(self, keyword = 'Tesla'):

        self.conn = http.client.HTTPConnection('api.mediastack.com')
        params = urllib.parse.urlencode({
            'access_key': self.api_key,
            'keywords': keyword,
            'sort': 'published_desc',
            'limit': 10,
            'date': datetime.now().strftime('%Y-%m-%d')
        })
        self.conn.request('GET', '/v1/news?{}'.format(params))
        fetched_data = self.conn.getresponse().read()
        print('Data fetched successfully!')

        return json.loads(fetched_data.decode('utf-8'))
    
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

        for article in data['data']:

            data_dict['title'].append(article['title'])
            data_dict['description'].append(article['description'])
            data_dict['content'].append(article['description'])
            data_dict['url'].append(article['url'])
            data_dict['image'].append(article['image'])
            data_dict['publishedat'].append(article['published_at'])
            data_dict['source'].append(article['source'])

        print('Converted data to dictionary, ready for upload!')
        
        return data_dict