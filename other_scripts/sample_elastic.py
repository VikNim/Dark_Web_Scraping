from bs4 import BeautifulSoup
import requests
from elasticsearch import Elasticsearch

ELASTICSEARCH_INDEX = 'sample_quotes'
ELASTICSEARCH_TYPE = '_doc'

es = Elasticsearch()
# url = 'http://quotes.toscrape.com/'
url = 'http://quotes.toscrape.com/page/2/'


def search(quote):
	search_body = {
		'query': {
			'match': {
				'quote': quote
			}
		}
	}
	result = es.search(index=ELASTICSEARCH_INDEX, doc_type=ELASTICSEARCH_TYPE, body=search_body)
	if len(result['hits']['hits']) == 0:
		return False
	else:
		return True


def insert(quote):
	if search(quote):
		print('Quote already exists...!')
		return
	else:
		es.index(index=ELASTICSEARCH_INDEX, doc_type=ELASTICSEARCH_TYPE, body={'quote': quote})
		print('Quote Added..!')


response = requests.get(url=url, timeout=5)
page_content = BeautifulSoup(response.content, "html.parser")
quotes = page_content.find_all('span', attrs={'itemprop': 'text'})

for q in quotes:
	insert(q.text)
