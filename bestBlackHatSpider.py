# -*- coding: utf-8 -*-
import scrapy
import logging
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from datetime import datetime as dt
from scrapy.selector import Selector
from scrapy.spiders import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor
from ..items import BestblackhatforumItem, ReplyItems


class BaseSpider(CrawlSpider):
	name = 'base'
	all_done = thread_done = False
	proxy = 'http://127.0.0.1:8081'
	cookie = dict()
	replies = ReplyItems()
	item = BestblackhatforumItem()
	visited_threads = replies_data = list()
	allowed_domains = ['bestblackhatforum.com']
	start_urls = [
		'https://bestblackhatforum.com/',
	]

	rules = (
		Rule(
			LinkExtractor(
				restrict_xpaths='//a[starts-with(@id,"tid_")]',  # '//td[@class="trow1"][2]/strong/a',
				allow_domains=allowed_domains,
				unique=True
			),
			callback='post_scrape'
		),
	)

	# def parse_item(self, response):
	# 	forum_links = LinkExtractor(
	# 		allow_domains=self.allowed_domains,
	# 		restrict_xpaths='//a[starts-with(@id,"tid_")]',
	# 		unique=True
	# 	).extract_links(response)
	#
	# 	for link in forum_links:
	# 		yield Request(url=link.url, callback=self.post_scrape, dont_filter=False)
	#
	# 	next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
	# 	if next_page is not None:
	# 		yield response.follow(url=next_page, callback=self.parse_item)

	def post_scrape(self, response):
		if 'Thread-' in response.url and response.url not in self.visited_threads \
				and 'Sorry but your accessing a page(s) that is no longer ' in response.text:
			self.visited_threads.append(response.url)
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass

			posts = response.xpath('//div[@id="posts"]/table[starts-with(@id,"post_")]')
			if not self.thread_done:
				post_info = posts[0]
				self.item['thread_url'] = response.url
				self.item['thread_group'] = response.xpath('//div[@class="navigation"]/a[2]/text()').extract_first()

				self.item['author_name'] = post_info.xpath('.//em/text()').extract_first()
				membership_level = len(post_info.xpath('.//td[@class="post_author"]/span/img'))

				try:
					join_date = post_info.xpath(
						'.//td[contains(@class," post_author_info")]/div/text()'
					).extract()[1].split('Joined: ')[1].strip()
					self.item['author_joined_date'] = dp.parse(join_date, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					self.item['author_joined_date'] = ''

				self.item['author_posts_count'] = post_info.xpath(
					'.//td[contains(@class," post_author_info")]/div/text()'
				).extract()[0].split('Posts: ')[1]

				timestamp = post_info.xpath('.//td[@class="tcat"]/div/text()').extract_first().strip()
				try:
					self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
				except (TypeError, AttributeError):
					self.item['thread_timestamp'] = ''

				thread_content = post_info.xpath('.//div[starts-with(@id,"pid_")]/..//*').extract()
				(self.item['thread_media_links'], self.item['thread_general_links']) = \
					self.extract_links(post_info, ' '.join(thread_content))

				thread_content = post_info.xpath('.//div[starts-with(@id,"pid_")]/..//text()').extract()
				self.item['thread_content'] = self.replace_patterns(thread_content, timestamp)

				self.item['author_membership_level'] = '4' if membership_level > 4 else str(membership_level)
				self.item['author_location'] = self.item['author_age'] = ''
				self.item['scraped_date'] = dt.now().isoformat()
				self.thread_done = True
				self.replies_data = []
				self.reply_scrape(posts[1:])

			next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False
			if next_page is not None:
				try:
					self.reply_scrape(requests.get(url=next_page, cookies=self.cookie, proxies={'http': self.proxy}))
				except Exception as e:
					print('Next Page Exception -> Exit', e)
			self.all_done = True if self.thread_done else False

			if self.all_done:
				self.replies_data = []
				self.thread_done = False
				self.item['thread_replies'] = self.replies_data
				self.item['thread_reply_no'] = len(self.replies_data)
				yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.unified.SelectorList:
			record = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url in self.visited_threads:
				return
			else:
				record = Selector(response).xpath('///table[starts-with(@id,"post_")]')
		elif self.all_done:
			return
		else:
			return
		for reply in record:
			try:
				author_info = reply.xpath('.//tr[2]')
				reply_author = author_info.xpath('.//em/text()').extract_first(default='')
				reply_author_membership = len(author_info.xpath('.//td[@class="post_author"]/span/img'))

				reply_content = ' '.join(
					reply.xpath('.//div[starts-with(@id,"pid_")]/..//*').extract())
				(self.replies['reply_media_links'], self.replies['reply_general_links']) = \
					self.extract_links(reply, reply_content)

				if '<blockquote>' in reply_content:
					reply_content = reply.xpath('.//div[starts-with(@id,"pid_")]/text()').extract()
				else:
					reply_content = reply.xpath('.//div[starts-with(@id,"pid_")]/.//text()').extract()

				try:
					reply_timestamp = reply.xpath('.//td[@class="tcat"]/div/text()').extract_first().strip()
					self.replies['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					reply_timestamp = self.replies['reply_timestamp'] = ''

				self.replies['reply_author'] = reply_author
				self.replies['reply_content'] = self.replace_patterns(reply_content, reply_timestamp)
				self.replies['reply_author_membership'] = '4' \
					if reply_author_membership > 4 else str(reply_author_membership)
			except Exception as e:
				logging.exception('Error while scraping reply:', e)
			finally:
				self.replies_data.append(dict(self.replies))
		if type(response) is scrapy.selector.unified.SelectorList:
			return
		next_page = Selector(response).xpath('//a[@rel="next"]/@href').extract_first()

		if self.thread_done and next_page is None:
			self.all_done = True
			return

		if next_page is not None:
			try:
				self.reply_scrape(requests.get(url=next_page, cookies=self.cookie, proxies={'http': self.proxy}))
			except Exception as e:
				print('Next Page Exception -> Exit', e)
				self.all_done = True if self.thread_done else False
				return
		self.all_done = True


	def extract_links(self, response, data):
		media_links = general_links = []
		if '<img ' in data:
			media_links = response.xpath(
				'.//div[starts-with(@id,"pid_")]/..//img/@src').extract()
		if '<iframe ' in data:
			media_links += response.xpath(
				'.//div[starts-with(@id,"pid_")]/..//iframe/@src').extract()
		if '<a ' in data:
			general_links += response.xpath(
				'.//div[starts-with(@id,"pid_")]/..//a/@href').extract()
		return ';'.join(media_links), ';'.join(general_links)

	def replace_patterns(self, content_data, timestamp):
		check_content = ''
		timestamp = '' if timestamp is None else timestamp
		try:
			content_data = self.val_transform(content_data)
			final_content = ' '.join(content_data)
			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except Exception as e:
				check_content = None
				logging.exception('Error while uni-decoding:', e)
			final_content = check_content if check_content is not None else final_content
			check_content = final_content = final_content.replace("  ", "")

			replaceable_patterns = {
				'#1': '',
				' .': '',
				'--': '',
				'++': '',
				'__': '',
				'*': '',
				'#': '',
				'!!': '',
				'..': '',
				'.)': '',
				'(:-': '',
				'-:)': '',
				':)': '',
				'(:': '',
				'Hide Content': '',
				'Show Content': '',
				'Hidden Content:': '',
				'\n': '',
				'\t': '',
				'\r': '',
				timestamp: '',
			}

			final_content = reduce(lambda a, kv: a.replace(*kv), replaceable_patterns.items(), final_content)
			final_content = final_content.replace('  ', '')
			check_content = final_content

		except Exception as e:
			logging.exception(e)
		finally:
			return check_content

	def val_transform(self, content_data):
		for i in range(len(content_data)):
			val = content_data[i].strip() if content_data[i] is not None else ''
			try:
				if val is None or val is "" or val is " ":
					val = ''
				elif val.startswith('(This post was modified') or val.startswith('This post was modified'):
					val = ''
				elif val.startswith('Ban Reason'):
					val = ''
				elif val.startswith('You must '):
					val = ''
				elif val.startswith('RE:'):
					val = ''
			except Exception as e:
				logging.exception('Error in val_transform()', e)
				continue
			finally:
				content_data[i] = val
		return content_data
