# -*- coding: utf-8 -*-
import scrapy
import logging
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from datetime import datetime as dt
from ..items import MalvultItem, ReplyItems
from scrapy.spiders import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor


class BaseSpider(CrawlSpider):
	name = 'base'
	proxy = 'http://127.0.0.1'
	cookie = dict()
	item = MalvultItem()
	replies = ReplyItems()
	all_done = thread_done = False
	visited_threads = replies_data = list()
	allowed_domains = ['malvult.net']
	start_urls = [
		# 'http://malvult.net/index.php/',
		# 'http://malvult.net/index.php?forums/silent-exploits-doc-exploit-pdf-exploit.11/',
		'http://malvult.net/index.php?forums/support-articles-tutorials.9/'
	]
	membership_list = [
		'Banned',
		['New Member', 'Basic Member'],
		['Member', 'Active Member'],
		['Well-Known Member', 'Administrator']
	]

	rules = (
		# Rule(
		# 	LinkExtractor(
		# 		restrict_xpaths='//a[@class="PreviewTooltip"]',
		# 		allow_domains=allowed_domains,
		# 		unique=True
		# 	),
		# ),
		Rule(
			LinkExtractor(
				restrict_xpaths='//div[@class="titleText"]/h3[@class="title"]/a',
				allow_domains=allowed_domains,
				unique=True
			),
			callback='post_scrape'
		),
	)

	# def parse_item(self, response):
	# 	forum_links = LinkExtractor(
	# 		allow_domains=self.allowed_domains,
	# 		restrict_xpaths='//div[@class="titleText"]/h3[@class="title"]/a',
	# 		unique=True
	# 	).extract_links(response)
	#
	# 	for link in forum_links:
	# 		yield Request(url=link.url, callback=self.post_scrape, dont_filter=False)
	#
	# 	next_page = response.xpath('//a[text()="Next >"]/@href').extract_first()
	# 	if next_page is not None:
	# 		yield response.follow(url=next_page, callback=self.parse_item)

	def post_scrape(self, response):
		if 'threads/' in response.url and response.url not in self.visited_threads:
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			self.visited_threads.append(response.url)
			posts = response.xpath('//ol[@id="messageList"]/li')
			if not self.thread_done:
				try:
					post_info = posts[0]
					self.item['thread_url'] = response.url
					self.item['thread_group'] = response.xpath(
						'//span[@class="crumbs"]/span[last()]/a/span[@itemprop="title"]/text()').extract_first()

					self.item['author_name'] = post_info.xpath(
						'.//a[@class="username"]/.//text()').extract_first(default='')
					membership_level = post_info.xpath('.//em[@class="userTitle"]/text()').extract_first(default='')

					timestamp = post_info.xpath('.//span[@class="DateTime"]/@title').extract_first()
					timestamp = post_info.xpath('.//abbr[@class="DateTime"]/@data-datestring').extract_first() if \
						timestamp is None else timestamp
					try:
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
					except (TypeError, AttributeError):
						self.item['thread_timestamp'] = ''

					thread_content = post_info.xpath('.//div[@class="messageContent"]/article/blockquote/..//*').extract()
					(self.item['thread_media_links'], self.item['thread_general_links']) = \
						self.extract_links(post_info, ' '.join(thread_content))

					thread_content = post_info.xpath('.//div[@class="messageContent"]/article/blockquote/..//text()').extract()
					self.item['thread_content'] = self.replace_patterns(thread_content, str(timestamp))

					self.item['author_membership_level'] = self.get_membership(membership_level)
					self.item['author_posts_count'] = self.item['author_joined_date'] = \
						self.item['author_location'] = self.item['author_age'] = ''
					self.item['scraped_date'] = dt.now().isoformat()

					self.thread_done = True
					self.replies_data = []
					self.reply_scrape(posts[1:])
				except Exception as e:
					logging.exception(e)
			next_page = response.xpath('//a[contains(text(),"Next >")]/@href').extract_first()
			self.all_done = True if self.thread_done and next_page is None else False
			if next_page is not None:
				try:
					self.reply_scrape(requests.get(url=next_page, cookies=self.cookie, proxies={'http': self.proxy}))
				except:
					pass
			self.all_done = True if self.thread_done else False
			if self.all_done:
				self.item['thread_replies'] = self.replies_data
				self.item['thread_reply_no'] = len(self.replies_data)
				self.replies_data = []
				self.thread_done = False

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.SelectorList:
			total_replies = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url not in self.visited_threads:
				total_replies = response.xpath('//ol[@id="messageList"]/li')
			else:
				return
		elif self.thread_done:
			return
		else:
			self.all_done = True
			return
		for reply in total_replies:
			try:
				author_info = reply.xpath('.//div[@class="messageUserInfo"]')
				reply_author = author_info.xpath('.//a[@class="username"]/text()').extract_first(default='')
				reply_author_membership = ''.join(author_info.xpath('.//em[@class="userTitle"]/text()').extract())

				reply_content = ' '.join(
					reply.xpath('.//div[@class="messageContent"]/article/blockquote/..//*').extract())
				(self.replies['reply_media_links'], self.replies['reply_general_links']) = \
					self.extract_links(reply, reply_content)

				if 'class="quote"' in reply_content:
					reply_content = reply.xpath(
						'.//div[@class="messageContent"]/article/blockquote/text()').extract()
				else:
					reply_content = reply.xpath(
						'.//div[@class="messageContent"]/article/blockquote/..//text()').extract()

				reply_timestamp = ''
				try:
					reply_timestamp = reply.xpath('.//span[@class="DateTime"]/@title').extract_first()
					reply_timestamp = reply.xpath('.//abbr[@class="DateTime"]/@data-datestring').extract_first() if \
						reply_timestamp is None else reply_timestamp
					self.replies['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					self.replies['reply_timestamp'] = ''

				self.replies['reply_author'] = reply_author
				self.replies['reply_content'] = self.replace_patterns(reply_content, str(reply_timestamp))
				self.replies['reply_author_membership'] = self.get_membership(reply_author_membership)
			except Exception as e:
				logging.exception(e)
			finally:
				self.replies_data.append(dict(self.replies))

		if type(response) is scrapy.selector.SelectorList:
			return

		next_page = response.xpath('//a[contains(text(),"Next >")]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False
		if self.all_done:
			return

		if next_page is not None:
			try:
				self.reply_scrape(requests.get(url=next_page, cookies=self.cookie, proxies={'http': self.proxy}))
			except Exception as e:
				logging.error('Next Page Parsing Error:', e)
		self.all_done = True

	def extract_links(self, response, data):
		media_links = general_links = []
		if '<img ' in data:
			media_links = response.xpath(
				'.//div[@class="messageContent"]/article/blockquote/..//img/@src').extract()
		if '<iframe ' in data:
			media_links += response.xpath(
				'.//div[@class="messageContent"]/article/blockquote/..//iframe/@src').extract()
		if '<a ' in data:
			general_links += response.xpath(
				'.//div[@class="messageContent"]/article/blockquote/..//a/@href').extract()
		return ';'.join(media_links), ';'.join(general_links)

	def replace_patterns(self, content_data, timestamp=''):
		check_content = ''
		try:
			content_data = self.val_transform(content_data)
			final_content = ' '.join(content_data)

			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except Exception as e:
				check_content = None
				logging.exception(e)
			final_content = check_content if check_content is not None else final_content

			replaceable_patterns = {
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
			val = content_data[i].strip()
			try:
				if val is None or val is "" or val is " ":
					val = ''
				elif val.startswith('(This post was modified') or val.startswith('This post was modified'):
					val = ''
				elif val.startswith('Ban Reason'):
					val = ''
				elif val.startswith('You must '):
					val = ''
			except Exception as e:
				logging.exception(e)
			finally:
				content_data[i] = val
		return content_data

	def get_membership(self, membership):
		if membership in self.membership_list[0]:
			return '0'
		elif membership in self.membership_list[1]:
			return '1'
		elif membership in self.membership_list[2]:
			return '2'
		elif membership in self.membership_list[3]:
			return '3'
		return ''
