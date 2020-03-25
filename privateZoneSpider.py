# -*- coding: utf-8 -*-
import scrapy
import logging
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from scrapy.http import Request
from datetime import datetime as dt
from scrapy.spiders import Rule, CrawlSpider
from ..items import PrivatezoneItem, ReplyItem
from scrapy.linkextractors import LinkExtractor


class BaseSpider(CrawlSpider):
	name = 'base'
	cookie = dict()
	item = PrivatezoneItem()
	reply_item = ReplyItem()
	proxy = 'http://127.0.0.1:8118'
	all_done = thread_done = False
	visited_threads = replies_dict = list()
	allowed_domains = ['prvtzone.ws']
	start_urls = [
		'https://prvtzone.ws/'
	]
	membership_list = ['Newcomer', ['Member', 'Student'], '', ['Experienced Member', 'VENDOR'], 'Staff Member']
	rules = (
		Rule(
				LinkExtractor(
						restrict_xpaths='///h3[@class="nodeTitle"]/a',
						allow_domains=allowed_domains,
						unique=True
				),
				callback='forum_scrape'
		),
	)
	# def parse(self, response):
	# 	forum_links = response.xpath('///h3[@class="nodeTitle"]/a/@href').extract()
	#
	# 	if forum_links:
	# 		for forum_link in forum_links:
	# 			if '#' in forum_link:
	# 				continue
	# 			yield Request(url='https://prvtzone.ws/' + forum_link, callback=self.forum_scrape)

	def forum_scrape(self, response):

		thread_links = LinkExtractor(
				allow_domains=self.allowed_domains,
				restrict_xpaths='//h3[@class="title"]/a[contains(@href,"threads/")]',
				unique=True
		).extract_links(response)

		if thread_links:
			for thread in thread_links:
				yield Request(url=thread.url, callback=self.post_scrape)
			next_page = response.xpath('//nav/a[contains(text(),"Next")]/@href').extract_first()
			if next_page is not None:
				next_page = 'https://prvtzone.ws/' + next_page
				yield Request(url=next_page, callback=self.forum_scrape)

	def post_scrape(self, response):
		if '' in response.url and response.url not in self.visited_threads:
			self.visited_threads.append(response.url)
			self.item['thread_url'] = response.url
			posts = response.xpath('//ol[@id="messageList"]/li')
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			if not self.thread_done:
				try:
					post_info = posts[0].xpath('.//div[contains(@class,"messageInfo")]')
					author_info = posts[0].xpath('.//div[contains(@class,"messageUserInfo")]')

					# Actor Info
					self.item['author_name'] = author_info.xpath('.//a[@class="username"]/text()').extract_first()
					author_joined = author_info.xpath('.//dl[@class="pairsJustified"][1]/dd/text()').extract_first(default='')
					try:
						self.item['author_joined_date'] = dp.parse(author_joined, languages=['en']).isoformat()
					except (AttributeError, TypeError):
						self.item['author_joined_date'] = ''

					author_membership = self.get_membership(author_info.xpath(
						'.//em[starts-with(@class,"userBanner ")]/strong/text()').extract_first(default='0').strip())

					# Thread_Details
					try:
						timestamp = post_info.xpath('.//*[@class="DateTime"]/@title').extract_first()
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
					except (AttributeError, TypeError):
						timestamp = self.item['thread_timestamp'] = ''

					total_post_content = post_info.xpath('.//div[@class="messageContent"]/article/blockquote/*').extract()
					if total_post_content:
						(self.item['thread_media_links'], self.item['thread_general_links']) = self.extract_links(
							post_info, ' '.join(total_post_content))

						total_post_content = post_info.xpath(
							'.//div[@class="messageContent"]/article/blockquote/.//text()').extract()

						if total_post_content:
							total_post_content = self.replace_patterns(total_post_content, timestamp)
					else:
						total_post_content = ''

					self.item['thread_content'] = total_post_content
					self.item['author_membership_level'] = author_membership
					self.item['author_age'] = ''
					self.item['author_location'] = ''
					self.item['author_posts_count'] = ''
					self.item['scraped_date'] = dt.now().isoformat()
					self.thread_done = True
					self.replies_dict = []
					self.reply_scrape(posts[1:])
				except Exception as e:
					logging.exception(e)
			next_page = response.xpath('//nav/a[contains(text(),"Next")]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False
			if next_page is not None:
				try:
					self.reply_scrape(requests.get(next_page, cookies=self.cookie, proxies={'http': self.proxy}))
				except Exception as e:
					logging.error('Next Page Parsing Error', e)

			self.all_done = True if self.thread_done else False
			if self.all_done:
				self.item['thread_replies'] = self.replies_dict
				self.item['thread_reply_no'] = len(self.replies_dict)
				self.replies_dict = []
				self.thread_done = False
				yield self.item

	def reply_scrape(self, response):

		if type(response) is scrapy.selector.SelectorList:
			total_replies = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url not in self.visited_threads:
				total_replies = response.xpath('//ol[@id="messageList"]/li')
			else:
				return
		elif self.all_done:
			return
		else:
			return

		for reply in total_replies:
			try:
				reply_author = reply.xpath('.//a[@class="username"]/text()').extract_first(default='')

				reply_timestamp = reply.xpath('.//span[@class="DateTime"]/text()').extract_first()
				if reply_timestamp is None:
					reply_timestamp = reply.xpath('.//abbr[@class="DateTime"]/@data-datestring').extract_first()
				try:
					self.reply_item['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					reply_timestamp = self.reply_item['reply_timestamp'] = ''

				reply_author_membership = self.get_membership(reply.xpath(
					'.//em[starts-with(@class,"userBanner ")]/strong/text()').extract_first(default='0').strip())

				reply_content = reply.xpath('.//div[@class="messageContent"]/article/blockquote/*').extract()
				(self.reply_item['reply_media_links'], self.reply_item['reply_general_links']) = \
					self.extract_links(reply, reply_content)

				if 'class="bbCodeBlock bbCodeQuote"' in reply_content:
					reply_content = reply.xpath('.//div[@class="messageContent"]/article/blockquote/text()').extract()
				else:
					reply_content = reply.xpath(
						'.//div[@class="messageContent"]/article/blockquote/.//text()').extract()

				self.reply_item['reply_author'] = reply_author
				self.reply_item['reply_author_membership'] = reply_author_membership
				self.reply_item['reply_content'] = self.replace_patterns(
						reply_content, reply_timestamp).replace('  ', ' ')
			except Exception as e:
				logging.exception(e)
			finally:
				self.replies_dict.append(dict(self.reply_item))

		if type(response) is scrapy.selector.SelectorList:
			return
		next_page = response.xpath('//nav/a[contains(text(),"Next")]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False
		if next_page is not None:
			try:
				self.reply_scrape(requests.get(next_page, cookies=self.cookie, proxies={'http': self.proxy}))
			except Exception as e:
				logging.error('Next Page Parsing Error', e)

	def get_membership(self, membership_status):
		if membership_status is not '0':
			for i in range(len(self.membership_list)):
				if membership_status in self.membership_list[i]:
					membership_status = str(i)
					break
		return membership_status

	def replace_patterns(self, content_data, check_time):
		final_content = ''
		try:

			content_data = self.val_transform(content_data)
			final_content = ' '.join(content_data).replace('  ', '')
			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except Exception as e:
				check_content = None
				logging.exception(e)
			final_content = check_content if check_content is not None else final_content

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
				"\n": "",
				"\t": "",
				"\r": "",
				'Hide Content': '',
				'Show Content': '',
				'Hidden Content:': '',
				check_time: ''
			}
			final_content = reduce(lambda a, kv: a.replace(*kv), replaceable_patterns.items(), final_content)
			final_content = final_content.replace('  ', '')
		except Exception as e:
			logging.exception(e)
		finally:
			return final_content

	def val_transform(self, content_data):
		for i in range(0, len(content_data)):
			val = content_data[i].strip().replace('\n', '').replace('\r', '').replace('\t', '')
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

	def extract_links(self, response, data):
		media_links = general_links = []
		if '<img' in data:
			media_links = response.xpath(
					'.//div[@class="messageContent"]/article/blockquote/..//img/@src').extract()
		if '<iframe' in data:
			media_links += response.xpath(
					'.//div[@class="messageContent"]/article/blockquote/..//iframe/@src').extract()
		if '<a' in data:
			general_links = response.xpath(
					'.//div[@class="messageContent"]/article/blockquote/..//a/@href').extract()
		return ';'.join(media_links), ';'.join(general_links)
