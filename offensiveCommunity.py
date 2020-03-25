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
from ..items import OffensivecommunityItem, ReplyItems


class BaseSpider(CrawlSpider):
	name = 'base'
	cookie = dict()
	proxy = 'http://127.0.0.1:8118'
	all_done = thread_done = False
	reply_items = ReplyItems()
	item = OffensivecommunityItem()
	visited_threads = replies_data = list()
	allowed_domains = ['offensivecommunity.net']
	start_urls = [
		'http://offensivecommunity.net/index.php'
	]
	rules = (
		Rule(
				LinkExtractor(
						restrict_xpaths='///td[@class="trow3 teerow"][2]/strong/a',
						allow_domains=allowed_domains,
						unique=True
				)
		),
		Rule(
				LinkExtractor(
						allow_domains=allowed_domains,
						restrict_xpaths='//span[starts-with(@id,"tid_")]/a',
						unique=True
				),
				callback='post_scrape'
		),
	)

	# def parse(self, response):
	# 	forum_links = response.xpath('///td[@class="trow3 teerow"][2]/strong/a/@href').extract()
	# 	for link in forum_links:
	# 		if link is None:
	# 			continue
	# 		else:
	# 			forum_link = 'http://offensivecommunity.net/' + link
	# 			yield Request(url=forum_link, callback=self.forums_scrape)

	def post_scrape(self, response):

		if 'showthread' in response.url and response.url not in self.visited_threads:
			self.visited_threads.append(response.url)
			record = Selector(response)
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			try:
				if 'The specified thread does not exist.' in response.text:
					return

				if not self.thread_done:
					# Taking whole HTML of thread data element in one variable to scrape in detail
					posts = record.xpath('//div[@id="posts"]/div')
					post_info = posts[0].xpath('.//div[@class="post_content"]')
					author_info = posts[0].xpath('.//div[@class="post_author"]')
					self.item['thread_url'] = response.url
					# Author Info
					author_name = author_info.xpath(
						'.//div[@class="author_information"]/..//span[@class="largetext"]/a/..//text()'
					).extract_first(default='').strip()

					membership_level = len(author_info.xpath(
						'.//div[@class="author_information"]/span[@class="smalltext"]/..//img').extract())
					membership_level = 4 if membership_level > 4 else membership_level

					post_count = author_info.xpath(
						'.//div[@class="author_statistics"]/text()').extract_first().split('Posts: ')[1]

					author_stats = ' '.join(author_info.xpath(
							'.//div[@class="author_statistics"]/text()').extract()).strip()
					if 'Joined:' in author_stats:
						join_date = author_stats.split('Joined:')[1].split('\n')[0]
						try:
							join_date = dp.parse(join_date, languages=['en']).isoformat()
						except (AttributeError, TypeError):
							join_date = ''
					else:
						join_date = ''

					if 'Location:' in author_stats:
						location = author_stats.split('Location:')[1].split('\n')[0]
					else:
						location = ''

					# Post Data
					timestamp = post_info.xpath(
						'.//div[@class="post_head"]/span[@class="post_date"]/text()').extract_first().strip()
					try:
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
					except (AttributeError, TypeError):
						timestamp = self.item['thread_timestamp'] = ''

					total_post_content = post_info.xpath('.//div[starts-with(@class,"post_body")]/*').extract()
					if total_post_content:
						(self.item['thread_media_links'], self.item['thread_general_links']) =\
							self.extract_links(post_info, ''.join(total_post_content))

						post_content = post_info.xpath(
								'.//div[starts-with(@class,"post_body")]/..//text()').extract()

						if post_content:
							post_content = self.replace_patterns(post_content, timestamp)

						self.item['thread_content'] = post_content
						self.item['author_posts_count'] = post_count
						self.item['author_name'] = author_name
						self.item['author_age'] = ''
						self.item['author_membership_level'] = membership_level
						self.item['author_joined_date'] = join_date
						self.item['author_location'] = location
						self.item['scraped_date'] = dt.now().isoformat()
						self.thread_done = True
						self.replies_data = []
						self.reply_scrape(posts[1:])

			except Exception as e:
				logging.exception(e)
			next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False

			if next_page is not None:
				try:
					self.reply_scrape(
							requests.get(url=response.url, proxies={'http': self.proxy}, cookies=self.cookie)
					)
				except:
					pass

			self.all_done = True if self.thread_done else False

			if self.all_done:
				self.item["thread_replies"] = self.replies_data
				self.item['thread_reply_no'] = len(self.replies_data)
				self.replies_data = []
				self.thread_done = False
				yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.SelectorList:
			total_replies = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url not in self.visited_threads:
				total_replies = response.xpath('//div[@id="posts"]/div')
			else:
				return
		elif self.all_done:
			return
		else:
			return
		for reply in total_replies:
			try:
				reply_content = ' '.join(reply.xpath('.//div[starts-with(@class,"post_body")]/*').extract())
				(self.reply_items['reply_media_links'], self.reply_items['reply_general_links']) = \
					self.extract_links(reply, reply_content)

				if '<blockquote' in reply_content:
					reply_content = reply.xpath('.//div[starts-with(@class,"post_body")]/text()').extract()
				else:
					reply_content = reply.xpath('.//div[starts-with(@class,"post_body")]/..//text()').extract()

				reply_author_membership = len(reply.xpath(
					'.//div[@class="author_information"]/span[@class="smalltext"]/img'))
				reply_author_membership = 4 if reply_author_membership > 4 else reply_author_membership

				reply_timestamp = reply.xpath('.//span[@class="post_date"]/text()').extract_first()
				try:
					self.reply_items['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					reply_timestamp = self.reply_items['reply_timestamp'] = ''

				self.reply_items['reply_author'] = reply.xpath(
					'.//span[@class="largetext"]/a/text()').extract_first(default='')

				self.reply_items['reply_content'] = self.replace_patterns(reply_content, reply_timestamp)
				self.reply_items['reply_author_membership'] = reply_author_membership
			except Exception as e:
				logging.exception(e)
			finally:
				self.replies_data.append(dict(self.reply_items))

		if type(response) is scrapy.selector.SelectorList:
			return
		next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False

		if next_page is not None:
			try:
				self.reply_scrape(requests.get(next_page, cookies=self.cookie, proxies={'http': self.proxy}))
			except:
				pass

	def extract_links(self, response, data):
		media_links = general_links = []
		try:
			if '<img ' in data:
				media_links = response.xpath('.//div[contains(@class,"post_body")]/..//img/@src').extract()
			if '<iframe ' in data:
				media_links += response.xpath('.//div[contains(@class,"post_body")]/..//iframe/@src').extract()
			if '<a ' in data:
				general_links = response.xpath('.//div[contains(@class,"post_body")]/..//a/@href').extract()
		except Exception as e:
			logging.exception(e)
		finally:
			return ';'.join(media_links), ';'.join(general_links)

	def replace_patterns(self, content_data, check_time):
		final_content = ''
		try:
			final_content = ' '.join(self.val_transform(content_data))
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
		for i in range(len(content_data)):
			val = content_data[i].strip().replace('\n', '').replace('\r', '').replace('\t', '')
			try:
				if val is None or val is '' or val is ' ':
					val = ''
				elif val.startswith('(This post was modified') or val.startswith('This post was modified'):
					val = ''
				elif val.startswith('#'):
					if len(val) > 5:
						val.replace('#', '')
					else:
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
