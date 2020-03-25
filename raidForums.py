# -*- coding: utf-8 -*-
import re
import scrapy
import logging
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from datetime import datetime as dt
from scrapy.selector import Selector
from scrapy.spider import Rule, CrawlSpider
from ..items import RaidforumsItem, ReplyItems
from scrapy.linkextractors import LinkExtractor


class RaidforumsSpider(CrawlSpider):
	name = 'raidForums'
	proxy = 'http://127.0.0.1:8118'
	cookie = dict()
	item = RaidforumsItem()
	reply_items = ReplyItems()
	all_done = thread_done = False
	visited_threads = replies_dict = list()
	allowed_domains = ['raidforums.com']
	start_urls = [
		'https://raidforums.com/'
	]
	rules = (
		Rule(
			LinkExtractor(
				allow_domains=allowed_domains,
				restrict_xpaths='///td[contains(@class,"trow3 teerow")]/strong/a',
				unique=True
			)
		),
		Rule(
				LinkExtractor(
						allow_domains=allowed_domains,
						restrict_xpaths='//a[@class="pagination_next"]/@href',
						canonicalize=True
				)
		),
		Rule(
				LinkExtractor(
						allow_domains=allowed_domains,
						restrict_xpaths=('///td[contains(@class,"trow3 teerow")]/strong/a', '//a[class="mycode_url"]'),
						unique=True
				),
				callback='post_scrape'
		),
	)

	def post_scrape(self, response):
		if 'Thread-' in response.url and response.url not in self.visited_threads:
			self.visited_threads.append(response.url)

			record = Selector(response)  # Taking whole HTML of thread data element in one variable to scrape in detail
			posts = record.xpath('//div[@id="posts"]/div')
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			if not self.thread_done:
				info = posts[0]
				try:
					# Author Info
					author_name = info.xpath(
						'.//a[contains(@href,"https://raidforums.com/User-")]/span/text()').extract_first()
					if author_name is None:
						author_name = info.xpath(
							'.//a[contains(@href,"https://raidforums.com/User-")]/span/span/text()').extract_first()

						membership_level = info.xpath(
							'.//a[contains(@href,"https://raidforums.com/User-")]/span/span/@class'
						).extract_first(default='-').split('-')[0]
					else:
						membership_level = info.xpath(
							'.//a[contains(@href,"https://raidforums.com/User-")]/span/@class'
						).extract_first(default='-').split('-')[0]

					membership_level = self.get_membership(membership_level) if membership_level is not '' else 0

					auth_stats = info.xpath('.//div[@class="author_statistics"]/span/..//text()').extract()
					if auth_stats:
						try:
							auth_stats = '-'.join(
								[a.strip().replace('  ', '') for a in auth_stats if a is not '\n' or a is not None]
							)
							post_count = auth_stats.split('Posts:--')[1].split('--')[0]
							join_date = auth_stats.split('Joined:--')[1].split('--')[0]
							try:
								join_date = dp.parse(join_date, languages=['en']).isoformat()
							except (AttributeError, TypeError):
								join_date = ''
						except:
							post_count = join_date = ''
					else:
						post_count = join_date = ''

					# Post Data
					timestamp = info.xpath('.//span[@class="post_date"]/text()').extract_first()
					try:
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
					except (AttributeError, TypeError):
						self.item['thread_timestamp'] = ''
					total_post_content = info.xpath(
						'.//div[@class="post_content"]/div[contains(@class,"post_body")]/*').extract()
					if total_post_content:
						(self.item['thread_media_links'], self.item['thread_general_links']) \
							= self.extract_links(info, ''.join(total_post_content))
						post_content = info.xpath(
							'.//div[@class="post_content"]/div[contains(@class,"post_body")]/.//text()').extract()
						if post_content:
							try:
								post_content.remove(self.item['author_name'])
							except Exception as e:
								logging.exception(e)
							finally:
								post_content = self.replace_patterns(post_content, timestamp)
						self.item['thread_content'] = post_content
					self.item['author_joined_date'] = join_date
					self.item['author_posts_count'] = post_count
					self.item['author_membership_level'] = membership_level
					self.item['author_name'] = author_name
					self.item['author_age'] = ''
					self.item['author_location'] = ''
					self.item['thread_url'] = response.url
					self.item['scraped_date'] = dt.now().isoformat()

					self.thread_done = True
					self.replies_dict = []
					self.reply_scrape(posts[1:])
				except Exception as e:
					logging.exception(e)
			next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False
			if next_page is not None:
				try:
					self.reply_scrape(
							requests.get(
									url='https://raidforums.com/' + next_page,
									cookies=self.cookie,
									proxies={'http': self.proxy}
							))
				except Exception as e:
					logging.error('Next Page Parsing Error: ', e)
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
				total_replies = response.xpath('//div[@id="posts"]/div')
			else:
				return
		elif self.all_done:
			return
		else:
			return

		for reply in total_replies:
			try:
				reply_author = reply.xpath(
						'.//a[contains(@href,"https://raidforums.com/User-")]/span/text()').extract_first()
				if reply_author is None:
					reply_author = reply.xpath(
							'.//a[contains(@href,"https://raidforums.com/User-")]/span/span/text()').extract_first()

					reply_author_membership = reply.xpath(
							'.//a[contains(@href,"https://raidforums.com/User-")]/span/span/@class'
					).extract_first(default='-').split('-')[0]
				else:
					reply_author_membership = reply.xpath(
							'.//a[contains(@href,"https://raidforums.com/User-")]/span/@class'
					).extract_first(default='-').split('-')[0]

				reply_author_membership = self.get_membership(reply_author_membership) \
					if reply_author_membership is not '' or reply_author_membership is not None else '0'

				reply_timestamp = reply.xpath('.//span[@class="post_date"]/text()').extract_first(default='')
				try:
					self.reply_items['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					self.reply_items['reply_timestamp'] = ''

				reply_content = ' '.join(reply.xpath('.//div[starts-with(@class,"post_body")]/*').extract())

				(self.reply_items['reply_media_links'], self.reply_items['reply_general_links']) \
					= self.extract_links(reply, reply_content)

				if '<blockquote' in reply_content:
					reply_content = reply.xpath('.//div[starts-with(@class,"post_body")]/text()').extract()
				else:
					reply_content = reply.xpath('.//div[starts-with(@class,"post_body")]/..//text()').extract()

				if reply_content:
					reply_content = \
						' '.join([r.strip() for r in reply_content if r is not '\n' or '\t']).replace('  ', ' ')
					reply_content = reply_content.replace(reply_timestamp, '') \
						if reply_timestamp in reply_content else reply_content
				else:
					reply_content = ''

				self.reply_items['reply_author'] = reply_author
				self.reply_items['reply_author_membership'] = reply_author_membership
				self.reply_items['reply_content'] = reply_content
			except Exception as e:
				logging.exception(e)
			finally:
				self.replies_dict.append(dict(self.reply_items))

		if type(response) is scrapy.selector.SelectorList:
			return
		next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False
		if next_page is not None:
			try:
				self.reply_scrape(
						requests.get(
								url='https://raidforums.com/' + next_page,
								cookies=self.cookie,
								proxies={'http': self.proxy}
						))
			except Exception as e:
				logging.error('Next Page Parsing Error: ', e)

	def get_membership(self, author_membership):
		reply_author_membership = 0
		if 'member' in author_membership:
			reply_author_membership = 1
		elif 'god' in author_membership or 'uber' in author_membership:
			reply_author_membership = 2
		elif 'owner' in author_membership:
			reply_author_membership = 4

		return reply_author_membership

	def replace_patterns(self, content_data, check_time):
		check_content = ''
		try:

			content_data = self.val_transform(content_data)

			check_content = final_content = ' '.join(content_data)
			replaceable_patterns = {
				"#1": "",
				"Unlock for 8 credits": "",
				"Hidden Content:": "",
				check_time: "",
				" .": "",
				"__": "",
				".)": "",
				"Hide Content": "",
				"Show Content": "",
				"(:-": "",
				"-:)": "",
				":)": "",
				"(:": "",
				"*": "",
				"#": "",
				"!!": "",
				"..": "",
				"\n": "",
				"\t": "",
				"\r": ""
			}
			final_content = reduce(lambda a, kv: a.replace(*kv), replaceable_patterns.items(), final_content)
			final_content = final_content.replace('  ', '')
			check_content = final_content
			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except Exception as e:
				logging.exception(e)
		except Exception as e:
			logging.exception(e)
		finally:
			return check_content

	def val_transform(self, content_data):
		reply_number = re.compile(r'[#]\d+')
		for i in range(len(content_data)):
			val = content_data[i].strip()
			if val is None or val is "" or val is " ":
				val = ''
			elif val.startswith('(This post was modified') or val.startswith('This post was modified'):
				val = ''
			elif val.startswith('Ban Reason'):
				val = ''
			elif val.startswith('You must '):
				val = ''

			if val.startswith('#'):
				try:
					remove_no = re.findall(reply_number, val)[0]
					val = val.replace(remove_no, '')
				except IndexError:
					val = val.replace('#', '')
			content_data[i] = val
		return content_data

	def extract_links(self, response, data):
		media_links = general_links = []
		if '<img ' in data:
			media_links = response.xpath('.//div[@class="post_content"]'
										'/div[contains(@class,"post_body")]/.//img/@src').extract()
		if '<iframe ' in data:
			media_links += response.xpath('.//div[@class="post_content"]'
										'/div[contains(@class,"post_body")]/.//iframe/@src').extract()
		if '<a ' in data:
			general_links = response.xpath('.//div[@class="post_content"]/'
										'div[contains(@class,"post_body")]/.//a/@href').extract()
		return ';'.join(media_links), ';'.join(general_links)
