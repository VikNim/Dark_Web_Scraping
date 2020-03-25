# -*- coding: utf-8 -*-
import scrapy
import logging
# import random
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from datetime import datetime as dt
from scrapy.selector import Selector
from scrapy.spiders import Rule, CrawlSpider
from ..items import BitshackingItem, ReplyItems
from scrapy.linkextractors import LinkExtractor


class BitshackingforumSpider(CrawlSpider):
	name = 'bitsHackingForum'
	replies = ReplyItems()
	item = BitshackingItem()
	proxy = 'http://127.0.0.1:8081'
	cookie = dict()
	visited_threads = replies_data = list()
	all_done = thread_done = False
	allowed_domains = ['bitshacking.com']
	start_urls = [
		'http://www.bitshacking.com/forum/',
		# 'http://www.bitshacking.com/forum/im-infected-help/',
	]
	rules = (
		Rule(
			LinkExtractor(
				restrict_xpaths='//td[@class="alt1Active"]//a',
				allow_domains=allowed_domains,
				unique=True
			)
		),
		Rule(
			LinkExtractor(
				restrict_xpaths='//a[@rel="next"]',
				allow_domains=allowed_domains,
				unique=True
			),
			follow=True
		),
		Rule(
			LinkExtractor(
				restrict_xpaths='//a[starts-with(@id,"thread_title_")]',
				allow_domains=allowed_domains,
				unique=True
			),
			callback='post_scrape'
		),
	)

	def post_scrape(self, response):
		if '.html' in response.url and 'forum/' in response.url and response.url not in self.visited_threads:
			self.visited_threads.append(response.url)
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			record = Selector(response)
			total_posts = record.xpath('///table[starts-with(@id,"post")]')

			if not self.thread_done:
				try:
					post_content = post_count = author_age = join_date = location = ''
					# Author Info
					author_info = total_posts[0].xpath('.//tr[2]/td/table/tr')
					post_info = total_posts[0].xpath('.//tr[3]')

					self.item['author_name'] = \
						author_info.xpath('.//a[@class="bigusername"]/span/text()').extract_first(default='')

					membership_level = author_info.xpath('.//td[2]/div[2]/text()').extract_first()
					if membership_level is None or 'New' in membership_level:
						membership_level = 0
					else:
						membership_level = len(author_info.xpath('.//td[2]/div[last()]/..//img').extract())
						membership_level = '4' if membership_level > 4 else str(membership_level)

					author_stats = author_info.xpath('.//td[4]/.//text()').extract()
					author_stats = ' '.join([a.strip() for a in author_stats if a is not None])

					if 'Posts:' in author_stats:
						post_count = author_stats.split('Posts:')[1].split('  ')[0].strip()
					if 'Location:' in author_stats:
						location = author_stats.split('Location:')[1].split('  ')[0].strip()
					if 'Age:' in author_stats:
						author_age = author_stats.split('Age:')[1].split('  ')[0].strip()
					if 'Join Date:' in author_stats:
						join_date = author_stats.split('Join Date:')[1].split('  ')[0].strip()
						try:
							join_date = dp.parse(join_date, languages=['en']).isoformat()
						except (AttributeError, TypeError):
							join_date = ''

					# Post Info
					timestamp = ' '.join(
						total_posts[0].xpath('.//td[contains(@id,"td_post_")]/div[2]/text()').extract()).strip()
					try:
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
					except (AttributeError, TypeError):
						self.item['thread_timestamp'] = ''

					try:
						(self.item['thread_media_links'], self.item['thread_general_links']) = \
							self.extract_links(post_info, ''.join(
							post_info.xpath('.//td/div[contains(@id,"post_message")]/*').extract()))

						post_content = post_info.xpath(
							'.//td/div[contains(@id,"post_message")]/..//text()').extract()
						if post_content:
							post_content = self.replace_patterns(post_content, timestamp)
					except Exception as e:
						logging.exception(e)

					self.item['thread_url'] = response.url
					self.item['thread_group'] = record.xpath('//h2[@class="myh2"]/text()').extract_first()
					self.item['thread_content'] = post_content
					self.item['author_post_count'] = post_count
					self.item['author_age'] = author_age
					self.item['author_membership_level'] = membership_level
					self.item['author_joined_date'] = join_date
					self.item['author_location'] = location
					self.item['scraped_date'] = dt.now().isoformat()

					self.thread_done = True
					self.replies_data = []
					self.reply_scrape(total_posts[1:])
				except Exception as e:
					logging.exception(e)

			next_page = record.xpath('//a[@rel="next"]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done is True else False
			if next_page is not None:
				try:
					self.reply_scrape(requests.get(url=next_page, cookies=self.cookie, proxies={'http': self.proxy}))
				except Exception as e:
					logging.error('Next Page Exception -> Exit', e)
			self.all_done = True if self.thread_done else False

			if self.all_done:
				self.item['thread_replies'] = self.replies_data
				self.item['thread_reply_no'] = str(len(self.replies_data))
				self.thread_done = False
				self.replies_data = []
				yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.unified.SelectorList:
			record = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url in self.visited_threads:
				return
			else:
				record = Selector(response).xpath('///table[starts-with(@id,"post")]')
		elif self.all_done:
			return
		else:
			return

		for reply in record:
			try:
				author_info = reply.xpath('.//tr[2]/td/table/tr')

				reply_author = author_info.xpath('.//a[@class="bigusername"]/span/text()').extract_first(default='')

				reply_author_membership = ''.join(author_info.xpath('.//td[2]/div[2]/.//text()').extract())
				if reply_author_membership is None or 'New' in reply_author_membership:
					reply_author_membership = '0'
				else:
					reply_author_membership = len(author_info.xpath('.//td[2]/div[last()]/..//img').extract())
					reply_author_membership = '4' if reply_author_membership > 4 else str(reply_author_membership)

				reply_content = ' '.join(reply.xpath('.//div[starts-with(@id,"post_message_")]/*').extract())
				(self.replies['reply_media_links'], self.replies['reply_general_links']) = \
					self.extract_links(reply, reply_content)

				if 'Quote:' in reply_content or 'Originally Posted by' in reply_content:
					reply_content = reply.xpath('.//div[starts-with(@id,"post_message_")]/.//text()').extract()
				else:
					reply_content = reply.xpath('.//div[starts-with(@id,"post_message_")]/..//text()').extract()

				try:
					reply_timestamp = ''.join(reply.xpath(
						'.//td[contains(@id,"td_post_")]/div[2]/text()').extract()).strip()
					self.replies['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					reply_timestamp = self.replies['reply_timestamp'] = ''

				self.replies['reply_author'] = reply_author
				self.replies['reply_content'] = self.replace_patterns(reply_content, reply_timestamp)
				self.replies['reply_author_membership'] = reply_author_membership
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
				'.//td/div[contains(@id,"post_message")]/..//img/@src').extract()
		if '<iframe ' in data:
			media_links += response.xpath(
				'.//td/div[contains(@id,"post_message")]/..//iframe/@src').extract()
		if '<a ' in data:
			general_links += response.xpath(
				'.//td/div[contains(@id,"post_message")]/..//a/@href').extract()

		for m in range(len(media_links)):
			if 'newreply.php' in media_links[m]:
				media_links[m] = ' '
			elif 'register.php' in media_links[m]:
				media_links[m] = ' '
		for g in range(len(general_links)):
			if 'newreply.php' in general_links[g]:
				general_links[g] = ' '
			elif 'register.php' in general_links[g]:
				general_links[g] = ' '

		media_links = ';'.join(media_links).replace(' ', '').strip()
		general_links = ';'.join(general_links).replace(' ', '').strip()

		return (';'.join(media_links), ';'.join(general_links)) \
			if (media_links or general_links) and not (media_links is ';' and general_links is ';') else ('', '')

	def replace_patterns(self, content_data, timestamp):
		check_content = ''
		timestamp = ' ' if timestamp is None else timestamp
		try:
			content_data = self.val_transform(content_data)
			final_content = ' '.join(content_data)
			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except Exception as e:
				check_content = None
				logging.exception('Error while uni-decoding', e)
			final_content = check_content if check_content is not None else final_content
			check_content = final_content = final_content.replace("  ", "").strip()
			replaceable_patterns = {
				' .': '.',
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
				self.item['author_name']: ''
			}

			final_content = reduce(lambda a, kv: a.replace(*kv), replaceable_patterns.items(), final_content)
			check_content = final_content.replace('  ', '')

		except Exception as e:
			logging.exception('Error while replacing patterns using reduce():', e)
		finally:
			return check_content

	def val_transform(self, content_data):
		for i in range(len(content_data)):
			val = content_data[i].strip() if content_data[i] is not None else ''
			try:
				if val.startswith('(This post was modified') or val.startswith('This post was modified'):
					val = ''
				if val.startswith('Ban Reason'):
					val = ''
				if val.startswith('You must '):
					val = ''
				if val.__contains__('You can not'):
					val = ''
				if val.__contains__('  You can register a new account'):
					val = ''
				if val.startswith('vbrep') or 'vbrep' in val:
					val = val.replace('vbrep', '')
				if val.startswith('_register('):
					val = ''
			except Exception as e:
				logging.exception('Error in val_transform()', e)
				continue
			finally:
				content_data[i] = val
		return content_data
