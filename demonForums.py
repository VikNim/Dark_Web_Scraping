# -*- coding: utf-8 -*-
import re
import scrapy
import cfscrape
import logging
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from datetime import datetime as dt
from scrapy.http import HtmlResponse
from scrapy.spider import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor
from ..items import DemonforumsItem, ReplyItems


class BaseSpider(CrawlSpider):
	name = 'base'
	proxy = 'http://127.0.0.1:8118'
	cookie = dict()
	item = DemonforumsItem()
	reply_items = ReplyItems()
	all_done = thread_done = False
	scraper = cfscrape.create_scraper()
	visited_threads = replies_data = list()
	allowed_domains = ['demonforums.net']
	start_urls = [
		# 'https://demonforums.net/index.php',
		'https://demonforums.net/forumdisplay.php?fid=393'
	]
	membership_lists = [
		['Rookie User', 'Novice Member', 'Junior Member'],
		['Challenger', 'One Hit Wonder', 'Lurker'],
		['Gold Member', 'Emerald', 'Diamond Member', 'Emerald Member', 'VIP Member', 'Admin', 'Head Staff']
	]
	rules = (
		Rule(
			LinkExtractor(
					allow_domains=allowed_domains,
					restrict_xpaths='///span[contains(@id,"tid_")]/a',
					unique=True
			),
			callback='post_scrape'
		),
	)

	# def parse(self, response):
	# 	forum_links = response.xpath('//div[contains(@id,"tabmenu")]/..//td[2]/strong/a/@href').extract()
	# 	if forum_links:
	# 		for forum_link in forum_links:
	# 			if forum_link:
	# 				forum_link = 'https://demonforums.net/' + forum_link
	# 				yield Request(url=forum_link, callback=self.forum_scrape)

	def post_scrape(self, response):
		if ('showthread' in response.url or 'Thread-' in response.url) and response.url not in self.visited_threads:

			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			self.visited_threads.append(response.url)
			self.item['thread_group'] = response.xpath('//ul[@class="navigation"]/li[2]//text()').extract_first()
			posts = response.xpath('//div[@id="posts"]/div[starts-with(@id,"pid")]')
			if not self.thread_done:
				try:
					author_info = posts[0].xpath('.//div[contains(@class," postbit-user ")]')
					post_info = posts[0].xpath('.//div[contains(@class," postbit-message ")]')

					# Author Info
					self.item['author_name'] = author_info.xpath(
						'.//div[@class="postbit-username"]/a//text()').extract_first(default='')

					temp = ''.join(author_info.xpath('.//div[contains(@class,"postbit-usertitle")]/'
													'.//text()').extract()).strip()
					membership_status = author_info.xpath('.//div[contains(@class,"postbit-usertitle")]/text()')\
						.extract_first(default=temp)
					membership_status = 0 if membership_status is None else self.get_membership(membership_status)

					# data = ' '.join(author_info.xpath(
					# 	'.//div[contains(@class,"postbit-stats ")]//text()').extract()).replace('\n', '').split('Likes')[0]
					post_count = author_info.xpath('.//div[@class="overflow"][2]/div[@class="right"]/text()').extract_first()

					join_date = author_info.xpath('.//div[@class="overflow"][3]/div[@class="right"]/text()').extract_first()
					try:
						join_date = dp.parse(join_date).isoformat()
					except (AttributeError, TypeError):
						join_date = ''
					try:
						age = author_info.xpath('.//div[contains(@class,"postbit-tag")]/text()').extract_first()
						author_age = int(re.findall('[0-9]', age)[0]) if age is not None else 0

					except IndexError:
						author_age = 0

					# Post Info Extraction
					timestamp = ' '.join(post_info.xpath(
						'.//div[contains(@class,"postbit-message-time ")]/text()').extract()).strip()
					try:
						timestamp = timestamp.split('Posted: ')[1].strip() if timestamp is not None else ''
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en']).isoformat()
					except (AttributeError, TypeError, IndexError):
						timestamp = self.item['thread_timestamp'] = ''

					total_post_content = post_info.xpath('.//div[@class="post_body"]').extract()
					media_links = general_links = []
					if total_post_content:
						(media_links, general_links) = self.extract_links(post_info, ''.join(total_post_content))
						total_post_content = post_info.xpath('.//div[@class="post_body"]/..//text()').extract()
						if total_post_content:
							(total_post_content, media_links, general_links) = self.replace_patterns(
								total_post_content,
								media_links=media_links,
								general_links=general_links,
								check_time=timestamp
							)
					else:
						total_post_content = ''

					self.item['thread_content'] = total_post_content
					self.item['thread_media_links'] = media_links
					self.item['thread_general_links'] = general_links
					self.item['thread_url'] = response.url

					self.item['author_joined_date'] = join_date
					self.item['author_membership_level'] = membership_status
					self.item['author_age'] = author_age
					self.item['author_posts_count'] = post_count
					self.item['author_location'] = ''
					self.item['scraped_date'] = dt.now().isoformat()

					self.thread_done = True
					self.replies_data = []
					self.reply_scrape(posts[1:])
				except Exception as e:
					logging.exception(e)
			next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False
			if next_page is not None:
				next_page = 'https://demonforums.net/' + next_page
				try:
					(tk, ua) = self.scraper.get_tokens(response.url, proxies={'http': self.proxy})

					self.reply_scrape(
						HtmlResponse(
							url=next_page,
							body=requests.get(
								url=next_page,
								cookies=self.cookie,
								proxies={'http': self.proxy},
								headers={'User Agent': ua}).content
						)
					)
				except Exception as e:
					logging.error('Next Page Parsing Error:', e)

			self.all_done = True if self.thread_done else False
			if self.all_done:
				self.item['thread_replies'] = self.replies_data
				self.item['thread_reply_no'] = len(self.replies_data)
				self.thread_done = False
				self.replies_data = []
				yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.SelectorList:
			total_posts = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url not in self.visited_threads:
				total_posts = response.xpath('//div[@id="posts"]/div')
			else:
				return
		elif self.all_done:
			return
		else:
			self.thread_done = True
			return
		if total_posts:
			for reply in total_posts:
				try:
					reply_author = reply.xpath(
							'.//div[@class="postbit-username"]/..//text()').extract_first(default='')

					reply_author_membership = reply.xpath(
							'.//div[contains(@class,"postbit-usertitle")]/text()').extract_first(default='')
					reply_author_membership = 0 if reply_author_membership is None \
						else self.get_membership(reply_author_membership)

					try:
						reply_timestamp = ''.join(reply.xpath(
								'.//div[contains(@class,"postbit-message-time")]/text()'
						).extract()).strip().replace('Posted: ', '')

						self.reply_items['reply_timestamp'] = dp.parse(
								reply_timestamp, languages=['en']).isoformat()
					except (AttributeError, TypeError):
						reply_timestamp = self.reply_items['reply_timestamp'] = ''

					reply_content = ' '.join(reply.xpath('.//div[@class="post_body"]/*').extract())

					if '<blockquote ' in reply_content:
						reply_message = reply.xpath('.//div[@class="post_body"]/text()').extract()
					else:
						reply_message = reply.xpath('.//div[@class="post_body"]/.//text()').extract()

					(reply_media_links, reply_general_links) = self.extract_links(reply, reply_content)
					(reply_message, reply_media_links, reply_general_links) = self.replace_patterns(
						reply_message,
						media_links=reply_media_links,
						general_links=reply_general_links,
						check_time=reply_timestamp
					)
					self.reply_items['reply_content'] = reply_message
					self.reply_items['reply_media_links'] = reply_media_links
					self.reply_items['reply_general_links'] = reply_general_links
					self.reply_items['reply_author'] = reply_author
					self.reply_items['reply_author_membership'] = reply_author_membership
				except Exception as e:
					logging.exception(e)
				finally:
					self.replies_data.append(dict(self.reply_items))
		else:
			return

		if type(response) is scrapy.selector.SelectorList:
			return
		next_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False
		if self.all_done:
			return
		if next_page is not None:
			next_page = 'https://demonforums.net/' + next_page
			(tk, ua) = self.scraper.get_tokens(response.url, proxies={'http': self.proxy})

			self.reply_scrape(
				HtmlResponse(
					url=next_page,
					body=requests.get(
						url=next_page,
						cookies=self.cookie,
						proxies={'http': self.proxy},
						headers={'User Agent': ua}).content
				)
			)

	def extract_links(self, response, data):
		media_links = general_links = []
		if '<img' in data:
			media_links = response.xpath('.//div[@class="post_body"]/..//img/@src').extract()
		if '<iframe' in data:
			media_links += response.xpath('.//div[@class="post_body"]/..//iframe/@src').extract()
		if '<a' in data:
			general_links = response.xpath('.//div[@class="post_body"]/..//a/@href').extract()

		try:  # Removing unwanted links
			for m in range(len(media_links)):
				if 'newreply.php' in media_links[m] \
						or 'register.php' in media_links[m] \
						or 'search.php' in media_links[m] \
						or 'upgrade.php' in media_links[m] \
						or '/' not in media_links[m]:
					media_links[m] = ''

			for m in range(len(general_links)):
				if 'newreply.php' in general_links[m] \
						or 'register.php' in general_links[m] \
						or 'search.php' in general_links[m] \
						or 'upgrade.php' in general_links[m] \
						or '/' not in general_links[m]:
					general_links[m] = ''

		except (TypeError, Exception) as e:
			logging.exception('Exception while parsing links..!', e)

		return (';'.join(media_links), ';'.join(general_links)) \
			if (media_links or general_links) and not (media_links is ';' and general_links is ';') else ('', '')

	def replace_patterns(self, content_data, media_links, general_links, check_time):
		check_content = ''
		try:
			final_content = ' '.join(self.val_transform(content_data))
			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except Exception as e:
				check_content = final_content
				logging.exception('Exception at Decoding :' + str(e))

			final_content = check_content

			replaceable_patterns = {
				' .': '',
				'--': '',
				'++': '',
				'__': '',
				'*': '',
				'#': '',
				'&&': '',
				'&': 'and',
				'\\': '',
				'!!': '',
				'..': '',
				',,'
				'.)': '',
				'(:-': '',
				'-:)': '',
				':)': '',
				'(:': '',
				'Hide Content': '',
				'Show Content': '',
				'Hidden Content:': '',
				'Content Hidden': '',
				'OP': '',
				"\n": "",
				"\t": "",
				"\r": "",
				check_time: ''
			}

			final_content = reduce(lambda a, kv: a.replace(*kv), replaceable_patterns.items(), final_content)
			check_content = final_content.replace('  ', '')

		except Exception as e:
			logging.exception(e)
		finally:
			return check_content, ';'.join(media_links), ';'.join(general_links)

	def val_transform(self, content_data):
		for i in range(len(content_data)):
			val = content_data[i].strip()
			try:
				if val is None or val is "" or val is " ":
					val = ''
				elif val.startswith('Posted') or val.startswith('OP '):
					val = ''
				elif val.startswith('#'):
					if len(val) > 5:
						val.replace('#', '')
					else:
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

	def get_membership(self, membership_status):
		if membership_status in self.membership_lists[0]:
			membership_status = 1
		elif membership_status in self.membership_lists[1]:
			membership_status = 3
		elif membership_status in self.membership_lists[2]:
			membership_status = 5
		else:
			membership_status = 0
		return membership_status
