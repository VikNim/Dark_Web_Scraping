# -*- coding: utf-8 -*-
import scrapy
import logging
import requests
import dateparser as dp
from functools import reduce
from unidecode import unidecode
from datetime import datetime as dt
from ..items import XdaforumsItem, ReplyItem
from scrapy.spiders import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings


class BaseSpider(CrawlSpider):
	name = 'base'
	proxy = 'http://127.0.0.1:8118'
	cookie = dict()
	item = XdaforumsItem()
	reply_item = ReplyItem()
	all_done = thread_done = False
	my_setting = get_project_settings()
	visited_threads = replies_data = list()
	allowed_domains = ['forum.xda-developers.com']
	start_urls = [
		'https://forum.xda-developers.com/all'
	]
	# 	'https://forum.xda-developers.com/general/general', 'https://forum.xda-developers.com/general/paid-software',
	# 	'https://forum.xda-developers.com/general/help',
	# 	'https://forum.xda-developers.com/general/device-reviews-and-information',
	# 	'https://forum.xda-developers.com/general/connected-car', 'https://forum.xda-developers.com/general/security',
	# 	'https://forum.xda-developers.com/general/rooting-roms', 'https://forum.xda-developers.com/general/accessories',
	# 	'https://forum.xda-developers.com/general/networking', 'https://forum.xda-developers.com/general/xda-tv',
	# 	'https://forum.xda-developers.com/general/xda-university', 'https://forum.xda-developers.com/general/off-topic',
	# 	'https://forum.xda-developers.com/u/tasker-tips-tricks', 'https://forum.xda-developers.com/u/best-apps',
	# 	'https://forum.xda-developers.com/u/reusing-devices', 'https://forum.xda-developers.com/u/app-design',
	# 	'https://forum.xda-developers.com/u/repair', 'https://forum.xda-developers.com/u/emulators',
	# 	'https://forum.xda-developers.com/hardware-hacking/chromebooks',
	# 	'https://forum.xda-developers.com/hardware-hacking/hardware',
	# 	'https://forum.xda-developers.com/hardware-hacking/nfc',
	# 	'https://forum.xda-developers.com/hardware-hacking/linkit-one',
	# 	'https://forum.xda-developers.com/hardware-hacking/pogoplug', 'https://forum.xda-developers.com/exodus-rom',
	# 	'https://forum.xda-developers.com/omni', 'https://forum.xda-developers.com/pac-rom',
	# 	'https://forum.xda-developers.com/bliss-roms', 'https://forum.xda-developers.com/ground-zero-roms',
	# 	'https://forum.xda-developers.com/paranoid-android', 'https://forum.xda-developers.com/slimroms',
	# 	'https://forum.xda-developers.com/aokp',
	# 	'https://forum.xda-developers.com/custom-roms/android-builders-collective',
	# 	'https://forum.xda-developers.com/tools', 'https://forum.xda-developers.com/coding',
	# 	'https://forum.xda-developers.com/marketing-analytics', 'https://forum.xda-developers.com/monetization',
	# 	'https://forum.xda-developers.com/lineage/general', 'https://forum.xda-developers.com/lineage/help',
	# 	'https://forum.xda-developers.com/market/buy', 'https://forum.xda-developers.com/market/deals',
	# 	'https://forum.xda-developers.com/emui-discussion/emui-tips--tricks',
	# 	'https://forum.xda-developers.com/emui-discussion/emui-requests',
	# 	'https://forum.xda-developers.com/emui-discussion/emui-bugs-and-issues',
	# 	'https://forum.xda-developers.com/project-ara/general',
	# 	'https://forum.xda-developers.com/project-ara/mdk',
	# 	'https://forum.xda-developers.com/themer/general',
	# 	'https://forum.xda-developers.com/themer/screenshots',
	# 	'https://forum.xda-developers.com/themer/zooper',
	# 	'https://forum.xda-developers.com/themer/zooper-templates',
	# 	'https://forum.xda-developers.com/other-special/encyclopedia',
	membership_list = ['', 'Member', 'Junior', 'Senior', 'Editor in Chief']
	rules = (
		Rule(
			LinkExtractor(
				deny=my_setting['BANNED_REGEX_PATTERN'],
				restrict_xpaths='//a[starts-with(@id,"thread_title_")]',
				allow_domains=allowed_domains,
				unique=True,
			),
			callback='post_scrape'
		),
		# Rule(
		# 	LinkExtractor(
		# 		deny=my_setting['BANNED_REGEX_PATTERN'],
		# 		restrict_xpaths='//a[starts-with(@id,"thread_title_")]',
		# 		allow_domains=allowed_domains,
		# 		unique=True
		# 	),
		# 	callback='post_scrape'
		# )
	)

	def post_scrape(self, response):
		try:
			self.proxy = response.meta['proxy']
			self.cookie = response.request.cookies
		except AttributeError:
			pass
		try:
			self.item['thread_url'] = response.url
			self.item['author_name'] = response.xpath(
				'//div[@id="thread-header-meta"]/..//a[contains(@class,"bigfusername ")]/text()'
			).extract_first(default='')

			self.item['author_posts_count'] = response.xpath(
				'//div[@id="thread-header-thanks"]/div[@class="user-posts"]/strong/text()').extract_first(default='')

			self.item['thread_group'] = response.xpath(
				'//div[@id="navbar_container"]/span[@class="navbar"][last()]/@title').extract_first(default='')

			thread_timestamp = response.xpath('//div[@id="thread-header-meta"]/text()').extract()[-2]
			try:
				thread_timestamp = thread_timestamp.split(' on ')[1].strip()
				self.item['thread_timestamp'] = dp.parse(thread_timestamp, languages=['en']).isoformat()
			except (AttributeError, TypeError, IndexError):
				thread_timestamp = self.item['thread_timestamp'] = ''

			thread_author_membership = response.xpath(
				'//div[@id="thread-header-meta"]/text()').extract()[-2]
			if thread_author_membership is not None or thread_author_membership is not '':
				try:
					thread_author_membership = thread_author_membership.split(' on ')[0].strip()
					self.item['author_membership_level'] = self.get_membership(thread_author_membership)
				except (IndexError, Exception):
					self.item['author_membership_level'] = '0'

			# Extracting Links
			thread_content = response.xpath(
				'//div[@id="posts"]/div[starts-with(@id,"edit")][1]/div[1]/..//*'
			).extract()
			post_xpath = response.xpath(
				'//div[@id="posts"]/div[starts-with(@id,"edit")][1]/..//div[starts-with(@id,"td_post_")]')
			self.item['thread_media_links'], self.item['thread_general_links'] = self.extract_links(
				post_xpath, ' '.join(thread_content))

			thread_content = response.xpath(
				'//div[@id="posts"]/div[starts-with(@id,"edit")][1]/..//div[starts-with(@id,"post_message_")]/..//text()'
			).extract()
			if thread_content:
				self.item['thread_content'] = self.replace_patterns(thread_content, thread_timestamp)

			self.item['author_joined_date'] = ''
			self.item['author_age'] = ''
			self.item['author_location'] = ''
			self.item['scraped_date'] = dt.now().isoformat()

			self.thread_done = True
			self.replies_data = []
			posts = response.xpath('//div[@id="posts"]/div[starts-with(@id,"edit")]')
			self.reply_scrape(posts[1:])
		except Exception as e:
			logging.exception(e)
		next_page = response.xpath('//a[@rel="next"]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False

		if next_page is not None:
			self.reply_scrape(
					requests.get(
							url='https://forum.xdaforums.com/' + next_page,
							cookies=self.cookie,
							proxies={'http': self.proxy}
					))
		if self.all_done is True:
			self.item['thread_replies'] = self.replies_data
			self.item['thread_reply_no'] = len(self.replies_data)
			self.replies_data = []
			self.thread_done = False
			yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.SelectorList:
			total_replies = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			if response.url not in self.visited_threads:
				total_replies = response.xpath('//div[@id="posts"]/div[starts-with(@id,"edit")]')
			else:
				return
		elif self.thread_done:
			return
		else:
			return

		for reply in total_replies:
			try:
				self.reply_item['reply_author'] = reply.xpath(
					'.//a[contains(@class,"bigfusername")]/text()').extract_first(default='')

				reply_author_membership = reply.xpath(
					'.//div[contains(@class,"pbuser user-title ")]/text()').extract_first(default='')

				reply_timestamp = reply.xpath('.//span[@class="time"]/text()').extract_first(default='')
				try:
					self.reply_item['reply_timestamp'] = reply_timestamp if reply_timestamp is not None else ''
					# dp.parse(reply_timestamp, languages=['en']).isoformat()
				except (AttributeError, TypeError):
					self.reply_item['reply_timestamp'] = ''

				content = ' '.join(reply.xpath('.//div[contains(@id,"post_message_")]/.//*').extract())
				if 'class="bbcode-quote-text"' in content:
					reply_content = reply.xpath('.//div[contains(@id,"post_message_")]/text()').extract()
				else:
					reply_content = reply.xpath('.//div[contains(@id,"post_message_")]/.//text()').extract()

				self.reply_item['reply_content'] = ' '.join(self.replace_patterns(
					reply_content,
					reply_timestamp
				)).strip()

				(self.reply_item['reply_media_links'], self.reply_item['reply_general_links']) = \
					self.extract_links(reply, content)
				self.reply_item['reply_author_membership'] = self.get_membership(reply_author_membership)

			except Exception as e:
				logging.error('Error while scraping replies..' + str(e))
			finally:
				self.replies_data.append(dict(self.reply_item))

		next_page = response.xpath('//a[@rel="next"]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False

		if next_page is not None:
			self.reply_scrape(
					requests.get(
							url='https://forum.xdaforums.com/' + next_page,
							cookies=self.cookie,
							proxies={'http': self.proxy}
					))

	def extract_links(self, response, data):
		media_links = general_links = []
		if '<img ' in data:
			media_links = response.xpath('.//img/@src').extract()
		if '<a ' in data:
			general_links = response.xpath('.//a/@href').extract()
		if '<iframe ' in data:
			media_links += response.xpath('.//iframe/@src').extract()
		return ';'.join(media_links), ';'.join(general_links)

	def replace_patterns(self, content_data, check_time):
		final_content = ''
		try:
			final_content = check_content = ''.join(self.val_transform(content_data))
			try:
				check_content = unidecode(str(bytes(final_content, encoding='utf-8'), encoding='utf-8'))
			except:
				check_content = final_content

			final_content = check_content
			replaceable_patterns = {
				check_time: "",
				" .": "",
				"__": "",
				".)": "",
				"(:-": "",
				"-:)": "",
				":)": "",
				"(:": "",
				"*": "",
				"#": "",
				"!!": "",
				"..": "",
				"\n": "",
				"\r": "",
				"\t": "",
			}
			final_content = reduce(lambda a, kv: a.replace(*kv), replaceable_patterns.items(), final_content)
			final_content = final_content.replace('  ', '')

		except Exception as e:
			logging.exception(e)
		finally:
			return final_content

	def val_transform(self, content_data):
		for i in range(len(content_data)):
			val = content_data[i].strip()
			try:
				val = content_data[i].strip()
				if val is None or val is "" or val is " ":
					val = ''
				elif val.startswith('(This post was modified') or val.startswith('This post was modified'):
					val = ''
				elif val.startswith('Ban Reason'):
					val = ''
				elif val.startswith('You must '):
					val = ''
			except (IndexError, Exception) as e:
				logging.error(e)
			finally:
				content_data[i] = val if val is not None else ''
		return content_data

	def get_membership(self, membership_level):
		for i in range(len(self.membership_list)):
			if self.membership_list[i] in membership_level:
				return i
		return 0
