# -*- coding: utf-8 -*-
import scrapy
import logging
import dateparser as dp
from unidecode import unidecode
from datetime import datetime as dt
from scrapy.spiders import Rule, CrawlSpider
from scrapy.linkextractors import LinkExtractor
from ..items import IntelcutoutItem, ReplyItems
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError


class BaseSpider(CrawlSpider):
	name = 'base'
	proxy = 'http://127.0.0.1'
	cookie = dict()
	item = IntelcutoutItem()
	reply_item = ReplyItems()
	all_done = thread_done = False
	replies_list = visited_threads = list()
	allowed_domains = ['knyutblrv7grn7am.onion']
	start_urls = ['http://knyutblrv7grn7am.onion/index.php']
	rules = (
		Rule(
			LinkExtractor(
				restrict_xpaths='///a[contains(@href,"viewforum.php?")]',
				unique=True
			),
			process_request='error_callback',
		),
		Rule(
			LinkExtractor(
				restrict_xpaths='///a[contains(@href,"viewtopic.php?")]',
				unique=True
			),
			callback='post_scrape',
			process_request='first_callback',
		),
	)
	# custom_settings = {
	# 	'RETRY_ENABLED': False,
	# }

	def first_callback(self, request):
		self.logger.debug('Error in request : ', request)

		return request.replace(errback=self.error_callback)

	def error_callback(self, failure):
		self.logger.error(repr(failure))

		if failure.check(HttpError):
			response = failure.value.response
			logging.error('HttpError : ', response.url)
		elif failure.check(DNSLookupError):
			request = failure.request
			logging.error('DNS Fucked Up : ', request.url)
		elif failure.check(TimeoutError):
			request = failure.request
			logging.error('Timeout Error: ', request.url)

	def post_scrape(self, response):
		if 'viewtopic' in response.url and response.url not in self.visited_threads:
			posts = response.xpath('//div[@id="brdmain"]/div[contains(@class,"blockpost ")]')
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass

			if not self.thread_done:
				post_info = posts[0].xpath('.//div[@class="postright"]')
				author_info = posts[0].xpath('.//div[@class="postleft"].//text()').extract()
				if author_info:
					author_info = '--'.join([a.strip() for a in author_info if a]).split('--', 4)
					self.item['author_name'] = author_info[1]
					self.item['author_age'] = self.item['author_location'] = ''
					self.item['author_posts_count'] = author_info[4].split('Posts: ')[1]
					self.item['author_joined_date'] = author_info[3].split('Registered: ')[1]

					# author_membership = '1' if 'Member' in author_info[2] else '0'
					self.item['author_membership_level'] = '1' if 'Member' in author_info[2] else '0'

				self.item['thread_url'] = response.url
				self.item['thread_group'] = response.xpath('//ul[@class="crumbs"]/li[2]/.//text()').extract()[-1]
				self.item['scraped_date'] = dt.now().isoformat()

				try:
					timestamp = posts[0].xpath('.//h2/.//a/text()').extract_first()
					self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en'])
				except (AttributeError, TypeError):
					self.item['thread_timestamp'] = ''

				self.item['thread_content'] = self.replace_patterns(post_info.xpath('.//text()').extract())
				(self.item['thread_media_links'], self.item['thread_general_links']) = self.extract_links(posts[0])

				self.thread_done = True
				self.replies_list = []
				self.reply_scrape(posts[1:])

			next_page = response.xpath('//nav/a[contains(text(),"Next")]').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False
			if next_page is not None:
				# self.reply_scrape()
				pass

			if self.all_done:
				self.item['thread_replies'] = self.replies_list
				self.item['thread_reply_no'] = len(self.replies_list)
				self.replies_list = []
				self.thread_done = False
				yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.http.HtmlResponse:
			thread_replies = response
		elif type(response) is scrapy.selector.SelectorList:
			if response.url not in response.url:
				thread_replies = response.xpath('//div[@id="brdmain"]/div[contains(@class,"blockpost ")]')
			else:
				return
		elif self.all_done:
			return
		else:
			return
		for reply in thread_replies:
			try:
				author_info = reply.xpath('.//div[@class="postleft"]/.//text()').extract()
				if author_info:
					author_info = '--'.join([a.strip() for a in author_info if a]).split('--', 4)
					self.reply_item['reply_author'] = author_info[1]
					self.reply_item['reply_author_membership'] = '1' if 'Member' in author_info[2] else '0'

				try:
					self.reply_item['reply_timestamp'] = dp.parse(
						reply.xpath('.//h2/.//a/text()').extract_first(), languages=['en'])
				except (AttributeError, TypeError):
					self.reply_item['reply_timestamp'] = ''

				reply_content = reply.xpath('.//div[@class="postright"]/.//text()').extract()
				self.reply_item['reply_content'] = self.replace_patterns(reply_content)

				(self.reply_item['reply_media_links'], self.reply_item['reply_general_links']) = \
					self.extract_links(reply)
			except Exception as e:
				logging.error('Thread Parsing Error:', e)

		if type(response) is scrapy.selector.SelectorList:
			return
		next_page = response.xpath('//nav/a[contains(text(),"Next")]').extract_first()
		if next_page is None and self.thread_done is True:
			self.all_done = True
			return

		if next_page is not None:
			pass
			# self.reply_scrape()

	def replace_patterns(self, thread_content):
		for t in range(len(thread_content)):
			if thread_content[t].startswith('Re:'):
				thread_content[t] = ''

		thread_content = ' '.join(thread_content).strip()
		try:
			check_content = unidecode(str(bytes(thread_content, encoding='utf-8'), encoding='utf-8'))
		except Exception as e:
			check_content = thread_content
			logging.exception('Exception at Decoding :', e)
		thread_content = check_content
		thread_content.replace('\n', '').replace('\t', '').replace('\r', '').replace('  ', ' ')
		return thread_content

	def extract_links(self, response):
		media_links = general_links = list()
		content = ' '.join(response.xpath('.//div[@class="postright"]/.//*').extract())

		if '<iframe ' in content:
			media_links = response.xpath('.//iframe/@src').extract()
		if '<img ' in content:
			media_links += response.xpath('.//img/@src').extract()
		if '<a ' in content:
			general_links = response.xpath('.//a/@href').extract()

		media_links = ';'.join(media_links)
		general_links = ';'.join(general_links)

		return (media_links, general_links) \
			if (media_links or general_links) and not(media_links is ';' and general_links is ';') \
			else ('', '')
