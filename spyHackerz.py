# -*- coding: utf-8 -*-
# from scrapy import Spider

import re
import scrapy
import logging
import requests
import dateparser as dp
from functools import reduce
from scrapy.http import Request
from scrapy.spiders import Rule, CrawlSpider
from unidecode import unidecode
from datetime import datetime as dt
from ..items import SpyhackerzItem, ReplyItems
from scrapy.linkextractors import LinkExtractor


class BaseSpider(CrawlSpider):
	name = 'base'
	proxy = 'http://127.0.0.1:8118'
	cookie = dict()
	replies = ReplyItems()
	item = SpyhackerzItem()
	all_done = thread_done = False
	visited_threads = replies_data = list()
	allowed_domains = ['spyhackerz.com']
	start_urls = [

		'https://www.spyhackerz.com/forum/'
	]
	rules = (
		Rule(
			LinkExtractor(
				restrict_xpaths='//h3[@class="nodeTitle"]/a',
				allow_domains=allowed_domains,
				unique=True,
			),
			callback='forums'
		),
		# Rule(
		# 	LinkExtractor(
		# 		allow_domains=allowed_domains,
		# 		restrict_xpaths='//a[@class="PreviewTooltip"]',
		# 		unique=True,
		# 		canonicalize=True
		# 	),
		# 	callback='post_scrape'
		# ),
		Rule(
			LinkExtractor(
				restrict_xpaths='//nav/a[contains(text(),"Next")]',
				allow_domains=allowed_domains,
				unique=True
			),
			callback='forums'
		),
		Rule(
			LinkExtractor(
				allow_domains=allowed_domains,
				restrict_xpaths='//a[@class="PreviewTooltip"]',
				unique=True,
				canonicalize=True
			)
		),
	)

	def forums(self, response):
		links = LinkExtractor(
			allow_domains=self.allowed_domains,
			restrict_xpaths='//a[@class="PreviewTooltip"]',
			unique=True,
			canonicalize=True
		).extract_links(response)
		for link in links:
			yield Request(link.url, callback=self.post_scrape)
		# next_page = response.xpath('//nav/a[contains(text(),"Next")]/@href').extract_first()
		# if next_page is not None:
		# 	next_page = 'https://www.spyhackerz.com/' + next_page
		# 	yield Request(url=next_page, callback=self.forums)

	def post_scrape(self, response):
		if 'threads/' in response.url and response.url not in self.visited_threads:
			self.visited_threads.append(response.url)
			try:
				self.proxy = response.meta['proxy']
				self.cookie = response.request.cookies
			except AttributeError:
				pass
			if not self.thread_done:
				self.item['thread_url'] = response.url
				posts = response.xpath('//ol[@id="messageList"]/li')
				try:
					post_info = posts[0]
					self.item['thread_group'] = response.xpath(
						'//span[@class="crumbs"]/span[3]/.//span[@itemprop="title"]/text()').extract_first(default='')

					self.item['author_name'] = post_info.xpath('.//a[@class="username"]/.//text()').extract_first(default='')
					membership_level = len(post_info.xpath('.//em[contains(@class,"userBanner")]/strong/img'))
					if membership_level is 1:
						try:
							membership_level = re.findall(
								'\d+',
								post_info.xpath(
									'.//em[contains(@class,"userBanner bannerHidden")]/strong/img/@src'
								).extract_first().split('/')[-1]
							)[0]
						except IndexError:
							try:
								membership_level = re.findall(
									'\d+',
									post_info.xpath(
										'.//em[contains(@class,"userBanner bannerHidden")]/strong/img/@src'
									).extract_first().split('/')[-2]
								)[0]
							except IndexError:
								logging.error('Thread Author Membership Error')
								membership_level = '1'
							except Exception:
								membership_level = '0'
						except Exception:
							membership_level = '0'

					self.item['author_posts_count'] = post_info.xpath(
						'.//dl[contains(@class,"pairsJustified")][1]/dd/.//text()').extract_first(default='')

					timestamp = post_info.xpath('.//*[@class="DateTime"]/@title').extract_first()
					try:
						self.item['thread_timestamp'] = dp.parse(timestamp, languages=['en', 'tr']).isoformat()
					except (TypeError, AttributeError):
						try:
							if timestamp is not None:
								# timestamp = ' '.join(timestamp.split(' ')[1:])
								self.item['thread_timestamp'] = dp.parse(' '.join(timestamp.split(' ')[1:])).isoformat()
						except (AttributeError, IndexError):
							timestamp = self.item['thread_timestamp'] = ''

					thread_content = post_info.xpath('.//div[@class="messageContent"]/article/blockquote/..//*').extract()
					(self.item['thread_media_links'], self.item['thread_general_links']) = \
						self.extract_links(post_info, ' '.join(thread_content))

					thread_content = post_info.xpath('.//div[@class="messageContent"]/article/blockquote/..//text()').extract()
					self.item['thread_content'], self.item['thread_media_links'], \
					self.item['thread_general_links'] = self.replace_patterns(
						thread_content,
						str(timestamp),
						self.item['thread_media_links'],
						self.item['thread_general_links']
					)

					self.item['author_membership_level'] = membership_level
					self.item['author_joined_date'] = self.item['author_location'] = self.item['author_age'] = ''
					self.item['scraped_date'] = dt.now().isoformat()

					self.thread_done = True
					self.replies_data = []
					self.reply_scrape(posts[1:])
				except Exception as e:
					logging.exception(e)

			next_page = response.xpath('//nav/a[contains(text(),"Next")]/@href').extract_first()
			self.all_done = True if next_page is None and self.thread_done else False
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
				yield self.item

	def reply_scrape(self, response):
		if type(response) is scrapy.selector.SelectorList:
			total_replies = response
		elif type(response) is scrapy.http.HtmlResponse or type(response) is requests.models.Response:
			total_replies = response.xpath('//ol[@id="messageList"]/li[starts-with(@id,"post-")]')
		elif self.thread_done:
			return
		else:
			return

		if total_replies:
			for reply in total_replies:
				try:
					author_info = reply.xpath('.//div[contains(@class,"messageUserInfo")]')
					reply_author = author_info.xpath('.//a[@class="username"]/.//text()').extract_first(default='')

					reply_author_membership = len(author_info.xpath('.//em[contains(@class,"userBanner")]/strong/img'))
					if reply_author_membership is 1:
						try:
							reply_author_membership = re.findall(
								'\d+',
								author_info.xpath(
									'.//em[contains(@class,"userBanner bannerHidden")]/strong/img/@src'
								).extract_first().split('/')[-1]
							)[0]
						except IndexError:
							try:
								reply_author_membership = re.findall(
									'\d+',
									author_info.xpath(
										'.//em[contains(@class,"userBanner bannerHidden")]/strong/img/@src'
									).extract_first().split('/')[-2]
								)[0]
							except IndexError:
								logging.error('Reply Author Membership Error')
								reply_author_membership = '1'
							except Exception:
								reply_author_membership = '0'
						except Exception:
							reply_author_membership = '0'

					reply_content = ' '.join(
						reply.xpath('.//div[@class="messageContent"]/article/blockquote/..//*').extract())
					(self.replies['reply_media_links'], self.replies['reply_general_links']) = \
						self.extract_links(reply, reply_content)

					if 'Quote:' in reply_content:
						reply_content = reply.xpath(
							'.//div[@class="messageContent"]/article/blockquote/.//text()').extract()
					else:
						reply_content = reply.xpath(
							'.//div[@class="messageContent"]/article/blockquote/text()').extract()

					reply_timestamp = reply.xpath('.//*[@class="DateTime"]/@title').extract_first()
					try:
						self.replies['reply_timestamp'] = dp.parse(reply_timestamp, languages=['en', 'tr']).isoformat()
					except (AttributeError, TypeError):
						try:
							if reply_timestamp is not None:
								self.replies['reply_timestamp'] = \
									dp.parse(' '.join(reply_timestamp.split(' ')[1:]), languages=['en', 'tr']).isoformat()
						except (AttributeError, TypeError):
							reply_timestamp = self.replies['reply_timestamp'] = ''

					self.replies['reply_author'] = reply_author
					self.replies['reply_content'], self.replies['reply_media_links'], \
					self.replies['reply_general_links'] = self.replace_patterns(
						reply_content,
						str(reply_timestamp),
						self.replies['reply_media_links'],
						self.replies['reply_general_links']
					)

					self.replies['reply_author_membership'] = reply_author_membership
				except Exception as e:
					logging.exception(e)
				finally:
					self.replies_data.append(dict(self.replies))
		else:
			self.all_done = True if self.thread_done else False
			return
		if type(response) is scrapy.selector.SelectorList:
			return
		next_page = response.xpath('//nav/a[contains(text(),"Next")]/@href').extract_first()
		self.all_done = True if next_page is None and self.thread_done else False
		if next_page is not None:
			try:
				self.reply_scrape(requests.get(next_page, cookies=self.cookie, proxies={'http': self.proxy}))
			except Exception as e:
				logging.error('Next Page Error:', e)


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

	def replace_patterns(self, content_data, timestamp, md_links, gn_links):
		check_content = ''
		timestamp = '' if timestamp is None else timestamp
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

			for l in range(len(md_links)):
				if '/login/' in md_links[l]:
					md_links[l] = ''
				if 'register/' in md_links[l]:
					md_links[l] = ''

			for l in range(len(gn_links)):
				if 'login/' in gn_links[l]:
					gn_links[l] = ''
				if 'register/' in gn_links[l]:
					gn_links[l] = ''

		except Exception as e:
			logging.exception(e)
		finally:
			return check_content, md_links, gn_links

	def val_transform(self, content_data):
		for i in range(len(content_data)):
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
