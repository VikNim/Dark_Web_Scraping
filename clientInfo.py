# -*- coding: utf-8 -*-
import scrapy
import logging
from ..items import ClientinfoItem

class BaseSpider(scrapy.Spider):
	name = 'base'
	allowed_domains = ['']
	start_urls = ['file:///media/vikram/14CA7057CA703758/VolonScraping/sample/Infocyte HUNT.html']

	def parse(self, response):
		item = ClientinfoItem()
		reports = response.xpath('//div[@class="report__content"]/div[@class="report__section"]/div')
		# logging.info(len(reports))

		for current_report in reports:
			item['file_path'] = item['sha1'] = item['file_name'] = item['ip_address'] = item['hostname'] = item['comment'] = ""

			item['hostname'] = current_report.xpath('.//div[2]/div[1]/div[1]/h3/text()').extract_first()
			item['ip_address'] = current_report.xpath('.//div[2]/div[1]/div[2]/text()').extract_first()
			file_names = current_report.xpath('.//div[@class="host-threat-list"]/..//p[1]/text()').extract()
			sha1_data = current_report.xpath('.//div[@class="host-threat-list"]/..//p[2]/text()').extract()
			file_paths = current_report.xpath('.//div[@class="host-threat-list"]/..//p[3]/text()').extract()
			# logging.info('Data :')
			# logging.info(data)
			# logging.info(item['hostname'])
			# logging.info(item['ip_address'])
			try:
				for f in range(0,len(file_names)):
					if '.exe' not in file_names[f] and '.dll' not in file_names[f]:
						item['comment'] = file_names[f]
						file_names[f] = file_names[f+1]
			except Exception as e:
				logging.exception(e)

			for (a, b, c) in zip(file_names, sha1_data, file_paths):
				item['file_name'] = a
				item['sha1'] = b
				item['file_path'] = c

				yield item