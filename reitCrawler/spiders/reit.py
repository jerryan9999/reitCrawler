import scrapy
from scrapy.shell import inspect_response
from collections import OrderedDict
import json


class ReitSpider(scrapy.Spider):
    name = "reit"
    start_urls = [
        'https://www.reit.com/investing/reit-funds/table?page={}'.format(i+1) for i in range(17)
    ]

    def parse(self,response):
      item = {}
      # inspect_response(response,self)
      reit_list = response.xpath('//div[@class="view-content"]/table/tbody/tr')
      for index,reit in enumerate(reit_list):
        item['fund_name'] = response.xpath('//div[@class="view-content"]/table/tbody/tr')[index].xpath('td/div/a/text()').extract_first()
        item['quote'] = response.xpath('//div[@class="view-content"]/table/tbody/tr')[index].xpath('td/span/text()')[0].extract()
        item['share_class_type'] = response.xpath('//div[@class="view-content"]/table/tbody/tr')[index].xpath('td')[1].xpath('text()').extract_first().strip()
        yield item


class ReitSpiderCompany(scrapy.Spider):
    name = "reitcompany"
    start_urls = [
      'https://www.reit.com/investing/reit-directory?page={}'.format(i) for i in range(25) 
    ]
    def parse(self,response):
      item = {}
      lis = response.xpath('//div[@class="view-content"]/ul/li')
      for l in lis:
        url = l.xpath('div/div[@class="overview"]/div/div/a/@href').extract_first()
        if not url:
          continue
        item['url'] = "https://www.reit.com" + url
        item['name'] = l.xpath('div/div[@class="overview"]/div/div/a/text()').extract_first()
        item['ticker'] = l.xpath('div/div[@class="overview"]/div[1]/div[2]/span/text()').extract()[0].strip()
        item['address'] = l.xpath('div/div[@class="overview"]/div[1]/div[2]/span/text()').extract()[1].strip()
        item['sector'] = l.xpath('div/div[@class="overview"]/div[2]/text()').extract_first().strip()

        item['price'] = l.xpath('div/div[@class="stock"]/div[1]/text()').extract_first().strip('\nStock Price: ')
        item['trend'] = l.xpath('div/div[@class="stock"]/div[2]/@class').extract_first().strip('trend-arrow trend-arrow--')
        item['month_return'] = l.xpath('div/div[@class="stock"]/div[3]/span[1]/span[1]/text()').extract_first()
        item['month_return_pct'] = l.xpath('div/div[@class="stock"]/div[3]/span[1]/span[2]/text()').extract_first()
        yield item

class ReitSpiderDetails(scrapy.Spider):
    name = "reitdetails"

    def start_requests(self):
        with open('items-reit.json') as f:
            quote = json.load(f,encoding='utf-8')

        for index,item in enumerate(quote):
            # if index>0:
            #   continue
            url = item['url']
            yield scrapy.Request(url=url,meta={'item':item})

    def parse(self,response):
        # inspect_response(response,self)
        item = response.meta['item']

        # Overview
        fields = response.xpath('//div[@class="ctools-collapsible-content"]')[0].xpath('ul/li/div/div[@class="reit-values__title"]/text()').extract()
        values = response.xpath('//div[@class="ctools-collapsible-content"]')[0].xpath('ul/li/div/div[@class="reit-values__value"]/text()').extract()
        fields = [f.strip() for f in fields if f.strip()]
        values = [v.strip() for v in values if v.strip()]
        for k, v in zip(fields,values):
            item[k] = v

        # Performace details
        fields = response.xpath('//div[@class="ctools-collapsible-content"]')[1].xpath('ul/li/div/div[@class="reit-values__title"]/text()').extract()
        values = response.xpath('//div[@class="ctools-collapsible-content"]')[1].xpath('ul/li/div/div[@class="reit-values__value"]/text()').extract()
        fields = [f.strip() for f in fields if f.strip()]
        values = [v.strip() for v in values if v.strip()]
        for k, v in zip(fields,values):
            item[k] = v

        return item


class ReitSpiderSector(scrapy.Spider):

    name = "reitsector"
    start_urls = ["https://www.reit.com/data-research/reit-indexes/real-time-index-returns/ftse-nareit-us"]

    def parse(self,response):
        reit = []
        tables = response.xpath('//*[@class="view-content"]/table/tbody/tr')
        for x in tables:
            t = {}
            t['type'] = x.xpath('td[1]/a/span/text()').extract_first()
            t['value'] = x.xpath('td[2]/text()').extract_first().strip()
            t['change'] = x.xpath('td[3]/span/text()').extract_first().strip()
            t['time'] = x.xpath('td[4]/span/@content').extract_first().strip()
            reit.append(t)
        for item in reit:
          yield item

