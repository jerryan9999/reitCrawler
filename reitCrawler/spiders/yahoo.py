import scrapy
from scrapy.shell import inspect_response
from collections import OrderedDict
import json


class YahooSpider(scrapy.Spider):
    """

        抓取yahoo上的reit-fund信息
        scrapy crawl yahoo-reit-fund-summary

    """
    name = "yahoo-reit-fund-summary"

    def start_requests(self):
        with open('items-reit-fund.json') as f:
            quote = json.load(f,encoding='utf-8')
        for q in quote:
            url = "https://finance.yahoo.com/quote/"+q['quote']
            yield scrapy.Request(url=url)


    def parse(self, response):
        # inspect_response(response,self)
        item = {}
        summary_table = response.xpath('//div[contains(@data-test,"summary-table")]//tr')
        summary_data = OrderedDict()
        for table_data in summary_table:
            raw_table_key = table_data.xpath('.//td[contains(@class,"C(black)")]//text()').extract_first()
            raw_table_value = table_data.xpath('.//td[contains(@class,"Ta(end)")]//text()').extract_first() or ''
            table_key = ''.join(raw_table_key).strip()
            table_value = ''.join(raw_table_value).strip()
            summary_data.update({table_key:table_value})            #TODO Sustainablity Rating
        item['summary'] = summary_data

        return scrapy.Request(url="https://query2.finance.yahoo.com/v10/finance/quoteSummary/{0}?formatted=true&lang=en-US&region=US&modules=summaryProfile%2CfinancialData%2CrecommendationTrend%2CupgradeDowngradeHistory%2Cearnings%2CdefaultKeyStatistics%2CcalendarEvents&corsDomain=finance.yahoo.com".format("STMDX"), callback=self.parse_other_details,meta={'item':item},dont_filter=True)

    def parse_other_details(self,response):
        # inspect_response(response,self)
        quoteSummary = json.loads(response.text)
        item = response.meta['item']
        item['profile'] = quoteSummary['quoteSummary']['result'][0].pop('summaryProfile')
        item['quoteSummary'] = quoteSummary
        return item


class YahooSpider(YahooSpider):
    """
        同上
    """
    name = "yahoo-reit-summary"

    def start_requests(self):
        quote = [{"quote":"SBAC"}]
        for q in quote:
            url = "https://finance.yahoo.com/quote/"+q['quote']
            yield scrapy.Request(url=url)


class YahooSpiderGraph(scrapy.Spider):
    """
        抓取图线数据：YTD, 1Y, 6M, 1M, 5D,返回json数据

        scrapy crawl yahoograph 

        输入文件： nareit_items_details.json,每只reit的详情文件
    """
    name = "yahoograph"
    options = {
            "YTD":('1mo','ytd'),
            "1Y":('1d','1y'),
            "6M":('1d','6m'),
            "1M":('30m','1mo'),
            "5d":('15m','5d')
                }

    def start_requests(self):
        with open('nareit_items_details.json') as f:
            quote = json.load(f,encoding='utf-8')
        for q in quote:
            for k,v in self.options.items():
                item = {}
                item['ticker'] = q['ticker']
                item['options'] = k
                url = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?region=US&lang=en-US&includePrePost=false&interval={1}&range={2}&corsDomain=finance.yahoo.com&.tsrc=finance".format(q['ticker'],v[0],v[1])
                yield scrapy.Request(url=url,meta={'item':item})

    def parse(self,response):
        item = response.meta['item']
        result = json.loads(response.text)
        item['chart'] = result['chart']['result']
        return item

class YahooSpiderGraphIndex(scrapy.Spider):
    """
        抓取一些重要指数的图表数据: 标普500，纳斯达克，比特币，离岸汇率等

        scrapy crawl yahoographindex

    """
    name = "yahoographindex"
    options = {
            "YTD":('1mo','ytd'),
            "1Y":('1d','1y'),
            "6M":('1d','6m'),
            "1M":('30m','1mo'),
            "5d":('15m','5d')
                }

    def start_requests(self):
        quote = [
            {'ticker':'%5EGSPC','name':'S&P 500'},
            {'ticker':'%5EIXIC','name':'NASDAQ'},
            {'ticker':'%5EDJI','name':'Dow Jones Industrial Average'},
            {'ticker':'BTC-USD','name':'Bitcoin USD'},
            {'ticker':'CNHZ18.CME','name':'Standard-Size USD/Offshore RMB'}    # 离岸汇率
        ]
        for q in quote:
            for k,v in self.options.items():
                item = {}
                item['ticker'] = q['name']
                item['options'] = k
                url = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?region=US&lang=en-US&includePrePost=false&interval={1}&range={2}&corsDomain=finance.yahoo.com&.tsrc=finance".format(q['ticker'],v[0],v[1])
                yield scrapy.Request(url=url,meta={'item':item})

    def parse(self,response):
        item = response.meta['item']
        result = json.loads(response.text)
        item['chart'] = result['chart']['result']
        return item
