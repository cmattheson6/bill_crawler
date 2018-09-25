# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy # for scraping
# For matching characters
import re
import unidecode

### -------- Start of the spider -------- ###

class BillCrawlerSpider(scrapy.Spider):
    # Set the name of the spider and the corresponding starting points of the scrape
    name = 'bill_crawler'
    allowed_domains = ['www.congress.gov']
    start_urls = ['https://www.congress.gov/search?q=%7B%22congress%22%3A%22115%22%2C%22source%22%3A%22legislation%22%7D&searchResultViewType=expanded&page=1']

#extrapolate the next level up: the list of all bills in this cycle
#add in the correct URL
#parse out the links to the bills
#have that feed into the below parse function
    # The first parse will pull all URLs from the page that link to individual bills
    def parse(self, response):
        list_of_urls = response.xpath('.//a/@href').re(r'^.*/bill/.*/.*/[0-9]*\?.*$')
        bill_urls = list_of_urls[0::2]
        print(bill_urls) ###formula checkpoint
        for u in bill_urls:
            url = u[0:re.search(r'\?', u).start()]
            # The loop should stop here based on if a URL is already in the list
            if sum(bill_url_df['url']==url) > 0:
                print('URL in list. All new bills have been uploaded.')
                break
            else:
                yield scrapy.Request(url = url,
                                     callback = self.parse_bill);

    # The second parse will pull out all of the bill's relevant information
    def parse_bill(self, response):
        # Parses out all of the bill info from the page
        bill_info = response.xpath(".//h1[@class='legDetail']/text()").extract_first()
        sponsor_info = response.xpath(".//table[@class='standard01']/tr/td/a/text()").extract_first()
        bill_summary = response.xpath(".//div[@id='bill-summary']/p/text()").extract_first()
        
        bill_id = bill_info[0:bill_info.index("-")].replace(" ", "").replace(".", "")
        bill_title = bill_info[bill_info.index("-")+1:len(bill_info)].strip()

        sponsor_id = create_pol_id(sponsor_info)

        # Assign all parts of the bill to a dictionary for pipeline discovery        
        bill_dict = {'bill_id':bill_id, 
                     'bill_title': bill_title, 
                     'bill_summary': bill_summary,
                     'sponsor_id': sponsor_id,
                     'bill_url': response.request.url,
                     'sponsor_info': sponsor_info}
                
        #Pull the link to the cosponsors of the bill.
        partial_link = response.xpath(".//a[contains(text(), 'Cosponsors')]/@href").extract_first()
        cosponsors_link = response.urljoin(partial_link)
        cosponsors_request = scrapy.Request(url = cosponsors_link, 
                                            callback = self.parse_cosponsors)
        # This generator needs to yield both the bill and a separate cosponsor request
        # The two scrapes need to be separate
        print(bill_dict) ###formula checkpoint       
        yield bill_dict
        yield cosponsors_request
        
        # Determine if there is a next page link, what that link is
        next_page = response.xpath(".//a[@class='next']/@href").extract_first()
        
        # Follow the next_page link from the top of the spider if available
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url = next_page,
                                 callback = self.parse)
    
    # Generates all cosponsors of the bill for a separate table
    def parse_cosponsors(self, response):
        # Parses out all cosponsor information
        bill_info = response.xpath(".//h1[@class='legDetail']/text()").extract_first()       
        cosponsors_info = response.xpath(".//table[@class = 'item_table']/tbody/tr/td/a/text()").extract()
        print(cosponsors_info) ###formula checkpoint
        bill_id = bill_info[0:bill_info.index("-")].replace(" ", "").replace(".", "")
        cosponsor_ids = (create_pol_id(pol) for pol in cosponsors_info)
        print(cosponsor_ids) ###formula checkpoint
        # There may be multiple cosponsors, so a dictionary is generated for each cosponsor
        for pol in cosponsor_ids:
            cosponsor_dict = {'bill_id': bill_id,
                              'cosponsor_id': pol,
                              'cosponsor_info': cosponsors_info}
            print(cosponsor_dict) ###formula checkpoint
            yield cosponsor_dict;
            
