# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy # for scraping
# For matching characters
import re
import unidecode # MAKE SURE THAT ALL FILES ARE ACTUALLY HERE *** NO. 3 THING TO DO TOMORROW

### -------- Define all custom fxns here -------- ###

# Pull from a raw string the relevant info on a politician (first name, last name, party, state)

def create_pol_dict(pol):
    regex = '[^a-zA-Z]'
    pol_fn = pol[pol.index(",")+1:pol.index("[")].strip()
    pol_ln = pol[pol.index(" ")+1:pol.index(",")]
    s = pol[pol.index("["):pol.index("]")+1]
    pol_party = s[s.index("[")+1:s.index("-")]
    if s.count('-') == 2:
        pol_state = s[find_character(s, '-')[0]+1:find_character(s, '-')[1]]
    elif s.count('-') == 1:
        pol_state = s[find_character(s, '-')+1:len(s)]
    else: Error
    pol_dict = {'first_name': pol_first_name,
                'last_name': pol_last_name,
                'party': pol_party,
                'state': pol_state}
    return pol_dict;
def create_bill_dict(b): #inputs raw bill info
    if b.count('to') > 0: 
        bill_id = b[0:b.index("to")].replace(" ", "").replace(".", "")
        amdt_id = b[b.index("to")+1:len(b)].strip()
        bill_title = b
        bill_sec_title = response.xpath(".//table[@class='standard01']/tr/td/text()").extract()
        bill_title = bill_title + bill_sec_title
        bill_dict = {'bill_id': bill_id,
                     'amdt_id': amdt_id,
                     'bill_title': bill_title}
        return bill_dict
    else:
        bill_id = b[0:b.index("-")].replace(" ", "").replace(".", "")
        amdt_id = None
        bill_title = b[b.index("-")+1:len(b)].strip()
        bill_dict = {'bill_id': bill_id,
                     'amdt_id': amdt_id,
                     'bill_title': bill_title}
        return bill_dict;

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
            # The loop should stop here based on if the date provided was not from yesterday
#             bill_date = ''
#             if bill_date > datetime.timedelta(days = 1): ### REVISE HERE
#                 print('URL in list. All new bills have been uploaded.')
#                 break
#             else:
#                 yield scrapy.Request(url = url,
#                                      callback = self.parse_bill);
        
#         # Because of the set-up of the site, this section should only be used once to build a full database the first time                                
#         # Determine if there is a next page link, what that link is
#         next_page = response.xpath(".//a[@class='next']/@href").extract_first()
        
#         # Follow the next_page link from the top of the spider if available
#         if next_page is not None:
#             next_page = response.urljoin(next_page)
#             yield scrapy.Request(url = next_page,
#                                  callback = self.parse)
                                        

    # The second parse will pull out all of the bill's relevant information
    def parse_bill(self, response):
        # Parses out all of the bill info from the page
        bill_info = response.xpath(".//h1[@class='legDetail']/text()").extract_first()
        bill_info = create_bill_dict(bill_info)
        sponsor_info = response.xpath(".//table[@class='standard01']/tr/td/a/text()").re_first(r'^.*\[.*\]$')
        bill_summary = response.xpath(".//div[@id='bill-summary']/p/text()").extract_first()
        
        bill_id = bill_info['bill_id']
        amdt_id = bill_info['amdt_id']
        bill_title = bill_info['bill_title']

        # Get all sponsor info parsed from raw info
        sponsor_info = create_pol_dict(sponsor_info)

        # Assign all parts of the bill to a dictionary for pipeline discovery        
        bill_dict = {'bill_id': bill_id, 
                     'amdt_id': amdt_id,
                     'bill_title': bill_title, 
                     'bill_summary': bill_summary,
                     'sponsor_fn': sponsor_info['first_name'],
                     'sponsor_ln': sponsor_info['last_name'],
                     'sponsor_party': sponsor_info['party'],
                     'sponsor_state': sponsor_info['state'],
                     'bill_url': response.request.url}
                
        #Pull the link to the cosponsors of the bill.
        partial_link = response.xpath(".//a[contains(text(), 'Cosponsors')]/@href").extract_first()
        cosponsors_link = response.urljoin(partial_link)
        cosponsors_request = scrapy.Request(url = cosponsors_link, 
                                            callback = self.parse_cosponsors)
        # This generator needs to yield both the bill and a separate cosponsor request
        # The two scrapes need to be separate     
        yield bill_dict
        yield cosponsors_request
    
    # Generates all cosponsors of the bill for a separate table
    def parse_cosponsors(self, response):
        # Parses out all cosponsor information
        bill_info = response.xpath(".//h1[@class='legDetail']/text()").extract_first()  
        bill_info = create_bill_dict(bill_info)
        cosponsors_info = response.xpath(".//table[@class = 'item_table']/tbody/tr/td/a/text()").extract()
        bill_id = bill_info['bill_id']
        amdt_id = bill_info['amdt_id']
        cosponsors_info = (create_pol_dict(pol) for pol in cosponsors_info)
        # There may be multiple cosponsors, so a dictionary is generated for each cosponsor
        # Then uploaded individually
        for pol in cosponsors_info:
            cosponsor_dict = {'bill_id': bill_id,
                              'amdt_id': amdt_id,
                              'cosponsor_fn': pol['first_name'],
                              'cosponsor_ln': pol['last_name'],
                              'cosponsor_party': pol['party'],
                              'cosponsor_state': pol['state'],
                             }
            yield cosponsor_dict;
            
