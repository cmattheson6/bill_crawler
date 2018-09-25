# -*- coding: utf-8 -*-

### -------- Import all of the necessary files -------- ###
import scrapy # for scraping
import psycopg2 # for connecting to database
# For matching characters
import re
import unidecode

import sys # for exiting the fxn
import pandas as pd # for dataframes
# For sending emails
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Open the connection to the database

hostname = 'localhost'
username = 'postgres'
password = 'postgres'
database = 'politics'
conn = psycopg2.connect(
    host = hostname,
    user = username,
    password = password,
    dbname = database)
cur = conn.cursor()

### -------- Pull in all of the queries needed to compare fxns -------- ###

# Used for ID mapping
select_query = "select id from politicians"
cur.execute(select_query)
pol_id_list = cur
pol_id_list = [i[0] for i in pol_id_list]

# Used for ID mapping
select_query = "select first_name, last_name, party, state from politicians"
cur.execute(select_query)
pol_tuple_check_list = list(cur) 

# Compare all previous bills to determine new bills
select_query = "select bill_id from bills"
cur.execute(select_query)
bill_id_list = list(cur)
bill_id_list = [i[0] for i in bill_id_list]

# Compare all previous bills to determine new bills
select_query = "select bill_url from bills"
cur.execute(select_query)
bill_url_df = pd.DataFrame(list(cur))
bill_url_df.columns = ['url']

# Used to correct nicknames when normalizing naming conventions
select_nicknames = """select nickname, full_name from nicknames"""
cur.execute(select_nicknames)
list_nicknames = pd.DataFrame(list(cur))
list_nicknames.columns = ['nickname', 'full_name']

# Close database connection
cur.close()
conn.close()

### -------- Define all custom fxns here -------- ###

# Find the location of needed character in a string
def find_character(s, ch):
    index_nums = []
    index = 0
    for x in s:
        if x == ch:
            index_nums.append(index)
            index = index + 1
        else:       
            index = index + 1;
    return index_nums

# Normalize policicians' nicknames for database
def fix_nickname(fn):
    if fn in list(list_nicknames['nickname']):
        full_name = list_nicknames[list_nicknames['nickname'] == fn].iloc[0,1]
        print(fn)
        return full_name
    else:
        print(fn)
        return fn;

# Normalize a politicians' full name to allow for normalized mapping
def scrub_name(s):
    if len(re.findall(r'[A-Z]\.', s)) == 1:
        u = unidecode.unidecode(s)
        v = re.sub(r' [A-Z]\.', '', u) #remove middle initials
        w = re.sub(r' \".*\"', '', v) #remove nicknames
        x = re.sub(r' (Sr.|Jr.|III|IV)', '', w) #remove suffixes
        y = re.sub(r'\,', '', x) #remove stray commas
        z = y.strip() #remove excess whitespace
        return fix_nickname(z);
    else:
        u = unidecode.unidecode(s)
        v = re.sub(r'\".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r'\,', '', w) #remove stray commas
        y = x.strip() #remove excess whitespace
        return fix_nickname(y);

# Map each politican to their corresponding ID from a raw name
def create_pol_id(pol):
    regex = '[^a-zA-Z]'
    pol_fn_raw = pol[pol.index(",")+1:pol.index("[")].strip()
    pol_first_name = scrub_name(pol_fn_raw)
    pol_ln_raw = pol[pol.index(" ")+1:pol.index(",")]
    pol_last_name = scrub_name(pol_ln_raw)
    s = pol[pol.index("["):pol.index("]")+1]
    pol_party = s[s.index("[")+1:s.index("-")]
    if s.count('-') == 2:
        pol_state = s[find_character(s, '-')[0]+1:find_character(s, '-')[1]]
    elif s.count('-') == 1:
        pol_state = s[find_character(s, '-')+1:len(s)]
    else: Error
    ###rewrit the state to be an if-then statement that covers both pols and sens
    pol_tuple = (pol_first_name,
                 pol_last_name,
                 pol_party,
                 pol_state)
    print(pol_tuple) ###formula checkpoint
    if pol_tuple in pol_tuple_check_list:
        pol_id = pol_id_list[pol_tuple_check_list.index(pol_tuple)]
    else: 
        pol_id = ValueError;
    return pol_id;

# Send an email when there is an error in the script
def send_email(b):
    from_addr = 'pipeline.error.bot6@gmail.com'
    to_addr = 'cmattheson6@gmail.com'
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = 'Error in bill_crawler Pipeline'
    
    body = b
    msg.attach(MIMEText(body, 'plain'))
    
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_addr, 'Pulisic22*')
    text = msg.as_string()
    server.sendmail(from_addr, to_addr, text)
    server.quit();

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
            