# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

### -------- Import all necessary modules -------- ###
import psycopg2 # For connecting to database
# For sending emails
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

### -------- Define all custom fxns here -------- ###

# Defines ability to send emails in case of error
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

### -------- Start of the pipeline -------- ###

# This pipeline is only to uplaod the bill items
class CongressBillInfoPipeline(object):
    # Opens the connection to database
    def open_spider(self, spider):
         hostname = 'localhost'
         username = 'postgres'
         password = 'postgres'
         database = 'politics'
         self.conn = psycopg2.connect(
             host = hostname,
             user = username,
             password = password,
             dbname = database)
         self.cur = self.conn.cursor()
         
    # Closes connection to database
    def close_spider(self, spider):
         self.cur.close()
         self.conn.close()
         
    # Uploads bill information to the database
    def process_item(self, item, spider):
        insert_query = """insert into bills (bill_id, 
                                             bill_title, 
                                             bill_summary, 
                                             sponsor_id, 
                                             bill_url)
        values (%s, %s, %s, %s, %s)"""
        bill_packet = (item.setdefault('bill_id', None), 
                       item.setdefault('bill_title', None), 
                       item.setdefault('bill_summary', None), 
                       item.setdefault('sponsor_id', None), 
                       item.setdefault('bill_url', None))
        # Filters out any cosponsor items and only uploads bill items
        if None in (list(bill_packet[i] for i in [0,1,3,4])): #figure out what should the condition be
            return item
        # Uploads bill items
        else:
            # Attempt to upload item to database
            try:
                self.cur.execute(insert_query, bill_packet)
                self.conn.commit()
                return item
            # If an error occurs, an email will be sent detailing the error.
            except Exception as e:
                send_email("""The bill_crawler pipeline returned an error due to the following inputs:\n
                            Bill ID: {0}\n
                            Bill Title: {1}\n
                            Bill Summary: {2}\n
                            Sponsor ID: {3}\n
                            Sponsor Info: {4}\n
                            Bill URL: {5}\n
                            Error: {6}\n \n
                            Please resolve the issue.""".format(item['bill_id'],
                                      item['bill_title'],
                                      item['bill_summary'],
                                      item['sponsor_id'],
                                      item['sponsor_info'],
                                      item['bill_url'],
                                      e))

# This pipeline is only to upload the bill cosponsors
class BillCosponsorsPipeline(object):
    # Opens database connection
    def open_spider(self, spider):
         hostname = 'localhost'
         username = 'postgres'
         password = 'postgres'
         database = 'politics'
         self.conn = psycopg2.connect(
             host = hostname,
             user = username,
             password = password,
             dbname = database)
         self.cur = self.conn.cursor()
    # Closes database connection     
    def close_spider(self, spider):
         self.cur.close()
         self.conn.close()
    
    # Builds and uploads query
    def process_item(self, item, spider):
        insert_query = """insert into bill_cosponsors (bill_id, cosponsor_id)
        values (%s, %s)"""
        cosponsor_packet = (item.setdefault('bill_id', None),
                            item.setdefault('cosponsor_id', None))
        
        # Filters out all bill items
        if None in cosponsor_packet: #figure out what should the condition be
            pass
        else:
            # Attempt to upload packet to database
            try:
                self.cur.execute(insert_query, cosponsor_packet)
                self.conn.commit()
            # If an error occurs, an email will be sent detailing the error.
            except Exception as e:
                send_email("""The bill_crawler pipeline returned an error due to the following inputs:\n
                            Bill ID: {0}\n
                            Cosponsor ID: {1}\n
                            Cosponsor Info: {2}\n
                            Error: {3}\n \n
                            Please resolve the issue.""".format(item['bill_id'],
                                      item['cosponsor_id'],
                                      item['cosponsor_info'],
                                      e))
