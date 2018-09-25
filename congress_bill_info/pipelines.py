# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

### -------- Import all necessary modules -------- ###
import scrapy

### -------- Start of the pipeline -------- ###

# This pipeline is only to uplaod the bill items
class CongressBillInfoPipeline(object):
    # Uploads bill information to the database
    def process_item(self, item, spider):
        bill_packet = (item.setdefault('bill_id', None), 
                       item.setdefault('bill_title', None), 
                       item.setdefault('bill_summary', None), 
                       item.setdefault('sponsor_id', None), 
                       item.setdefault('bill_url', None))
        # Filters out any cosponsor items and only uploads bill items
        if None in (list(bill_packet[i] for i in [0,1,3,4])):
            return item
        # Uploads bill items
        else:
            # Attempt to upload item to database
            try:
                
            #ADD IN GCP SCRIPT HERE
                pass
                return item

# This pipeline is only to upload the bill cosponsors
class BillCosponsorsPipeline(object):
    
    # Builds and uploads query
    def process_item(self, item, spider):
        cosponsor_packet = (item.setdefault('bill_id', None),
                            item.setdefault('cosponsor_id', None))
        
        # Filters out all bill items
        if None in cosponsor_packet:
            pass
        else:
            # Attempt to upload packet to database
            try:
                pass
                return item;
