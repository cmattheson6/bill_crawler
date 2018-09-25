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
                
            #ADD IN GCP SCRIPT HERE
                cred_dict = {
                                 "auth_provider_x509_cert_url": spider.settings.get('auth_provider_x509_cert_url'),
                                 "auth_uri": spider.settings.get('auth_uri'),
                                 "client_email": spider.settings.get('client_email'),
                                 "client_id": spider.settings.get('client_id'),
                                 "client_x509_cert_url": spider.settings.get('client_x509_cert_url'),
                                 "private_key": spider.settings.get('private_key'),
                                 "private_key_id": spider.settings.get('private_key_id'),
                                 "project_id": spider.settings.get('project_id'),
                                 "token_uri": spider.settings.get('token_uri'),
                                 "type": spider.settings.get('account_type')
                     }
                cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
                print(cred_dict)

                credentials = service_account.Credentials.from_service_account_info(cred_dict)
                print(credentials)
                print("I haven't set up the client yet, but I built the credentials!")
                publisher = pubsub.PublisherClient(credentials = credentials)
                print(publisher)
                print("The client was set up!")

                topic = 'projects/{project_id}/topics/{topic}'.format(
                     project_id='politics-data-tracker-1',
                     topic='house_pols')
                project_id = 'politics-data-tracker-1'
                topic_name = 'bill_info'
                topic_path = publisher.topic_path(project_id, topic_name)
                data = u'This is a representative in the House.'
                data = data.encode('utf-8')
                print("The topic was built!")
                publisher.publish(topic_path, data=data,
                                  bill_id = item['bill_id'],
                                  amdt_id = item['amdt_id'],
                                  bill_title = item['bill_title'],
                                  bill_summary = item['bill_summary'],
                                  sponsor_id = item['sponsor_id'],
                                  bill_url = item['bill_url'])
                print("We published! WOOOO!")
                
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
                cred_dict = {
                                 "auth_provider_x509_cert_url": spider.settings.get('auth_provider_x509_cert_url'),
                                 "auth_uri": spider.settings.get('auth_uri'),
                                 "client_email": spider.settings.get('client_email'),
                                 "client_id": spider.settings.get('client_id'),
                                 "client_x509_cert_url": spider.settings.get('client_x509_cert_url'),
                                 "private_key": spider.settings.get('private_key'),
                                 "private_key_id": spider.settings.get('private_key_id'),
                                 "project_id": spider.settings.get('project_id'),
                                 "token_uri": spider.settings.get('token_uri'),
                                 "type": spider.settings.get('account_type')
                     }
                cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
                print(cred_dict)

                credentials = service_account.Credentials.from_service_account_info(cred_dict)
                print(credentials)
                print("I haven't set up the client yet, but I built the credentials!")
                publisher = pubsub.PublisherClient(credentials = credentials)
                print(publisher)
                print("The client was set up!")

                topic = 'projects/{project_id}/topics/{topic}'.format(
                     project_id='politics-data-tracker-1',
                     topic='cosponsors')
                project_id = 'politics-data-tracker-1'
                topic_name = 'house_pols'
                topic_path = publisher.topic_path(project_id, topic_name)
                data = u'This is a representative in the House.'
                data = data.encode('utf-8')
                print("The topic was built!")
                publisher.publish(topic_path, data=data,
                                  bill_id = item['bill_id'],
                                  amdt_id = item['amdt_id'],
                                  cosponsor_id = item['cosponsor_id'],
                                  cosponsor_info = item['cosponsor_info'])
                print("We published! WOOOO!")
                return item;
