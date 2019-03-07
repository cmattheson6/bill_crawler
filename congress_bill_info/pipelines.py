"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.

In order to handle two different types of dictionaries that need to go to two different topics, they need to be
handled accordingly:
 - There are two separate pipelines in the settings.py file: CongressBillInfoPipeline and BillCosponsorsPipeline
 - Check that the keys in the item exactly match the keys we are looking for.
 - If they check out, then it's good to go. If not, it gets passed to the other pipeline.

2)
"""
### -------- Import all necessary modules -------- ###
import scrapy
from google.cloud import pubsub
from google.oauth2 import service_account
import logging

### -------- Start of the pipeline -------- ###

# This pipeline is only to uplaod the bill items
class CongressBillInfoPipeline(object):
    # Uploads bill information to the database
    def process_item(self, item, spider):
        # item = (
        #     item.setdefault('bill_id', None),
        #     item.setdefault('bill_title', None),
        #     item.setdefault('sponsor_fn', None),
        #     item.setdefault('sponsor_ln', None),
        #     item.setdefault('sponsor_party', None),
        #     item.setdefault('sponsor_state', None),
        #     item.setdefault('bill_url', None))
        bill_info_keys = ['bill_id',
                            'amdt_id'
                            'bill_title',
                            'bill_summary',
                            'sponsor_fn',
                            'sponsor_ln',
                            'sponsor_party',
                            'sponsor_state',
                            'bill_url']
        # Filters out any cosponsor items and only uploads bill items.
        if all([i in bill_info_keys for i in item.keys()]):
            """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
            In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
            and dynamically created in the script."""
            # Pull all of the credential info from the Scrapy platform into a dictionary.
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
            logging.info('Credentials downloaded from Scrapy server.')
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')

            # Build a Credentials object from the above dictionary. This will properly allow access as part of a
            # Google Cloud Client.
            credentials = service_account.Credentials.from_service_account_info(cred_dict)
            logging.info('Credentials object created.')

            # Create Publisher client.
            publisher = pubsub.PublisherClient(credentials=credentials)
            logging.info('Publisher Client created.')

            # Set location of proper publisher topic
            project_id = 'politics-data-tracker-1'
            topic_name = 'bill_info'
            topic_path = publisher.topic_path(project_id, topic_name)
            data = u'This is a representative in the House.' #Consider how to better use this.
            data = data.encode('utf-8')
            publisher.publish(
                topic_path,
                data=data,
                bill_id = item['bill_id'],
                amdt_id = item['amdt_id'],
                bill_title = item['bill_title'],
                bill_summary = item['bill_summary'],
                sponsor_fn = item['sponsor_fn'],
                sponsor_ln = item['sponsor_ln'],
                sponsor_state = item['sponsor_state'],
                sponsor_party = item['sponsor_party'],
                bill_url = item['bill_url'])
            logging.info('Published item: {0}'.format(item))
            yield item
        # If not true, sends the item to the next pipeline as-is.
        else:
            yield item

# This pipeline is only to upload the bill cosponsors
class BillCosponsorsPipeline(object):
    
    # Builds and uploads query
    def process_item(self, item, spider):
        # cosponsor_packet = (item.setdefault('bill_id', None),
        #                     item.setdefault('cosponsor_fn', None),
        #                     item.setdefault('cosponsor_ln', None),
        #                     item.setdefault('cosponsor_party', None),
        #                     item.setdefault('cosponsor_state', None))
        
        # Filters out all bill items
        cosponsor_keys = ['bill_id',
                          'amdt_id',
                          'cosponsor_fn',
                          'cosponsor_ln',
                          'cosponsor_party',
                          'cosponsor_state']
        # Filters out any cosponsor items and only uploads bill items.
        if all([i in cosponsor_keys for i in item.keys()]):
            # Pull all of the credential info from the Scrapy platform into a dictionary.
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
            logging.info('Credentials downloaded from Scrapy server.')
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')

            # Build a Credentials object from the above dictionary. This will properly allow access as part of a
            # Google Cloud Client.
            credentials = service_account.Credentials.from_service_account_info(cred_dict)
            logging.info('Credentials object created.')

            # Create Publisher client.
            publisher = pubsub.PublisherClient(credentials=credentials)
            logging.info('Publisher Client created.')

            # Set location of proper publisher topic
            project_id = 'politics-data-tracker-1'
            topic_name = 'cosponsors'
            topic_path = publisher.topic_path(project_id, topic_name)

            #Figure out how to use this better.
            data = u'This is the cosponsors related to bill {0}.'.format(item['bill_id'])
            data = data.encode('utf-8')
            publisher.publish(
                topic_path,
                data=data,
                bill_id = item['bill_id'],
                amdt_id = item['amdt_id'],
                sponsor_fn = item['cosponsor_fn'],
                sponsor_ln = item['cosponsor_ln'],
                sponsor_state = item['cosponsor_state'],
                sponsor_party = item['cosponsor_party'])
            logging.info('Published item: {0}'.format(item))
            yield item
        # If not true, sends the item to the next pipeline as-is.
        else:
            yield item