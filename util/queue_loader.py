#!/usr/bin/env python
from collections import defaultdict
from datetime import datetime, timedelta
from logging import getLogger
import operator

from affine import config
from affine import librato_tools
from affine.aws import sqs
from affine.model import IngestionSettings, DomainBlacklist, DomainGamesList, \
    dont_process_url
from affine.vcr.constants import DOWNLOAD_FAILED_REASONS, DOWNLOAD_STAGES
from affine.vcr.dynamodb import DynamoIngestionStatusClient

# Ingestion Settings
# Retry failed URLs only if they failed at least these many days ago
MIN_AGE_FOR_RETRY_FAILED_KEY = "qs_min_age_for_retry_failed"
# Retry completed URLs only if they were completed at least these many days ago
MIN_AGE_FOR_RETRY_COMPLETED_KEY = "qs_min_age_for_retry_completed"
# Percent of completed pages to revisit relative to new + failed pages to ingest
PERCENT_COMPLETED_PAGES_TO_REVISIT_KEY = "qs_percent_completed_pages_to_revisit"
# Queueing thresholds - default, by domain and by country
DEFAULT_QUEUEING_THRESHOLD_KEY = "qs_default_queueing_threshold"
QUEUEING_THRESHOLDS_BY_DOMAIN_KEY = "qs_queueing_thresholds_by_domain"
QUEUEING_THRESHOLDS_BY_COUNTRY_KEY = "qs_queueing_thresholds_by_country"

# Total number of URLs enqueued in this run
METRIC_QL_URLS_ENQUEUED_COUNT = "queue_loader.gauge.urls_enqueued_count"
# Total number of completed URLs retrieved for revisiting - before cropping the list for ingestion
METRIC_QL_URLS_REVISIT_COUNT = "queue_loader.gauge.urls_revisit_count"

logger = getLogger("affine.queue_loader")


class QueueLoaderOutputProcessor(object):
    def __init__(self):
        self.blacklist_domains = DomainBlacklist.domains()
        self.games_domains = DomainGamesList.domains()
        # Retrieve latest queueing settings at each run
        self.min_age_for_retry_failed = timedelta(days=eval(
            IngestionSettings.get_setting(MIN_AGE_FOR_RETRY_FAILED_KEY, "14")))
        self.min_age_for_retry_completed = timedelta(days=eval(
            IngestionSettings.get_setting(MIN_AGE_FOR_RETRY_COMPLETED_KEY,"30")))
        self.percent_pages_to_revisit = eval(
            IngestionSettings.get_setting(PERCENT_COMPLETED_PAGES_TO_REVISIT_KEY,"20"))
        self.default_queueing_threshold = eval(IngestionSettings.get_setting(DEFAULT_QUEUEING_THRESHOLD_KEY, "200"))
        self.queueing_thresholds_by_domain = eval(
            IngestionSettings.get_setting(QUEUEING_THRESHOLDS_BY_DOMAIN_KEY,
                                          "{'youtube.com': 15,'youtube.co.uk': 15,'youtube.co.de': 15}"))
        self.queueing_thresholds_by_country = eval(
            IngestionSettings.get_setting(QUEUEING_THRESHOLDS_BY_COUNTRY_KEY,"{}"))
        # Dynamo & SQS clients
        self.dynamo = DynamoIngestionStatusClient()
        self.download_queue = sqs.get_queue(config.sqs_download_queue_name())

        self.urls_to_enqueue_final = None

    def process_data(self, data):
        logger.info("Length of data to process and load: " + str(len(data)))
        logger.info("Filtering data")
        urls_to_enqueue = self._apply_filter_threshold_blacklist_duplicates(data)
        urls_to_enqueue, urls_to_revisit = self._apply_filter_already_processed_items(urls_to_enqueue)

        logger.info("Prioritising data")
        urls_to_enqueue = self._apply_priority_by_url_count(urls_to_enqueue)
        urls_to_revisit = self._apply_priority_by_url_count(urls_to_revisit)
        librato_tools.submit_value(METRIC_QL_URLS_REVISIT_COUNT, len(urls_to_revisit))

        logger.info("Limiting items to revisit")
        max_num_pages_to_revisit = (self.percent_pages_to_revisit / 100) * len(urls_to_enqueue)
        urls_to_revisit = urls_to_revisit[:max_num_pages_to_revisit]

        self.urls_to_enqueue_final = urls_to_enqueue + urls_to_revisit

    def load_data(self, created_at_time):
        logger.info("Enqueuing URLs & updating Dynamo")
        logger.info("Number of URLs to enqueue: " + str(len(self.urls_to_enqueue_final)))
        self._enqueue_urls(self.urls_to_enqueue_final, created_at_time)
        librato_tools.submit_value(METRIC_QL_URLS_ENQUEUED_COUNT, len(self.urls_to_enqueue_final))
        logger.info("Finished enqueuing URLs & updating Dynamo")

    def _apply_filter_threshold_blacklist_duplicates(self, items):
        # Filter given URL data by the domain blacklist, existing urls and removing any
        # duplicates (For instance, the same URL could show up with a different entry type)
        urls_already_filtered = set()
        urls_to_enqueue = []
        for item in items:
            qs_threshold = min(self.queueing_thresholds_by_domain.get(item.tld, self.default_queueing_threshold),
                               self.queueing_thresholds_by_country.get(item.country, self.default_queueing_threshold)
            )
            if (item.entry_type == 'qs-query' and item.count < qs_threshold) or \
                            item.tld is None or item.tld == 'localhost' or \
                            len(item.url) > 1024 or \
                            item.url in urls_already_filtered or \
                    dont_process_url(item.url, item.domain, self.blacklist_domains, self.games_domains):
                continue
            urls_already_filtered.add(item.url)
            urls_to_enqueue.append(item)
        return urls_to_enqueue

    def _apply_filter_already_processed_items(self, items):
        # Filter items to two lists:
        # 1) Items that have not been processed yet or are in Failed status for longer than a predefined time period
        # 2) Items that were successfully completed but have not been revisited for longer than a predefined time period
        now = datetime.utcnow()
        all_dynamo_data = self.dynamo.batch_get([item.url for item in items])
        unprocessed_items = []
        revisit_items = []
        for item in items:
            dynamo_data = all_dynamo_data.get(item.url)
            if dynamo_data is None:
                unprocessed_items.append(item)
            # Retry old entries which failed in text stage.
            elif dynamo_data['status'] == 'Failed' and dynamo_data['download_stage'] == DOWNLOAD_STAGES['Text']:
                item_age = now - dynamo_data['updated_at']
                if item_age >= self.min_age_for_retry_failed:
                    unprocessed_items.append(item)
            # Revisit old completed items, in any stage.
            elif (dynamo_data['status'] == 'Failed' and dynamo_data['download_stage'] == DOWNLOAD_STAGES['Video']) or \
                            dynamo_data['status'] == 'Complete':
                item_age = now - dynamo_data['updated_at']
                if item_age >= self.min_age_for_retry_completed:
                    revisit_items.append(item)
        return unprocessed_items, revisit_items

    def _apply_priority_by_url_count(self, items):
        # Apply prioritization in reverse order of the url count
        # In future, we could consider adding prioritization based on domain or other factors
        items_sorted_by_count = sorted(items, key=operator.attrgetter('count'), reverse=True)
        return items_sorted_by_count

    def _enqueue_urls(self, items, created_at_time):
        urls_to_enqueue = []
        for url_info in items:
            urls_to_enqueue.append({
                'url': url_info.url,
                'status': 'Queued',
                'download_stage': DOWNLOAD_STAGES['Text'],
                'failed_reason': DOWNLOAD_FAILED_REASONS['NotApplicable'],
                'created_at': created_at_time,
                'url_count': url_info.count,
            })
        self.dynamo.batch_put(urls_to_enqueue)
        # URLs will be enqueued in SQS in the order they exist in `items`
        for item in urls_to_enqueue:
            item.pop('created_at')
            sqs.write_to_queue(self.download_queue, item)
