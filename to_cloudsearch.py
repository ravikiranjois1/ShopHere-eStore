import json
import time
import boto.cloudsearch2
from boto.cloudsearch2.domain import Domain


def get_connection():
    conn = boto.cloudsearch2.connect_to_region(region_name='us-east-1',
                                               aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
                                               aws_secret_access_key='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')

    domain_data = conn.describe_domains('cs-products')

    domain_data = (domain_data['DescribeDomainsResponse']['DescribeDomainsResult']['DomainStatusList'])

    domain = Domain(conn, domain_data[0])

    doc_service = domain.get_document_service()
    return domain, doc_service, conn


def insert_data_to_cloudsearch():
    with open("dynamo-export/ProductsData_search.json", "r") as f:
        list_of_items = json.load(f)
        count = 0
        start_time = time.time()
        data_count =0
        domain, doc_service, conn = get_connection()
        while len(list_of_items)>data_count:
            for entry in list_of_items[data_count:]:
                # print(entry, list_of_items.index(entry))
                if time.time() - start_time > 300:
                    domain, doc_service, conn = get_connection()
                    start_time = time.time()
                    # print("Here", start_time)
                doc_service.add(entry['asin'], entry)
                data_count += 1
                count += 1
                try:

                    if count == 350:
                        print("Im here")
                        result = doc_service.commit()
                        count = 0
                except:
                    continue

        # To commit the remaining data
        result = doc_service.commit()


if __name__ == '__main__':
    insert_data_to_cloudsearch()