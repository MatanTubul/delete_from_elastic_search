#!/bin/python
import elasticsearch
import sys, getopt
import json

def main(argv):
    number = 10
    search = 'metrics.changes.total:0 AND type:puppet-report'
    doc_type = ""
    try:
        opts, args = getopt.getopt(argv, "hs:n:", ["search=", "number=","doc_type="])
    except getopt.GetoptError:
        print 'delete_from_elasticsearch.py -s <search_expression> -n <number_per_shard | optional> -d <document type | optional>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'delete_from_elasticsearch.py -s <search_expression> -n <number_per_shard> -d <document type>'
            sys.exit()
        elif opt in ("-s", "--search"):
            search = arg
        elif opt in ("-n", "--number"):
            number = arg
        elif opt in ("-d", "--document"):
            doc_type = arg
    print 'I will search for "', search
    print 'I will delete these in batches of "', number
    delete_docs(search, number,doc_type)


def delete_docs(search, number=10,docType=None):
    # Setup elasticsearch connection.
    elasticsearch.Elasticsearch()
    es = elasticsearch.Elasticsearch(
        ['localhost'],
        # sniff before doing anything
        sniff_on_start=True,
        # refresh nodes after a node fails to respond
        sniff_on_connection_fail=True,
        # and also every 60 seconds
        sniffer_timeout=60
    )

    # Start the initial search.
    # print json.loads(search)
    # print
    es.search()
    hits = es.search(
        index="example",
        doc_type=docType,
        body=json.loads(search),
        size=number,
        search_type="scan",
        scroll='5m',
    )
    # print
    # print json.dumps(hits, indent=4, sort_keys=True)
    # print
    # Now remove the results.
    while True:
        try:
            # Git the next page of results.
            scroll = es.scroll(scroll_id=hits['_scroll_id'], scroll='5m', )
        except elasticsearch.exceptions.NotFoundError:
            break
            # We have results initialize the bulk variable.
        bulk = ""

        # Remove the variables.
        print
        for result in scroll['hits']['hits']:
            print "DELETE: " +str(result)
            print
            bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(
                result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
        # print "Items left " + str(scroll['hits']['total']) + ' deleting ' + str(bulk.count('delete')) + ' items.'
        #    print bulk
        es.bulk(body=bulk)



if __name__ == "__main__":
    main(sys.argv[1:])