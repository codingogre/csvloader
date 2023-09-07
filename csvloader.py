import getopt
import sys

import pandas as pandas
import simplejson as json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

csvfile = ''
index = ''

ES = Elasticsearch(
     cloud_id="Sandbox:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo0NDMkNGE1NzQ5OWUwZDQ2NDJlZGI2ODI3N2U0ODQ1YjdkMjYkMzg4ZGE3Yzc0MDBkNDg5M2EyNDRlOTYyZGUxMDQ2N2U=",
     basic_auth=("elastic", "DeRiIWGNA9ngmawF8PZU7Hx7")
)


def main(argv):
    global csvfile
    global index
    try:
        opts, args = getopt.getopt(argv, "hf:i:", ["csvfile=", "index="])
    except getopt.GetoptError:
        print('csvloader.py -f <csvfile> -i <index>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('csvloader.py -f <csvfile> -i <index>')
            sys.exit()
        elif opt in ("-f", "--csvfile"):
            csvfile = arg
        elif opt in ("-i", "--index"):
            index = arg
    print('Input file: ', csvfile)
    print('Index: ', index)


def generate_actions(file):
    csv_df = pandas.read_csv(file, na_values=" ")
    for row in csv_df.to_dict(orient="records"):
        # key = (row["combined_key"] + os.path.basename(url)).encode()
        yield {
                "_index": index,
                "_op_type": "index",
                # "_id": hashlib.sha1(key).hexdigest(),
                "_source": json.dumps(row, ignore_nan=True)
              }


if __name__ == '__main__':
    main(sys.argv[1:])
    for success, info in streaming_bulk(client=ES, actions=generate_actions(csvfile)):
        if not success:
            print('A document failed:', info)
