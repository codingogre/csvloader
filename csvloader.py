import sys
import getopt
import simplejson as json
import pandas as pandas
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


csvfile = ''
index = ''

ES = Elasticsearch(
     cloud_id="COVID-19:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQ0MmRkYWE2NTg4Yjc0NDkxYjU4ZjdhZDhkZTRlZjM0YiQ3Mjg4ODZjNTRiNTA0MjIzOTM0N2NiNjNjZDBkM2YyMw==",
     http_auth=("elastic", "YZe4U2q1RxVMQLM6MC7TpyWQ")
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
