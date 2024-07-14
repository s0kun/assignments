import http.client
import ssl
import json

from pyspark.sql.types import *
from pyspark.sql import SparkSession

class Loader(object):
    def __init__(self, spark, JSONfname="countryCovid_data.json",CSVfname = ".covid_data.csv"):
        self.spark = spark
        self.JSONfname = JSONfname
        self.CSVfname = CSVfname

    def loadJSON(self):
        # Function to write data to CSV file
        def flatTable(statistics, sep = '/'):
            # Recursive function to flatten nested dictionaries

            def flatten(d, parent_key=''):
                for k, v in (d.items() if isinstance(d,dict) else enumerate(d)):
                    new_key = parent_key + sep + str(k) if parent_key else str(k)
                    if isinstance(v, dict) or isinstance(v, list):
                        flatten(v, new_key)
                    else:
                        items.append((new_key, v))
                return

            items = []
            flatten(statistics)

            return dict(items)

        filename = self.JSONfname

        try:
            with open('data_loader/data/'+filename,"r") as f:
                entries = json.load(f)
            print("Data loaded from local storage")
        except Exception:
            # Disable SSL certificate verification
            ssl._create_default_https_context = ssl._create_unverified_context

            conn = http.client.HTTPSConnection("disease.sh")

            conn.request("GET", "/v3/covid-19/countries")

            res = conn.getresponse()

            if(res.getcode() != 200):
                print("Response Error")
                raise Exception("Failed to load data.")

            txt = res.read().decode("utf-8")

            json_data = json.loads(txt)
            assert isinstance(json_data, list)

            # Flatten Tree-shaped data:
            entries = []
            for element in json_data:
                entries.append(flatTable(element))

            # Save data:
            with open('data_loader/data/'+filename, "w") as f:
                f.write(json.dumps(entries))

            print("Data loaded from HTTPS request to API")

        safeSchema = StructType([StructField(col, StringType(), True) for col in entries[0].keys()])
        safeDF = self.spark.createDataFrame(entries, safeSchema)

        return safeDF


    def loadCSV(self):

        dat = self.spark.sparkContext.textFile(self.CSVfname)

        def format(line: str) -> tuple:
            tokens = [c.strip() for c in line.split(sep = ',')]
            # tokens[0] is 'ID' ; tokens[-1] is 'fieldData' & tokens[1:-1] is 'fieldName' ;
            return tokens[0], ('_'.join(tokens[1:-1]), tokens[-1])

        # O(n*n) steps group/reduce operation??
        safeDF = dat.map(format).groupByKey().map(lambda X: {p[0]:p[1] for p in X[1]}).toDF()

        return safeDF

