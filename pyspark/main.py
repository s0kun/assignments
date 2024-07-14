from data_loader.loader import Loader
from processing.proc import Proc, showTable

from pyspark.sql import SparkSession
import http.server
import socketserver

if __name__ == "__main__":
    # Spark Session initiated:
    spark = SparkSession.builder.master("local[*]").appName("covidAnalysis").getOrCreate()

    # Load Data:
    ldrObj = Loader(spark)
    rawDF = ldrObj.loadJSON()

    # Analysis:
    procObj = Proc(spark,rawDF)
    handlers = {
        "mostAffected": lambda : procObj.deathsByCases("max"),
        "leastAffected": lambda : procObj.deathsByCases("min"),
        "highestCases": lambda : procObj.casesWise("max"),
        "lowestCases": lambda : procObj.casesWise("min"),
        "totalCases": lambda : procObj.totalCases(),
        "mostEfficient": lambda : procObj.recoveryPerCase("max"),
        "leastEfficient": lambda : procObj.recoveryPerCase("min"),
        "highestCritical": lambda : procObj.criticalWise("max"),
        "lowestCritical": lambda : procObj.criticalWise("min")
    }

    class service(http.server.BaseHTTPRequestHandler):

        def do_GET(self):
            # Extract request path and query parameters
            request_path = self.path.split("/")[-1]

            # Check if a handler exists for the request path
            if request_path in handlers:
                response = handlers[request_path]()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(response.encode())
            else:
                # Generate HTML content for the landing page with links
                html_content = """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>COVID Analysis API</title>
                </head>
                <body>
                    <h1>COVID-19 Analysis API Endpoints</h1>
                    <hr>
                    <p>Available analysis options:</p>
                    <ul>
                """

                # Create links for each handler
                for key, value in handlers.items():
                    html_content += f'<li><a href="/{key}">{key.replace("_", " ")}</a></li>'

                html_content += """
                    </ul>
                </body>
                </html>
                """
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(html_content.encode())

                table = showTable(procObj.filterDF)
                self.wfile.write(table.encode())

    HOST = 'localhost'
    PORT = 8000
    with socketserver.TCPServer((HOST, PORT), service) as httpd:
        print(f"Serving at http://{HOST}:{PORT}")
        httpd.serve_forever()



    # Q1. Find the most affected country: (Total deaths / Total cases)
    # procObj.deathsByCases("max")
    # +------------+------------+----------------+-----+------+-------------------------------+
    # |Country Name|Country Code|Total Population|Cases|Deaths|Fraction of Population Affected|
    # +------------+------------+----------------+-----+------+-------------------------------+
    # |       Yemen|         YEM|        31154867|11945|  2159|            0.18074508162411052|
    # +------------+------------+----------------+-----+------+-------------------------------+


    # Q2. Least affected among all the countries ( total death/total covid cases)
    # procObj.deathsByCases("min")
    # +--------------------+------------+----------------+-----+------+-------------------------------+
    # |        Country Name|Country Code|Total Population|Cases|Deaths|Fraction of Population Affected|
    # +--------------------+------------+----------------+-----+------+-------------------------------+
    # |Falkland Islands ...|         FLK|            3539| 1930|     0|                            0.0|
    # |Holy See (Vatican...|         VAT|             799|   29|     0|                            0.0|
    # |                Niue|         NIU|            1622| 1059|     0|                            0.0|
    # |        Saint Helena|         SHN|            6115| 2166|     0|                            0.0|
    # |             Tokelau|         TKL|            1378|   80|     0|                            0.0|
    # +--------------------+------------+----------------+-----+------+-------------------------------+


    # Q3. Country with highest covid cases
    # procObj.casesWise("max")
    # +------------+------------+----------------+-----------+
    # |Country Name|Country Code|Total Population|Covid Cases|
    # +------------+------------+----------------+-----------+
    # |         USA|         USA|       334805269|  111729242|
    # +------------+------------+----------------+-----------+


    # Q4. Country with lowest covid cases
    # procObj.casesWise("min")
    # +--------------+------------+----------------+-----------+
    # |  Country Name|Country Code|Total Population|Covid Cases|
    # +--------------+------------+----------------+-----------+
    # |Western Sahara|         ESH|          626161|         10|
    # +--------------+------------+----------------+-----------+



    # Q5. Total Cases over in the world
    # procObj.totalCases()
    # +-----------+
    # |Total Cases|
    # +-----------+
    # |  704445210|
    # +-----------+

    # Q6. Country that handled covid most efficiently
    # procObj.recoveryPerCase("max")
    # +--------------------+------------+----------+-----------+---------------+---------------------------+
    # |        Country Name|Country Code|Population|Total Cases|Total Recovered|Fraction of Cases Recovered|
    # +--------------------+------------+----------+-----------+---------------+---------------------------+
    # |Falkland Islands ...|         FLK|      3539|       1930|           1930|                        1.0|
    # |Holy See (Vatican...|         VAT|       799|         29|             29|                        1.0|
    # +--------------------+------------+----------+-----------+---------------+---------------------------+

    # Q7. Country that handled covid least efficiently
    # procObj.recoveryPerCase("min")
    # Data seems to be incorrect
    # +--------------------+------------+----------+-----------+---------------+---------------------------+
    # |        Country Name|Country Code|Population|Total Cases|Total Recovered|Fraction of Cases Recovered|
    # +--------------------+------------+----------+-----------+---------------+---------------------------+
    # |             Andorra|         AND|     77463|      48015|              0|                        0.0|
    # |            Anguilla|         AIA|     15230|       3904|              0|                        0.0|
    # |          Bangladesh|         BGD| 167885689|    2048588|              0|                        0.0|
    # |              Belize|         BLZ|    412190|      71381|              0|                        0.0|
    # |British Virgin Is...|         VGB|     30596|       7392|              0|                        0.0|
    # |          Costa Rica|         CRI|   5182354|    1238883|              0|                        0.0|
    # |             Estonia|         EST|   1321910|     627927|              0|                        0.0|
    # |       Faroe Islands|         FRO|     49233|      34658|              0|                        0.0|
    # |    French Polynesia|         PYF|    284164|      79254|              0|                        0.0|
    # |             Georgia|         GEO|   3968738|    1855289|              0|                        0.0|
    # |           Gibraltar|         GIB|     33704|      20550|              0|                        0.0|
    # |              Greece|         GRC|  10316637|    6101379|              0|                        0.0|
    # |          Guadeloupe|         GLP|    399794|     203235|              0|                        0.0|
    # |            Honduras|         HND|  10221247|     474590|              0|                        0.0|
    # |             Iceland|         ISL|    345393|     209903|              0|                        0.0|
    # |               India|         IND|1406631776|   45033644|              0|                        0.0|
    # |                Iran|         IRN|  86022837|    7627186|              0|                        0.0|
    # |         Isle of Man|         IMN|     85732|      38008|              0|                        0.0|
    # |             Jamaica|         JAM|   2985094|     156869|              0|                        0.0|
    # |               Japan|         JPN| 125584838|   33803572|              0|                        0.0|
    # +--------------------+------------+----------+-----------+---------------+---------------------------+

    # Q8. Country least suffering from covid
    # procObj.criticalWise("min")
    # +-------------------+------------+-----------+--------------------+
    # |       Country Name|Country Code|Total Cases|Total Critical Cases|
    # +-------------------+------------+-----------+--------------------+
    # |        Afghanistan|         AFG|     232152|                   0|
    # |            Albania|         ALB|     334863|                   0|
    # |            Algeria|         DZA|     272010|                   0|
    # |            Andorra|         AND|      48015|                   0|
    # |             Angola|         AGO|     107327|                   0|
    # |           Anguilla|         AIA|       3904|                   0|
    # |Antigua and Barbuda|         ATG|       9106|                   0|
    # |          Argentina|         ARG|   10094643|                   0|
    # |            Armenia|         ARM|     451831|                   0|
    # |            Austria|         AUT|    6081287|                   0|
    # |         Azerbaijan|         AZE|     835031|                   0|
    # |            Bahrain|         BHR|     729549|                   0|
    # |         Bangladesh|         BGD|    2048588|                   0|
    # |           Barbados|         BRB|     110575|                   0|
    # |            Belarus|         BLR|     994037|                   0|
    # |            Belgium|         BEL|    4861319|                   0|
    # |             Belize|         BLZ|      71381|                   0|
    # |              Benin|         BEN|      28036|                   0|
    # |            Bermuda|         BMU|      18860|                   0|
    # |             Bhutan|         BTN|      62697|                   0|
    # +-------------------+------------+-----------+--------------------+

    # Q9. Country most suffering from covid
    # procObj.criticalWise("max")
    # +------------+------------+-----------+--------------------+
    # |Country Name|Country Code|Total Cases|Total Critical Cases|
    # +------------+------------+-----------+--------------------+
    # |         USA|         USA|  111729242|                1191|
    # +------------+------------+-----------+--------------------+








