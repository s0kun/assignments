from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sqlFn


def showTable(df):
    # Collect data and column names as lists
    data = df.collect()
    columns = list(df.columns)

    # Build the HTML table string
    html = ["<table>"]

    # Create the header row
    html.append("  <tr>")
    for col in columns:
        html.append(f"    <th>{col}</th>")
    html.append("  </tr>")

    # Add data rows
    for row in data:
        html.append("  <tr>")
        for value in row:
            html.append(f"    <td>{value}</td>")
        html.append("  </tr>")

    html.append("</table>")
    return "".join(html)

class Proc(object):
    def __init__(self, spark: SparkSession, DF: DataFrame):
        self.spark = spark
        self.safeDF = DF
        self.filterDF = self.filterAndInfer()

        self.tableName = "covidData"
        self.filterDF.createOrReplaceTempView(self.tableName)

    def filterAndInfer(self):
        # Analysis of entries[0] yields:
        filterSchema = StructType([
            StructField("updated",StringType(),True), # UNIX timestamp
            StructField("country",StringType(),True),
            StructField("countryInfo/iso3", StringType(), True),
            StructField("cases", LongType(), True),
            StructField("deaths", LongType(), True),
            StructField("recovered", LongType(),True),
            StructField("active", LongType(), True),
            StructField("critical", LongType(), True),
            StructField("tests", LongType(), True),
            StructField("population",LongType(), True),
            StructField("continent", StringType(), True)
        ])

        filterDF = self.safeDF.select([sqlFn.col(c).cast(filterSchema[c].dataType) for c in filterSchema.fieldNames()])
        return filterDF

    def showAnomalies(self):
        """
        Show records where:
            cases != deaths + active + recovered
        NOTE: active = critical + non_critical

        Anomalies are never in the data,
        they are only ever in our understanding of it.
        """
        query = """
        select
            cases,
            deaths,
            active,
            recovered,
            critical,
            cases - (deaths + active + recovered) as `Error/Delta`
        from
            {tableName}
        where
            cases != (deaths + active + recovered)
        """.format(tableName=self.tableName)
        res = self.spark.sql(query)

        res.show(10)
        print(f"There are total {res.count()} anomalous records.\n\n")

    def deathsByCases(self,typ="min"):
        """
        Least/Most affected country among all the countries ( total death/total covid cases)
        :param typ: Either "max" or "min" as per requirement
        """
        query =  """
        select
            S.country as `Country Name`,
            S.`countryInfo/iso3` as `Country Code`,
            S.population as `Total Population`,
            S.cases as `Cases`,
            S.deaths as `Deaths`,
            deaths/if(cases=0,1,cases) as `Fraction of Population Affected`
        from
            {tableName} as S
        where
            deaths/if(cases=0,1,cases) = (select 
                                            {typ}(deaths/if(cases=0,1,cases)) 
                                        from 
                                            {tableName} 
                                        where 
                                            not `countryInfo/iso3` is null)
            and
            (not S.`countryInfo/iso3` is null)
        """.format(tableName = self.tableName,typ=typ)

        res = self.spark.sql(query)

        return showTable(res)

    def casesWise(self, typ = "max"):
        """
        Country with max/min covid cases.
        :param typ: Either "max" or "min" as per requirement
        """
        query = """
        select
            country as `Country Name`,
            `countryInfo/iso3` as `Country Code`,
            population as `Total Population`,
            cases as `Covid Cases`
        from
            {tableName} as T
        where
            cases = (select {typ}(cases) from {tableName} where not `countryInfo/iso3` is null) and
            not (T.`countryInfo/iso3` is null)
        """.format(tableName=self.tableName,typ=typ)

        res = self.spark.sql(query)

        return showTable(res)

    def totalCases(self):
        """
        Total covid cases in the world
        """
        query = "select sum(cases) as `Total Cases` from {tableName}".format(tableName=self.tableName)

        res = self.spark.sql(query)

        return showTable(res)

    def recoveryPerCase(self,typ="max"):
        """
        Country that handled the covid most/least efficiently ( total recovery/ total covid cases).
        :param typ: Either "max" or "min" as per requirement
        """
        query = """
        select
            country as `Country Name`,
            `countryInfo/iso3` as `Country Code`,
            population as Population,
            cases as `Total Cases`,
            recovered as `Total Recovered`,
            recovered/if(cases=0,1,cases) as `Fraction of Cases Recovered`
        from 
            {tableName}
        where
            recovered/if(cases=0,1,cases) = (select 
                                                {typ}(recovered/if(cases=0,1,cases)) 
                                            from
                                                {tableName}
                                            where
                                                `countryInfo/iso3` is not null)
            and
            `countryInfo/iso3` is not null
        """.format(tableName=self.tableName,typ=typ)

        res = self.spark.sql(query)

        return showTable(res)

    def criticalWise(self,typ="max"):
        """
        Show countries with most/least number of critical cases
        :param typ: Either "max" or "min" as per requirement
        """
        query = """
        select 
            country as `Country Name`,
            `countryInfo/iso3` as `Country Code`,
            cases as `Total Cases`,
            critical as `Total Critical Cases`
        from
            {tableName}
        where
            critical = (select {typ}(critical) from {tableName} where `countryInfo/iso3` is not null)
            and `countryInfo/iso3` is not null 
        """.format(tableName=self.tableName,typ=typ)

        res = self.spark.sql(query)

        return showTable(res)



