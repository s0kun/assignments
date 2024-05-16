COVID-19 Analysis API

This Python application provides a web server with various COVID-19 data analysis endpoints.
The data is loaded and processed using Apache Spark, and the results are served via HTTP.

Dependencies
    Python 3.x
    pyspark: For handling large-scale data processing
    http.server: Standard library module for creating HTTP servers.
    socketserver: Standard library module for network servers.

Install the dependencies using pip:
    pip install pyspark


Main File Overview
    main.py : This is the main entry point of the application.
    It initializes a Spark session, loads the data, processes the data,
    and sets up an HTTP server with various endpoints for COVID-19 data analysis.

        (Implemented in ./data_loader/loader.py)
        Loader(spark: SparkSession) class : The `Loader` class is responsible
        for loading data into a Spark DataFrame.
        `spark`: An instance of `SparkSession`

            loadJSON(self) -> DataFrame
            Returns: A Spark DataFrame containing the loaded JSON data.

        (Implemented in ./processing/proc.py)
        Proc(spark: SparkSession, rawDF: DataFrame) class: The `Proc`
        class handles data processing and analysis.
        `spark`: An instance of `SparkSession`
        `rawDF`: A Spark DataFrame containing the raw data

            deathsByCases(self, mode: str) -> str
            Parameters:
                `mode`: A string indicating whether to get the "max" or "min" deaths by cases.
            Returns: A string representation of the analysis result.

            casesWise(self, mode: str) -> str
            Parameters:
                `mode`: A string indicating whether to get the "max" or "min" cases.
            Returns: A string representation of the analysis result.

            totalCases(self) -> str
            Returns: A string representation of the total cases.

            recoveryPerCase(self, mode: str) -> str
            Parameters:
                `mode`: A string indicating whether to get the "max" or "min" recovery per case.
            Returns: A string representation of the analysis result.

            criticalWise(self, mode: str) -> str
            Parameters:
                `mode`: A string indicating whether to get the "max" or "min" critical cases.
            Returns: A string representation of the analysis result.

            filterDF: DataFrame
            This is an attribute/field of procObj that points to the filtered data obtained from rawDF

            showTable(df: DataFrame) -> str
                df: The DataFrame for which an HTML table representation is generated.
            Returns: A valid HTML string that represents the 'df' table


Running the Application

1. Ensure all dependencies are installed.
2. Run the application using the command:
    - python main.py
3. Access the server at `http://localhost:8000` to see the available endpoints.

API Endpoints

The following endpoints are available:

- `/mostAffected`: Analysis of the most affected regions by deaths.
- `/leastAffected`: Analysis of the least affected regions by deaths.
- `/highestCases`: Regions with the highest cases.
- `/lowestCases`: Regions with the lowest cases.
- `/totalCases`: Total number of cases.
- `/mostEfficient`: Regions with the highest recovery rate per case.
- `/leastEfficient`: Regions with the lowest recovery rate per case.
- `/highestCritical`: Regions with the highest number of critical cases.
- `/lowestCritical`: Regions with the lowest number of critical cases.

Each endpoint provides an analysis result in plain text.
