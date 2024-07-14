#!/usr/bin/env python
# coding: utf-8

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlFn
import time
import sys

spark = SparkSession.builder.master("local[*]").getOrCreate()

salesDF = spark.read.format("csv") \
    .option("inferschema","true").option("header","true") \
    .load("bigSales.csv").withColumn("Sales Id", monotonically_increasing_id())

productsDF = spark.read.format("csv") \
    .option("inferschema","true").option("header","true").load("products.csv")

# Data Format:
salesDF.printSchema()
productsDF.printSchema()

# Sneak peek:
# salesDF.show(5)
# productsDF.show()

# It's SQL'ing Time!
salesDF.createOrReplaceTempView("sales")
productsDF.createOrReplaceTempView("products")

joinedDF = salesDF.join(productsDF, salesDF["Product Id"] == productsDF["Product Id"], "fullouter") \
    .drop(salesDF["Product Id"]) # Product Id column is redundant

# Cached joinedDF as an action is to be performed 
nullsDF = joinedDF.filter(' is null or '.join(f"`{col}`" for col in joinedDF.columns) + "is null")

# nullsDF.show(5)

completeDF = joinedDF \
    .filter(
        # All records that are NULL in all columns except 'Sales Id'
        ' is not null or '.join(f"`{col}`" for col in joinedDF.columns if (col!="Sales Id")) + "is not null"
    ).persist()

# Q. Total amount spend by each customer:
def customerTotal(completeDF):
    # `Sales Id` is not null -> (Customer Id, Quantity, Product Price) not null
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy(completeDF["Customer Id"]) \
        .agg(sqlFn.sum(completeDF["Product Price"]*completeDF["Quantity"]).alias("Total Spent")) \
        .show()

# Q. Total spend on each Product: (Assumption: Product Id 1-to-1 Product Name)
def productSales(completeDF):
    completeDF.where(completeDF["Product Id"].isNotNull()) \
        .groupBy(completeDF["Product Name"]) \
        .agg( \
            sqlFn.sum(sqlFn.ifnull(completeDF["Product Price"],
                                   sqlFn.lit(0))*sqlFn.ifnull(completeDF["Quantity"],sqlFn.lit(0))) \
            .alias("Total Spent")).show()

# Q. Total amount of sales in each month:
def monthlySales(completeDF):
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy(
            sqlFn.year(completeDF["Date"]).alias("Year"),
            sqlFn.month(completeDF["Date"]).alias("Month")
        ) \
        .agg(
            sqlFn.sum(completeDF["Product Price"]*completeDF["Quantity"]).alias("Total Spent"),
            sqlFn.count(completeDF["Sales Id"]).alias("Total Orders"),
            sqlFn.sum(completeDF["Quantity"]).alias("Items Bought")
        ).orderBy("Year","Month").show()

# Q. Yearly Sales:
def yearlySales(completeDF):
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy(sqlFn.year("Date").alias("Year")) \
        .agg(
        sqlFn.sum(completeDF["Product Price"] * completeDF["Quantity"]).alias("Total Spent"),
        sqlFn.sum("Quantity").alias("Items Bought")
    ).show()

# Q. Quarterly Sales:
def quarterlySales(completeDF):
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy(
            sqlFn.year("Date").alias("Year"),
            (sqlFn.round((sqlFn.month("Date")-sqlFn.lit(1)) / sqlFn.lit(4),0) + 1).alias("Quarter")
        ) \
        .agg(
            sqlFn.sum(completeDF["Product Price"] * completeDF["Quantity"]).alias("Total Spent"),
            sqlFn.sum("Quantity").alias("Items Bought")
        ).orderBy("Year","Quarter").show()

# Q. Total number of orders by each category: 
def productOrders(completeDF):
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy(
            sqlFn.year("Date").alias("Year"),
            (sqlFn.round((sqlFn.month("Date")-sqlFn.lit(1)) / sqlFn.lit(4),0) + 1).alias("Quarter")
        ) \
        .agg(
            sqlFn.sum(completeDF["Product Price"] * completeDF["Quantity"]).alias("Total Spent"),
            sqlFn.sum("Quantity").alias("Items Bought")
        ).orderBy("Year","Quarter").show()

# Q. Top 5 ordered items:
def top5Products(completeDF):
    completeDF.where(completeDF["Product Id"].isNotNull()) \
        .groupBy(completeDF["Product Id"]) \
        .agg(
            sqlFn.max("Product Name").alias("Product Name"),
            sqlFn.sum("Quantity").alias("Items Bought"),
            sqlFn.count(completeDF["Sales Id"]).alias("Total Orders")
        ).orderBy(["Total Orders"],ascending=[0]).limit(5).show() # Order by "Items Bought" or "Total Orders"

# Q. Frequency of Customer visit:
def customerFrequency(completeDF):
    completeDF.where(completeDF["Customer Id"].isNotNull()) \
        .groupBy("Customer Id") \
        .agg(
            sqlFn.count(completeDF["Sales Id"]).alias("Frequency of Purchases")
        ).show()

# Q. Total sales by each country:
def countrySales(completeDF):
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy("Location") \
        .agg(
            sqlFn.sum(completeDF["Product Price"]*completeDF["Quantity"]).alias("Country Sales Amount")
        ).show()

# Q. Total sales by order source:
def platformSales(completeDF):
    completeDF.where(completeDF["Sales Id"].isNotNull()) \
        .groupBy("Source") \
        .agg(
            sqlFn.sum(completeDF["Product Price"]*completeDF["Quantity"]).alias("Source Sales Amount")
        ).show()

tests = 1
avg = 0
for t in range(1,tests+1):
    start = time.time()

    yearlySales(completeDF)

    if (t!= tests):
        sys.stdout.flush()

    end = time.time()
    avg = (avg*(t-1) + (end-start))/t
print("Average time: ",avg)

