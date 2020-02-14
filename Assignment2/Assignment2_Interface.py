#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    # #Implement RangeQuery Here.
    con = openconnection
    cur = con.cursor()

    cur.execute("SELECT * FROM RangeRatingsMetadata")
    numberOfTables = cur.fetchall()
    for row in numberOfTables:
        tableName = "RangeRatingsPart" + str(row[0])

        if not ((ratingMinValue > row[2]) or (ratingMaxValue < row[1])):
            cur.execute("SELECT * FROM " + tableName + " WHERE rating >= " + str(ratingMinValue) +
                        " AND rating <= " + str(ratingMaxValue))
            results = cur.fetchall()
            with open(outputPath, "a") as file:
                for result in results:
                    file.write(
                        str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")

    cur.execute("SELECT * FROM RoundRobinRatingsMetadata;")
    numberOfPartitions = cur.fetchall()[0][0]
    for partition in range(0, numberOfPartitions):
        tableName = "RoundRobinRatingsPart" + str(partition)
        cur.execute("SELECT * FROM " + tableName + " WHERE rating >= " + str(ratingMinValue) +
                    " AND rating <= " + str(ratingMaxValue))
        results = cur.fetchall()
        with open(outputPath, "a") as file:
            for result in results:
                file.write(
                    str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    con = openconnection
    cur = con.cursor()

    cur.execute("SELECT * FROM RangeRatingsMetadata")
    numberOfTables = cur.fetchall()
    for row in numberOfTables:
        if row[0] == 0 and row[1] <= ratingValue <= row[2]:
            partitionNumber = row[0]
        elif row[1] < ratingValue <= row[2]:
            partitionNumber = row[0]

    tableName = "RangeRatingsPart" + str(partitionNumber)
    cur.execute("SELECT * FROM " + tableName + " WHERE rating = " + str(ratingValue) )
    results = cur.fetchall()
    with open(outputPath, "a") as file:
        for result in results:
            file.write(
                str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")

    cur.execute("SELECT partitionnum FROM RoundRobinRatingsMetadata;")
    numberOfPartitions = cur.fetchall()[0][0]

    for partition in range(0, numberOfPartitions):
        tableName = "RoundRobinRatingsPart" + str(partition)
        cur.execute("select * from " + tableName + " where rating = " + str(ratingValue))
        results = cur.fetchall()
        with open(outputPath, "a") as file:
            for result in results:
                file.write(
                    str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")