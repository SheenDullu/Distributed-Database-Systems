#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import sys
import threading

THREADS = 5
RANGE_PARTITION = "rangepartition"


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    cur.execute("SELECT MIN("+SortingColumnName+"), MAX("+SortingColumnName+") FROM "+InputTable)
    minimumValue, maximumValue = cur.fetchone()

    partition = abs(maximumValue - minimumValue)/THREADS

    threads = []
    startIndex = minimumValue
    endIndex = minimumValue+partition
    for i in range(THREADS):
        if i > 0:
            startIndex = endIndex
            endIndex = endIndex+partition
        rangePartitionTable = RANGE_PARTITION + str(i)
        thread = threading.Thread(target=partitionAndSort, args=(InputTable, rangePartitionTable, SortingColumnName,
                                                                 startIndex, endIndex, openconnection))
        threads.append(thread)
        thread.start()

    cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    cur.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 1=2")

    for i in range(THREADS):
        threads[i].join()
        rangePartitionTable = RANGE_PARTITION + str(i)
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + rangePartitionTable)
        cur.execute("DROP TABLE IF EXISTS " + rangePartitionTable)



def partitionAndSort(InputTable, rangePartitionTable, SortingColumnName, startIndex, endIndex, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + rangePartitionTable)
    cur.execute("CREATE TABLE " + rangePartitionTable + " AS SELECT * FROM " + InputTable + " WHERE 1=2")
    if rangePartitionTable == 'rangepartition0':
        cur.execute("INSERT INTO " + rangePartitionTable +
                    " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName+" >= " + str(startIndex) +
                    " AND " + SortingColumnName + " <= " + str(endIndex) + " ORDER BY " + SortingColumnName + " ASC")
    else:
        cur.execute("INSERT INTO " + rangePartitionTable +
                    " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(startIndex) +
                    " AND " + SortingColumnName + " <= " + str(endIndex) + " ORDER BY " + SortingColumnName + " ASC")


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT MIN(" + Table1JoinColumn + "), MAX(" + Table1JoinColumn + ") FROM " + InputTable1)
    minimumValueTable1, maximumValueTable1 = cur.fetchone()
    cur.execute("SELECT MIN(" + Table2JoinColumn + "), MAX(" + Table2JoinColumn + ") FROM " + InputTable2)
    minimumValueTable2, maximumValueTable2 = cur.fetchone()

    minimumValue = min(minimumValueTable1, minimumValueTable2)
    maximumValue = max(maximumValueTable1, maximumValueTable2)

    partition = abs(maximumValue - minimumValue)/float(THREADS)
    threads = []
    startIndex = minimumValue
    endIndex = minimumValue + partition
    for i in range(THREADS):
        if i > 0:
            startIndex = endIndex
            endIndex = endIndex+partition
        rangePartitionTable = RANGE_PARTITION + str(i)
        thread = threading.Thread(target=partitionAndCreate, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,
                                                          startIndex, endIndex, rangePartitionTable, openconnection))
        threads.append(thread)
        thread.start()

    cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    cur.execute("CREATE TABLE " + OutputTable +
                " AS (SELECT * FROM " + InputTable1 + ", " + InputTable2 + " WHERE 1=2)")
    for i in range(THREADS):
        threads[i].join()
        rangePartitionTable = RANGE_PARTITION + str(i)
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + rangePartitionTable)
        cur.execute("DROP TABLE IF EXISTS " + rangePartitionTable)



def partitionAndCreate(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, startIndex, endIndex, rangePartitionTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + rangePartitionTable)
    cur.execute("CREATE TABLE " + rangePartitionTable + " AS SELECT * FROM " + InputTable1 + ", " + InputTable2 + " WHERE 1=2")
    if rangePartitionTable == 'rangepartition0':
        cur.execute("INSERT INTO " + rangePartitionTable +
                    " SELECT * FROM " + InputTable1 + " AS t1, " + InputTable2 + " AS t2" +
                    " WHERE t1." + Table1JoinColumn + " = t2." + Table2JoinColumn +
                    " AND t1." + Table1JoinColumn + " >= " + str(startIndex) + " AND t1." + Table1JoinColumn + " <= " + str(endIndex) +
                    " AND t2." + Table2JoinColumn + " >= " + str(startIndex) + " AND t2." + Table2JoinColumn + " <= " + str(endIndex))
    else:
        cur.execute("INSERT INTO " + rangePartitionTable +
                    " SELECT * FROM " + InputTable1 + " AS t1, " + InputTable2 + " AS t2" +
                    " WHERE t1." + Table1JoinColumn + " = t2." + Table2JoinColumn +
                    " AND t1." + Table1JoinColumn + " > " + str(startIndex) + " AND t1." + Table1JoinColumn + " <= " + str(endIndex) +
                    " AND t2." + Table2JoinColumn + " > " + str(startIndex) + " AND t2." + Table2JoinColumn + " <= " + str(endIndex))



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
