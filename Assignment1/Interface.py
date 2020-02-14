#!/usr/bin/python2.7
#
# Interface for the assignement
#
# Sheen Dullu

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute(dropTableIfExists(ratingstablename))

    cur.execute("CREATE TABLE " + ratingstablename + "(userid INTEGER, extraColon1 CHAR, movieid INTEGER, "
                                                 "extraColon2 CHAR, rating FLOAT, extraColon3 CHAR, timestamp BIGINT);")
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':',
                  columns=('UserID', 'extraColon1', 'MovieID', 'extraColon2', 'Rating', 'extraColon3', 'Timestamp'))
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN extraColon1, DROP COLUMN extraColon2, "
                                                "DROP COLUMN extraColon3, DROP COLUMN Timestamp;")
    cur.close()
    con.commit()


def dropTableIfExists(tablename):
    return "DROP TABLE IF EXISTS " + tablename


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()

    RANGE_TABLE_PREFIX = 'range_part'
    dividedRatings = 5/numberofpartitions

    for partition in range(0, numberofpartitions):
        fromRange = partition * dividedRatings
        toRange = fromRange + dividedRatings
        rangeTableName = RANGE_TABLE_PREFIX + str(partition)

        cur.execute(dropTableIfExists(rangeTableName))
        cur.execute(createTable(rangeTableName))

        if partition == 0:
            cur.execute("INSERT INTO " + rangeTableName + " (userid, movieid, rating) SELECT * FROM " + ratingstablename +
                                        " WHERE rating >= " + str(fromRange) + " AND rating <= " + str(toRange) + ";")
        else:
            cur.execute("INSERT INTO " + rangeTableName + " (userid, movieid, rating) SELECT * FROM " + ratingstablename +
                                        " WHERE rating > " + str(fromRange) + " AND rating <= " + str(toRange) + ";")
    cur.close()
    con.commit()


def createTable(rangeTableName):
    return "CREATE TABLE " + rangeTableName + " (userid INTEGER, movieid INTEGER, rating FLOAT);"


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()

    RROBIN_TABLE_PREFIX = 'rrobin_part'

    for partition in range(0, numberofpartitions):
        rRobinTableName = RROBIN_TABLE_PREFIX + str(partition)

        cur.execute(dropTableIfExists(rRobinTableName))
        cur.execute(createTable(rRobinTableName))
        cur.execute("INSERT INTO " + rRobinTableName + " (userid, movieid, rating) SELECT userid, movieid, rating "
                    "FROM (SELECT userid, movieid, rating, ROW_NUMBER() over() as rowNum FROM " +  ratingstablename +
                    ") AS pseudo WHERE mod(pseudo.rowNum-1, " + str(numberofpartitions) + ") = " + str(partition) + ";")
    cur.close()
    con.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()

    RROBIN_TABLE_PREFIX = 'rrobin_part'

    cur.execute(insertData(itemid, rating, ratingstablename, userid))
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
    totalRows = (cur.fetchall())[0][0]

    cur.execute(countTablesWith(RROBIN_TABLE_PREFIX))
    numberofpartitions = cur.fetchone()[0]

    pointer = (totalRows - 1) % numberofpartitions
    tableName = RROBIN_TABLE_PREFIX + str(pointer)

    cur.execute(insertData(itemid, rating, tableName, userid))

    cur.close()
    con.commit()


def countTablesWith(TABLE_PREFIX):
    return "SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + TABLE_PREFIX + "%';"


def insertData(itemid, rating, tableName, userid):
    return "INSERT INTO " + tableName + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");"


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    cur.execute(countTablesWith(RANGE_TABLE_PREFIX))
    numberofpartitions = cur.fetchone()[0]

    interval = 5 / numberofpartitions
    partition = findPartition(interval, rating)
    tableName = RANGE_TABLE_PREFIX + str(partition)

    cur.execute(insertData(itemid, rating, tableName, userid))

    cur.close()
    con.commit()


def findPartition(interval, rating):
    fromRange = 0
    toRange = interval
    partition = 0

    while True:
        if partition == 0:
            if fromRange <= rating <= toRange:
                return 0
            else:
                partition += 1
                fromRange = toRange
                toRange += interval
        else:
            if fromRange < rating <= toRange:
                return partition
            else:
                partition += 1
                fromRange = toRange
                toRange += interval



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()
