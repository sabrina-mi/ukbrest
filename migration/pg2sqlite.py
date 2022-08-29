import psycopg2
import sqlite3
import sys
import datetime

def table2sqlite(table):
    query = "select column_name,data_type from information_schema.columns where table_name = '%s'" % table
    pgCursor.execute(query)
    columns = pgCursor.fetchall()
    print("CREATE TABLE")
    createTable(table, columns)
    pgCursor.execute("SELECT * FROM %s" % table)
    rows=pgCursor.fetchall()
    print("INSERT INTO")
    for row in rows:
        addRow(table, row)

def createTable(table, columns):
    sqliteCursor.execute("DROP TABLE IF EXISTS %s" % table)
    query = "CREATE TABLE %s (" % table
    query += ",".join("'%s' %s" % (col[0], col[1].split()[0].upper()) for col in columns)
    query +=")"
    sqliteCursor.execute(query)
    sqliteConnection.commit()

def value(x):
    if x is None:
        return("null")
    elif x == True:
        return "1"
    elif x == False:
        return "0"
    elif isinstance(x, str):
        return("'%s'" % x.replace("'", "''"))
    elif isinstance(x, datetime.datetime):
        return("'%s'" % str(x))
    else:
        return(str(x))

def addRow(table, row):
    query = ("INSERT INTO %s VALUES (" % table)
    query += ",".join(value(x) for x in row)
    query += ")"
    sqliteCursor.execute(query)


if __name__ == '__main__':
    file = sys.argv[1]
    # open sqlite database connection
    sqliteConnection = sqlite3.connect("ukbrest.db")
    sqliteCursor = sqliteConnection.cursor()

    # connect to ukbrest postgreSQL database
    pgConnectString = "host='localhost' dbname='ukbrest' user='postgres' password=''"
    pgConnection=psycopg2.connect(pgConnectString)
    pgCursor = pgConnection.cursor()

    # list of tables to migrate
    file = open(sys.argv[1], "r") 
    tables = file.read().splitlines()
    file.close()

    for table in tables:
        print("Loading table: ", table)
        table2sqlite(table)  
        sqliteConnection.commit()
        sqliteCursor.execute("SELECT * FROM %s LIMIT 3" % table)
        print("SELECT *")
        print(sqliteCursor.fetchall())

    sqliteConnection.close()
    pgConnection.close()
