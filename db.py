import json
import mysql.connector
import MySQLdb
import csv

class Database:
    def __init__(self, propertiesFile):
        data = self.getProperties(propertiesFile)
        self.username = data['username']
        self.password = data['password']
        self.host = data['host']
        self.db_name = data['db_name']

    def getProperties(self, propertiesFile):
        try:
            with open(propertiesFile) as f:
                data = json.load(f)
                return data
        except Exception:
            return None

    def getConnection(self):
        #conn = mysql.connector.connect(user=self.username, password=self.password, host=self.host, database=self.db_name)
        conn = MySQLdb.connect(self.host, self.username, self.password, self.db_name)
        self.connection = conn

    def getCursor(self):
        try:
            self.cursor = self.connection.cursor()
            return self.cursor
        except Exception:
            self.getConnection()
            self.cursor = self.connection.cursor()
            return self.cursor

    def executeQueryFromFile(self, filePath):
        with open(filePath, 'r') as f:
            sql_command = f.read()
            sql_command = sql_command.strip()
            self.cursor.execute(sql_command)

small_data = []
def readCSV(filePath):
    with open(filePath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            small_data.append(row)


'''
User functions
'''
def getUserRow(csvRow):
    row = []
    row.append(0)
    row.append("NONE")
    row.append(csvRow[1].decode('utf8'))
    row.append(csvRow[2].decode('utf8'))
    row.append(csvRow[0].decode('utf8'))
    row.append(csvRow[3].decode('utf8'))
    row.append(csvRow[4].decode('utf8'))
    row.append("NONE")
    row.append(csvRow[6].decode('utf8'))
    row.append(csvRow[7].decode('utf8'))
    row.append(csvRow[8].decode('utf8'))
    row.append(csvRow[9].decode('utf8'))
    #print len(row)
    print row
    return tuple(row)

def fastAdd(db, query, rows, START_POINT, END_POINT, BATCH_SIZE=100):
    '''
    Adding multiple rows in bulk would reduce overhead
    So, send groups of 100 rows to the db for insertion
    #TODO: Run each group in a separate thread with a new connection
    '''
    for i in xrange(START_POINT, END_POINT + 1, BATCH_SIZE):
        # [i: i + 100]
        rows_to_add = rows[i: min(END_POINT, i + BATCH_SIZE - 1)]
        print "Inserting {0} to {1}".format(i, min(END_POINT, i + BATCH_SIZE - 1))
        db.cursor.executemany(query, rows_to_add)
        print "Inserted!"
        db.connection.commit()

def addAllUsers(db, rows):
    cnt = 0
    for row in rows:
        add_user = """INSERT INTO user VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        db.cursor.execute(add_user, row)
        db.connection.commit()
        print cnt
        cnt += 1

def getUniqueUsers():
    unique_usernames = set()
    unique_rows = list()
    for row in small_data:
        name = row[1].decode('utf8') + " " + row[2].decode('utf8')
        if name not in unique_usernames:
            unique_rows.append(row)
            unique_usernames.add(name)
    return unique_rows

'''
Category functions
'''
def addAllCategories(db, rows):
    cnt = 0
    for row in rows:
        add_category = """INSERT INTO category VALUES (%s, %s, %s)"""
        db.cursor.execute(add_category, row)
        db.connection.commit()
        print cnt
        cnt += 1

def getUniqueCategories():
    unique_descriptions = set()
    unique_categories = list()
    cnt = 0
    for row in small_data:
        if cnt == 0:
            cnt += 1
            continue
        desc = row[19].decode('utf8')
        if desc not in unique_descriptions:
            unique_categories.append(row)
            unique_descriptions.add(desc)
    return unique_categories

def getCategoryRow(csvRow, cnt):
    row = []
    row.append(0)
    row.append("Category {0}".format(cnt))
    row.append(csvRow[19].decode('utf8'))
    return tuple(row)

def rowLen(row):
    return len(row)

def main():
    db = Database('properties.json')
    cursor = db.getCursor()
    readCSV('assignment03-load.csv')
    #unique_users = [getUserRow(row) for row in getUniqueUsers()]
    #addAllUsers(db, unique_users)
    unique_categories = [getCategoryRow(row, idx) for idx,row in enumerate(getUniqueCategories())]
    addAllCategories(db, unique_categories)


if __name__ == '__main__':
    main()
