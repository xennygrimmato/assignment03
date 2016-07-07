import json
import mysql.connector
import MySQLdb
import csv
import datetime

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

'''
Product functions
'''

def getProductRow(csvRow):
    row = []
    row.append(0)
    row.append(csvRow[15].decode('utf8'))
    row.append(csvRow[17].decode('utf8'))
    row.append(csvRow[12].decode('utf8'))
    row.append(csvRow[16].decode('utf8'))
    return tuple(row)

def getUniqueProducts():
    unique_product_names = set()
    unique_products = []
    cnt = 0
    for row in small_data:
        if cnt == 0:
            cnt += 1
            continue
        if (row[15].decode('utf8'), row[18].decode('utf8')) not in unique_product_names:
            unique_product_names.add((row[15].decode('utf8'), row[18].decode('utf8')))
            unique_products.append(row)
    return unique_products

def addAllProducts(db, rows):
    add_product = """INSERT INTO product VALUES (%s, %s, %s, %s, %s)"""
    db.cursor.executemany(add_product, rows)
    db.connection.commit()

'''
CategoryProduct functions
'''
def product_category(products, categories):
    # map from product id to category id
    product_category_map = {}
    product_name_id_map = {} # map product name to its id
    category_name_id_map = {} # map category description to its id
    for product in products:
        product_name_id_map[product[1]] = product[0]
    for category in categories:
        category_name_id_map[category[2]] = category[0]

    cnt = 0
    for row in small_data:
        if cnt == 0:
            cnt += 1
            continue
        product_name = row[15]
        category_desc = row[19]
        product_id = product_name_id_map[product_name]
        category_id = category_name_id_map[category_desc]
        product_category_map[product_id] = category_id
        #print product_id, category_id
    return product_category_map

def getCategoryProductRows(product_category_map):
    rows = [] #[(category_id, product_id)]
    for x in product_category_map:
        rows.append((product_category_map[x], x))
    return rows

def addCategoryProduct(db, rows):
    add_p_c = """INSERT INTO category_product VALUES (%s, %s)"""
    db.cursor.executemany(add_p_c, rows)
    db.connection.commit()

'''
Order functions
'''
def userNameIdMap(users):
    # map first name + " " + last name -> user id
    user_name_id_map = {}
    for user in users:
        name = user[2] + " " + user[3]
        user_name_id_map[name] = user[0]
    return user_name_id_map

def convertToDateTime(dateStr):
    if len(dateStr) > 0:
        if '/' in dateStr:
            dateList = map(int, dateStr.split("/"))
            dateObj = datetime.datetime(dateList[0], dateList[1], dateList[2])
        elif '-' in dateStr:
            try:
                dateList = map(int, dateStr.split("-"))
                dateObj = datetime.datetime(dateList[0], dateList[1], dateList[2])
            except:
                dateObj = datetime.datetime(2003,5,20)
        return dateObj
    else:
        return datetime.datetime(2003,5,20)

def getAllOrders(user_name_id_map):
    rows = []
    for i in xrange(1, len(small_data)):
        row = small_data[i]
        if row[13] == 'NULL':
            continue
        name = row[1] + " " + row[2]
        oid = 0
        uid = user_name_id_map[name]
        amount = float(row[13]) * float(row[14])
        timestamp = convertToDateTime(row[10])
        status = row[11]
        rows.append((oid,uid,amount,timestamp,status))
    return rows

def addAllOrders(db, rows):
    add_order = """INSERT INTO orders VALUES (%s, %s, %s, %s, %s)"""
    db.cursor.executemany(add_order, rows)
    db.connection.commit()

'''
OrderProduct functions
'''
def orderProduct(row):
    

'''
Generic functions
'''
def select(db, tableName):
    # returns list of rows in table in the form of list of tuples
    query = """SELECT * FROM %s""" % (tableName)
    db.cursor.execute(query)
    rows = db.cursor.fetchall()
    #print rows
    return rows

def main():
    db = Database('properties.json')
    cursor = db.getCursor()
    readCSV('assignment03-load.csv')
    #unique_users = [getUserRow(row) for row in getUniqueUsers()]
    #addAllUsers(db, unique_users)
    #unique_categories = [getCategoryRow(row, idx) for idx,row in enumerate(getUniqueCategories())]
    #addAllCategories(db, unique_categories)
    #unique_products = [getProductRow(row) for row in getUniqueProducts()]
    #addAllProducts(db, unique_products)
    #products = select(db, "product")
    #categories = select(db, "category")
    #product_category_map = product_category(products, categories)
    #category_product = getCategoryProductRows(product_category_map)
    #addCategoryProduct(db, category_product)
    #users = select(db, "user")
    #user_name_id_map = userNameIdMap(users)
    #orders = getAllOrders(user_name_id_map)
    #addAllOrders(db, orders)
    orders = select(db, "orders")
    products = select(db, "products")



if __name__ == '__main__':
    main()
