import json
import mysql.connector
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
        conn = mysql.connector.connect(user=self.username, password=self.password, host=self.host,
                database=self.db_name)
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

def main():
    #db = Database('properties.json')
    #cursor = db.getCursor()
    readCSV('assignment03-load.csv')
    

if __name__ == '__main__':
    main()
