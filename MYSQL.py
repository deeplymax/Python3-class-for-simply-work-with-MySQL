import mysql.connector

class MYSQL:
    host = "localhost"
    user = "root"
    password = ""
    databaseName =  "myDatabase"
    database = mysql.connector.connect()


    def __init__(self, host, user, password, databaseName):
        self.host = host
        self.user = user
        self.password = password
        self.databaseName = databaseName


    def connect(self):
        """
        :return: mysql.connector object
        """
        self.database = mysql.connector.connect(
            host = self.host,
            user = self.user,
            passwd = self.password,
            database = self.databaseName
        )
        return self.database


    def query(self, query, commit=False):
        """
        :param query: query string
        :param commit: Flag on whether to commit after the request
        :return: cursor object
        """
        mycursor = self.database.cursor()
        mycursor.execute(query)
        if(commit):
            self.database.commit()
        return mycursor


    def select(self, table, *args):
        """
        :param table: table in db
        :param args: colums which need to select
        :return: json (column : value)
        """
        genQ = "SELECT {} FROM {}".format(','.join(['`' + str(it) + '`' for it in args]),  table)
        print(genQ)
        mycursor = self.query(genQ)
        q = mycursor.fetchall()

        #to JSON
        names = [i[0] for i in mycursor.description]
        jsonAnswer = []
        for it in range(len(q)):
            jsonAnswer.append(dict())
            for x in range(len(names)):
                jsonAnswer[it][names[x]] = q[it][x]
        return jsonAnswer


    def insert(self, table, **kwargs):
        """
        :param table: table in db
        :param kwargs: columnName = Value
        :return: ok or exception
        """
        columns = [it for it in kwargs]
        values = [kwargs[it] for it in kwargs]
        q = "INSERT INTO {} ({}) VALUES ({})".format('`' + table + '`',
                                                     ','.join(['`' + str(it) + '`' for it in columns]),
                                                     ','.join('\'' + str(it) + '\'' for it in values))
        self.query(q, commit=True)
        return "Ok! {}".format(q)


    def delete(self, table, args):
        """
        :param table: table in bd
        :param args: string with param which must delete
        :return:
        """
        q = "DELETE FROM {} WHERE {}".format('`' + table + '`', args)
        self.query(q, commit=True)


    def update(self, table, args, **kwargs):
        """
        :param table: table in db
        :param args: where will update
        :param kwargs: what will update
        :return:
        """
        columns = [str(it + "=" + '\'' + kwargs[it] + '\'') for it in kwargs]
        q = "UPDATE {} SET {} WHERE {}".format('`' + table + '`', ','.join([str(it) for it in columns]), args)
        self.query(q, commit=True)

    def replace(self, table, **kwargs):
        """
        :param table:
        :param kwargs:
        :return:
        """
        columns = [it for it in kwargs]
        values = [kwargs[it] for it in kwargs]
        q = "REPLACE INTO {} ({}) VALUES ({})".format('`' + table + '`',
                                                      ','.join(['`' + str(it) + '`' for it in columns]),
                                                      ','.join('\'' + str(it) + '\'' for it in values))
        self.query(q, commit=True)