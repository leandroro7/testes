import pymysql as ms
from pymysql import cursors

class MysqlModel():
    def __init__(self, host, port, user, password, db, charset, tableName):
        self.host = host
        self.port = port
        self.user = user
        self.password = password                         
        self.db = db
        self.tableName = tableName
        self.charset = charset
        self.connection = self.get_connection()['result']
        self.cursor = self.get_cursor()
        
        if self.get_table_exists(self.tableName)['result'] == 0:
            self.create_table(self.tableName)
                        
        
    def get_connection(self):
        try:
            return_dict = {}
            return_conn = ms.connect(host=self.host, port = self.port, user=self.user, password=self.password, db=self.db, charset=self.charset, cursorclass=cursors.DictCursor)
            return_dict["success"] = True           
            return_dict["result"] = return_conn
        except ms.IntegrityError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "Integrity Error"
            return_dict["debug"] = str(e)
        except ms.ProgrammingError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "ProgrammingError"
            return_dict["debug"] = str(e)
        except ms.DataError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "DataError"
            return_dict["debug"] = str(e)
        except ms.NotSupportedError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "NotSupportedError"
            return_dict["debug"] = str(e)
        except ms.OperationalError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationalError"
            return_dict["debug"] = str(e)	
        except Exception as e :
            return_dict["success"] = False
            return_dict["failure_short"] = "Unknown Failure " + str(e)
		
        return return_dict
        
                    
    
    def get_cursor(self):
        if self.connection.open:
            return self.connection.cursor()
        else:
            self.get_connection()
            return self.connection.cursor()
            
    
    
    def get_table_exists(self, tableName):
        sqlQuery = "show tables like '{}'".format(tableName)  
        exists = self.execute_command(sqlQuery)
        return exists
    
    
    def create_table(self, tableName):
        sqlQuery = "CREATE TABLE {} (latitude decimal(12,10), longitude decimal(12,10), road TEXT(100), house_number TEXT(10), suburb TEXT(100), city TEXT(100), postcode TEXT(10), state TEXT(50), country TEXT(20), description TEXT(1000), primary key (latitude, longitude));".format(tableName)    
        res = self.execute_command(sqlQuery)
        return res        
    
    
    #Executa as querys de CRUD submetidas ao ddb
    def execute_select(self, sqlQuery):
        try: 
            return_dict = {}
            res = self.cursor.execute(sqlQuery)	
            return_dict["success"] = True
            return_dict["afected_rows"] = res
            return_dict["result"] = self.cursor.fetchall()
        except ms.IntegrityError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "Integrity Error"
            return_dict["debug"] = str(e)
        except ms.ProgrammingError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "ProgrammingError"
            return_dict["debug"] = str(e)
        except ms.DataError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "DataError"
            return_dict["debug"] = str(e)
        except ms.NotSupportedError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "NotSupportedError"
            return_dict["debug"] = str(e)
        except ms.OperationalError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationalError"
            return_dict["debug"] = str(e)	
        except Exception as e :
            return_dict["success"] = False
            return_dict["failure_short"] = "Unknown Failure " + str(e)
		
        return return_dict
    
    #Executa as querys de CRUD submetidas ao ddb
    def execute_command(self, sqlQuery):
        try:
            return_dict = {}
            res = self.cursor.execute(sqlQuery)	
            self.connection.commit()
            return_dict["success"] = True
            return_dict["result"] = res
        except ms.IntegrityError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "Integrity Error"
            return_dict["debug"] = str(e)
        except ms.ProgrammingError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "ProgrammingError"
            return_dict["debug"] = str(e)
        except ms.DataError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "DataError"
            return_dict["debug"] = str(e)
        except ms.NotSupportedError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "NotSupportedError"
            return_dict["debug"] = str(e)
        except ms.OperationalError as e :
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationalError"
            return_dict["debug"] = str(e)	
        except Exception as e :
            return_dict["success"] = False
            return_dict["failure_short"] = "Unknown Failure " + str(e)
		
        return return_dict