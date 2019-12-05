from pymongo import MongoClient
from pymongo import ReadPreference
from pymongo import errors


class MongoModel:
    def __init__(self, user, password, instancia, db_name, collection_name):
        self.user = user
        self.password = password
        self.instancia = instancia        
        self.db_name = db_name       
        self.collection_name = collection_name
        self.strConnection = 'mongodb+srv://' + self.user + ':' + self.password + '@' + self.instancia
                
        return_dict = self.get_client() 
        if return_dict['success'] == False:
            raise Exception(return_dict['debug'] + '\n' + self.strConnection)
                
        self.client = return_dict['MongoClient']
               
        return_dict = self.get_database() 
        
        if return_dict['success'] == False:
            raise Exception(return_dict['debug'])
            
        self.database = return_dict['MongoDB']
        
        return_dict = self.get_collection() 
        
        if return_dict['success'] == False:
            raise Exception(return_dict['debug'])
        
        self.collection = return_dict["Collection"]
        
    #Retorna a conexão com a instancia do MOngo
    def get_client(self):
        try:
            return_dict = {}
            return_dict["success"] = True
            return_dict["get_client"] = "Executou corretamente"
            return_dict["MongoClient"] = MongoClient(self.strConnection, read_preference=ReadPreference.PRIMARY)
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
               
    
    #Retorna o banco de dados Mongo 
    def get_database(self):
        try:
            return_dict = {}
            return_dict["success"] = True
            return_dict["get_database"] = "Executou corretamente"
            return_dict["MongoDB"] = self.client[self.db_name]
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
       
        
    #Retorna a collection do banco de dados Mongo 
    def get_collection(self):
        try:
            return_dict = {}
            return_dict["success"] = True
            return_dict["get_collection"] = "Executou corretamente"
            return_dict["Collection"] = self.database[self.collection_name]
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
        
    # Insere um registro no MONGODB
    def insert_one(self, jsonData):  
        try:                      
            res = self.collection.insert_one(jsonData)
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["insert_one"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
            

    # Insere varios registros no MONGODB
    def insert_many(self, jsonData):
        try:
            res = self.collection.insert_many(jsonData)
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["insert_many"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
            
    
    # Deleta um registro no MONGODB
    def delete_one(self, jsonData):            
        try:
            res = self.collection.delete_one(jsonData)
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["delete_one"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
    
    # Deleta registros de acordo com o query
    def delete_many(self, query):
        try:
            res = self.collection.delete_many(query)
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["delete_many"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
        
           
    #Deleta todos os registros
    def delete_all(self):
        try:
            res = self.collection.delete_many({})
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["delete_all"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
                
    
    # Retorna  o primeito registro da collection no MONGO                
    def find_one(self):
        try:
            res = self.collection.find_one()
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["find_one"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["return return_dictfailure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e) 
        finally:
            return return_dict
       
    
    #Retorna  todos os registros da collection no MONGO    
    def find_all(self):
        try:
            res = self.collection.find({})
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["delete_all"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
        

    #Retorna todos os registros que cooincidam com a query na collection no MONGO   
    #O parametro query deve ter o formato chave:valor: "address": "Park Lane 38"
    def find_by_query(self,query):
        try:
            myquery = { query }
            res = self.collection.find(myquery)
            return_dict = {}
            return_dict["success"] = True
            return_dict["result"] = res
            return_dict["delete_all"] = "Executou corretamente"           
        except errors.AutoReconnect as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "AutoReconnect"
            return_dict["debug"] = str(e)
        except errors.BulkWriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "BulkWriteError"
            return_dict["debug"] = str(e)
        except errors.CollectionInvalid as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CollectionInvalid"
            return_dict["debug"] = str(e)
        except errors.ConfigurationError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConfigurationError"
            return_dict["debug"] = str(e)
        except errors.ConnectionFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ConnectionFailure"
            return_dict["debug"] = str(e)
        except errors.CursorNotFound as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "CursorNotFound"
            return_dict["debug"] = str(e)
        except errors.DocumentTooLarge as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DocumentTooLarge"
            return_dict["debug"] = str(e)
        except errors.DuplicateKeyError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "DuplicateKeyError"
            return_dict["debug"] = str(e)
        except errors.EncryptionError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "EncryptionError"
            return_dict["debug"] = str(e)
        except errors.ExceededMaxWaiters as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExceededMaxWaiters"
            return_dict["debug"] = str(e)
        except errors.ExecutionTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ExecutionTimeout"
            return_dict["debug"] = str(e)
        except errors.InvalidName as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidName"
            return_dict["debug"] = str(e)
        except errors.InvalidOperation as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidOperation"
            return_dict["debug"] = str(e)
        except errors.InvalidURI as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "InvalidURI"
            return_dict["debug"] = str(e)
        except errors.NetworkTimeout as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NetworkTimeout"
            return_dict["debug"] = str(e)
        except errors.NotMasterError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "NotMasterError"
            return_dict["debug"] = str(e)
        except errors.OperationFailure as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "OperationFailure"
            return_dict["debug"] = str(e)
        except errors.ProtocolError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ProtocolError"
            return_dict["debug"] = str(e)
        except errors.PyMongoError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "PyMongoError"
            return_dict["debug"] = str(e)
        except errors.ServerSelectionTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "ServerSelectionTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WTimeoutError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WTimeoutError"
            return_dict["debug"] = str(e)
        except errors.WriteConcernError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteConcernError"
            return_dict["debug"] = str(e)
        except errors.WriteError as e:
            return_dict["success"] = False
            return_dict["failure_message"] = "WriteError"
            return_dict["debug"] = str(e)
        finally:
            return return_dict
        
        