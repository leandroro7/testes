import BatchInsertControler as dg
import MongoModel as da
import MysqlModel as ma
import Functions as fu

class Main():
    def __init__(self, path):
        self.path = path

    # Le o conteuso dos arquivos de coordenadas fornecidos e gera arquivos com inserts para msql e monto
    def genarate_inserts(self):
        data = dg.BathInsertControler(self.path, 'teste4All')
        inserts, erros = data.Run()     
   

    #Le os arquivos de insert gerados e para cada linha executa o insert no db
    def insert_mysql_data(self, host, port, user, password, db, charset, tableName):
        #  host, port, user, password, db, charset, tableName
        mysql = ma.MysqlModel(host, port, user, password, db, charset, tableName)
        
        #Leio os arquivos *mysql do diretorio da dados
        function = fu.Functions()
        pathTxt = function.get_files_dir(self.path, '_insertsMysql.txt')      
           
        listaErros = []   
        #Para cada arquivo localizado no diretorio informado        
        for arquivo in pathTxt:
            try:
                #Printo no terminal o nome do arquivo, apenas para feedback visual
                print(arquivo)
                count = 0
                #Crio o arquivo de dados e o arquivo de logs que irei gravar
                ref_arquivo = open(arquivo, 'r', encoding='utf-8')
                log = arquivo.replace('.txt','_INSERT_LOG.txt')
                ref_log_insert_mysql = open(log, 'w', encoding='utf-8')
              
                lista_de_linhas = ref_arquivo.readlines()         
                #Percorro o arquivo de dados linha por lilnha  
                for linha in lista_de_linhas:
                    res = mysql.execute_command(linha)
                    count += 1
                    if res["success"] == False:
                        listaErros.append(res["debug"])
                        print('Erro no arquivo \n {}'.format(arquivo, linha))
                    else:
                        print('Inseriu a linha {} do arquivo {}'.format(count, arquivo))
                   
            except Exception as e:
                listaErros.append('Ocoreu o erro: {} \n {}'.format(str(e), linha))                
                continue
            finally:
                for erro in listaErros:
                    ref_log_insert_mysql.write(erro["debug"])
                    ref_log_insert_mysql.write('\n')
                
                ref_arquivo.close()    
                ref_log_insert_mysql.close()


    ''' PARA LIMPARA ABASE DE DADOS EXECUTA FUNÇÃO limpa_base_mysql
    '''
    def limpa_base_mysql(self, host, port, user, password, db, charset, tableName):
        mysql = ma.MysqlModel(host, port, user, password, db, charset, tableName)
        sql = 'DELETE FROM teste4All'
        res = mysql.execute_command(sql)
        return res

    
    def insert_mongo_data(self, user, password, instancia, db_name, collection_name):
        model = da.MongoModel(user, password, instancia, db_name, collection_name)     
        function = fu.Functions()
        pathTxt = function.get_files_dir(self.path, '_insertsMongo.txt')      
        
        listaErros = []
        #Para cada arquivo localizado no diretorio informado
        for arquivo in pathTxt:
            ref_arquivo = open(arquivo, 'r', encoding='utf-8')
            log = arquivo.replace('.txt','_INSERT_LOG.txt')   
            ref_log_insert_mongo = open(log, 'w', encoding='utf-8')
            try:        
                lista_de_linhas = ref_arquivo.readlines()    
                lista_limpa = []
                for linha in lista_de_linhas:
                    if linha != None:
                        lista_limpa.append(linha)                
                try:
                    model.insert_many([dict(eval(linha)) for linha in lista_limpa])
                except Exception as err:                   
                    print(str(err))
                    continue
                    
            except Exception as e:
                listaErros.append('Ocoreu o erro: {}'.format(str(e)))                             
            finally:
                for erro in listaErros:
                    ref_log_insert_mongo.write(erro)
                    ref_log_insert_mongo.write('\n')
                    
            ref_arquivo.close() 
            ref_log_insert_mongo.close()

    def test_mongo_data(self):
        model = da.MongoModel('admin', '(teste4All)', 'cluster0-9u5so.mongodb.net/test?retryWrites=true&w=majority', 'teste4All', 'teste4All')
        model.find_one()
        
#//////////////////////////////////////////////////////////////////////////////////
#  FUNÇãO DE ENTRADA
#//////////////////////////////////////////////////////////////////////////////////

''' NO PARAMETRO PATH, INFORME A PASTA AONDE FORAL SALVOS OS ARQUIVOS DE DADOS EDNTRO DA PASTA DO PROJETO
'''
path = 'data_points/'
main = Main(path = 'data_points/')

#//////////////////////////////////////////////////////////////////////////////////
#  GERAÇO DE ARQUIVOS PARA INSERT
#//////////////////////////////////////////////////////////////////////////////////

''' PARA GERAR OS ARQUIVOS COM DADOS PARA UTILIZAÇÃO NAS FUNÇÕES DE INSERT  DESCOMENTE EXECUTE O TRECHO DE CÓDIGO ABAIXO
OBSERVE QUE OS ARQUIVOS COM OS DADOS BASICOS DEVEM ESTAR NO DIRETORIO INFORMADO

res = main.genarate_inserts()
'''
    
''' ESTOU UTILIZANDO UMA INSTANCIA DO MYSQL NA AMAZON: 'teste4all.c3bcgryswpmu.us-east-2.rds.amazonaws.com'
    UTILIZE A MESMA INSTANCIA OU ALTERE PARA UMA  DE SUA PREFERENCIA
'''

''' CASO OPTE POR UTILIZAR A MESMA INSTANCIA, EXECUTE A LIMPESA DA TABELA DESCOMENTE O TRECHO DE CÓDIGO ABAIXO E EXECUTE A FUNÇÃO  LIMPA_BASE_MYSQL

try:
    host = 'teste4all.c3bcgryswpmu.us-east-2.rds.amazonaws.com' 
    port = 3306
    user = 'admin'
    password = 'teste4All'
    db = 'teste4All'
    charset = 'utf8'
    tableName = 'teste4All'
    res = main.limpa_base_mysql(host, port, user, password, db, charset, tableName)
except Exception as e:
    print(str(e))
    
'''

#//////////////////////////////////////////////////////////////////////////////////
#  MYSQL DB
#//////////////////////////////////////////////////////////////////////////////////

''' PARA INSERIR DADOS NO MYSQL DESCOMENTE EXECUTE O TRECHO DE CÓDIGO ABAIXO
    OBSERVE QUE PARA EXECUTAR ESSA FUNÇÃO OS ARQUIVOS JA DEVEM HAVER SIDO GERADOS PELA FUNÇÃO genarate_inserts

try:
    host = 'teste4all.c3bcgryswpmu.us-east-2.rds.amazonaws.com' 
    port = 3306
    user = 'admin'
    password = 'teste4All'
    db = 'teste4All'
    charset = 'utf8'
    tableName = 'teste4All'
    res = main.insert_mysql_data(host, port, user, password, db, charset, tableName)
except Exception as e:
    print(str(e))
'''

#//////////////////////////////////////////////////////////////////////////////////
#  MONGO DB
#//////////////////////////////////////////////////////////////////////////////////

''' PARA INSERIR DADOS NO MONGODB DESCOMENTE EXECUTE O TRECHO DE CÓDIGO ABAIXO
    
    Estou utilizando uma instancia free @cluster0-9u5so.mongodb.net
    Utilize a mesma instancia ou altere para uma  de sua preferencia
    

try:
    user = 'admin'
    password = '(teste4All)'
    instancia = 'cluster0-9u5so.mongodb.net/test?retryWrites=true&w=majority'
    db_name =  'teste4All'
    collection_name = 'teste4All'  
    
    model = da.MongoModel(user, password, instancia, db_name, collection_name)  
    
    model.delete_all()
    
    main.insert_mongo_data(user, password, instancia, db_name, collection_name)
    
except Exception as e:
    print(str(e))
'''





