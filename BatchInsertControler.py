import Functions as func
import requests
import xml.etree.ElementTree as ET

class BathInsertControler():
    def __init__(self, path, tableName):
        self.path = path     
        self.listaErros = []
        self.listaInserts = []
        self.tableName = tableName

    #Busco os dados complementares da localização no openstreetmap
    def GetAddressData(self, latitude, longitude):
        try:
            print('Buscando dados complementares das coordenadas {},{}'.format(latitude, longitude))
            link = 'http://nominatim.openstreetmap.org/reverse?lat={}&lon={}'.format(latitude, longitude)   
            data = requests.get(link)
            root = ET.fromstring(data.text)   
            
            #A variavel description recebe o valor do elemento <result>, nela estão todas as informações da localização 
            #Coleto essa informação pois nela existem dados nem sempre presentes no elemento <addressparts> 
            description = root.findall('.//result')[0].text
            
            
            #O elemento <road> contem o nome da rua, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/road')) > 0:
                road = root.findall('.//addressparts/road')[0].text
            else:
                road = 'ND'
            
            #O elemento <house_number> contem o numero na rua, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/house_number')) > 0:
                house_number = root.findall('.//addressparts/house_number')[0].text
            else:
                house_number = 'ND'
            
            #O elemento <suburb> contem o nome do bairro, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/suburb')) > 0:
                suburb = root.findall('.//addressparts/suburb')[0].text
            else:
                suburb = 'ND'
            
            #O elemento <city> contem o nome da cidade, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/city')) > 0:
                city = root.findall('.//addressparts/city')[0].text
            else:
                city = 'ND'
            
            #O elemento <postcode> contem o CEP, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/postcode')) > 0:
                postcode = root.findall('.//addressparts/postcode')[0].text
            else:
                postcode = 'ND'
                
            #O elemento <state> contem Estado, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/state')) > 0:
                state = root.findall('.//addressparts/state')[0].text
            else:
                state = 'ND'
                
            #O elemento <country> contem o Pais, mas nem todas as localizações possuem esse elemento
            if len(root.findall('.//addressparts/country')) > 0:
                country = root.findall('.//addressparts/country')[0].text
            else:
                country = 'ND'
                
            #Retorno um Dictionary com as informações coletadas
            addressValues = [latitude, longitude, road, house_number, suburb, city, postcode, state, country, description]
            addressKeys = ['latitude', 'longitude','road','house_number','suburb','city','postcode','state','country', 'description']
            address = dict(zip(addressKeys, addressValues))
            strValues = "values({},{},'{}','{}','{}','{}','{}','{}','{}','{}');".format(latitude, longitude, road, house_number, suburb, city, postcode, state, country, description)
            valuesToInsert = "insert into {} {}".format(self.tableName, strValues)
            return address, valuesToInsert
        except Exception as e:                
            self.listaErros.append('Ocoreu o erro NA URL:{} \n{}'.format(link, e))
            return None, None
    
    #Faz o parsing do arquivo linha a linha, complementando os dados e gerando as informações para o inset no DB Mongo
    def LerArquivo(self, path):
        ref_arquivo = open(path, 'r', encoding='utf-8')
        lista_de_linhas = ref_arquivo.readlines()   
        listaDados = []   
        
        ref_insertsMongo = open(path.replace('.txt','_insertsMongo.txt'), 'w', encoding='utf-8')
        ref_insertsMysql = open(path.replace('.txt','_insertsMysql.txt'), 'w', encoding='utf-8')       
                
        try:
            #Contador para sinalisar o final de um registro; necessário pois os dadosde uma localização estão espalhados em  3(tres) linhas
            cont = 0
            for linha in lista_de_linhas:
                linha = linha.strip().split(' ')
              
                #Coleto a Latitude em graus e em decimal
                if linha[0].find('Latitude') != -1:
                    listaDados.append(linha[1].strip())
                    listaDados.append(linha[len(linha)-1].strip())
                    cont = 1
                #Coleto a Longitude em graus e em decimal
                elif linha[0] == 'Longitude:':
                     listaDados.append(linha[1].strip())
                     listaDados.append(linha[len(linha)-1].strip())
                     cont = 2
                #Coleto a Distancia e o Bearing
                elif linha[0] == 'Distance:':
                    listaDados.append(linha[1].strip())
                    listaDados.append(linha[len(linha)-1].strip())
                    cont = 3     
                
                #Quando o contador chega ao 3(tres) tenho os dados referentes a uma ocalização
                if cont == 3:  
                    #BUsco os dados complementares a partir das Latidute e Longitude Decimal
                    mongoData, mysqlData = self.GetAddressData(listaDados[1], listaDados[3])  
                    #Insiro os dados enriquecidos na lista para inserção no DB Mongo
                    if type(mongoData) != None:
                        print('Gravando dados nos arquivos')
                        self.listaInserts.append('Dados das coordenadas {} / {} inseridos com sucesso \n'.format(listaDados[1], listaDados[3]))
                        ref_insertsMongo.write(str(mongoData)+'\n')
                        ref_insertsMysql.write(str(mysqlData)+'\n')                        
                        listaDados = []           
                        cont = 0      
                    else:
                        cont = 0 
                        continue
                    
        except Exception as e:                
            self.listaErros.append('Ocoreu o erro: {}'.format(e))           
        finally:
            ref_arquivo.close()
            ref_insertsMongo.close()
            ref_insertsMysql.close()
        

    def Run(self):
        
        function = func.Functions()
        pathTxt = function.get_files_dir(self.path, '.txt')      
        
        #Para cada arquivo localizado no diretorio informado
        for arquivo in pathTxt:            
            try:
                ref_log_erros = open(arquivo.replace('.txt','_log_erros.txt'), 'w', encoding='utf-8')
                ref_log_inserts = open(arquivo.replace('.txt','_log_inserts.txt'), 'w', encoding='utf-8')
                print('Lendo o arquivo: {}'.format(arquivo))
                self.LerArquivo(arquivo)               
            except Exception as e: 
                self.listaErros.append('Ocoreu o erro: {}'.format(e))                
                continue
            finally:
                ref_log_erros.write('Erros no arquivo {}'.format(arquivo) + '\n')
                for erro in self.listaErros:                    
                    ref_log_erros.write(erro + '\n')
                
                ref_log_inserts.write('Registros no arquivo {}'.format(arquivo) + '\n')
                for insert in self.listaInserts:
                    ref_log_inserts.write(insert + '\n')
                    
                ref_log_erros.close()                    
                ref_log_inserts.close()
                         
        return self.listaInserts, self.listaErros