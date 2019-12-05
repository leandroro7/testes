# Diretorio de Códigos do Testes 


### Instale as libs e suas dependencias
* pip3 install bson 
* pip3 install pymongo
* pip3 install dnspython
* pip3 install pymysql

1) Crie uma pasta para os arquivos do projeto

2) Copie o os arquivos e a pasta data_poins para a pasta criada

3) O arquivo de testes das funcionalidade é o Main.pay, siga as instruções nos comentario do arquivo para testas as funcionalidades desenvolvidas.


# Comentários
Nos arquivos existem coordenadas para locais de diferentes tipos, existem muitos tipos de localização, e para alguns certas informações como Rua, Numero não estão presentes,
Um exemplo é a loclização do link: https://nominatim.openstreetmap.org/reverse?lat=-30.06761588&lon=-51.23976111
do tipo <cycleway>

'<addressparts>'
    '<address29>'Parque Gigante'</address29>'
    '<cycleway>'Ciclovia Beira-Rio'</cycleway>'
    <suburb>Praia de Belas</suburb><
    city_district>Porto Alegre</city_district>
    <city>Porto Alegre</city>
    <county>Região Geográfica Imediata de Porto Alegre
    </county><state_district>Região Geográfica Intermediária de Porto Alegre</state_district>
    <state>Rio Grande do Sul</state>
    <postcode>90810-180</postcode>
    <country>Brasil</country>
    <country_code>br</country_code>
</addressparts>        

Para mais informações acesse: https://wiki.openstreetmap.org/wiki/Pt:Map_Features#Elementos_Principais

Para contornar essa situação criei mais uma coluna no DB chamada description que contem a informação em formato Texto, complementar as outras informações existentes no XML de retorno.


