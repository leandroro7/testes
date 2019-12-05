import os

class Functions:
    def __init__(self):
        self.AddressTypes = self.GetAddressTypes()
        
    def GetAddressTypes(self):
        return ['aerialway','aeroway','amenity','emergency','geological','highway','sidewalk','cycleway','busway','historic','landuse','amenity','barrier','boundary','building','craft']
    
    def get_files_dir(self, path, end_name_arq):
         # listo os arquivos, pastas e subpastas do diretorio informado em path
        caminhoAbsoluto = os.path.abspath(path)
        pathTxt = []
        
        #Monto o caminho absoluto do arquivo
        for pastaAtual, subPastas, arquivos  in os.walk(caminhoAbsoluto):
            pathTxt.extend([os.path.join(pastaAtual,arquivo) for arquivo in arquivos if arquivo.find(end_name_arq) > 0])
        
        return pathTxt   
        