import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions



pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = ['id','data_iniSE','casos','ibge_code','cidade','uf','cep','latitude','longitude']

def txt_for_list(element, delimiter='|'):
    """Recebe um texto e retorna uma lista de elementos pelo delimitador"""
    return element.split(delimiter)
def list_for_dictionary(element, colunas):
    """passa a lista para dicionario"""
    return dict(zip(colunas,element))
def data_transformation(element):
    """Recebe um dicionário e cria um novo campo com ano-mes, retorna o mesmo dicionário com o novo campo"""
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element
def uf_pk(element):
    """recebe um dicionario e retorna uma tupla com o UF e o elemento (UF, dicionario)"""
    chave = element['uf']
    return (chave, element)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >>
        ReadFromText('C:/Users/calel/OneDrive/Documentos/Datasets/casos_dengue.txt', skip_header_lines=1)
    | "Passa o arquivo original de txt para lista" >> beam.Map(lambda a:a.split("|"))
    #| "Passa o arquivo original de txt para lista" >> beam.Map(txt_for_list)
    | "Passa de lista para dicionario" >> beam.Map(lambda a,b: dict(zip(b,a)),colunas_dengue)
    #| "Passa de lista para dicionario" >> beam.Map(list_for_dictionary, colunas_dengue)
    #| "Cria o campo ano_mes" >> beam.Map(data_transformation)
    | "Cria o campo ano_mes" >> beam.Map(lambda a: {'ano_mes': '-'.join(a['data_iniSE'].split('-')[:2]), **a})
    #| "Cria chave pelo estado uf" >> beam.Map(uf_pk)
    | "Cria chave pelo estado uf" >> beam.Map(lambda a: (a['uf'], a))
    | "agrupando os registros por uf" >> beam.GroupByKey()
    | "Mostrar resultados" >> beam.Map(print)
)
pipeline.run()
