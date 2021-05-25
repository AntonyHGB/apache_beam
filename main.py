from __future__ import division
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
import re

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

columns_dengue= [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude'
                ]

def list_to_dict (element,columns):
    return dict (zip(columns, element))

def text_to_list(element, divisor = '|'):
    return element.split(divisor)

def treats_data(element):
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element

def key_uf(element):
    key = element['uf']
    return (key,element)

def cases_dengue(element):
    uf, records = element
    for record in records:
        if bool(re.search(r'\d',record['casos'])):
            yield (f"{uf}-{record['ano_mes']}", float(record['casos']))
        else:
             yield (f"{uf}-{record['ano_mes']}", 0.0)

def key_uf_year_month_list(element):
    data, mm, uf = element
    year_month = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{year_month}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def rounds(element):
    key, mm = element
    return (key, round(mm, 1))

# dengue = (
#     pipeline
#     | "Read Dataset Dengue" >> ReadFromText('casos_dengue.txt',skip_header_lines = 1)
#     | "Text to list Dengue" >> beam.Map(text_to_list)
#     | "List to dict Dengue" >> beam.Map(list_to_dict, columns_dengue)
#     | "Treats Data Dengue" >> beam.Map(treats_data)
#     | "Key of UF Dengue" >> beam.Map(key_uf)
#     | "GroupBy Key Dengue" >> beam.GroupByKey()
#     | "Unzip Cases Dengue" >> beam.FlatMap(cases_dengue)
#     | "Sum cases Dengue">> beam.CombinePerKey(sum)
#     | "Show Results Dengue" >> beam.Map(print)   
# ) 

chuvas = (
    pipeline
    | "Read Dataset Chuvas" >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | "Text to list Chuvas" >> beam.Map(text_to_list, divisor=',')
    | "Key UF-YEAR-MONTH Chuvas" >> beam.Map(key_uf_year_month_list)
    | "Sum Key Chuvas" >> beam.CombinePerKey(sum)
    | "Rounds Chuvas" >> beam.Map(rounds)
    | "Show Results Chuvas" >> beam.Map(print)
)
pipeline.run()