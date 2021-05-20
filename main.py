import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions


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


dengue = (
    pipeline
    | "Read Dataset" >> 
        ReadFromText('casos_dengue.txt',skip_header_lines = 1)
    | "Text to list" >>
        beam.Map(text_to_list)
    | "List to dict" >>
        beam.Map(list_to_dict, columns_dengue)
    | "Show Results" >>
        beam.Map(print)
)
pipeline.run()