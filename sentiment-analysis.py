from __future__ import division
import boto3
import os
import time
import pandas as pd
import matplotlib as plt
from botocore.exceptions import ClientError
from datetime import date
import json
import seaborn as sns
import numpy as np
import csv

#leemos los json de nuestro directorio local
file_list = []
for audio_file in os.listdir():
    if audio_file.split('.')[-1] in ['json']:
        file_list.append(audio_file)

audio_data1 = pd.DataFrame({'file_name': file_list})

print(audio_data1)

DIRNAME = r'/Users/franciscocobo/Dropbox/#Master IA UNIR/Bancolombia/Texto en json/'
OUTPUTFILE = r'/Users/franciscocobo/Dropbox/#Master IA UNIR/Bancolombia/sentiment-analysis.csv'

#función para coger los file path del directorio que se ha dado como entrada
def get_file_paths(dirname):
    file_paths = []
    for root, directories, files in os.walk(dirname):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    return file_paths

#para sortear el límite de 5000 bytes de texto de AWS comprehend
#se usa esta función que divide el texto en "chunks" de esa longitud
def start_comprehend_job(text):
    list_parts = []
    text_for_analysis = ''
    for sentence in text.split('|'):
        current_text = text_for_analysis + f'{sentence}.'

        if len(current_text.encode('utf-8')) > 5000:
            list_parts.append([len(text_for_analysis), text_for_analysis])
            text_for_analysis = f'{sentence}.'
        else:
            text_for_analysis += f'{sentence}.'

    list_parts.append([len(text_for_analysis), text_for_analysis])
    dict_comprehend = {}

    for t_parts in list_parts:
        comprenhend_client = boto3.client(service_name='comprehend', region_name = 'us-east-1')
        sentimentData = comprenhend_client.detect_sentiment(Text=t_parts[1], LanguageCode = 'es')

        dict_comprehend[t_parts[0]] = sentimentData
        dict_comprehend[t_parts[0]]['ratio'] = t_parts[0]/float(len(text))

    final_dict = {'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Mixed': 0}
    list_sentiments = ['Positive', 'Negative', 'Neutral', 'Mixed']

    for sentiment in list_sentiments:
        for key, value in dict_comprehend.items():
            final_dict[sentiment] += value.get('SentimentScore').get(sentiment) * value.get('ratio')

    return final_dict

#llamamos a la función transcribe para ver el sentimiento
def transcribe_file(json_file):
    with open(json_file, encoding = 'utf-8') as json_file:
        data = json.load(json_file)
        call_id = data['jobName']
        try:
            #cogemos el apartado de trasncript
            text = data['results']['transcripts'][0]['transcript']
            print(json_file)
            print(text)
        except:
            #print(Activity_id, 'label missing')
            print('label missing')
            return
    comprehend_results = start_comprehend_job(text)
    sentiment_df = pd.DataFrame(comprehend_results.items()).round(3)
    sentiment_df.rename(columns={0:'sentiment_class', 1:'sentiment_value'}, inplace = True)
    sentiment_df.insert(0, 'Call_id', call_id)

    return sentiment_df


files = get_file_paths(DIRNAME)
print(files)
for file in files:
    (filepath, ext) = os.path.splitext(file)
    file_name = os.path.basename(file)
    if ext == '.json':
        print(file)
        a = transcribe_file(file)
        with open(OUTPUTFILE, 'a') as f:
            try:
                a.to_csv(f, header = None, index = False)
                #print("done")
            except:
                print('label missing')
