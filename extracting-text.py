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

#creación de una lista con los archivos de audio
file_list = []
for audio_file in os.listdir():
    if audio_file.split('.')[-1] in ['mp4', 'WAV', 'wav']:
        file_list.append(audio_file)
print(file_list)

#creación de un dataframe con los archivos de audio
audio_data = pd.DataFrame({'file_name' : file_list})

#llamamos a los servicios de AWS
s3 = boto3.client('s3')
response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]
for bucket in buckets:
    print(bucket)

#seleccionamos la región y creamos el bucket donde trabajaremos
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
bucket_name = 'renewalfinal'
client_s3 = boto3.client('s3')
client_s3.create_bucket(Bucket = bucket_name)

#subimos los audios al bucket seleccionado
for audio_file in audio_data.file_name.values:
    print(audio_file)
    client_s3.upload_file(audio_file, bucket_name, audio_file)

#definimos la ubicación de los buckets donde irán los audios
for index, row in audio_data.iterrows():
    bucket_location = boto3.client('s3').get_bucket_location(Bucket = bucket_name)
    object_url = f"https://{bucket_name}.s3.amazonaws.com/{row['file_name'].replace(' ', '+')}"
    audio_data.at[index, 'url'] = object_url
    print(object_url)

#aquí podríamos definir un diccionario
'''
data_key = "vocabulario.txt"
data_location = 's3://{}/{}'.format(bucket_name, data_key)

transcribe_keyword = pd.read_csv(data_location, header = 'infer', names = ['Key'])
x = list(transcribe_keywords['Key'])
client_transcribe = boto3.client('transcribe', region_name = 'us-east-1')
response = client_transcribe.create_vocabulary(VocabularyName = 'spa_vocab', LanguageCode = 'es-ES', Phrases = x)
print(response)

'''
#definimos client_transcribe
client_transcribe = boto3.client('transcribe', region_name = 'us-east-1')

#creamos la función de transcribe
def start_transcription(bucket, job_name, file_url, wait_process = True):
    client_transcribe.start_transcription_job(
        TranscriptionJobName = job_name,
        Media = {'MediaFileUri': file_url},
        MediaFormat = 'wav',
        LanguageCode = 'es-ES',
        OutputBucketName = bucket,
        Settings =
        {
        #'VocabularyName' :
        'ShowSpeakerLabels': True,
        'MaxSpeakerLabels': 2
        }
    )
    if wait_process:
        while True:
            status = client_transcribe.get_transcription_job(TranscriptionJobName = job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            print("Not ready yet...")
            time.sleep(20)

        print('Transcription finished')
        return status


#borramos los jobs que queden pendientes
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('renewalfinal')
for s3_object in my_bucket.objects.all():
    path, filename = os.path.split(s3_object.key)
    if filename.split('.')[-1] in ['wav', 'WAV']:
        print(filename)
        client_transcribe = boto3.client('transcribe', region_name = 'us-east-1')
        try:
            client_transcribe.delete_transcription_job(TranscriptionJobName = filename)
        except:
            print ("missed", filename)



#iteramos sonre las URLs de los audios y llamamos a la funcion de transcribe definida anteriormente
for index, row in audio_data.iterrows():
    print(f'{row.file_name}', row.url)
    #CHECK THIS
    #client_transcribe.delete_transcription_job(TranscriptionJobName=f'{row.file_name}')
    #llamo a s3 con boto3 y los guardo en alguna variable
    #ya teniendo la lista de los archivos y así me crea un nombre diferente para cada job
    start_transcription(bucket_name, f'{row.file_name}', row.url, wait_process = False)
    audio_data.at[index, 'transcription_url'] = f"https://{bucket_name}.s3.amazonaws.com/{row.file_name}.json"
    audio_data.at[index, 'json_transcription'] = f"{row.file_name}.json"

#nos descargamos el json de transcribe de nuestro bucket
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('renewalfinal')
for s3_object in my_bucket.objects.all():
    path, filename = os.path.split(s3_object.key)
    if filename.split('.')[-1] in ['json']:
        my_bucket.download_file(s3_object.key, filename)
        print(filename)

#borramos los jobs que queden pendientes
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('renewalfinal')
for s3_object in my_bucket.objects.all():
    path, filename = os.path.split(s3_object.key)
    if filename.split('.')[-1] in ['wav', 'WAV']:
        print(filename)
        client_transcribe = boto3.client('transcribe', region_name = 'us-east-1')
        try:
            client_transcribe.delete_transcription_job(TranscriptionJobName = filename)
        except:
            print ("missed", filename)



client_transcribe = boto3.client('transcribe', region_name = 'us-east-1')
response_transcribe = client_transcribe.list_transcription_jobs(Status = 'COMPLETED')

print(response_transcribe)
