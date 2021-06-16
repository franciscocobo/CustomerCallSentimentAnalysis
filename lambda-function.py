import json
import boto3
import os
import time
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
import json

s3_client = boto3.client('s3', region_name='us-east-2')
s3_resources = boto3.resource('s3', region_name='us-east-2')
client_transcribe = boto3.client('transcribe', region_name = 'us-east-2')
comprenhend_client = boto3.client(service_name='comprehend', region_name = 'us-east-2')
        
def lambda_handler(event, context):
    
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file = event["Records"][0]["s3"]["object"]["key"]

    os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
    client_s3 = boto3.client('s3')
    
    media_uri = f"s3://{bucket}/{file}"
    
    try:
        start_transcription(bucket, file, media_uri)
    except:
        msg = "The job already exists."
        print(msg)
    
    try:
        transcription = get_transcription(bucket, file)
    except:
        msg = "Error in the transcription."
        print(msg)
    
    try:
        sentimentData = get_sentiment(transcription)
    except:
        msg = "Error in sentiment analysis."
        print(msg)
    
    try:
        content = get_finalresult(bucket, file, transcription, sentimentData)
    except:
        msg = "Error in generating the sentiment file."
        print(msg)
    
    
    return {
        'statusCode': 200,
        'body': content
    
    }

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

def get_transcription(bucket, job_name):
    transcription = s3_client.get_object(Bucket = bucket, Key = job_name + ".json")
    transcription = transcription["Body"].read()
    transcription = json.loads(transcription)
    transcription = transcription['results']['transcripts'][0]['transcript']
    return transcription 

def get_sentiment(text):
    sentimentData = comprenhend_client.detect_sentiment(Text=text, LanguageCode = 'es')
    return sentimentData['Sentiment']

def get_finalresult(bucket, file, transcription, sentimentData):
    content = transcription + '\n' + "Sentiment: " + sentimentData
    bucket = str(bucket)
    file = str(file)
    name_file = 'sentimentAnalysisResult'  + file + '.txt'
    s3_resources.Object(bucket, name_file).put(Body=content)
    return name_file
