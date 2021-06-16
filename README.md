# Customer Call Sentiment Analysis

This project aims to be a tool for sentiment analysis of customer calls. 

The code provided in the repo is divided in two files (extracting-text.py and sentiment-analysis.py) where the AWS services Transcribe and Comprenhend are used for audio files processing, translating them to json text files and using NLP. 

There is also a file for a AWS Lambda provided (lambda_function.py), where the code in the other two files is used for implemetation of a simple audio processing process, done by following a couple of steps:

1) Sample audios are provided.
2) These samples audios are transcribed to .json files, using AWS Transcribe.
3) The .json files are analised using AWS Comprenhend.
4) Finally, the output provided consits of a txt file with the transcription and the predominant sentiment for each audio.

<img width="1431" alt="Captura de pantalla 2021-05-30 a las 15 24 43" src="https://user-images.githubusercontent.com/32374880/120105926-33ca1b00-c15b-11eb-9693-47129de440cf.png">

Difficulties encountered:
- Lambda roles for executing the different AWS services.
- Granting lambda the permission to write in S3.
- Working with json files
