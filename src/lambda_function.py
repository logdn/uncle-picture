import os
import random
import json
from pathlib import Path
import tweepy
import boto3
import csv
import pandas as pd
from datetime import datetime

ROOT = Path(__file__).resolve().parents[0]

def get_photo(photo_to_post):
    s3_client = boto3.client('s3')
    # Download the object from the S3 bucket
    photo_to_save = '/tmp/' + photo_to_post
    s3_client.download_file('bot-bucket-uncle-picture', photo_to_post, photo_to_save)
    # Return the path to the downloaded file
    return photo_to_save


def tweet_photo():    
    # Get CSV of posted photos
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('bot-bucket-csv')
    key = 'posted_photos.csv'
    tweets_file = '/tmp/posted_photos.csv'
    s3_resource.Bucket('bot-bucket-csv').download_file(key,tweets_file)
    df = pd.read_csv(tweets_file,header=0)
    
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    access_token = os.getenv("ACCESS_TOKEN")
    access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
    
    # Get list of photos in S3 bucket
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects(Bucket='bot-bucket-uncle-picture')
    
    # Authenticate Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # Loop through objects and post to Twitter if not already posted
    status = False
    for obj in objects['Contents']:
        if obj['Key'] not in df['Photos'].to_numpy():
            # Get photo
            tweet_photo = get_photo(obj['Key'])
            # Post photo to Twitter
            media = api.media_upload(tweet_photo)
            tweet = ""
            post_result = api.update_status(status=tweet, media_ids=[media.media_id])
            # Add photo to list of posted photos
            df_local = pd.DataFrame([obj['Key']], columns =['Photos'])
            df = pd.concat([df, df_local], ignore_index = True)
            status = True
            break
    
    if status == True:
        # Save updated list of posted photos to temp CSV file
        df.to_csv(tweets_file,index=False)
        # Upload updated CSV to S3 bucket
        bucket.upload_file(tweets_file, key)
        # Doubt this is necessary but ah well
        os.remove(tweet_photo)
    else:
        now = datetime.now()
        dict = {'Photos':['tried to post on ' + str(now)]}
        df_local = pd.DataFrame(dict)
        df = pd.concat([df, df_local], ignore_index = True)
        df.to_csv(tweets_file,index=False)
        bucket.upload_file(tweets_file, key)
    
def lambda_handler(event, context):
    print("Tweet photo")
    tweet_photo()
