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
    photo_bucket = os.getenv("AWS_PHOTO_BUCKET")
    # Download the object from the S3 bucket
    photo_to_save = '/tmp/' + photo_to_post
    s3_client.download_file(photo_bucket, photo_to_post, photo_to_save)
    # Return the path to the downloaded file
    return photo_to_save


def tweet_photo():    
    # Get CSV of posted photos
    s3_resource = boto3.resource('s3')
    csv_bucket = os.getenv("AWS_CSV_BUCKET")
    bucket = s3_resource.Bucket(csv_bucket)
    key = 'posted_photos.csv'
    tweets_file = '/tmp/posted_photos.csv'
    s3_resource.Bucket(csv_bucket).download_file(key,tweets_file)
    df = pd.read_csv(tweets_file,header=0)
    
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    access_token = os.getenv("ACCESS_TOKEN")
    access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
    
    # Get list of photos in S3 bucket
    s3_client = boto3.client('s3')
    photo_bucket = os.getenv("AWS_PHOTO_BUCKET")
    objects = s3_client.list_objects(Bucket=photo_bucket)
    
    # Authenticate Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # Loop through objects and post to Twitter if not already posted
    status = False
    for x in range(len(objects['Contents'])):
        # Select random photo
        df_sample = pd.DataFrame.from_dict(objects['Contents']).sample()
        random_key = df_sample['Key'].values[0]
        # Tweet photo if we haven't tweeted it yet
        if random_key not in df['Photos'].to_numpy():
            # Get photo
            tweet_photo = get_photo(random_key)
            # Post photo to Twitter
            media = api.media_upload(tweet_photo)
            tweet = ""
            post_result = api.update_status(status=tweet, media_ids=[media.media_id])
            # Add photo to list of posted photos
            df_local = pd.DataFrame([random_key], columns =['Photos'])
            df = pd.concat([df, df_local], ignore_index = True)
            status = True
            break
        # just to be explicit
        else:
            continue
        
    if status == True:
        # Save updated list of posted photos to temp CSV file
        df.to_csv(tweets_file,index=False)
        # Upload updated CSV to S3 bucket
        bucket.upload_file(tweets_file, key)
        # Doubt this is necessary but ah well
        os.remove(tweet_photo)
    else:
        # Keep record of failed attempt
        now = datetime.now()
        dict = {'Photos':['tried to post on ' + str(now)]}
        df_local = pd.DataFrame(dict)
        df = pd.concat([df, df_local], ignore_index = True)
        df.to_csv(tweets_file,index=False)
        bucket.upload_file(tweets_file, key)
        
def lambda_handler(event, context):
    print("Tweet photo")
    tweet_photo()
