import json
import os
import time
import logging
import tweepy as tw
import boto3

#Authentication Keys
consumer_key=os.getenv('consumer_key')
consumer_secret=os.getenv('consumer_secret')
access_token = os.getenv('access_token')
access_token_secret= os.getenv('access_token_secret')

bucket_name = "   "

s3 = boto3.resource('s3')

def make_csv_row(in_tuple):
    """
    function takes a tuple as input and returns a string in the format of a csv row
    """
    out_string = ''
    cnt = 0
    for x in in_tuple:
        if type(x).__name__ =='NoneType':
            x = ''
        if type(x).__name__ != 'str':
            x = str(x)
        x = '"' + x + '"'
        out_string += x
        cnt += 1
        if cnt < len(in_tuple):
            out_string += ', '
    return out_string.replace('\n','')


def scraper(search_word, count):
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=False)
    #should probably get tweets in chunk, run the processing until no more tweets 
    #are available from the API due to rate limit and then end

    tweets = tw.Cursor(api.search,q=search_word + " -filter:retweets",lang='en', tweet_mode='extended').items(count)
    json_list = [tweet._json for tweet in tweets]
    tup_list = [(t['id'],
                 time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(t['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
                 t['full_text'],
      t['user']['id'],t['user']['screen_name'],t['user']['location'],
      t['user']['description'],t['geo'],t['place'])
     for t in json_list]

    out_file = 'id, time, full_text, user_id, user_screen_name, user_location, user_description, geo, place\n' + '\n'.join([make_csv_row(x) for x in tup_list])
    encoded_string = out_file.encode("utf-8")
    return encoded_string

#write tweets
def write_file(encoded_string):
    file_name = "tweets" + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + ".csv"
    #lambda_path = "/tmp/" + file_name
    s3_path = "raw/" + file_name
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_string)
    return True

def validate_payload(event):
    #run basic checks on the payload
    assert event['search_word'] is not None, 'failed'
    search_word = event['search_word']
    count = event['count']
    if count is None:
        count = 18000
    return search_word, count

def lambda_handler(event, context):
    search_word, count = validate_payload(event)
    encoded_string = scraper(search_word, count)
    
    try:
        write_file(encoded_string)
        return {
            'statusCode': 200,
            'body': json.dumps('succesfull')
            }
    except:
        return {
            'statusCode': 500,
            'body': json.dumps('failed')
        }





