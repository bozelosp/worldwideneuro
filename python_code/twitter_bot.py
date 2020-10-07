import os,sys,re,pickle,json,datetime,pytz,tweepy
from dateutil.parser import parse
from pprint import pprint
from pathlib import Path

consumer_key = 'BN4W5KUg76Wu9Wlg7D46SxDp7' 
consumer_secret = 'P4btSGWIYcyVRJgYuegRvxl1mEShq2dyvqjPhvmWyCVbJWSCIC' 
access_token = '1244747050580877321-iyB0PvssPoI6WNaLb3k9fV2nF1LG5B' 
access_token_secret = 'LhhRmqJ1RfznsUuNPWDm0AFeR65ra6uiAIpJmPzF4UHRO' 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

def public_tweet():
	if datetime.date.today().weekday() == 0:
		tweet_to_publish = 'Hi everyone, today is Monday.   #Monday '

	return tweet_to_publish

def parse_date(seminar_date,seminar_time,seminar_timezone):

	seminar_timezone=pytz.timezone(seminar_timezone)

	start_datetime=parse(seminar_date + ' ' + seminar_time, fuzzy=True)
	start_datetime = seminar_timezone.localize(start_datetime).astimezone(pytz.UTC)

	return start_datetime

def should_i_tweet_about_it(seminar_event):

	seminar_date=seminar_event['seminar_date']
	seminar_time=seminar_event['seminar_time']

	seminar_timezone=seminar_event['timezone']

	start_datetime=parse_date(seminar_date,seminar_time,seminar_timezone)

	now_utc=datetime.datetime.now(datetime.timezone.utc)

	time_diff=start_datetime-now_utc
	time_diff_in_seconds=time_diff.total_seconds()

	if time_diff_in_seconds>=86100 and time_diff_in_seconds<86700:
		return ['first_tweet','tomorrow']
	elif time_diff_in_seconds>=2400 and time_diff_in_seconds<3000:
		return ['reminder',int(time_diff_in_seconds/60)]
	else:
		return [None,None]

def what_should_i_tweet(first_tweet_or_reminder,time_diff,seminar_id,x):

	if len(x['seminar_title'])>7:
		try:
			if len(x['speaker_twitter'])>2:
				speaker_twitter=' ' + x['speaker_twitter']
			else:
				speaker_twitter=''
			if len(x['speaker_title'])!=0:
				speaker_title=' ' + x['speaker_title']
			else:
				speaker_title=''
			if first_tweet_or_reminder=='first_tweet':
				tweet_to_publish='Tomorrow: Online seminar by' + speaker_title + ' ' + x['seminar_speaker'] + speaker_twitter + ' on "' + x['seminar_title'] + '", hosted by ' + x['hosted_by'] + ' | #bot⚡ https://www.worldwideneuro.com/seminar-event.html?id=' + str(seminar_id)
			elif first_tweet_or_reminder=='reminder':
				tweet_to_publish='Online seminar in ⏳ ' + str(time_diff) + ' min by' + speaker_title + ' ' + x['seminar_speaker'] + speaker_twitter + ' on "' + x['seminar_title'] + '", hosted by ' + x['hosted_by'] + ' | #bot⚡ https://www.worldwideneuro.com/seminar-event.html?id=' + str(seminar_id)	
			return tweet_to_publish
		except:
			return None
	else:
		return None

working_dir=str(Path.home()) + '/Dropbox/websites/worldwideneuro.com/'

with open(working_dir+'seminar_data.json') as json_file:
    data = json.load(json_file)

for k,v in data.items():

	[first_tweet_or_reminder,time_diff]=should_i_tweet_about_it(v)

	if first_tweet_or_reminder!=None:
		tweet_to_publish=what_should_i_tweet(first_tweet_or_reminder,time_diff,k,v)
		if tweet_to_publish!=None:
			try:
				api.update_status(tweet_to_publish)
				print('Just tweeted this: ')
				print(tweet_to_publish)
			except Exception as e:
				
				print('!! I tried to tweet this: !!')
				print(tweet_to_publish)
				print(e)
			current_date_time=str(datetime.datetime.now())
			print(current_date_time)