import os,sys,re,gspread,pickle,json,datetime,uuid,pytz,hashlib,time,random
from dateutil.parser import parse
from pprint import pprint
from pathlib import Path

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

def ensure_cross_sheet_compatibility(this_dict):

	new_dict=dict()
	for k,v in this_dict.items():

		if k in cross_sheet_dict:
			k=cross_sheet_dict[k]

		new_dict[k]=v

	return new_dict

def decapitalize_seminar_tags(seminar_tags):
	if len(seminar_tags)>0:
		seminar_tags=[x.strip() for x in re.split(r'[,;]',seminar_tags)]
		seminar_tags=[x for x in seminar_tags if len(x)>0]
		decapitalized=[]
		for x in seminar_tags:
			if x[0].isupper() and x not in ['Alzheimer\'s','Dravet\'s','Parkinson\'s','Dravet']:
				status=True
				if any([i.isupper() for i in x[1:]]):
					status=False
				if status:
					x=x.lower()
			if len(x.split())<4 and x not in ['neuroscience','tutorial']:
				decapitalized.append(x)
		decapitalized=['Spanish 🗣️' if x=='spanish' else x for x in decapitalized]
		decapitalized=['Alzheimer\'s' if x=='AD' else x for x in decapitalized]
		decapitalized=['behaviour' if x=='behavior' else x for x in decapitalized]
		decapitalized=['fMRI' if x.lower()=='fmri' else x for x in decapitalized]

		return ', '.join(decapitalized)
	else:
		return seminar_tags

def parse_date(seminar_date,seminar_time,seminar_timezone):

	seminar_timezone=pytz.timezone(seminar_timezone)

	start_datetime=parse(seminar_date + ' ' + seminar_time, fuzzy=True)
	end_datetime=start_datetime+datetime.timedelta(hours=1)

	start_datetime = seminar_timezone.localize(start_datetime).astimezone(pytz.UTC)
	end_datetime = seminar_timezone.localize(end_datetime).astimezone(pytz.UTC)

	start_datetime=start_datetime.strftime("%Y%m%dT%H%M%S")
	end_datetime=end_datetime.strftime("%Y%m%dT%H%M%S")
	
	return start_datetime,end_datetime

def use_this_row(x):

	if x[3].strip().lower()=='yes':
		return True
	else:
		return False

def create_ical_file(my_dict,unique_hash):

	try:

		seminar_date=my_dict['seminar_date']
		seminar_time=my_dict['seminar_time']

		seminar_timezone=my_dict['timezone']

		start_datetime,end_datetime=parse_date(seminar_date,seminar_time,seminar_timezone)

		speaker_title=my_dict['speaker_title']
		seminar_speaker=my_dict['seminar_speaker']
		speaker_affil=my_dict['speaker_affil']

		seminar_title=my_dict['seminar_title']
		hosted_by=my_dict['hosted_by']

		ical_event_string='BEGIN:VEVENT\nUID:'
		ical_event_string+=unique_hash+'\n'
		ical_event_string+='SUMMARY:'+speaker_title+' '+seminar_speaker+'\nDTSTART;VALUE=DATE-TIME:'
		ical_event_string+=start_datetime + 'Z\n'
		ical_event_string+='DTEND;VALUE=DATE-TIME:'
		ical_event_string+=end_datetime + 'Z\n'
		if hosted_by!='':
			ical_event_string+='DESCRIPTION:This is an online seminar hosted by: ' + hosted_by + '\\n\\n'
		else:
			ical_event_string+='DESCRIPTION:This is an online seminar\\n\\n'
		ical_event_string+='Speaker: ' + speaker_title + ' ' + seminar_speaker + ' | ' + speaker_affil + '\\n\\n'
		if seminar_title!='':
			ical_event_string+='Seminar Title: ' + seminar_title + '\\n\\n'
		ical_event_string+='For more details on this event (summary text, teleconforencing link, etc), please check on the website www.worldwideneuro.com\n'
		ical_event_string+='END:VEVENT\n'
	
		return ical_event_string
	
	except:
	
		return ''

def check_if_calendar_file_exists_and_is_unchanged(ical_event_string,seminar_id):

	just_generated_ical_string='BEGIN:VCALENDAR'+re.sub(r'\s','',ical_event_string,flags=re.DOTALL)+'END:VCALENDAR'

	try:
		with open(working_dir +'individual_calendar_events/seminar_event_'+str(seminar_id)+'.ics','r') as f:
			file_line=f.read()
		file_line=re.sub(r'\s','',file_line,flags=re.DOTALL)
		
		if file_line==just_generated_ical_string:
			return 'same'
		else:
			return 'different'
	except:
		return None

def get_seminar_series_headers(seminar_series_data):

	regex=r'((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*'

	my_dict=dict(zip(seminar_series_data[2],seminar_series_data[3]))

	pop_these_out=[]
	for k in my_dict.keys():
		if k not in ['Series Name','Organized by','Banner Image','About this Series']:
			pop_these_out.append(k)
	for k in pop_these_out:
		my_dict.pop(k, None)

	try:
		organized_by=[i.strip() for i in my_dict['Organized by'].split(';')]
		org_links=[]
		for i in organized_by:
			s=re.search(regex,i).span()
			text=i[0:s[0]].strip()
			url=i[s[0]:s[1]]
			org_links.append({'text':text,'url':url})
		my_dict['Organized by']=org_links
	except:
		my_dict['Organized by']=''

	return my_dict

def get_seminar_event_rows(seminar_events_data):

	rows=[[str(i)]+[y.strip() for y in x] for i,x in enumerate(seminar_events_data[3:]) if use_this_row(x)]
	rows=[['' if y.lower()=='tba' else y for y in x] for x in rows]

	return rows

def get_unique_hash(my_dict):

	string_to_digest=my_dict['Row Number'] + my_dict['seminar_speaker'] + ' ' + my_dict['hosted_by'] + my_dict['sheet_id']
	bytes_to_digest=bytes(string_to_digest.encode())
	unique_hash=hashlib.sha256(bytes_to_digest).hexdigest()

	if unique_hash in already_added_unique_hashes:

		seminar_id=seminar_speaker_unique_hash_to_seminar_id_and_date_added[unique_hash][0]
		time_of_addition=seminar_speaker_unique_hash_to_seminar_id_and_date_added[unique_hash][1]
		
		return unique_hash,seminar_id,time_of_addition

	else:

		return unique_hash,None,None

def what_spreadsheets_should_I_fetch():

	regex=r'spreadsheets/d/(.*?)/.*?$'

	with open(working_dir_python_code+'world_wide_series_gsheets.pkl','rb') as f:
		world_word_series_gsheet=pickle.load(f)

	google_sheets_list=[x['Google Sheet'] for x in world_word_series_gsheet]
	google_sheets_list=[re.search(regex,x).group(1) for x in google_sheets_list]

	gsheets_dict={re.search(regex,x['Google Sheet']).group(1):x for x in world_word_series_gsheet}

	random.shuffle(google_sheets_list)

	# If modifying these scopes, delete the file token.pickle.
	scope = ['https://www.googleapis.com/auth/drive.metadata.readonly']
	credentials = ServiceAccountCredentials.from_json_keyfile_name(working_dir_python_code+'imposing-timer-196815-615db1a19785.json', scope)
	service = build('drive', 'v3', credentials=credentials)
	
	# Call the Drive v3 API
	results = service.files().list(
		pageSize=100, fields="nextPageToken, files(id, modifiedTime)").execute()
	items = results.get('files', [])
	mod_time_gsheets={ item['id'] : parse(item['modifiedTime']) for item in items }

	#with open(working_dir_python_code + 'mod_time.pkl','rb') as f:
	#	mod_time_local=pickle.load(f)

	datetime_now = datetime.datetime.utcnow()
	datetime_now = datetime_now.replace(tzinfo=pytz.utc) 

	fetch_these_gsheets=[]

	for gsheet_id in google_sheets_list:
		try:
			time_difference = datetime_now - mod_time_gsheets[gsheet_id]
			total_seconds = time_difference.total_seconds()
			#print(gsheet_id,'%d hours, %d minutes, %d seconds' % (total_seconds//3600, total_seconds%3600//60, total_seconds%60))
			if total_seconds > 0 and total_seconds <= 330:
				print('This was modified over the past 5 minutes:',gsheet_id,total_seconds)
				fetch_these_gsheets.append(gsheet_id)
		except:
			fetch_these_gsheets.append(gsheet_id)
			pass

	mod_time_local=mod_time_gsheets
	#with open(working_dir_python_code + 'mod_time.pkl','wb') as f:
	#	pickle.dump(mod_time_local,f)

	return gsheets_dict,google_sheets_list,fetch_these_gsheets

def sync_all_every_past_hour():

	dt = datetime.datetime.now()
	if dt.hour%12==0:
		dt_hour = dt.replace(minute=0, second=0, microsecond=0) # Returns a copy
		time_difference=dt-dt_hour
		total_seconds = time_difference.total_seconds()
		
		if total_seconds<=300:
			return True
		else:
			return False
	else:
		return False

#time.sleep(5)

cross_sheet_dict={
	"Date":"seminar_date",
	"Time":"seminar_time",
	"Timezone":"timezone",
	"Post":"posted",
	"Seminar Link":"seminar_link",
	"Password":"password",
	"Watch Again":"video_on_demand",
	"Title":"speaker_title",
	"Speaker Name":"seminar_speaker",
	"Affiliation":"speaker_affil",
	"Twitter":"speaker_twitter",
	"Website":"speaker_website",
	"Topic Tags":"topic_tags",
	"Seminar Title":"seminar_title",
	"Abstract":"seminar_abstract",
	"Seminar Series":"hosted_by",
}

working_dir_python_code=str(Path.home()) + '/Dropbox/websites/worldwideneuro.com/python_code/'
working_dir=str(Path.home()) + '/Dropbox/websites/worldwideneuro.com/'
working_dir_ics_files=str(Path.home()) + '/Dropbox/websites/worldwideneuro.com/ics_files/'

world_wide_series_gsheet_id='18r8rVWZQpvxM10hygsJq4-4wR17i4Xld7PQFI1QVPAI'

scope = ['https://www.googleapis.com/auth/drive.metadata.readonly']
credentials = ServiceAccountCredentials.from_json_keyfile_name(working_dir_python_code+'imposing-timer-196815-615db1a19785.json', scope)
service = build('drive', 'v3', credentials=credentials)

# Call the Drive v3 API
results = service.files().get(fileId=world_wide_series_gsheet_id, fields="modifiedTime").execute()
world_wide_series_gsheet_mod_time=parse(results['modifiedTime'])

datetime_now = datetime.datetime.utcnow()
datetime_now = datetime_now.replace(tzinfo=pytz.utc) 

time_difference = datetime_now - world_wide_series_gsheet_mod_time
total_seconds = time_difference.total_seconds()

if total_seconds > 0 and total_seconds <= 300:

	# REQUEST AND HOPEFULLY GET ACCESS TO THE PARTICULAR SERIES GOOGLE SHEET DATA

	try: 
		scope = ['https://spreadsheets.google.com/feeds']
		credentials = ServiceAccountCredentials.from_json_keyfile_name(working_dir_python_code+'imposing-timer-196815-245fab6211ac.json', scope)
		gc = gspread.authorize(credentials)
		spreadsheet = gc.open_by_key(world_wide_series_gsheet_id)
		world_wide_series_gsheet=spreadsheet.worksheet('Series Info')
		world_wide_series_gsheet_data=world_wide_series_gsheet.get_all_values()

		headers=world_wide_series_gsheet_data[0]
		rows=world_wide_series_gsheet_data[1:]

		world_word_series_gsheet=[]
		for x in rows:
			my_dict=dict(zip(headers,x))
			my_dict['domain']=[i.strip() for i in my_dict['Domain'].split(';')]
			if ['Neuroscience']==my_dict['domain']:
				world_word_series_gsheet.append(my_dict)

		with open(working_dir_python_code+'world_wide_series_gsheets.pkl','wb') as f:
			pickle.dump(world_word_series_gsheet,f)

	except Exception as e:

		pass

scope = ['https://spreadsheets.google.com/feeds']

gsheets_dict,google_sheets_list,fetch_these_gsheets=what_spreadsheets_should_I_fetch()
if sync_all_every_past_hour():
	fetch_these_gsheets=google_sheets_list

seminar_series=dict()
all_seminar_entries=[]

ii=0
for google_sheet_id in google_sheets_list:

	fetched_status=False

	ii+=1

	if google_sheet_id in fetch_these_gsheets:

		time.sleep(random.randint(15,25))

		try:

			# REQUEST AND HOPEFULLY GET ACCESS TO THE PARTICULAR SERIES GOOGLE SHEET DATA

			credentials = ServiceAccountCredentials.from_json_keyfile_name(working_dir_python_code+'imposing-timer-196815-245fab6211ac.json', scope)
			gc = gspread.authorize(credentials)
			spreadsheet = gc.open_by_key(google_sheet_id)
			seminar_events_sheet=spreadsheet.worksheet('Seminar Events')
			seminar_events_data=seminar_events_sheet.get_all_values()

			seminar_series_sheet=spreadsheet.worksheet('Seminar Series')
			seminar_series_data=seminar_series_sheet.get_all_values()

			with open(working_dir_python_code+'series_gsheets/'+google_sheet_id+'.pkl','wb') as f:
				pickle.dump([seminar_events_data,seminar_series_data],f)

			print('Just fetched this Google Sheet:',google_sheet_id)
			current_date_time=str(datetime.datetime.now())
			print(current_date_time)

			fetched_status=True

		except Exception as e:

			print(e)
			print('>',ii,'- I couldn\'t parse this sheet so I will try again:',google_sheet_id)

			time.sleep(random.randint(30,40))

			try:

				# REQUEST AND HOPEFULLY GET ACCESS TO THE PARTICULAR SERIES GOOGLE SHEET DATA

				credentials = ServiceAccountCredentials.from_json_keyfile_name(working_dir_python_code+'imposing-timer-196815-245fab6211ac.json', scope)
				gc = gspread.authorize(credentials)
				spreadsheet = gc.open_by_key(google_sheet_id)
				seminar_events_sheet=spreadsheet.worksheet('Seminar Events')
				seminar_events_data=seminar_events_sheet.get_all_values()

				seminar_series_sheet=spreadsheet.worksheet('Seminar Series')
				seminar_series_data=seminar_series_sheet.get_all_values()

				with open(working_dir_python_code+'series_gsheets/'+google_sheet_id+'.pkl','wb') as f:
					pickle.dump([seminar_events_data,seminar_series_data],f)

				print('Just fetched this Google Sheet:',google_sheet_id)
				current_date_time=str(datetime.datetime.now())
				print(current_date_time)

				fetched_status=True

			except Exception as e:

				print(e)
				print('>',ii,'- I couldn\'t parse this sheet so I used the pickled file instead:',google_sheet_id)

				try:

					with open(working_dir_python_code+'series_gsheets/'+google_sheet_id+'.pkl','rb') as f:
						[seminar_events_data,seminar_series_data]=pickle.load(f)

				except Exception as e:

					print(e)
					print('>',ii,'- I couldn\'t find any pickle file to resort to:',google_sheet_id)
					current_date_time=str(datetime.datetime.now())
					print(current_date_time)

					continue

	else:

		try:
			with open(working_dir_python_code+'series_gsheets/'+google_sheet_id+'.pkl','rb') as f:
				[seminar_events_data,seminar_series_data]=pickle.load(f)
		except:
			continue

		
	# PARSE THE SEMINAR SERIES INFORMATION DATA

	try:
		seminar_series_data=get_seminar_series_headers(seminar_series_data)
		domain=gsheets_dict[google_sheet_id]['domain']
		seminar_series_data['domain']=domain
		seminar_series[seminar_series_data['Series Name']]=seminar_series_data
	except:
		continue

	seminar_series_name=seminar_series_data['Series Name'].strip()
	if seminar_series_name=='' or 'Neuroscience' not in domain:
		continue

	seminar_series_name=re.sub(r'[^A-Za-z0-9-& ]',' ',seminar_series_name)
	seminar_series_name=re.sub(r'\s\s+',' ',seminar_series_name)

	if fetched_status:
		print('>>>',seminar_series_name)

	# PARSE THE SEMINAR DATA OF THIS PARTICULAR SERIES

	headers=seminar_events_data[2]
	headers=['Row Number']+headers
	headers.append('Seminar Series')
	rows=get_seminar_event_rows(seminar_events_data)

	for x in rows:

		x.append(seminar_series_name)
		my_dict=dict(zip(headers,x))
		my_dict['sheet_id']=google_sheet_id
		my_dict['domain']=domain

		all_seminar_entries.append(my_dict)

all_seminar_entries=[ensure_cross_sheet_compatibility(my_dict) for my_dict in all_seminar_entries]

seminars=dict()
icalendar='BEGIN:VCALENDAR\n'

with open(working_dir_python_code+'seminar_speaker_unique_hash_to_seminar_id_and_date_added.pkl', 'rb') as f:
	seminar_speaker_unique_hash_to_seminar_id_and_date_added=pickle.load(f)

already_added_seminar_event_ids=list(seminar_speaker_unique_hash_to_seminar_id_and_date_added.values())
already_added_seminar_event_ids=list(sorted([i[0] for i in already_added_seminar_event_ids]))
already_added_unique_hashes=list(seminar_speaker_unique_hash_to_seminar_id_and_date_added.keys())

individual_ics_files_to_be_uploaded=[]

for my_dict in all_seminar_entries:

	if my_dict['seminar_speaker']=='':
		continue

	if my_dict['hosted_by']=='Ad hoc':
		invalid_date_format=my_dict['seminar_date']
		invalid_date_format=datetime.datetime.strptime(invalid_date_format,'%m/%d/%Y')
		valid_date_format=datetime.datetime.strftime(invalid_date_format, '%a, %b %d, %Y')
		my_dict['seminar_date']=valid_date_format

		invalid_time_format=my_dict['seminar_time']
		am_or_pm=invalid_time_format[-2:]

		if am_or_pm=='AM':
			my_re=re.search(r'(^\d+:\d+)',invalid_time_format)
			valid_time_format=my_re.group(1)
			if len(valid_time_format)==4:
				valid_time_format='0'+valid_time_format
		else:
			my_re_hour=re.search(r'^(\d+):\d+',invalid_time_format)
			hour=my_re_hour.group(1)
			if hour!='12':
				hour=str(int(hour)+12)
			my_re_minutes=re.search(r'^\d+:(\d+)',invalid_time_format)
			minutes=my_re_minutes.group(1)
			valid_time_format=hour+':'+minutes

		my_dict['seminar_time']=valid_time_format

	my_dict['seminar_title']=my_dict['seminar_title'].rstrip('.')
	my_dict['seminar_title']=my_dict['seminar_title'].strip('"')
	my_dict['seminar_abstract']=my_dict['seminar_abstract'].strip('"')

	unique_hash,seminar_id,time_of_addition=get_unique_hash(my_dict)

	if time_of_addition==None:

		seminar_id=max(already_added_seminar_event_ids)+1
		already_added_seminar_event_ids.append(seminar_id)
		time_of_addition=str(datetime.datetime.now().strftime("%a, %b %d, %Y %H:%M"))

	my_dict['calendar_event_hash']=unique_hash

	seminar_speaker_unique_hash_to_seminar_id_and_date_added[unique_hash]=[seminar_id,time_of_addition]

	my_dict['seminar_id']=seminar_id
	my_dict['time_of_addition']=time_of_addition

	my_dict['topic_tags']=decapitalize_seminar_tags(my_dict['topic_tags'])

	my_dict.pop('sheet_id', None)
	my_dict.pop('Row Number', None)
	my_dict.pop('', None)
	
	seminars[seminar_id]=my_dict

	ical_event_string=create_ical_file(my_dict,unique_hash)
	icalendar+=ical_event_string

	ics_status=check_if_calendar_file_exists_and_is_unchanged(ical_event_string,seminar_id)
	if ics_status!='same':
		ics_fname=working_dir +'individual_calendar_events/seminar_event_'+str(seminar_id)+'.ics'
		individual_ics_files_to_be_uploaded.append(ics_fname)
		with open(ics_fname,'w') as f:
			f.write('BEGIN:VCALENDAR\n'+ical_event_string+'END:VCALENDAR\n')
		os.system('/usr/local/bin/aws s3 cp ' + ics_fname + ' s3://www.worldwideneuro.com/individual_calendar_events/seminar_event_'+str(seminar_id)+'.ics')

icalendar+='END:VCALENDAR\n'

with open(working_dir_python_code+'seminar_speaker_unique_hash_to_seminar_id_and_date_added.pkl', 'wb') as f:
	pickle.dump(seminar_speaker_unique_hash_to_seminar_id_and_date_added,f)

# UPLOAD seminars_ical.ics IF CALENDAR HAS CHANGED

with open(working_dir +'seminars_ical.ics','r') as f:
	old_icalendar=f.read()

if sorted(old_icalendar.splitlines())!=sorted(icalendar.splitlines()):

	with open(working_dir +'seminars_ical.ics','w') as f:
		f.write(icalendar)

	os.system('/usr/local/bin/aws s3 cp ' + working_dir + 'seminars_ical.ics s3://www.worldwideneuro.com')

# UPLOAD seminars_data.json & flexsearch_index.json IF SEMINARS DATA HAVE CHANGED

with open(working_dir+'seminar_data.json','r') as f:
	old_seminars=json.load(f)

if old_seminars!=seminars:

	with open(working_dir+'seminar_data.json','w') as f:
		json.dump(seminars,f)

	os.system('/usr/local/bin/aws s3 cp ' + working_dir + 'seminar_data.json s3://www.worldwideneuro.com')

	os.system('/usr/bin/node ' + working_dir_python_code + 'create_the_index.js')
	os.system('/usr/local/bin/aws s3 cp ' + working_dir + 'flexsearch_index.json s3://www.worldwideneuro.com')

# UPLOAD seminars_series_data.json IF SEMINAR SERIES DATA HAVE CHANGED

def get_seminar_datetime(x):
	seminar_timezone=pytz.timezone(x['timezone'])
	start_datetime=parse(x['seminar_date'] + ' ' + x['seminar_time'], fuzzy=True)
	end_datetime=start_datetime+datetime.timedelta(hours=720)
	end_datetime = seminar_timezone.localize(end_datetime).astimezone(pytz.UTC)

	return end_datetime

def which_seminar_series_are_still_active(seminar_series):

	datetime_now = datetime.datetime.utcnow()
	datetime_now = datetime_now.replace(tzinfo=pytz.utc) 

	for this_series in seminar_series:
		these_seminars=[x for x in seminars.values() if x['hosted_by']==this_series]
		future_seminars=[x for x in these_seminars if get_seminar_datetime(x)>datetime_now]
		if len(future_seminars)==0:
			seminar_series[this_series]['active']=False
		else:
			seminar_series[this_series]['active']=True

	return seminar_series

seminar_series=which_seminar_series_are_still_active(seminar_series)

with open(working_dir+'seminar_series_data.json','r') as f:
	old_seminar_series=json.load(f)

if old_seminar_series!=seminar_series:

	with open(working_dir+'seminar_series_data.json','w') as f:
		json.dump(seminar_series,f)

	os.system('/usr/local/bin/aws s3 cp ' + working_dir + 'seminar_series_data.json s3://www.worldwideneuro.com')

# UPLOAD INDIVIDUAL CALENDAR FILES THAT HAVE BEEN MARKED AS NEW OR CHANGED

current_date_time=str(datetime.datetime.now())
print(current_date_time)