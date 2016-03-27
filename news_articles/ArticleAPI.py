import json
import requests
import os
from calendar import monthrange


#Function to make API request
def make_request(payload, url):
	#Make API request
	r = requests.get(url, params = payload)
	#Get json object
	if r.status_code == 200:
		data = r.json()
	else:
		#empty page, return docs is none
		data = {"response":{"meta":{"hits":286,"time":57,"offset":1000},"docs":[]},"status":"OK","copyright":"Copyright (c) 2013 The New York Times Company.  All Rights Reserved."}
	return data

#Append to file function
def append_to_file(docs, path='data/0000-00-00', mode='a'):
	#article names are number increments eg. 1.txt, 2.txt, etc...
    fileName = 1;
    for doc in docs:
		with open(path+str(fileName)+'.txt', mode) as myfile:
			print 'Fetching article: '+doc['pub_date']
			#write date first
			myfile.write(doc['pub_date'].encode('utf-8') + '\n')
			#write headline
			myfile.write(doc['headline']['main'].encode('utf-8') + '\n')
			if doc['lead_paragraph'] is not None and doc['abstract'] is not None:
				if doc['lead_paragraph'] != doc['abstract']:
					#Append both to file
					myfile.write(doc['lead_paragraph'].encode('utf-8') + '\n')
					myfile.write(doc['abstract'].encode('utf-8') + '\n')
				else:
					#Append lead_paragraph
					myfile.write(doc['lead_paragraph'].encode('utf-8') + '\n')
			#Check if Lead Paragraph exist, add it if exist
			elif doc['lead_paragraph'] is not None:
				#Append lead_paragraph to file
				myfile.write(doc['lead_paragraph'].encode('utf-8') + '\n')
			#Check if Abstract is None, add it if exist
			elif doc['abstract'] is not None:
				#Append abstract to file
				myfile.write(doc['abstract'].encode('utf-8') + '\n')
			else:
				#Do nothing
				print "both none"
			myfile.write('\n')
		fileName = fileName + 1

#main function
pageNum = 0
maxPageNum = 10
api_url = 'http://api.nytimes.com/svc/search/v2/articlesearch.json'

#create data dir if doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

for year in range(2002, 2012):
	for month in range(1, 13):
		for day in range(1, monthrange(year, month)[1]+1):
			#Set page num to 0
			pageNum = 0
			#Format month and day to be 2 digit integers
			monthStr = "%02d" % (month,)
			dayStr = "%02d" % (day,)

			while (pageNum <= maxPageNum):
				#Setup API params
				payload = {'fq': 'section_name.contains:("Business" "Technology") AND headline:("blackberry")', 'sort': 'newest', 'page': pageNum, 'begin_date': str(year)+monthStr+dayStr, 'end_date': str(year)+monthStr+dayStr, 'api-key': '3a645e822d9a019d4e84aaa0609b30e5:19:73615271'}
				#make request
				data = make_request(payload, api_url)
				docs = data['response']['docs']
				#Break loop if docs is empty, have reached the end of this query
				if not docs:
					break
				else:
					#create the directory with the date as name
					path = 'data/'+str(year)+monthStr+dayStr+'/'
					if not os.path.exists(path):
						os.makedirs(path)
					append_to_file(data['response']['docs'], path)
					pageNum = pageNum + 1