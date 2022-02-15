import json
import requests
import time
import os.path
import argparse
import csv
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os

timeBetweenRequests = 900

#Gets the command line args
parser = argparse.ArgumentParser(description='Retrieve data from TomTom on a road every 5 minutes')
parser.add_argument('apiKey', help="A TomTom API key")
parser.add_argument('-v', '--verbose', action='store_true', help='Output collected data to terminal')

# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': 'traffic-data-collector-330909',
})
db = firestore.client()


args = parser.parse_args()
if args.verbose:
    print("This will be verbose")




while(True):
    if args.verbose: print("Making requests")
    dt_string = str(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    docs = db.collection(u'segments').stream()


    for row in docs:
        row = row.to_dict()
        if args.verbose: print(row)
        #make the request
        r = requests.get('https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key='+args.apiKey+'&point='+row['longlat'])
        jsondata = r.json()

        #print the data to console
        if args.verbose: print(("Id: ", str(row['segId']), "Current Speed: ", int(jsondata['flowSegmentData']['currentSpeed']), "Free Flow Speed",
            int(jsondata['flowSegmentData']['freeFlowSpeed']), "Current Travel Time: ",int(jsondata['flowSegmentData']['currentTravelTime']), 
            "Free Flow Travel Time: ",int(jsondata['flowSegmentData']['freeFlowTravelTime']),"Confidence: ",
            float(jsondata['flowSegmentData']['confidence'])))
            
        #CSVs
        with open('./CSVs/'+str(row['segId'])+'.csv', mode='a') as data:
            writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([str(datetime.now().strftime("%Y/%m/%d %H:%M:%S")), float(jsondata['flowSegmentData']['currentSpeed']), float(jsondata['flowSegmentData']['confidence'])])

        # firebase
        doc_ref = db.collection(u'roads').document(u'str(row[0])').collection("records").document(dt_string)
        doc_ref.set({
            u'ds': dt_string,
            u'speed': float(jsondata['flowSegmentData']['currentSpeed']),
            u'confidence': float(jsondata['flowSegmentData']['confidence']),
            u'closed':bool(jsondata['flowSegmentData']['roadClosure'])
        })



    time.sleep(timeBetweenRequests)
