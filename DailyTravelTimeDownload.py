#!/usr/bin/env python3
"""
Program to utilize Acyclica's API key to download travel time data across a 
user defined time period. User will enter a start and end date. Those dates 
will be converted to Epoch time. The programming will open a csv containing 
RouteID and Route Name information for all main routes within the city for 
each direction. For each RouteID, a 24 hour period will be looped through from 
starting to ending date and download each 24 hour period. When all time 
periods for a route are downloaded, they will be pieced together into a single 
.csv file before moving onto the next Route. Once complete, the user will be 
notified with the number of Routes downloaded along with the total number of 
days.

Sample URL = 
https://cr.acyclica.com/datastream/route/csv/time/APIKey/Route/Start/End/
"""


import csv
import datetime
import glob
import os
import os.path
import sys
import time
import urllib
from datetime import datetime, timedelta
from dateutil import tz
from urllib import request
from urllib.request import urlretrieve
import pandas as pd
from tqdm import tqdm

"""
# Work Flow intended:

create a route dictionary of routeID and routeNames
for key, value in tqdm(acyclicaRoutes.items()):
    read csv of master file to a dataframe
    get last DateTime
    get now time
    convert times to UTC time zone
    convert times to epoch
    get delta of lastDateTime to nowDateTime
    for day in delta:
        download csv files
    combine files
    revert to local time
    append to master
    delete 2 years prior
"""

# create a route dictionary of routeID and routeNames
def route_dict():
    """
    Trys to open a csv file containing Acyclica Route IDs,Route Names and 
    places them in a dictionary to be used for download URLs as well as file 
    and folder creation.
    """
    try:
        routeCSV = open(r"C:/Python Test Folder 2/AcyclicaArterialRoutes.csv")
        routeDict = {}
        for line in routeCSV:
            entry = line.strip()
            routeID, routeName = entry.split(',')
            routeDict[routeID] = routeName
    except FileNotFoundError:
        print("The .csv file containing routes cannot be found. "
              + "Please check file location")
    return routeDict

#Folder check
def folder_creation(routeName):
    """
    Creates the folder structure for a route if there are no folders currently 
    present.
    """
    routeFolder = f"C:/Python Test Folder 2/{routeName}"
    downloadFolder = f"{routeFolder}/Downloads"
    if not os.path.isdir(routeFolder):
        os.makedirs(routeFolder)
        print(f"New folder created at {routeFolder}")
    if not os.path.isdir(downloadFolder):
        os.makedirs(downloadFolder)
        print(f"New Download folder created at {downloadFolder}")
    return routeFolder, downloadFolder

def base_url_creation():
    """Creates the base of the url download request with the API"""
    Base_URL = f"https://cr.acyclica.com/datastream/route/csv/time"
    APIKey = "qAgh5gIDungtzIuC1dyRBMfimhwKQlWm2hhdROA4"
    apiURL = f"{Base_URL}/{APIKey}"
    return apiURL

# TODO if master file is not present, create it 

# TODO for key, value in tqdm(acyclicaRoutes.items()):
AcyclicaRoutes = route_dict()
acyclicaBaseURL = base_url_creation()
for key, value in tqdm(AcyclicaRoutes.items()):
    # make sure folders are created and in place
    routeFolder, downloadFolder = folder_creation(value)
    # read csv of master file
    masterFile = master_file_check(value, routeFolder)
    # get last DateTime
    lastDate = get_last_date(masterFile)
    # modify last date for the next time frame and convert to UTC
    fromDate, fromDateString, fromDateUTC = download_from_date(lastDate)
    # get now time and convert to UTC
    toDate, toDateString, toDateUTC = midnight_today()
    # convert times to epoch
    fromDateEpoch, toDateEpoch = convert_to_epoch(fromDateUTC, toDateUTC)
    # delta epoch and remainder
    wDays, extraSec = epoch_differences(fromDateEpoch, toDateEpoch)
    # TODO  finish  -  download_files(downloadFolder, StartTime, URL_Base, Days, key, value)

def master_file_check(routeName, folderLocation):
    """
    Sets the location of the master file for each route.
    TODO Need to add a check to see if file location exists. Create if otherwise.
    """
    masterFile = f"{folderLocation}/{routeName} - Master.csv"
    return masterFile

def get_last_date(masterFile):
    """
    Reads the master .csv file for the route requested. 
    TODO If not date exists, needs to create a date for 2 years and 15 min prior.
    """ 
    df = pd.read_csv(masterFile)
    lastDateString = max(df["DateTime"])
    lastDate = datetime.strptime(lastDateString, '%Y-%m-%d %H:%M:%S')
    return lastDateString, lastDate

def download_from_date(lastDate):
    """
    Adds 15 minuets to the lastDate from the master file to reference a starting point for the downloads.
    """
    fromDate = lastDate + timedelta(minutes = 15)
    fromDateString = fromDate.strftime('%Y-%m-%d %H:%M:%S')
    fromDateUTC = fromDate.astimezone(tz.tzutc())
    return fromDate, fromDateString, fromDateUTC

def midnight_today():
    """
    Calculates the date and time of the midnight just recently passed to be used as an end time.
    """
    toDate = datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)
    toDateString = toDate.strftime('%Y-%m-%d %H:%M:%S')
    toDateUTC = toDate.astimezone(tz.tzutc())
    return toDate, toDateString, toDateUTC

def convert_to_epoch(fromDateUTC, toDateUTC):
    """
    Takes the from and to time frames and converts them to time from epoch in seconds. 
    """
    epochTime = datetime.utcfromtimestamp(0)
    epochTimeUTC = epochTime.replace(tzinfo=tz.tzutc())
    fromDateEpoch = int((fromDateUTC - epochTimeUTC).total_seconds())
    toDateEpoch = int((toDateUTC - epochTimeUTC).total_seconds())
    return fromDateEpoch, toDateEpoch

def epoch_differences(fromDateEpoch, toDateEpoch):
    """
    Calculates total days between the fromDate and toDate along with remainder to give reference for the download loop. i.e. 1.5 days = 129600 seconds. WholeDays = 1 and partialDays = 43200. Loop would run for wholeDays + 1(if partial > 0). Request URL would go startEpoch to (startEpoch + 86400) for the first loop and (startEpoch + 86400) to (startEpoch + 86400 + 43200) for the second.
    """
    deltaSeconds = toDateEpoch - fromDateEpoch
    wholeDaysRequested = deltaSeconds // 86400
    remainingSeconds = deltaSeconds % 86400
    if remainingSeconds > 0:
        wholeDaysRequested += 1
    return wholeDaysRequested, remainingSeconds

# Download data loop

def download_files(downloadFolder, 
                   fromDateEpoch, 
                   acyclicaBaseURL, 
                   wDays, 
                   extraSec, 
                   key, 
                   value
                   ):
    """
    Downloads a day of data from Acyclica at a time by piecing together the 
    url with start and end times each time period between the user request 
    start and end dates.
    """
    for day in range(wDays):
        startTime = str(fromDateEpoch + (86400 * day))
        if extraSec == 0:
            endTime = str(fromDateEpoch + (86400 * (day + 1)))
        else:
            endTime = str(fromDateEpoch + (86400 * day) + extraSec)
        acyclicaURL = f"{acyclicaBaseURL}/{key}/{startTime}/{endTime}/"
        fileName = f"{downloadFolder}/{value} {startTime}.csv"
        urllib.request.urlretrieve(acyclicaURL, fileName)





""" 
Receive data in the following format:

1543622418970
.
.
.
.
1543881596472
"""
# Format files
# then
# Combine Files
# then
# Append Files to the master file
# then 
# Delete rows over 2 yeras old

"""
TODO
Convert to format of YYYY-MM-DD HH:DD:SS in 15 min bins and adjust for time difference of UTC to America/Chicago accounting for daylight savings time differences:

2018-12-01 00:00:00
2018-12-01 00:15:00
2018-12-01 00:30:00
.....
"""

# Delete rows from 2 years before 

#deleteFromDateString = min(df["DateTime"])
#deleteFromDate = datetime.strptime(deleteFromDateString, "%Y-%m-%d %H:%M:%S")
# TODO Read the master file into a datafram
deleteYear = toDate.strftime('%Y')
deleteToYear = int(deleteYear) - 2
deleteToDate = toDate.replace(year=deleteToYear)
df.drop(df[df["DateTime"] < deleteToDateString ].index, inplace = True)
# TODO Replace the master file with the new dataframe