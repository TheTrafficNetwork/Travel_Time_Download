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
from datetime import datetime
from urllib import request
from urllib.request import urlretrieve
import pandas as pd
from tqdm import tqdm


def username_input(API_Key=None, User_Name=None):
    """
    Requests username, looks in a csv file for the username, and returns the 
    associated API key for the username.
    """
    APIKeyHeaders = ["Username", "API_Key"]
    APIKey_df = pd.read_csv(
        r"C:/Python Test Folder 2/API Key CSV.csv", skiprows=1,
        names=APIKeyHeaders)
    while not API_Key:
        try:
            User_Name = str.lower(input("Username: "))
            API_Key = APIKey_df.query('Username==@User_Name')[
                'API_Key'].values.item()
            print(f"{User_Name}'s API Key is {API_Key}.")
        except ValueError:
            print("No Username or multiple instances of Username in .csv.")
    return API_Key


def base_url_creation():
    """Creates the base of the url download request with the API"""
    Base_URL = f"https://cr.acyclica.com/datastream/route/csv/time"
    APIKey = username_input()
    API_URL = f"{Base_URL}/{APIKey}"
    return API_URL


def validate_date_format(date_string):
    """Check if the given input string is in correct date format."""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return 'Correct Format'
    except ValueError:
        return 'Error: Incorrect date format.'


def ask_input_date(message):
    """
    This function asks the user to input a correct date, it validates
    and asks for input until the user enters a correct date.
    """
    while True:
        input_date = input(message)
        if validate_date_format(input_date) == 'Correct Format':
            break
        else:
            print(f'{validate_date_format(input_date)}\n')
    return input_date


def user_date_input():
    """
    This is the date input function, it asks the user to enter
    start and end dates, it then checks if the ending date is after
    the starting date and shows the result at the end and formats to datetime.
    """
    while True:
        start_date_request = ask_input_date('Start date(yyyy-mm-dd): ')
        end_date_request = ask_input_date('End date(yyyy-mm-dd): ')

        if start_date_request <= end_date_request:
            print(
                f"Requested date range is {start_date_request} to {end_date_request}.")
            start_date = datetime.strptime(start_date_request, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_request, '%Y-%m-%d')
            break
        else:
            print('Starting date is after Ending date. Please try again.\n')
    return start_date, end_date, start_date_request, end_date_request


def start_end_times():
    """
    Converts user inputed dates to epoch times for use in url download 
    requests. 
    """
    StartDate, EndDate, StartDate_Str, EndDate_Str = user_date_input()
    Delta = EndDate - StartDate
    TotalDays = Delta.days + 1
    Epoch = datetime.utcfromtimestamp(0)
    StartDateInEpoch = StartDate - Epoch
    StartEpochSeconds = int((StartDateInEpoch).total_seconds())
    print(f"Requesting {TotalDays} total days of data.")
    return StartEpochSeconds, TotalDays, StartDate_Str, EndDate_Str


def timedelta_h_m_s(delta):
    """Formatting conversion of ms to hh:mm:ss for travel times."""
    h = delta.seconds // 60 // 60
    m = delta.seconds // 60
    s = delta.seconds % 60
    return'{:0>2}:{:0>2}:{:0>2}'.format(h, m, s)


def route_dict():
    """
    Trys to open a csv file containing Acyclica Route IDs,Route Names and 
    places them in a dictionary to be used for download URLs as well as file 
    and folder creation.
    """
    try:
        RouteCSV = open(r"C:/Python Test Folder 2/AcyclicaArterialRoutes.csv")
        RouteDict = {}
        for line in RouteCSV:
            entry = line.strip()
            RouteID, RouteName = entry.split(',')
            RouteDict[RouteID] = RouteName
    except FileNotFoundError:
        print("The .csv file containing routes cannot be found. Please check file location")
    return RouteDict


def folder_creation(RouteName):
    """
    Creates the folder structure for a route if there are no folders currently 
    present.
    """
    FolderPath = f"C:/Python Test Folder 2/{RouteName}"
    SubFolder = f"{FolderPath}/Downloads"
    if not os.path.isdir(FolderPath):
        os.makedirs(FolderPath)
        print(f"New folder created at {FolderPath}")
    if not os.path.isdir(SubFolder):
        os.makedirs(SubFolder)
        print(f"New Download folder created at {SubFolder}")
    return FolderPath, SubFolder


def download_files(SubFolder, StartTime, URL_Base, Days, key, value):
    """
    Downloads a day of data from Acyclica at a time by piecing together the 
    url with start and end times each 24 hour period between the user request 
    start and end dates.
    """
    for i in range(Days):
        Start = str(StartTime + 86400 * i)
        End = str(StartTime + 86400 * (i + 1))
        Acyclica_URL = f"{URL_Base}/{key}/{Start}/{End}/"
        FileName = f"{SubFolder}/{value} {Start}.csv"
        urllib.request.urlretrieve(Acyclica_URL, FileName)


def merge_downloaded_files(FolderPath, SubFolder, value, StartDateStr, EndDateStr):
    """
    Takes the first 6 columes of every .csv in SubFolder and concatenates them 
    into a single csv in the main folder.
    """
    CSV_Files = glob.glob(SubFolder + '/*.csv')
    MergedFile = pd.concat(pd.read_csv(
        f, index_col=[0, 1, 2, 3, 4, 5]) for f in CSV_Files)
    CombinedFile = f"{FolderPath}/{value} from {StartDateStr} to {EndDateStr}.csv"
    MergedFile.to_csv(CombinedFile)
    delete_downloaded_files(SubFolder)
    return CombinedFile


def delete_downloaded_files(SubFolder):
    """Cycles through all files in SubFolder and deletes every .csv file"""
    for filename in os.listdir(SubFolder):
        if filename.endswith('.csv'):
            os.remove(f'{SubFolder}/{filename}')


def format_new_files(CombinedFileToBeFormatted):
    """
    Formats the combined file for use in Excel
    -Removes lines containing 0s (missing data) as to not influence averages
    -Averages based on 15min time periods
    -Converts ms into h:mm:ss formatting
    -Splits datatime into multiple columns for different Excel formulas
    """
    df = pd.read_csv(CombinedFileToBeFormatted)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit='ms')
    df = df[(df != 0).all(1)]
    df = df.resample('15min', base=0, on="Timestamp").mean()
    df = df.reset_index()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms').apply(
        '{:%B %A %w %Y-%m-%d %H:%M:%S}'.format)
    df.Strengths = pd.to_timedelta(
        df.Strengths, unit='ms').apply(timedelta_h_m_s)
    df.Firsts = pd.to_timedelta(df.Firsts, unit='ms').apply(timedelta_h_m_s)
    df.Lasts = pd.to_timedelta(df.Lasts, unit='ms').apply(timedelta_h_m_s)
    df.Minimums = pd.to_timedelta(
        df.Minimums, unit='ms').apply(timedelta_h_m_s)
    df.Maximums = pd.to_timedelta(
        df.Maximums, unit='ms').apply(timedelta_h_m_s)
    df['Month'] = df.Timestamp.str.split(' ').str.get(0)
    df['Day'] = df.Timestamp.str.split(' ').str.get(1)
    df['DoW'] = df.Timestamp.str.split(' ').str.get(2)
    df['Date'] = df.Timestamp.str.split(' ').str.get(3)
    df['Time'] = df.Timestamp.str.split(' ').str.get(4)
    df['DoW'] = df['DoW'].astype(int)
    df.DoW = df.DoW + 1
    df['DateTime'] = df.Date + " " + df.Time
    df['Date'] = pd.to_datetime(df['Date']).apply('{:%Y-%m-%d}'.format)
    df['DateTime'] = pd.to_datetime(df['DateTime']).apply(
        '{:%Y-%m-%d %H:%M:%S}'.format)
    del df['Timestamp']
    df = df[['DateTime', 'Month', 'Day', 'DoW', 'Date', 'Time',
             'Strengths', 'Firsts', 'Lasts', 'Minimums', 'Maximums']]
    df.to_csv(CombinedFileToBeFormatted, index=False)


def day_syntax(Days):
    """Formatting for finished reporting of days"""
    if Days == 1:
        DayText = "day"
    else:
        DayText = "days"
    return DayText


def route_syntax(AcyclicaRoutes):
    """Formatting for finished reporting of routes"""
    if len(AcyclicaRoutes) == 1:
        RouteText = "route"
    else:
        RouteText = "routes"
    return RouteText


def finished(Days, AcyclicaRoutes):
    """Reporting of finished product"""
    DayText = day_syntax(Days)
    RouteText = route_syntax(AcyclicaRoutes)
    print(
        f"Operation Complete. {str(Days)} {DayText} of data for {str(len(AcyclicaRoutes))} {RouteText} have been downloaded and formatted.")


def main():
    """Main Function that runs the entire program"""
    URL_Base = base_url_creation()
    StartEpoch, Days, StartDateStr, EndDateStr = start_end_times()
    AcyclicaRoutes = route_dict()
    for key, value in tqdm(AcyclicaRoutes.items()):
        StartTime = StartEpoch
        FolderPath, SubFolder = folder_creation(value)
        download_files(SubFolder, StartTime, URL_Base, Days, key, value)
        CombinedFileToBeFormatted = merge_downloaded_files(
            FolderPath, SubFolder, value, StartDateStr, EndDateStr)
        format_new_files(CombinedFileToBeFormatted)
    finished(Days, AcyclicaRoutes)


if __name__ == '__main__':
    main()
