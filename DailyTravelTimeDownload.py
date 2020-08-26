#!/usr/bin/env python3
"""
Program to utilize Acyclica's API to download travel time data. Looks for a
master file for each route specified in the route dictionary. If no master
file exists, it will create one. Each master file will hold up to 2 years
worth of travel time data for comparison purposes. This process will check the
last date of each master file and download all missing data from the last date
to the last midnight passed before the current time. Times are converted to
epoch and adjusted for timezones to place in the API URL. Downloads are
processed in batches of 24 hour periods or less, and looped
until caught up to the current day. All downloads are merged, formatted, and
appended to the master. After the new data has been added, time frames greater
than two years will be removed.

Sample URL =
https://cr.acyclica.com/datastream/route/csv/time/APIKey/Route/Start/End/

Acyclica's API Guide:
https://acyclica.zendesk.com/hc/en-us/articles/360003033252-API-Guide
"""


import csv
import datetime
import glob
import logging
import os
import os.path
import requests
import sys
import time
from datetime import datetime, timedelta
from dateutil import tz
import pandas as pd
import numpy as np
from tqdm import tqdm

logging.basicConfig(
    filename="Logs.txt",
    level=logging.ERROR,
    format="--- %(asctime)s - Line:%(lineno)d - %(levelname)s - %(message)s",
)


def route_dict():
    """
    Trys to open a csv file containing Acyclica Route IDs,Route Names and
    places them in a dictionary to be used for download URLs as well as file
    and folder creation.

    Returns:
        routeDict (dictionary): Routes and Route IDs combinations
    """
    csvFile = "AcyclicaRoutes.csvs"
    try:
        with open(csvFile) as routeCSV:
            routeDict = {}
            for line in routeCSV:
                entry = line.strip()
                routeID, routeName = entry.split(",")
                routeDict[routeID] = routeName
    except FileNotFoundError:
        logging.error(
            f"{csvFile} does not exist. Check naming convention and file structure."
        )
        print(f"\nThe file: '{csvFile}' cannot be found. Please check file location.")
        sys.exit(1)
    return routeDict


def base_url_creation():
    """
    Creates the base of the url download request with the API

    Returns:
        apiURL (string): URL used in Acyclica's API to download data
    """
    Base_URL = f"https://cr.acyclica.com/datastream/route/csv/time"
    try:
        APIKey = open(r"API_KEY.csv", "r").readline()
    except FileNotFoundError:
        print(
            "The .csv file containing API information cannot be found. "
            + "Please check file location."
        )
    apiURL = f"{Base_URL}/{APIKey}"
    return apiURL


def folder_creation(routeName):
    """
    Creates the folder structure for a route if there are no folders currently
    present.

    Args:
        routeName (string): Name of route being downloaded

    Returns:
        routeFolder (string): Location of folder for the route
        downloadFolder (string): Location of folder to download data to
    """
    routeFolder = f"AcyclicaData\\{routeName}"
    downloadFolder = f"{routeFolder}\\Downloads"
    if not os.path.isdir(routeFolder):
        os.makedirs(routeFolder)
        logging.info(f"New folder created at {routeFolder}")
    if not os.path.isdir(downloadFolder):
        os.makedirs(downloadFolder)
        logging.info(f"New Download folder created at {downloadFolder}")
    return routeFolder, downloadFolder


def check_old_files(downloadFolder):
    """
    Checks downloadFolder to see if there are any files left over and deletes
    them if so.

    Args:
        downloadFolder (String): [Folder location for downloading travel times]
    """
    if os.path.exists(downloadFolder) and os.path.isdir(downloadFolder):
        if not os.listdir(downloadFolder):
            pass
        else:
            logging.info(f"Removed files left over in {downloadFolder}")
            for fileName in os.listdir(downloadFolder):
                os.remove(f"{downloadFolder}/{fileName}")


def master_file_check(routeName, folderLocation):
    """
    Sets the location of the master file for each route. Checks to see if file
    exists. If it does not, then it creates it with applicable headers.

    Args:
        routeName (string): Name of the route going to be downloaded
        folderLocation (string): Location where the route files will be located

    Returns:
        masterFile (string): Name of the file where travel times are kept
    """
    masterFile = f"{folderLocation}\\{routeName} - Master.csv"
    if not os.path.isfile(masterFile):
        with open(masterFile, "w") as newFile:
            newFile.write(
                "DateTime,Month,Day,DoW,Date,Time,Strengths,Firsts,Lasts,Minimums,Maximums\n2020-04-30 23:45:00,April,Thursday,30,2020-04-30,23:45:00,00:00:00,00:00:00,00:00:00,00:00:00,00:00:00\n"
            )
        # TODO Need to move find date before this point so that we can populate a generic time line from 2 years ago so that get_last_date can have a reference for the beginning download.
    return masterFile


def get_last_date(masterFile):
    """
    Reads the master .csv file for the route requested.
    TODO If not date exists, needs to create a date for 2 years and 15 min
    prior.

    Args:
        masterFile (string): Location of the main file containing route data

    Returns:
        lastDate (datetime): Lastest date for data in the master file
    """
    df = pd.read_csv(masterFile)
    try:
        lastDateString = max(df["DateTime"])
    except ValueError:
        print("No dates in the masterfile to get a max value from.")
    lastDate = datetime.strptime(lastDateString, "%Y-%m-%d %H:%M:%S")
    return lastDate


def download_from_date(lastDate):
    """
    Adds 15 minuets to the lastDate from the master file to reference a
    starting point for the downloads.

    Args:
        lastDate (datetime): Latest date for the data in the master file

    Returns:
        fromDate (datetime): Date to start downloading data from
        fromDateUTC (datetime): fromDate in UTC timezone
    """
    fromDate = lastDate + timedelta(minutes=15)
    fromDateUTC = fromDate.astimezone(tz.tzutc()).replace(tzinfo=None)
    return fromDate, fromDateUTC


def midnight_today():
    """
    Calculates the date and time of the midnight just recently passed to be
    used as an end time.

    Returns:
        toDate (datetime): Date to download data to. Yesterday's midnight.
        toDateUTC (datetime): toDate in UTC
    """
    toDate = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    toDateUTC = toDate.astimezone(tz.tzutc()).replace(tzinfo=None)
    return toDate, toDateUTC


def convert_to_epoch(fromDateUTC, toDateUTC):
    """
    Takes the from and to time frames and converts them to time from epoch in
    seconds.

    Args:
        fromDateUTC (datetime): Starting date for downloading data
        toDateUTC (datetime): Ending date for downloading data

    Returns:
        fromDateEpoch (int): Epoch conversion of fromDateUTC for API use
        toDateEpoch (int): Epoch converstion of toDateUTC for API use
    """
    epochTime = datetime.utcfromtimestamp(0)
    # TODO check to see if the epcoh in UTC is needed or breaking the time system
    fromDateEpoch = int((fromDateUTC - epochTime).total_seconds())
    toDateEpoch = int((toDateUTC - epochTime).total_seconds())
    return fromDateEpoch, toDateEpoch


def epoch_differences(start, finish):
    """
    Calculates total days between the fromDate and toDate along with remainder
    to give reference for the download loop. i.e. 1.5 days = 129600 seconds.
    days = 1 with partialDays = 43200 as remainder. Loop would run for
    daysRequested. Request URL would go startEpoch to (startEpoch + 86400) for
    the first loop and (startEpoch + 86400) to (startEpoch + 86400 + 43200) for
    the second.

    Args:
        start (int): Start time of download date range
        finish (int): End time of download date range

    Returns:
        daysRequested (int): Number of days requested in download
        remaningSeconds (int): Number of seconds left over not totalling a day
    """
    deltaSeconds = finish - start
    daysRequested = -(-deltaSeconds // 86400)
    remainingSeconds = deltaSeconds % 86400
    return daysRequested, remainingSeconds


def loop_download(url, routeID, routeName, folder, start, days, seconds):
    """
    Creates the start and end times for each period in the total requested set
    of data. Forwards each time period to the download module.

    Args:
        url (string): url used for Acyclica's API
        routeID (string): The ID of the route being downloaded
        routeName (string): Name of the route being downloaded
        folder (string): location for the download to save to
        start (int): starting epoch time of the download date range
        days (int): number of days to download data for
        seconds (int): extra seconds that don't fill a day to download data for
    """
    if seconds == 0:
        for day in tqdm(range(days), desc=f"Downloading {routeName}"):
            startTime = str(start + (86400 * day))
            endTime = str(start + (86400 * (day + 1)))
            download_file(url, routeID, routeName, folder, startTime, endTime)
    else:
        for day in tqdm(range(days), desc=f"Downloading {routeName}"):
            startTime = str(start + (86400 * day))
            if day < (days - 1):
                endTime = str(start + (86400 * (day + 1)))
            else:
                endTime = str(start + (86400 * day) + seconds)
            download_file(url, routeID, routeName, folder, startTime, endTime)


def download_file(url, routeID, routeName, folder, startTime, endTime):
    """
    Downloads up to a 24 hour period of data from Acyclica's site using their
    API url and specifying a new file name for each download.

    Args:
        url (string): url used for Acyclica's API
        routeID (string): The ID of the route being downloaded
        routeName (string): Name of the route being downloaded
        folder (string): location for the download to save to
        startTime (string): starting time in epoch to insert into url
        endTime (string): ending time in epoch to insert into url
    """
    acyclicaURL = f"{url}/{routeID}/{startTime}/{endTime}/"
    fileName = f"{folder}/{routeName} {startTime}.csv"
    routeData = requests.get(acyclicaURL)
    if routeData.status_code != 200:
        raise ConnectionError(
            f"\nError downloading route: {routeName} with ID: {routeID}.\nTime frame : {startTime} to {endTime}.\nError Code: {routeData.status_code}\nURL: {acyclicaURL}"
        )
    with open(fileName, "wb") as file:
        file.write(routeData.content)


def merge_downloaded_files(routeFolder, downloadFolder, value):
    """
    Takes the first 6 columes of every .csv in SubFolder and concatenates them
    into a single csv in the main folder.

    Args:
        routeFolder (string): Folder containing all of a route's data
        downloadFolder (string): Folder where all data is downloaded
        value (string): Name of route for downloaded data

    Returns:
        mergedFilePath: Location of merged file containing downloaded data
    """
    csvFiles = glob.glob(downloadFolder + "/*.csv")
    mergedFile = pd.concat(
        pd.read_csv(f, index_col=[0, 1, 2, 3, 4, 5]) for f in csvFiles
    )
    mergedFilePath = f"{routeFolder}/{value} temp.csv"
    mergedFile.to_csv(mergedFilePath)
    delete_downloaded_files(downloadFolder)
    return mergedFilePath


def delete_downloaded_files(downloadFolder):
    """
    Cycles through all files in downloadFolder and deletes every .csv file

    Args:
        downloadFolder (string): Location of the folder for downloading data
    """
    for fileName in os.listdir(downloadFolder):
        if fileName.endswith(".csv"):
            os.remove(f"{downloadFolder}/{fileName}")


def timedelta_h_m_s(delta):
    """Formatting conversion of ms to hh:mm:ss for travel times.

    Args:
        delta (datetime): milliseconds of travel time

    Returns:
        datetime: time in hh:mm:ss format
    """
    h = delta.seconds // 60 // 60
    m = delta.seconds // 60
    s = delta.seconds % 60
    return "{:0>2}:{:0>2}:{:0>2}".format(h, m, s)


def file_fill(df, fromDateEpoch, toDateEpoch):
    """
    If downloaded data is empty, i.e. detector was offline, for the entire time
    period downloaded, fill in the .csv with blank data for the start and end
    times to prevent future downloading of blank data. Start and end times are
    converted from epoch seconds to milliseconds due to the data downloads
    being in milliseconds.

    Args:
        df (dataframe): pandas dataframe of the merged .csv file (blank)
        fromDateEpoch (int): Epoch version of the fromDate in seconds
        toDateEpoch (int): Epoch version of the toDate in seconds

    Returns:
        df (dataframe): dataframe with a start and end time added (in ms)
    """
    timeRange = [str(fromDateEpoch * 1000), str(toDateEpoch * 1000)]
    for time in timeRange:
        df = df.append({"Timestamp": time}, ignore_index=True)
    return df


def format_new_files(mergedFilePath, fromDateEpoch, toDateEpoch):
    """
    Formats the combined file for use in Excel
    -Removes lines containing 0s (missing data) as to not influence averages
    -Averages based on 15min time periods
    -Converts ms into h:mm:ss formatting
    -Splits datetime into multiple columns for different Excel formulas

    Args:
        mergedFilePath (Epoch): Location of the downloaded merged data
        fromDateEpoch (int): Epoch version of the fromDate
        toDateEpoch (int): Epoch version of the toDate
    """
    df = pd.read_csv(mergedFilePath)
    if df.empty == True:
        df = file_fill(df, fromDateEpoch, toDateEpoch)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")
    df = df.replace(0, np.nan)
    df = df.resample("15min", base=0, on="Timestamp").mean()
    # TODO possible interpolate over x amount of Nan rows? Max of 1-3?
    df = df.reset_index()
    df["Timestamp"] = df["Timestamp"].dt.tz_localize("utc").dt.tz_convert("US/Central")
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms").apply(
        "{:%B %A %w %Y-%m-%d %H:%M:%S}".format
    )
    df.Strengths = pd.to_timedelta(df.Strengths, unit="ms").apply(timedelta_h_m_s)
    df.Firsts = pd.to_timedelta(df.Firsts, unit="ms").apply(timedelta_h_m_s)
    df.Lasts = pd.to_timedelta(df.Lasts, unit="ms").apply(timedelta_h_m_s)
    df.Minimums = pd.to_timedelta(df.Minimums, unit="ms").apply(timedelta_h_m_s)
    df.Maximums = pd.to_timedelta(df.Maximums, unit="ms").apply(timedelta_h_m_s)
    df["Month"] = df.Timestamp.str.split(" ").str.get(0)
    df["Day"] = df.Timestamp.str.split(" ").str.get(1)
    df["DoW"] = df.Timestamp.str.split(" ").str.get(2)
    df["Date"] = df.Timestamp.str.split(" ").str.get(3)
    df["Time"] = df.Timestamp.str.split(" ").str.get(4)
    df["DoW"] = df["DoW"].astype(int)
    df.DoW = df.DoW + 1
    df["DateTime"] = df.Date + " " + df.Time
    df["Date"] = pd.to_datetime(df["Date"]).apply("{:%Y-%m-%d}".format)
    df["DateTime"] = pd.to_datetime(df["DateTime"]).apply("{:%Y-%m-%d %H:%M:%S}".format)
    del df["Timestamp"]
    df = df[
        [
            "DateTime",
            "Month",
            "Day",
            "DoW",
            "Date",
            "Time",
            "Strengths",
            "Firsts",
            "Lasts",
            "Minimums",
            "Maximums",
        ]
    ]
    df.to_csv(mergedFilePath, index=False)


def append_new_timeframes(mergedFilePath, masterFile):
    """
    Appends the new temp file to the master file.

    Args:
        mergedFilePath (string): Location of the downloaded merged data
        masterFile (string): Location of the master file for the route
    """
    with open(masterFile, "ab") as fout:
        with open(mergedFilePath, "rb") as f:
            next(f)
            fout.write(f.read())
    # TODO put a check in to make sure the files were appended before deleting
    delete_temp_file(mergedFilePath)


def delete_temp_file(mergedFilePath):
    """
    Deletes the merged file

    Args:
        mergedFilePath (string): Location of the downloaded merged data
    """
    os.remove(mergedFilePath)


def delete_old_timeframes(toDate, masterFile):
    """
    Reads the master file for the Route and deletes entries older than 2 years

    Args:
        toDate (datetime): Date of last downloaded data
        masterFile (string): Location of the master file for the route
    """
    deleteYear = toDate.strftime("%Y")
    deleteToYear = int(deleteYear) - 2
    deleteToDate = toDate.replace(year=deleteToYear)
    deleteToDateString = datetime.strftime(deleteToDate, "%Y-%m-%d %H:%M:%S")
    df = pd.read_csv(masterFile)
    df.drop(df[df["DateTime"] < deleteToDateString].index, inplace=True)
    df.to_csv(masterFile, index=False)


def download_from_acyclica():
    """
    Main function that runs through the process to download route data.
    - Creates a dictionary of Route IDs and Route Names.
    - Creates the base for url used by Acyclicas API to retrieve data.
    - For each route in the dictionary of routes:
        - Downloads data starting from the last date in the master file.
        - Formats data and merges into the master file while purging duplicates.
        - Deletes data older than 2 years from the last date downloaded.
    """
    acyclicaRoutes = route_dict()
    acyclicaBaseURL = base_url_creation()
    for key, value in tqdm(acyclicaRoutes.items(), desc="Download All"):
        routeFolder, downloadFolder = folder_creation(value)
        masterFile = master_file_check(value, routeFolder)
        lastDate = get_last_date(masterFile)
        fromDate, fromDateUTC = download_from_date(lastDate)
        toDate, toDateUTC = midnight_today()
        if toDate > fromDate:
            fromDateEpoch, toDateEpoch = convert_to_epoch(fromDateUTC, toDateUTC)
            wDays, extraSec = epoch_differences(fromDateEpoch, toDateEpoch)
            loop_download(
                acyclicaBaseURL,
                key,
                value,
                downloadFolder,
                fromDateEpoch,
                wDays,
                extraSec,
            )
            mergedFilePath = merge_downloaded_files(routeFolder, downloadFolder, value)
            format_new_files(mergedFilePath, fromDateEpoch, toDateEpoch)
            append_new_timeframes(mergedFilePath, masterFile)
            delete_old_timeframes(toDate, masterFile)
        else:
            continue


# TODO log downloading data fromDateString toDateString


if __name__ == "__main__":
    download_from_acyclica()
