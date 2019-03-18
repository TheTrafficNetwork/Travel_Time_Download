# TravelTimeDownload

Program to utilize Acyclica's API key to download travel time data across a user defined time period. User will enter a start and end date. Those dates will be converted to Epoch time. The programming will open a csv containing RouteID and Route Name information for all main routes within the city for each direction. Each RouteID will be looped in 24 hour periods from starting to ending date and download. When all time periods for a route are downloaded, they will be pieced together into a single .csv file before moving onto the next Route. Once complete, the user will be notified with the number of Routes downloaded along with the total number of days.

# DailyTravelTimeDownload

Program to utilize Acyclica's API to download travel time data on a daily basis. Looks for a master file for each route specified in the route dictionary. If no master file exists, it will create one. Each master file will hold up to 2 years worth of travel time data for comparison purposes. This process will check the last date of each master file and download all missing data from the last date to the last midnight passed before the current time. Times are converted to epoch and adjusted for timezones to place in the API URL. Downloads are processed in 24 hour periods, or less if missing a partial day, and looped until caught up to the current day. All downloads are merged, formatted, and appended to the master. After the new data has been added, time frames greater than two years will be removed. 

Sample URL:  https://cr.acyclica.com/datastream/route/csv/time/{APIKey}/{Route}/{Start}/{End}/

Acyclica's API Guide:  https://acyclica.zendesk.com/hc/en-us/articles/360003033252-API-Guide

Acyclica's Travel Time Algorithms:  https://acyclica.zendesk.com/hc/en-us/articles/360001897712-What-are-the-5-Travel-Time-Algorithms-