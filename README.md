# Travel Time Downloads

Program to utilize Acyclica's API key to download travel time data across a user defined time period. User will enter a start and end date. Those dates will be converted to Epoch time. The programming will open a csv containing RouteID and Route Name information for all main routes within the city for each direction. Each RouteID will be looped in 24 hour periods from starting to ending date and download. When all time periods for a route are downloaded, they will be pieced together into a single .csv file before moving onto the next Route. Once complete, the user will be notified with the number of Routes downloaded along with the total number of days.

Sample URL = https://cr.acyclica.com/datastream/route/csv/time/{APIKey}/{Route}/{Start}/{End}/
