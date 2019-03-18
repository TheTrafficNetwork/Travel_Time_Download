TO DO LIST
===========

### Current Code
- [x] Modify time pulls for local time of both daylight savings and standard. Currently pulls UDT time frames.
- [x] Create master files for each route that contain 2 years worth of data.
- [x] Skip over up to date files.
- [ ] Interpolate over short data gaps (1 to 3 time periods possibly?)
- [ ] Condense formatting code.
- [ ] Multithread the download and format section for quicker run times.
- [ ] Make sure the Download folder is empty BEFORE downloads start as to not incorporate eroneous data.
- [ ] Incorporate error handling of HTTP failures.
- [ ] Prompt user for file location for apikey and routes OR ask user input if default file location is not found.
- [ ] Prompt user for save location (have a default).
- [ ] Create a log file for both errors and successful downloads to write to.
- [ ] Email upon errors for dailydownload to know when to check for issues. 

 
### Future Code
- [x] Create a single master file for each route that is referenced for start day and downloads missing days through local time
- [x] Auto download every day at 2am from last download point for each route
- [ ] Combine all master files into a database
- [ ] Create graphical displays that compare different date ranges/time of day periods so Excel processing is no longer needed
 
 
### Files
- [x] Create a To Do List
- [x] Create a ReadMe
- [ ] Create a Setup
- [ ] Create Requirements
- [ ] Proper structuring of project files/folder