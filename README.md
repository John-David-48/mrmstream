# mrmstream
Makes pulling in MRMS data a breeze. Finds and pulls files from Amazon Web Services, 
returning an xarray or downloading netcdf files. 

import mrmstream as mrm

Simply give the aws name and the target datetime of the mrms product you are searching for, 
and the mrm.stream() function will do the rest for you, handing you an xarray dataframe of the 
closest matching file. If you need an xarray with multiple times, hand a list of times to 
mrm.streams() and let mrm work its magic. 

## CONSISTENT SCIENTIFIC MATCHING
Observation times are not evenly spaced. By taking arguments for the acceptable margin of error 
and using a consistent matching algorithm, mrmstream ensures a fair and accurate matching and 
downloading of the target data.

## NAVIGATES AWS S3 FOR YOU
mrmstream handles the AWS side of downloading the data, so you can focus your efforts on building 
your applications instead of accessing the data to begin with

## HANDLES THE DIFFICULT FILE TYPES FOR YOU
mrmstream handles the geozipped GRIB2 files for you, handing you a clean xarray output and 
downloading a netcdf of the array.

## TRACK PROGRESS WITH TQDM
Optionally, track your progress on large projects with the tqdm package, cleanly ensuring you 
understand where mrm is working.

## EXAMPLES:
Two output image files are included in this repository using the outputs of mrm.stream() and 
mrm.streams(), using 30 minutes of composite reflectivity data from June 1, 2025.

Note: ChatGPT was used in the writing of code specifically for unzipping GRIB files and the 
preliminary functions for navigating AWS.
