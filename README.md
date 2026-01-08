# mrmstream
Makes pulling in Multi-Radar Multi-Sensor System (MRMS) data a breeze. Finds and pulls files 
from Amazon Web Services (AWS), returning an xarray or downloading netcdf files. 

# import mrmstream as mrm

Provide the AWS name and the target datetime of the MRMS product you are searching for, 
and the mrm.stream() function will do the rest for you, handing you an xarray dataframe of the 
closest matching file. If you need an xarray with multiple times, hand a list of times to 
mrm.streams() and let mrm work its magic. 

## CONSISTENT SCIENTIFIC MATCHING
Observation times are not evenly spaced in AWS. By taking arguments for the acceptable margin 
of error and using a consistent matching algorithm, mrmstream helps ensure fair and accurate 
matching and downloading of the target data.

## NAVIGATES AWS S3 FOR YOU
mrmstream handles the AWS side of downloading the data, so you can focus your efforts on 
your own applications instead of having to learn to access the data you want to work with.

## HANDLES THE DIFFICULT FILE TYPES FOR YOU
mrmstream handles the geozipped GRIB2 files for you, handing you a clean xarray output and 
downloading a netcdf of the dataarray. mrmstream can build consistent file paths on its own, 
and is also compatible with creating custom file path names if desired.

## TRACK PROGRESS WITH TQDM
Optionally, monitor large projects with the tqdm package, cleanly ensuring you understand the 
amount of progress mrm is making.

## EXAMPLES:
Two output image files are included in this repository using the outputs of mrm.stream() and 
mrm.streams(), using 30 minutes of composite reflectivity data from June 1, 2025.


## Notes
ChatGPT was used in the writing of code specifically for unzipping GRIB files and the preliminary 
functions for navigating AWS. ChatGPT was also used in troubleshooting the early versions of the 
function used for plotting the example data.

This package is still in its early stages. Comments and requests are welcome as I work on building 
this tool -- if you encounter errors, please let me know!



