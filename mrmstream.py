# mrmstream.py

#-----------#
#  IMPORTS  #
#-----------#

import pandas as pd  # PROD_DF, find_mrms: handling MRMS product information
from datetime import datetime, timedelta  # find_mrms: handling dates and times
import boto3  # find_mrms: aws access
import botocore  # find_mrms: creating signature
from botocore.client import Config  # find_mrms: creating signature
import os  # download_aws_mrms: handling local filepaths
import gzip  # unzip_gz: unzipping the aws file
import shutil  # unzip_gz: saving the unzipped file
import xarray as xr  # grib2array: read GRIB and deliver dataarray
import numpy as np  # grib2array: save search_time correctly

#----------------------------#
#  Import Product Dataframe  #
#----------------------------#

try:
    PROD_DF = pd.read_csv("./UserTable_MRMS_v12.2.csv")
except FileNotFoundError as e:
    PROD_DF=None
    print("Imported functions, but no MRMS Product Dataframe was found. Some functions may require manual input. " \
    "\nView the dataframe at https://github.com/NOAA-National-Severe-Storms-Laboratory/mrms-support/tree/main/GRIB2_TABLES" \
    "\nYou can manually set the MRMS Product Dataframe with the set_PROD_DF function.")

#-------------------#
#  Basic Functions  #
#-------------------#

def find_mrms(aws_product, timestamp, time_error, location="CONUS", verbose=True):
    """Finds the path for the best target file.
    
    Keyword arguments:
    timestamp -- datetime of the target file
    product -- aws folder for full MRMS product name (e.g. 'MergedReflectivityQCComposite_00.50')
    location -- set of MRMS data to check (default 'CONUS')
    time_error -- window of time in seconds around timestamp that specifies if the closest match 
        is acceptable for the function to return
    """
    
    # Shorten variable names for f strings
    t = timestamp
    p = aws_product
    e = time_error
    
    # Assemble Amazon Web Services bucket object and prefix
    s3 = boto3.resource("s3", config=Config(signature_version=botocore.UNSIGNED))  # NOTE: Make global?
    bucket = s3.Bucket("noaa-mrms-pds")  # NOTE: Make global?
    prefix = f"{location}/{p}/{t:%Y%m%d}/MRMS_{p}_{t:%Y%m%d}"

    # Identify the pattern for timestamps in filenames
    t_start = len(location) + (1+1+6+1) + 2*len(aws_product) + (8)
    t_end = t_start + (8+1+6)
    t_format = "%Y%m%d-%H%M%S"  # NOTE: Make global?

    # Assemble dictionary of times of files in a bucket
    all_day_objects = {}
    aws_times = []
    for obj in bucket.objects.filter(Prefix=prefix):
        t_str = obj.key[t_start:t_end]
        aws_time = datetime.strptime(t_str, t_format)
        aws_times.append(aws_time)
        all_day_objects[aws_time] = obj.key

    # Find the window for the Margin Of Error (MOE)
    tpe = t + timedelta(0, e)  # timestamp plus time_error
    tme = t - timedelta(0, e)  # timestamp minus time_error
    
    # Include next day if within MOE
    if t.day != tpe.day:
        tpe_prefix = f"{location}/{p}/{tpe:%Y%m%d}/MRMS_{p}_{tpe:%Y%m%d}"
        for obj in bucket.objects.filter(Prefix=tpe_prefix):
            t_str = obj.key[t_start:t_end]
            aws_time = datetime.strptime(t_str, t_format)
            aws_times.append(aws_time)
            all_day_objects[aws_time] = obj.key
        
    # Include previous day if within MOE
    if t.day != tme.day:
        tme_prefix = f"{location}/{p}/{tme:%Y%m%d}/MRMS_{p}_{tme:%Y%m%d}"
        for obj in bucket.objects.filter(Prefix=tme_prefix):
            t_str = obj.key[t_start:t_end]
            aws_time = datetime.strptime(t_str, t_format)
            aws_times.append(aws_time)
            all_day_objects[aws_time] = obj.key

    if verbose:
        print(f"There were {len(aws_times)} files found for {p} with given parameters. Finding best match.")

    target_path = None
    true_date_time = None

    # Find the best match and record true timestamp
    if len(aws_times) > 0:
        min_tdelt = abs(t - aws_times[0])
        closest_aws = aws_times[0]
        for aws in aws_times:
            if min_tdelt > abs(t - aws):
                min_tdelt = abs(t - aws)
                closest_aws = aws
        
        if (tme < closest_aws) and (closest_aws < tpe):
            target_path = all_day_objects[closest_aws]
            true_date_time = closest_aws
        else:
            print(f"The closest datetime to the target was {closest_aws:%Y-%m-%d %H:%M:%S}, \n \
                  which was not within your time_error of {time_error} seconds. Please adjust your time_error if desired. \n \
                  Closest aws filepath found: {all_day_objects[closest_aws]} \n \
                  Please check https://noaa-mrms-pds.s3.amazonaws.com/index.html to explore available files.")

    else:
        print(f"No files were found for aws product folder {prefix}. \n \
              Please check https://noaa-mrms-pds.s3.amazonaws.com/index.html to verify existence of a valid file.")

    return target_path, true_date_time

def aws_download(target_path, local_dir=None, local_gz_path=None, overwrite=False):

    # Assemble Amazon Web Services bucket object
    s3 = boto3.resource("s3", config=Config(signature_version=botocore.UNSIGNED))  # NOTE: Make global?
    bucket = s3.Bucket("noaa-mrms-pds")  # NOTE: Make global?

    # Determine where to download the aws file
    if local_dir is None:
        user_dir = os.path.expanduser("~")  # NOTE: Make global?
        local_dir = user_dir

    if local_gz_path is None:
        mrmstream_folder = "mrmstream-downloads"  # NOTE: Make global?
        gzip_folder = "gzip"
        local_gz_path = os.path.join(mrmstream_folder, gzip_folder, target_path)
    
    abs_path = os.path.join(local_dir, local_gz_path)

    # Check if the file is already downloaded at local_path
    if not overwrite and os.path.exists(abs_path):
        print("A file already existed at the returned path. No changes were made.")
        return abs_path

    # Create the directory for the file to be saved to
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)

    # Download the gzipped file from aws to the local path
    bucket.download_file(target_path, abs_path)

    return abs_path

def unzip_gz(gz_path, grib_path=None, overwrite=False, remove_gz=True):

    # Determine where to store the unzipped file
    if grib_path is None:
        grib_name = os.path.basename(gz_path).replace(".gz","")
        grib_dir = os.path.dirname(gz_path).replace("gzip","grib")
        grib_path = os.path.join(grib_dir, grib_name)

    # Check if the file is already downloaded at grib_path
    if not overwrite and os.path.exists(grib_path):
        print("A file already existed at the returned path. No changes were made.")
    
    else:
        # Create the directory for the file to be saved to
        os.makedirs(os.path.dirname(grib_path), exist_ok=True)

        # Unzip the file
        with gzip.open(gz_path, "rb") as f_in:
            with open(grib_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    if remove_gz:
        os.remove(gz_path)
    
    return grib_path

def grib2array(grib_path, nc_path=None, overwrite=False, remove_grib=True, save_nc=True, true_time=None, nc_compression=3, search_time=None):
    
    ds = xr.open_dataset(grib_path)

    if true_time is not None:
        ds["time"] = true_time
        ds["valid_time"] = true_time
    
    if search_time is not None:
        ds["time"] = search_time

    ds.to_dataarray()

    if save_nc:
        if nc_path is None:
            nc_name = os.path.basename(grib_path).replace(".grib2",".nc")
            nc_dir = os.path.dirname(grib_path).replace("grib","netcdf")
            nc_path = os.path.join(nc_dir, nc_name)

        if not overwrite and os.path.exists(nc_path):
            print("A file already existed at the returned path. No changes were made.")

        else:
            # Create the directory for the file to be saved to
            os.makedirs(os.path.dirname(nc_path), exist_ok=True)

            ds.to_netcdf(nc_path,encoding={"unknown":{"zlib": True, "complevel": nc_compression}})
    
    if remove_grib:
        os.remove(grib_path)
        os.remove(grib_path + ".9093e.idx")

    return ds, nc_path

def match_product(aws_folder_name, product_dataframe=PROD_DF):
    idx, prod = None, None
    for i, p in enumerate(product_dataframe["Name"]):
        if p == aws_folder_name[:len(p)]:
            idx, prod = i, p
            break
    return idx, prod

def find_frequency(product_index, product_dataframe=PROD_DF):
    freq_secs = None
    freq_string = product_dataframe["Frequency"][product_index]
    if freq_string[-4:] == "-min":
        freq_secs = 60 * int(freq_string[:-4])
    return freq_secs

def make_timeslist(first_datetime, last_datetime, interval):

    if first_datetime > last_datetime:
        raise ValueError("Ensure the start date is before the end date.")
    
    timeslist = []
    current_datetime = first_datetime
    
    while current_datetime <= last_datetime:
        timeslist.append(current_datetime)
        current_datetime = current_datetime + timedelta(0, interval)

    return timeslist

def set_PROD_DF(prod_df):
    global PROD_DF
    PROD_DF = prod_df
    return PROD_DF

#-------------------------#
#  Streamlined Functions  #
#-------------------------#

def stream(product, timestamp,*, time_error="resolution", product_dataframe=PROD_DF, location="CONUS", verbose=True, local_dir=None, local_gz_path=None, overwrite=False, 
           local_grib_path=None, nc_path=None, remove_gz=True, remove_grib=True, save_nc=True, nc_compression=3, skips_ok=False):

    timestring = f"{timestamp:%Y-%m-%d %H:%M:%S}"

    # Find the time_error if not specified
    if time_error == "resolution":
        if PROD_DF == None:
            print("(0/7) No time_error or aws product_dataframe was provided. Please provide a time_error (in seconds) or check your dataframe.")
            time_error = 0
        else:
            idx, mrms_product = match_product(product,product_dataframe=product_dataframe)
            if mrms_product == None:
                time_error = 0
                print("(0/7) No MRMS product was found matching in the available MRMS dataframe. \n \
                    Please either provide a time_error or check your dataframe.")
            else:
                time_error = find_frequency(idx)
    if verbose:
        print(f"(1/7) Proceeding with time_error {time_error} seconds.")

    # Find the best match file
    if verbose:
        print(f"(2/7) Finding the closest aws match for datetime {timestring}")
    target_path, true_date_time = find_mrms(product, timestamp, time_error=time_error, location=location, verbose=verbose)
    if target_path is None:
        if skips_ok:
            return None
        else:
            raise FileNotFoundError(f"No acceptable file was found on aws for the given product {product}, datetime {timestring}, and time_error {time_error} seconds. \nPlease check https://noaa-mrms-pds.s3.amazonaws.com/index.html to verify the existence of a valid file, and turn on verbose for additional troubleshooting.")
    if verbose:
        print(f"(2/7) Found a file with datetime {true_date_time:%Y-%m-%d %H:%M:%S} at {target_path}.")

    # Download the best match file
    if verbose:
        print(f"(3/7) Downloading the aws geozipped (gz) binary (GRIB) file found at {target_path} to the computer.")
    gz_path = aws_download(target_path, local_dir=local_dir, local_gz_path=local_gz_path, overwrite=overwrite)
    if verbose:
        print(f"(3/7) Downloaded a gz file to {gz_path}")

    # Unzip the downloaded gz path
    if verbose:
        print(f"(4/7) Unzipping the gz file.")
    grib_path = unzip_gz(gz_path, grib_path=local_grib_path, overwrite=overwrite, remove_gz=remove_gz)
    if verbose:
        if remove_gz:
            print(f"(4/7) Unzipped the gz file to {grib_path} \nRemoved gz {gz_path}")
        else:
            print(f"(4/7) Unzipped the gz file to {grib_path} \nDid not remove gz {gz_path}")

    # Read in the grib to an xarray
    if verbose:
        print(f"(5/7) Reading the GRIB file into an xarray dataarray.")
    dataarray, nc_path = grib2array(grib_path, nc_path=nc_path, overwrite=overwrite, remove_grib=remove_grib, save_nc=save_nc, true_time=true_date_time, 
                                    nc_compression=nc_compression, search_time=timestamp)
    if verbose:
        print(f"(5/7) Finished creating dataarray.")
        if save_nc:
            print(f"(6/7) Saved dataarray to {nc_path}")
        else:
            print(f"(6/7) Did not save dataarray to a netcdf file.")
        if remove_grib:
            print(f"(7/7) Removed GRIB2 from {grib_path}")
        else:
            print(f"(7/7) Didn't remove GRIB {grib_path}")        

    return dataarray

def streams(product, timeslist,*, time_error="resolution", product_dataframe=PROD_DF, location="CONUS", verbose=True, local_dir=None, overwrite=False, 
            final_nc_path=None, save_big_nc=True, remove_gz=True, remove_grib=True, save_small_ncs=False, nc_compression=3, use_tqdm=True, skips_ok=False, 
            concat_along="valid_time"):
    
    arrays_list = []
    
    if use_tqdm:
        try:
            from tqdm import tqdm  # streams: show progresson downloading MRMS data.
        except ImportError as e:
            print(f"Error importing tqdm. Either install tqdm or set use_tqdm=False. {e}")
        for timestamp in tqdm(timeslist, "Building dataarrays"):
            a = stream(product, timestamp, time_error=time_error, product_dataframe=product_dataframe, location=location, verbose=verbose, local_dir=local_dir, local_gz_path=None, 
                       overwrite=overwrite, local_grib_path=None, nc_path=None, remove_gz=remove_gz, remove_grib=remove_grib, save_nc=save_small_ncs, nc_compression=nc_compression, 
                       skips_ok=skips_ok)
            arrays_list.append(a)
    else:
        for timestamp in timeslist:
            a = stream(product, timestamp, time_error=time_error, product_dataframe=product_dataframe, location=location, verbose=verbose, local_dir=local_dir, local_gz_path=None, 
                       overwrite=overwrite, local_grib_path=None, nc_path=None, remove_gz=remove_gz, remove_grib=remove_grib, save_nc=save_small_ncs, nc_compression=nc_compression, 
                       skips_ok=skips_ok)
            arrays_list.append(a)

    final_dataarray = xr.concat(arrays_list, concat_along)

    # Determine where to download the file
    if local_dir is None:
        user_dir = os.path.expanduser("~")  # NOTE: Make global?
        local_dir = user_dir

    if final_nc_path is None:
        mrmstream_folder = "mrmstream-downloads"  # NOTE: Make global?
        nc_folder = "netcdf"
        final_nc_path = os.path.join(mrmstream_folder, nc_folder, f"{product}_{timeslist[0]:%Y%m%d%H%M%S}-{timeslist[-1]:%Y%m%d%H%M%S}")

    if save_big_nc:
        final_dataarray.to_netcdf(final_nc_path,encoding={"unknown":{"zlib":True, "complevel":nc_compression}})

    return final_dataarray

#------------------------#
#  Examples and Testing  #
#------------------------#

if __name__ == "__main__":
    print("__name__ == '__main__'")

    ### EXAMPLE ONE: Download one frame of MRMS Composite Reflectivity on June 1, 2025 ###

    product = 'MergedReflectivityQCComposite_00.50'
    timestamp = datetime(2025, 6, 1, 0, 0, 0)

    x = stream(product, timestamp, time_error=120)

    ### EXAMPLE TWO: Download first 30 mins of frames of MRMS Composite Reflectivity on June 1, 2025 ###

    product = 'MergedReflectivityQCComposite_00.50'
    timestamp1 = datetime(2025, 6, 1, 0, 0, 0)
    timestamp2 = datetime(2025, 6, 1, 0, 30, 0)
    timeslist = make_timeslist(timestamp1, timestamp2)

    y = streams(product, timeslist, time_error=120)