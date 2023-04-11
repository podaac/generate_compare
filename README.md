# compare

Compare L2P Granules.

This script compares the L2P granules produced in OPS with those produced in a
test environment. It compares the number of granules produced as well as the
content of the NetCDF files.

This file can be run with command line arguments and contains the following:

    * Compare: Class that executes the comparison operations
    * Functions to facilties querying for and retrieving NetCDF files.
    
Requires the following environmental variables be set and have access to
AWS infrastructure:

    * AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY
    * AWS_DEFAULT_REGION
    
NOTE: Direct S3 access is untested while awaiting deployment.

# installation

Copy script and install requirements found in `requirements.txt`.

# execution

Command line arguments:
-g : Name of granule to search and compare. Optional. Include instead of start and end date.
-s : Start date if searching by temporage range: YYYY-MM-DDTHH:MM:SS. Optional. Include instead of granule name.
-e : End date if searching by temporage range: YYYY-MM-DDTHH:MM:SS. Optional. Include instead of granule name.
-c : Short name of collection to search in. Required.
-d : Indicates that NetCDF files should be downloaded. Optional.
-o : Path download files to. Optional.
-r : Path to store reports at. Required.
-t : Indicates that downloaded NetCDF files should be deleted. Optional.

## Granule
python3 run_compare.py -g "20230330080000-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0" -c "MODIS_A-JPL-L2P-v2019.0" -d -o "/generate/data/compare" -r "/generate/data/compare/reports"

## Temporal range
python3 run_compare.py -s "2023-03-30T06:30:00" -e " 2023-03-30T9:30:00" -c "MODIS_A-JPL-L2P-v2019.0" -d -o "/generate/data/compare" -r "/generate/data/compare/reports"
