"""Compare L2P Granules.

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
"""

# Standard imports
import argparse
import base64
import datetime
import json
import logging
import pathlib
import sys

# Third-party imports
import boto3
import botocore
import requests
from requests.auth import HTTPBasicAuth

# Local imports
from netcdf import compare_netcdfs_s3, compare_netcdfs_dl, write_netcdf_report

# Constants
S3_OPS = "https://archive.podaac.earthdata.nasa.gov/s3credentials"
S3_TEST = "https://archive.podaac.uat.earthdata.nasa.gov/s3credentials"
    
class Compare:
    """Class that compares test environment L2P granules with ops environment 
    L2P granules.
    """
    
    # Constants
    DATASET_DICT = {
        "MODIS_A-JPL-L2P-v2019.0": "aqua",
        "MODIS_T-JPL-L2P-v2019.0": "terra",
        "VIIRS_NPP-JPL-L2P-v2016.2": "viirs"
    }
    OPS_CMR = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    OPS_TOKEN = "https://urs.earthdata.nasa.gov/api/users/tokens"
    TEST_CMR = "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"
    TEST_TOKEN = "https://uat.urs.earthdata.nasa.gov/api/users/tokens"
    
    def __init__(self, logger):
        """
        Attributes
        ----------
        logger: logging.Logger
            Use to log status
        """
        
        self.logger = logger
        self.downloads = []
        self.test_granules = []
        self.edl_creds = get_edl_creds(logger)
        self.test_token = get_token(self.edl_creds, self.TEST_TOKEN, logger)
        self.ops_granules = []
        self.ops_token = get_token(self.edl_creds, self.OPS_TOKEN, logger)
        self.granule_diffs = {
            "ops_only": [],
            "test_only": []
        }
        self.netcdf = {}
            
    def compare_granules(self, to_download, download_dir):
        """Compare test and ops granules to produce a report on differences."""
        
        # Test only
        if to_download:
            ops_prefix = f"https://{'/'.join(self.ops_granules[0].split('/')[2:-1])}"
        else:
            ops_prefix = f"s3://{'/'.join(self.ops_granules[0].split('/')[2:-1])}"
        self.granule_diffs["test_only"] = [ test for test in self.test_granules if f"{ops_prefix}/{test.split('/')[-1]}" not in self.ops_granules ]
        
        # Ops only
        if to_download:
            test_prefix = f"https://{'/'.join(self.test_granules[0].split('/')[2:-1])}"
        else:
            test_prefix = f"s3://{'/'.join(self.test_granules[0].split('/')[2:-1])}"
        self.granule_diffs["ops_only"] = [ ops for ops in self.ops_granules if f"{test_prefix}/{ops.split('/')[-1]}" not in self.test_granules ]
        
        # Intersection
        ops = set([ops.split('/')[-1] for ops in self.ops_granules])
        test = set([test.split('/')[-1] for test in self.test_granules])
        granule_intersection = list(ops.intersection(test))
        
        # Run comparison
        if to_download:
            self.downloads = download_files(granule_intersection, download_dir, ops_prefix, test_prefix, self.test_token, self.logger)
            self.netcdf = compare_netcdfs_dl(granule_intersection, download_dir, self.logger)
        else:
            try:
                s3_creds = get_s3_creds(self.logger)
            except botocore.exceptions.ClientError as e:
                raise e
            self.netcdf = compare_netcdfs_s3(granule_intersection, ops_prefix, test_prefix, s3_creds, self.logger)
        
    def query_date(self, shortname, start, end, to_download):
        """Query by temporal range and populate test and ops granules lists."""
        
        temporal_range = f"{start}Z,{end}Z"
        self.test_granules = run_query_date(shortname, temporal_range, self.test_token, self.TEST_CMR, to_download)
        self.ops_granules = run_query_date(shortname, temporal_range, self.ops_token, self.OPS_CMR, to_download)

    def query_name(self, shortname, granule_name, to_download):
        """Query by granule name and populate test and ops granules lists."""
        
        # Search for and store granules for different environments
        self.test_granules.extend(run_query_name(shortname, granule_name, self.test_token, self.TEST_CMR, to_download))
        self.ops_granules.extend(run_query_name(shortname, granule_name, self.ops_token, self.OPS_CMR, to_download))
   
    def write_report(self, report_dir, shortname, netcdf=None):
        """Write report on comparisons between ops and test files."""
        
        date_str = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        report_file = report_dir.joinpath(f"report_{self.DATASET_DICT[shortname]}_{date_str}.txt")
        
        # Write granule differences
        with open(report_file, 'w') as rf:
            rf.write(f"===== Granule Report for {shortname} =====\n")
            rf.write("\n<<<< OPS vs. Test Granule Differences >>>>\n")
            rf.write(f"\tNumber of granules in ops: {len(self.ops_granules)}.\n")
            rf.write(f"\tNumber of granules in test: {len(self.test_granules)}.\n")
            rf.write(f"=====================================================================\n")
            
            # Write out differences in granules
            if len(self.granule_diffs["ops_only"]) > 0:
                rf.write("\tGranules in OPS only:\n")
                for granule in self.granule_diffs["ops_only"]: rf.write(f"\t\t{granule}\n")
                rf.write(f"=====================================================================\n")
            if len(self.granule_diffs["test_only"]) > 0:
                rf.write("\tGranules in Test only:\n")
                for granule in self.granule_diffs["test_only"]: rf.write(f"\t\t{granule}\n")
                rf.write(f"=====================================================================\n")
        
            # Write out granules that were found if not writing NetCDF comparison
            if not netcdf:
                if len(self.ops_granules) > 0:
                    rf.write("\tGranules in OPS:\n")
                    for granule in self.ops_granules: rf.write(f"\t\t{granule}\n")
                if len(self.test_granules) > 0:
                    rf.write("\tGranules in Test:\n")
                    for granule in self.test_granules: rf.write(f"\t\t{granule}\n")
        
        # Write results of NetCDF comparison
        if netcdf:
            write_netcdf_report(self.netcdf, report_file, shortname)
            
        self.logger.info(f"Report written: {report_file}.")
        
    def delete_downloads(self):
        """Delete downloaded files."""
        
        for download in self.downloads:
            download.unlink()
            self.logger.info(f"Deleted: {download}")
            
        
def get_edl_creds(logger):
    """Return Earthdata Login creds stored in SSM Parameter Store."""
    
    # Get EDL credentials
    try:
        ssm_client = boto3.client('ssm', region_name="us-west-2")
        username = ssm_client.get_parameter(Name="generate-edl-username", WithDecryption=True)["Parameter"]["Value"]
        password = ssm_client.get_parameter(Name="generate-edl-password", WithDecryption=True)["Parameter"]["Value"]
        logger.info("Retrieved EDL username and password.")
    except botocore.exceptions.ClientError as error:
        logger.error("Could not retrieve EDL credentials from SSM Parameter Store.")
        logger.error(error)
        raise error
    
    return {
        "username": username,
        "password": password
    }
        
def get_token(edl_creds, url, logger):
    """Return EDL bearer token based on url parameter.
    
    Accesses EDL username and password stored in SSM Parameter Store.
    
    Raises botocore.exceptions.ClientError
    """
    
    # Get EDL bearer token
    post_response = requests.get(url, 
                                  headers={"Accept": "application/json"}, 
                                  auth=HTTPBasicAuth(edl_creds["username"], edl_creds["password"]))
    token_data = post_response.json()
    if len(token_data) == 0:
        logger.error(token_data)
        logger.error(f"Could not retrieve token from: {url}.")
        return None
    else:
        logger.info(f"Successfully retrieved token from {url}.")
        return token_data[0]["access_token"]
    
def run_query_date(shortname, temporal_range, token, url, to_download):
    """Executes temporal range CMR query and returns S3 urls.""" 
    
    # Search for granule in test environment
    headers = { "Authorization": f"Bearer {token}" }
    params = {
        "short_name": shortname,
        "temporal": temporal_range,
        "page_size": 2000
    }
    res = requests.post(url=url, headers=headers, params=params)        
    granule = res.json()
    if to_download:
        s3_granules = [ url["URL"] for item in granule["items"] for url in item["umm"]["RelatedUrls"] if url["Type"] == "GET DATA" ]
    else:
        s3_granules = [ url["URL"] for item in granule["items"] for url in item["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS" ]
    
    return s3_granules
      
def run_query_name(shortname, granule_name, token, url, to_download):
    """Executes granule name CMR query and return S3 urls."""
    
    headers = { "Authorization": f"Bearer {token}" }
    params = {
        "short_name": shortname,
        "readable_granule_name": granule_name
    }
    res = requests.post(url=url, headers=headers, params=params)        
    granule = res.json()
    if to_download:
        s3_granule = [ url["URL"] for item in granule["items"] for url in item["umm"]["RelatedUrls"] if url["Type"] == "GET DATA" ]
    else:
        s3_granule = [ url["URL"] for item in granule["items"] for url in item["umm"]["RelatedUrls"] if url["Type"] == "GET DATA VIA DIRECT ACCESS" ]
    
    return s3_granule

def download_files(granules, download_dir, ops_prefix, test_prefix, test_token, logger):
    """Download granules to download directory."""
    
    downloads = []
    
    # Ops
    for granule in granules:
        granule_name = download_dir.joinpath("ops", granule.split('/')[-1])
        granule_name.parent.mkdir(exist_ok=True)
        granule_url = f"{ops_prefix}/{granule}"
        downloads.append(download(granule_url, granule_name, logger))
        
    # Test
    for granule in granules:
        granule_name = download_dir.joinpath("test", granule.split('/')[-1])
        granule_name.parent.mkdir(exist_ok=True)
        granule_url = f"{test_prefix}/{granule}"
        downloads.append(download(granule_url, granule_name, logger, token=test_token))
        
    return downloads
    
def download(granule, granule_name, logger, token=None):
    """Download granule."""
    
    if token: 
        headers = { "Authorization": f"Bearer {token}" }
        request = requests.get(granule, headers=headers, stream=True, )
    else:
        request = requests.get(granule, stream=True)
    with open(granule_name, "wb") as nc:
        for chunk in request.iter_content(chunk_size=1024):
            if chunk: nc.write(chunk)
    logger.info(f"Downloaded: {granule}.")
    return granule_name

def get_s3_creds(edl_creds, logger):
    """Query SSM Parameter Store for EDL login and generate S3 credentials."""
    
    # Request EDL creds
    auth = f"{edl_creds['username']}:{edl_creds['password']}"
    encoded_auth  = base64.b64encode(auth.encode('ascii'))
    
    # OPS creds
    ops_response = query_s3_endpoint(S3_OPS, encoded_auth)
    
    # Test creds
    test_response = query_s3_endpoint(S3_TEST, encoded_auth)
    
    return {
        "ops": {
            "key": ops_response["accessKeyId"],
            "secret": ops_response["secretAccessKey"],
            "token": ops_response["sessionToken"]
        },
        "test": {
            "key": test_response["accessKeyId"],
            "secret": test_response["secretAccessKey"],
            "token": test_response["sessionToken"]
        }
    }
    
def query_s3_endpoint(endpoint, encoded_auth):
    """Query S3 endpoint and return JSON response."""
    
    login = requests.get(endpoint, allow_redirects=False)
    login.raise_for_status()
    

    auth_redirect = requests.post(
        login.headers['location'],
        data = {"credentials": encoded_auth},
        headers= { "Origin": endpoint },
        allow_redirects=False
    )
    auth_redirect.raise_for_status()
    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)
    results = requests.get(endpoint, cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()
    response = json.loads(results.content)
    return response

def create_args():
    """Create and return argparser with arguments."""

    arg_parser = argparse.ArgumentParser(description="Retrieve a list of S3 URIs")
    arg_parser.add_argument("-g",
                            "--granulename",
                            type=str,
                            help="Name of granule to search and compare.")
    arg_parser.add_argument("-s",
                            "--startdate",
                            type=str,
                            help="Start date if searching by temporage range: YYYY-MM-DDTHH:MM:SS")
    arg_parser.add_argument("-e",
                            "--enddate",
                            type=str,
                            help="End date if searching by temporage range: YYYY-MM-DDTHH:MM:SS")
    arg_parser.add_argument("-c",
                            "--shortname",
                            type=str,
                            help="Short name of collection to search granule")
    arg_parser.add_argument("-d",
                            "--download",
                            action='store_true',
                            help="Indicates that NetCDF files should be downloaded")
    arg_parser.add_argument("-t",
                            "--delete",
                            action='store_true',
                            help="Indicates that downloaded NetCDF files should be deleted")
    arg_parser.add_argument("-o",
                            "--downloaddir",
                            type=str,
                            help="Path download files to")
    arg_parser.add_argument("-r",
                            "--reportdir",
                            type=str,
                            help="Path to store reports at")
    return arg_parser

def get_logger():
    """Return a formatted logger object."""
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create a handler to console and set level
    console_handler = logging.StreamHandler()

    # Create a formatter and add it to the handler
    console_format = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s : %(message)s")
    console_handler.setFormatter(console_format)

    # Add handlers to logger
    logger.addHandler(console_handler)

    # Return logger
    return logger
        
def compare_handler():
    """Lambda handler that uses class methods to compare data."""
    
    start = datetime.datetime.now()
    
    # Command line arguments
    arg_parser = create_args()
    args = arg_parser.parse_args()
    
    shortname = args.shortname
    granule_name = args.granulename
    start_time = args.startdate
    end_time = args.enddate
    to_download = args.download
    download_dir = pathlib.Path(args.downloaddir)
    report_dir = pathlib.Path(args.reportdir)
    to_delete = args.delete
    logger = get_logger()
    
    # Begin comparison operations
    compare = Compare(logger)
    if (compare.test_token is None) or (compare.ops_token is None):
        logger.error("Test and/or OPS bearer tokens could not be retrieved.")
        logger.error("Check EDL bearer tokens at EDL site. Exit.")
        sys.exit(1)
    
    if granule_name:
        compare.query_name(shortname, granule_name, to_download)
    else:
        compare.query_date(shortname, start_time, end_time, to_download)
        
    if len(compare.ops_granules) == 0 and len(compare.test_granules) > 0:
        logger.info("No granules were found in ops.")
        logger.info(f"# of test granules: {len(compare.test_granules)}.")
        compare.write_report(report_dir, shortname)
        logger.info("Cannot compare. Exit.")
        sys.exit(0)
        
    elif len(compare.test_granules) == 0 and len(compare.ops_granules) > 0:
        logger.info("No granules were found in test.")
        logger.info(f"# of ops granules: {len(compare.ops_granules)}.")
        compare.write_report(report_dir, shortname)
        logger.info("Cannot compare. Exit.")
        sys.exit(0)
        
    elif len(compare.ops_granules) == 0 and len(compare.test_granules) == 0:
        logger.info("No ops or test granules were found.")
        logger.info("Cannot compare. Exit.")
        sys.exit(0)
    
    else:
        try:
            compare.compare_granules(to_download, download_dir)
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error - {e}")
            logger.error("Encountered error while trying to compare granules. Exit.")
            sys.exit(1)
        
        compare.write_report(report_dir, shortname, netcdf=True)
    
    if args.delete:
        compare.delete_downloads()
    
    end = datetime.datetime.now()
    logger.info(f"Execution time - {end - start}.")

if __name__ == "__main__":
    compare_handler()