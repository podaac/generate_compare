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
from netcdf import compare_netcdfs_s3, compare_netcdfs_dl
from write import write_txt_report, write_html_reports

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
            self.logger.info(f"Downloading {len(self.ops_granules)} ops granules and {len(self.test_granules)} test granules.")
            self.downloads = download_files(granule_intersection, download_dir, ops_prefix, test_prefix, self.ops_token, self.test_token, self.logger)
            self.netcdf = compare_netcdfs_dl(granule_intersection, download_dir, self.logger)
        else:
            try:
                s3_creds = get_s3_creds(self.logger)
                self.netcdf = compare_netcdfs_s3(granule_intersection, ops_prefix, test_prefix, s3_creds, self.logger)
            except botocore.exceptions.ClientError as e:
                raise e
    
    def query_date(self, shortname, start, end, to_download, search_revision, logger):
        """Query by temporal range and populate test and ops granules lists."""
        
        temporal_range = f"{start}Z,{end}Z"
        self.test_granules = run_query_date(shortname, temporal_range, self.test_token, self.TEST_CMR, to_download, search_revision, logger)
        self.ops_granules = run_query_date(shortname, temporal_range, self.ops_token, self.OPS_CMR, to_download, search_revision, logger)

    def query_name(self, shortname, granule_name, to_download):
        """Query by granule name and populate test and ops granules lists."""
        
        # Search for and store granules for different environments
        self.test_granules.extend(run_query_name(shortname, granule_name, self.test_token, self.TEST_CMR, to_download))
        self.ops_granules.extend(run_query_name(shortname, granule_name, self.ops_token, self.OPS_CMR, to_download))
    
    def write_reports(self, report_dir, html_dir, shortname, start_time, create_html, netcdf=False):
        
        granule_data = write_txt_report(report_dir, shortname, start_time,
                                        self.ops_granules, self.test_granules, 
                                        self.granule_diffs, self.netcdf, 
                                        self.logger, netcdf)
        
        if create_html:
            write_html_reports(html_dir, shortname, report_dir, start_time, 
                               self.ops_granules, self.test_granules, 
                               self.granule_diffs, granule_data, self.logger)
    
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
        logger.info(f"Retrieved EDL username: {username} and password.")
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
    
def run_query_date(shortname, temporal_range, token, url, to_download, search_revision, logger):
    """Executes temporal range CMR query and returns S3 urls.""" 
    
    # Search for granule in test environment
    headers = { "Authorization": f"Bearer {token}" }
    if search_revision:
        params = {
            "short_name": shortname,
            "revision_date": temporal_range,
            "page_size": 2000
        }
    else:
        params = {
            "short_name": shortname,
            "temporal": temporal_range,
            "page_size": 2000
        }
    logger.info(f"Search URL: {url}")
    logger.info(f"Search parameters: {params}")
    res = requests.post(url=url, headers=headers, params=params)    
    granule = res.json()
    # logger.info(f"Search response: {granule}")
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

def download_files(granules, download_dir, ops_prefix, test_prefix, ops_token, test_token, logger):
    """Download granules to download directory."""
    
    downloads = []
    
    # Ops
    for granule in granules:
        granule_name = download_dir.joinpath("ops", granule.split('/')[-1])
        granule_name.parent.mkdir(exist_ok=True)
        granule_url = f"{ops_prefix}/{granule}"
        downloads.append(download(granule_url, granule_name, logger, token=ops_token))
        
    # Test
    for granule in granules:
        granule_name = download_dir.joinpath("test", granule.split('/')[-1])
        granule_name.parent.mkdir(exist_ok=True)
        granule_url = f"{test_prefix}/{granule}"
        downloads.append(download(granule_url, granule_name, logger, token=test_token))
        
    return downloads
    
def download(granule, granule_name, logger, token=None):
    """Download granule."""
    
    headers = { "Authorization": f"Bearer {token}" }
    request = requests.get(granule, headers=headers, stream=True)
    logger.info(f"Request headers for {granule.split('/')[-1]}: {request.headers['Content-Type']}, {request.headers['Content-Length']}")
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
    arg_parser.add_argument("-l",
                            "--logdir",
                            type=str,
                            help="Path to store logs at")
    arg_parser.add_argument("-v",
                            "--revision",
                            action='store_true',
                            help="Whether to search by revision date")
    arg_parser.add_argument("-w",
                            "--html",
                            action='store_true',
                            help="Write HTML files to display report instead of txt")
    arg_parser.add_argument("-p",
                            "--htmldir",
                            type=str,
                            default="",
                            help="Directory path to store HTML pages")
    return arg_parser

def get_logger(log_file):
    """Return a formatted logger object."""
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    log_format = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s : %(message)s")

    # Create a handler to console and set level and format
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    
    # Create a handler to file and set level and format
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(log_format)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

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
    log_dir = pathlib.Path(args.logdir)
    to_delete = args.delete
    search_revision = args.revision
    create_html = args.html
    html_dir = pathlib.Path(args.htmldir)
    
    # Create user directories
    download_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Logging
    if start_time:
        date_str = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").strftime("%Y%m%dT%H%M%S")
    else:
        date_str = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = log_dir.joinpath(f"{shortname}_{date_str}.log")
    logger = get_logger(log_file)
    
    # Begin comparison operations
    compare = Compare(logger)
    if (compare.test_token is None) or (compare.ops_token is None):
        logger.error("Test and/or OPS bearer tokens could not be retrieved.")
        logger.error("Check EDL bearer tokens at EDL site. Exit.")
        sys.exit(1)
    
    if granule_name:
        compare.query_name(shortname, granule_name, to_download)
    else:
        compare.query_date(shortname, start_time, end_time, to_download, search_revision, logger)
        
    if len(compare.ops_granules) == 0 and len(compare.test_granules) > 0:
        logger.info("No granules were found in ops.")
        logger.info(f"# of test granules: {len(compare.test_granules)}.")
        compare.write_reports(report_dir, html_dir, shortname, start_time, create_html)
        logger.info("Cannot compare. Exit.")
        sys.exit(0)
        
    elif len(compare.test_granules) == 0 and len(compare.ops_granules) > 0:
        logger.info("No granules were found in test.")
        logger.info(f"# of ops granules: {len(compare.ops_granules)}.")
        compare.write_reports(report_dir, html_dir, shortname, start_time, create_html)
        logger.info("Cannot compare. Exit.")
        sys.exit(0)
        
    elif len(compare.ops_granules) == 0 and len(compare.test_granules) == 0:
        logger.info("No ops or test granules were found.")
        logger.info("Cannot compare. Exit.")
        compare.write_reports(report_dir, html_dir, shortname, start_time, create_html)
        sys.exit(0)
    
    else:
        try:
            compare.compare_granules(to_download, download_dir)
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error - {e}")
            logger.error("Encountered error while trying to compare granules. Exit.")
            sys.exit(1)
        
        compare.write_reports(report_dir, html_dir, shortname, start_time, create_html, netcdf=True)
    
    if to_delete:
        compare.delete_downloads()
    
    end = datetime.datetime.now()
    logger.info(f"Execution time - {end - start}.")

if __name__ == "__main__":
    compare_handler()