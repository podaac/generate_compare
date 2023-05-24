"""Writes reports and HTML files to display compare stats.

TXT: Write TXT report file of compare stats.

Creates TXT file to display the details of the produced from comparison of 
production and test granules. Writes one TXT file per run.

HTML: Write HTML files to display compare stats.

Creates HTML files to display stats produced from comparison of production and
test granules:
- index.html : Top-level report
- timeline.html : Aggregation of data over time
- archive/ : Archive of past data
- detailed_reports/ : TXT files of detailed NetCDF granule comparisons
"""

# Standard imports 
from collections import OrderedDict
import datetime
import json
import os
import pathlib
import shutil

# Local imports
from netcdf import write_netcdf_report

# Constants
DATASET_DICT = {
    "MODIS_A-JPL-L2P-v2019.0": "aqua",
    "MODIS_T-JPL-L2P-v2019.0": "terra",
    "VIIRS_NPP-JPL-L2P-v2016.2": "viirs"
}

def write_txt_report(report_dir, shortname, start_time, ops_granules, 
                     test_granules, granule_diffs, netcdf_data, logger, 
                     netcdf=False):
    """Write report on comparisons between ops and test files."""
    
    if start_time:
        date_str = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").strftime("%Y%m%dT%H%M%S")
    else:
        date_str = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    report_file = report_dir.joinpath(f"report_{DATASET_DICT[shortname]}_{date_str}.txt")
    
    # Write granule differences
    with open(report_file, 'w') as rf:
        rf.write(f"===== Granule Report for {shortname} =====\n")
        rf.write("\n<<<< OPS vs. Test Granule Differences >>>>\n")
        rf.write(f"\tNumber of granules in ops: {len(ops_granules)}.\n")
        rf.write(f"\tNumber of granules in test: {len(test_granules)}.\n")

        # Write out differences in granules
        if len(granule_diffs["ops_only"]) > 0:
            rf.write("\n\t----------------------------------------------------------------------------------\n")
            rf.write("\tGranules in OPS only:\n")
            for granule in granule_diffs["ops_only"]: rf.write(f"\t\t{granule}\n")
            
        if len(granule_diffs["test_only"]) > 0:
            rf.write("\n\t----------------------------------------------------------------------------------\n")
            rf.write("\tGranules in Test only:\n")
            for granule in granule_diffs["test_only"]: rf.write(f"\t\t{granule}\n")
        
        # Write out granules that were found if not writing NetCDF comparison
        if not netcdf:
            if len(ops_granules) > 0:
                rf.write("\n\tAll Granules in OPS:\n")
                for granule in ops_granules: rf.write(f"\t\t{granule}\n")
            if len(test_granules) > 0:
                rf.write("\n\tAll Granules in Test:\n")
                for granule in test_granules: rf.write(f"\t\t{granule}\n")
                
        rf.write("\n======================================================================================\n")
    
    # Write results of NetCDF comparison
    granule_data = {}
    if netcdf:
        granule_data = write_netcdf_report(netcdf_data, report_file, shortname)
        
    logger.info(f"Report written: {report_file}.")
    return granule_data
    
def write_html_reports(html_dir, shortname, report_dir, start_time, ops_granules, 
                       test_granules, granule_diffs, granule_data, logger):
    """Write report in HTML format to html_dir which points to a hosted
    web space."""
    
    dataset = DATASET_DICT[shortname]
    
    # Set up for HTML page creation
    setup_html(html_dir, report_dir, logger)
    
    # HTML report name
    if start_time:
        date_str = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").strftime("%Y%m%dT%H%M%S")
    else:
        date_str = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        
    # Write overview of granule differences
    html_file = html_dir.joinpath(f"index-{dataset}-new.html")
    html_fh = open(html_file, 'w')
    write_html_header(html_fh, dataset)
    logger.info("Wrote HTML header for hourly page.")

    nc_not_equal = check_not_equal_status(granule_data, len(ops_granules), len(test_granules))
    no_granule_data = True if not granule_data else False
    write_html_overview(date_str, html_fh, ops_granules, test_granules, 
                        granule_diffs, nc_not_equal, no_granule_data)
    logger.info("Wrote HTML overview table and stats for hourly page.")
    
    # Append detailed report for each NetCDF
    if granule_data:
        write_granule_html(html_fh, granule_data)
        logger.info("Wrote HTML granule-level table and stats for hourly page.")
        
    # Close file
    html_fh.write("</body>")
    html_fh.close()
    logger.info(f"Completed overview and granule level HTML report for: {date_str}.")
    
    # Archive previous report and replace with current
    archive_index = archive_html_report(html_dir, dataset, logger)
    
    # Write historic report
    write_timeline_html(html_dir, dataset, date_str, 
                        ops_granules, test_granules, nc_not_equal,
                        archive_index, logger)
    
def setup_html(html_dir, report_dir, logger):
    """Create HTML directories, copy style sheet, move detail reports."""
    
    # Archive
    archive = html_dir.joinpath("archive")
    archive.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created archive directory if needed: {archive}.")
    
    # Detailed reports
    detail_reports = html_dir.joinpath("detail-reports")
    detail_reports.mkdir(parents=True, exist_ok=True)
    with os.scandir(report_dir) as entries:
        for entry in entries: shutil.move(entry, detail_reports.joinpath(entry.name))
    logger.info(f"Moved all NetCDF detail reports to: {detail_reports}.")
    
    # Stylesheet
    css = pathlib.Path().absolute().joinpath("html_files", "style.css")
    shutil.copy(css, html_dir.joinpath(css.name))
    logger.info(f"Copied css file to web directory: {html_dir.joinpath(css.name)}.")
    
    shutil.copy(css, html_dir.joinpath("archive", css.name))
    logger.info(f"Copied css file to archive directory: {html_dir.joinpath('archive', css.name)}.")
    
    # HTML
    html = pathlib.Path().absolute().joinpath("html_files", "index.html")
    shutil.copy(html, html_dir.joinpath(html.name))
    logger.info(f"Copied html file to web directory: {html_dir.joinpath(html.name)}.")
    
def write_html_header(html_file, dataset):
    
    # Header
    html_file.write("<!DOCTYPE html>\n<html>\n<head>\n<link rel='stylesheet' href='style.css'>\n</head>\n<body>\n")
    
    # Nav bar
    html_file.write("<ul class='nav'>\n")
    html_file.write("<li class='nav'><a href='index.html'>Home</a></li>\n")
    html_file.write(f"<li class='nav'><a href='index-{dataset}.html'>Overview</a></li>\n")
    html_file.write(f"<li class='nav'><a href='timeline-{dataset}.html'>Timeline</a></li>\n")
    html_file.write("<li class='nav'><a href='detail-reports'>Detail Reports</a></li>\n")
    html_file.write("<li class='nav'><a href='archive'>Archives</a></li>\n")
    html_file.write("</ul>\n")
    
def check_not_equal_status(granule_data, ops_num, uat_num):
    """Check if an error was encountered or all NetCDF files were the same."""
    
    if "nc_not_equal" in granule_data.keys():
        nc_not_equal = granule_data["nc_not_equal"]
    elif not granule_data:
        if ops_num == 0 and uat_num == 0:
            nc_not_equal = []
        else:
            nc_not_equal = ["Error"]
    else:
        nc_not_equal = []
    return nc_not_equal
    
def write_html_overview(date_str, html_file, ops_granules, test_granules, 
                        granule_diffs, nc_not_equal, no_granule_data):
    """Write top level report for granule differences."""
    
    # Table
    html_file.write(f"<h1>{date_str} Generate L2P Granule Comparison: OPS vs. UAT</h1>\n")
    columns = ["Hour", "# of OPS Granules", "# of UAT Granules"]
    table_head = f"<thead>\n<tr><th>{'</th><th>'.join(columns)}</th>\n</tr>\n</thead>"
    table_body = f"\n<tbody>\n<tr><td>{date_str}</td><td>{len(ops_granules)}</td><td>{len(test_granules)}</td></tr>\n</tbody>\n"
    html_file.write("<h2>Overview Comparison</h2>\n")
    html_file.write(f"<table>\n{table_head}{table_body}</table>\n")
    
    if len(granule_diffs["ops_only"]) > 0 or len(granule_diffs["test_only"]) > 0:
        html_file.write("<h2>Overview Differences</h2>\n")        
    # OPS only
    if len(granule_diffs["ops_only"]) > 0:
        html_file.write("<b>Granules in OPS only: </b>\n")
        write_html_list(html_file, granule_diffs["ops_only"])
    # Test only
    if len(granule_diffs["test_only"]) > 0:
        html_file.write("<b>Granules in UAT only: </b>\n")
        write_html_list(html_file, granule_diffs["test_only"])
        
    # Write out granules that were found if not writing NetCDF comparison
    if  no_granule_data:
        if len(ops_granules) > 0:
            html_file.write("<h2>All Granules in OPS:</h2>\n")
            write_html_list(html_file, ops_granules)
        if len(test_granules) > 0:
            html_file.write("<h2>All Granules in UAT:</h2>\n")
            write_html_list(html_file, test_granules)
        
    # NetCDF files that were not equal
    if len(nc_not_equal) > 0:
        if nc_not_equal[0] != "Error":
            html_file.write("<h2>Unequal NetCDF Granules: </h2>\n")
            html_file.write("<ul>\n")
            for granule in nc_not_equal: html_file.write(f"<li>{granule}</li>\n")
            html_file.write("</ul>\n")

def write_html_list(html_file, data):
    """Write out an unorder HTML list."""
    
    html_file.write("<ul>\n")
    for element in data: html_file.write(f"<li>{element}</li>\n")
    html_file.write("</ul>\n")
        
def write_granule_html(html_fh, granule_data):
    """Generate HTML table of granule-level comparison details."""
    
    columns = ["Granule", "OPS Creation Time", "UAT Creation Time", "Global Attribue Equality", "Dimension Equality", "Variable Equality", "Report File"]
    table_head = f"<thead>\n<tr><th>{'</th><th>'.join(columns)}</th>\n</tr>\n</thead>\n"
    table_body = "<tbody>\n"
    report_file = granule_data['report_file']
    for granule, data in granule_data["granules"].items():
        equal = is_equal(data)
        if not equal:
            table_body += f"<tr class='not_equal'>"
        else:
            table_body += f"<tr>"
        table_body += f"<td>{granule}</td>"
        table_body += f"<td>{data['ops_date']}</td>"
        table_body += f"<td>{data['uat_date']}</td>"
        table_body+= f"<td>{data['equal_atts']}</td>"
        table_body+= f"<td>{data['equal_dims']}</td>"
        table_body+= f"<td>{data['equal_vars']}</td>"
        table_body+= f"<td><a href='detail-reports/{report_file}' target='_blank'>{report_file}</a></td></tr>\n"
    table_body+= "</tbody>\n"
    
    html_fh.write("<h2>Granule-Level Comparison</h2>\n")
    html_fh.write(f"<table>\n{table_head}{table_body}</table>\n")
    
def is_equal(granule_data):
    """Determine if granule is not equal between OPS and UAT."""
    
    is_equal = True
    for value in granule_data.values():
        if not value:
            is_equal = False
            break
    return is_equal

def archive_html_report(html_dir, dataset, logger):
    """Place previous report in archive directory named after date string."""
    
    # Determine if previous index.html file
    archive_index = ""
    previous_index = html_dir.joinpath(f"index-{dataset}.html")
    if previous_index.exists():
        # Get date string
        with open(previous_index) as html_fh:
            for line in html_fh.readlines(): 
                if line.startswith("<h1>"):
                    date_str = line.split("<h1>")[-1].split(' ')[0]
    
        # Rename and move to archive directory
        archive_index = html_dir.joinpath("archive", f"{date_str}-{dataset}.html")
        shutil.move(previous_index, archive_index)
        logger.info(f"Moved: {previous_index} to {archive_index}")
        update_nav(archive_index)
        logger.info("Updated archived file navigation bar.")       
    
    # Rename current report
    current_index = html_dir.joinpath(f"index-{dataset}-new.html")
    shutil.move(current_index, previous_index)
    logger.info(f"Renamed: {current_index} to '{previous_index}'.")
    
    return archive_index

def update_nav(archive_index):
    """Update navigation bar for archived page."""
    
    with open(archive_index) as fh:
        archive_data = fh.read().splitlines()
        
    for i in range(len(archive_data)):
        if "class='nav'" in archive_data[i]:
            archive_data[i] = archive_data[i].replace("href='", "href='../")
    
    archive_data = '\n'.join(archive_data)
    with open(archive_index, 'w') as fh:
        fh.write(archive_data)

def write_timeline_html(html_dir, dataset, date_str, ops_granules, 
                        test_granules, nc_not_equal, archive_file, logger):
    """Write historic time line table HTML page."""
    
    # Load previous data
    json_file = html_dir.joinpath("json", f"timeline-{dataset}.json")
    json_file.parent.mkdir(parents=True, exist_ok=True)
    previous_data = {}
    if json_file.exists():
        with open(json_file) as jf:
            previous_data = json.load(jf)
        previous_data = OrderedDict(sorted(previous_data.items(), reverse=True))
        
    # Create page
    html_file = html_dir.joinpath(f"timeline-{dataset}.html")
    html_fh = open(html_file, 'w')
    write_html_header(html_fh, dataset)
    logger.info(f"Wrote header for timeline page of historic data.")    
    
    # Format table
    columns = ["Hour", "# OPS", "# UAT", "Equality", "Archive File"]
    table_head = f"<thead>\n<tr><th>{'</th><th>'.join(columns)}</th>\n</tr>\n</thead>\n"
    table_body = "<tbody>\n"
    
    # Format current data 
    table_body = write_current_timeline(table_body, dataset, date_str,
                                        ops_granules, test_granules, 
                                        nc_not_equal)

    # Format previous data
    if previous_data:
        table_body = write_previous_timeline(table_body, previous_data, 
                                              archive_file)
    table_body+= "</tbody>\n"
    
    # Display data
    html_fh.write("<h1>Timeline Data</h1>\n")
    html_fh.write(f"<table>\n{table_head}{table_body}</table>\n")
    logger.info("Wrote table for timeline page of historic data.")
    
    # Write updated data to timeline.json
    write_timeline_json(html_dir, dataset, previous_data, date_str, 
                        ops_granules, test_granules, nc_not_equal)
    logger.info(f"Wrote timeline JSON: {json_file}.")
    
    # Close file
    html_fh.write("</body>")
    html_fh.close()
    logger.info(f"Completed timeline page for historic date: {html_file}.")
    
def write_current_timeline(table_body, dataset, date_str, ops_granules, 
                           test_granules, nc_not_equal):
    """Write current data to timeline table."""
        
    is_equal = len(nc_not_equal) == 0
    if is_equal:
        table_body += f"<tr>"
    else:
        table_body += f"<tr class='not_equal'>"
    table_body += f"<td>{date_str}</td>"
    table_body += f"<td>{len(ops_granules)}</td>"
    table_body += f"<td>{len(test_granules)}</td>"
    table_body+= f"<td>{is_equal}</td>"
    table_body+= f"<td><a href='index-{dataset}.html'>Current</a></td></tr>\n"
    return table_body
    
def write_previous_timeline(table_body, previous_data, archive_file):
    """Display previous data in a table."""
    
    # Update previous archive file
    for hour in previous_data.keys():
        if previous_data[hour]['archive'] == "Current":
            previous_data[hour]['archive'] = archive_file.name
    
    # Format table
    for hour, data in previous_data.items():
        if not data['equality']:
            table_body += f"<tr class='not_equal'>"
        else:
            table_body += f"<tr>"
        table_body += f"<td>{hour}</td>"
        table_body += f"<td>{data['num_ops']}</td>"
        table_body += f"<td>{data['num_uat']}</td>"
        table_body+= f"<td>{data['equality']}</td>"
        table_body+= f"<td><a href='archive/{data['archive']}' target='_blank'>{data['archive']}</a></td></tr>\n"
    return table_body
    
def write_timeline_json(html_dir, dataset, previous_data, date_str, 
                        ops_granules, test_granules, nc_not_equal):
    """Write out updated timeline JSON data."""
    
    if previous_data:
        previous_data[date_str] = {
            "num_ops": len(ops_granules),
            "num_uat": len(test_granules),
            "equality": len(nc_not_equal) == 0,
            "archive": "Current"
        }
    else:
        previous_data = {
            date_str : {
                "num_ops": len(ops_granules),
                "num_uat": len(test_granules),
                "equality": len(nc_not_equal)  == 0,
                "archive": "Current"
            }
        }
    json_file = html_dir.joinpath("json", f"timeline-{dataset}.json")
    with open(json_file, 'w') as jf:
        json.dump(previous_data, jf, indent=2)