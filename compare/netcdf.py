"""Compare NetCDF files.

Provides functionality for direct S3 access and downloaded or local files.
"""

# Standard imports
from functools import reduce

# Third-party imports
import fsspec
from netCDF4 import Dataset
import numpy as np

def compare_netcdfs_dl(nc_files, downloads_dir, logger):
    """Compare NetCDFs that have been downloaded."""
    
    netcdf_dict = {}
    for nc_file in nc_files:
        logger.info(f"Comparing: {nc_file}.")
        
        # Open datasets
        dev_ds = Dataset(f"{downloads_dir.joinpath('test', nc_file)}")
        prod_ds = Dataset(f"{downloads_dir.joinpath('ops', nc_file)}")
        
        # Compare
        netcdf_dict[nc_file] = {}
        netcdf_dict[nc_file]["dim_dict"] = {}
        netcdf_dict[nc_file]["att_dict"] = {}
        netcdf_dict[nc_file]["var_dict"] = {}
        
        # Dimensions
        netcdf_dict[nc_file]["dim_dict"] = compare_dimensions(dev_ds, prod_ds)
        # Global Attributes
        netcdf_dict[nc_file]["att_dict"] = compare_attributes(dev_ds, prod_ds)
        # Variables
        netcdf_dict[nc_file]["var_dict"] = compare_variables(dev_ds, prod_ds)

        # Close open handles
        dev_ds.close()
        prod_ds.close()
        
    return netcdf_dict

def compare_netcdfs_s3(nc_files, prod_prefix, dev_prefix, s3_creds, logger):
    """Compare NetCDFs directly in S3."""

    netcdf_dict = {}
    for nc_file in nc_files:        
        
        # Open datasets
        prod_fs = fsspec.open(f"s3://{prod_prefix}/{nc_file}", mode="rb",
                              key=s3_creds["ops"]["key"], secret=s3_creds["ops"]["secret"],
                              token=s3_creds["ops"]["token"])            
        prod_file = prod_fs.open()
        prod_ds = Dataset("prod_file", mode="r", memory=prod_file.read())
        
        dev_fs = fsspec.open(f"s3://{dev_prefix}/{nc_file}", mode="rb",
                              key=s3_creds["test"]["key"], secret=s3_creds["test"]["secret"],
                              token=s3_creds["test"]["token"])
        dev_file = dev_fs.open()
        dev_ds = Dataset("dev_file", mode="r", memory=dev_file.read())
        
        # Compare
        netcdf_dict[nc_file] = {}
        netcdf_dict[nc_file]["dim_dict"] = {}
        netcdf_dict[nc_file]["att_dict"] = {}
        netcdf_dict[nc_file]["var_dict"] = {}
        
        # Dimensions
        netcdf_dict[nc_file]["dim_dict"] = compare_dimensions(dev_ds, prod_ds)
        # Global Attributes
        netcdf_dict[nc_file]["att_dict"] = compare_attributes(dev_ds, prod_ds)
        # Variables
        netcdf_dict[nc_file]["var_dict"] = compare_variables(dev_ds, prod_ds)

        # Close open handles
        prod_ds.close()
        prod_file.close()
        prod_fs.close()
        dev_ds.close()
        dev_file.close()
        dev_fs.close()
        
    return netcdf_dict

def compare_attributes(dev_ds, prod_ds):
    """Compare NetCDF global attributes between NetCDF Dataset objects. """

    att = { 
        "prod_present_only": [], 
        "dev_present_only": [], 
        "global_att": [] 
    }

    for k in dev_ds.__dict__.keys():
        if k not in prod_ds.__dict__.keys(): att["dev_present_only"].append(k)

    for k in prod_ds.__dict__.keys():
        if k not in dev_ds.__dict__.keys(): att["prod_present_only"].append(k)

    # Compare global attributes
    prod_dict = dict(prod_ds.__dict__.items())
    for k,v in dev_ds.__dict__.items():
        # Check if variable is present in production
        if not k in prod_ds.__dict__.keys():
            continue

        # Check if equal
        if prod_dict[k] != v: att["global_att"].append((k, prod_dict[k], v))
    
    return att

def compare_dimensions(dev_ds, prod_ds):
    """Compare NetCDF dimensions between NetCDF Dataset objects. """

    dim = {
        "prod_present_only": [],
        "dev_present_only": [],
        "names_not_equal": [],
        "size_not_equal": []
    }

    for k in dev_ds.dimensions.keys():
        if k not in prod_ds.dimensions.keys(): dim["dev_present_only"].append(k)

    for k in prod_ds.dimensions.keys():
        if k not in dev_ds.dimensions.keys(): dim["prod_present_only"].append(k)

    for k,v in dev_ds.dimensions.items():
        # Check if variable is present in production
        if not k in prod_ds.dimensions.keys():
            continue

        # Check if names are equal
        if prod_ds.dimensions[k].name != v.name: dim["names_not_equal"].append((k, prod_ds.dimensions[k].name, v.name))
        
        # Check if sizes are equal
        if prod_ds.dimensions[k].size != v.size: dim["size_not_equal"].append((k, prod_ds.dimensions[k].size, v.size))
    
    return dim

def compare_variables(dev_ds, prod_ds):
    """Compare NetCDF variables between NetCDF Dataset objects. """

    var = {
        "prod_present_only": [],
        "dev_present_only": [],
        "var_content": {}
    }

    for k in dev_ds.variables.keys():
        if k not in prod_ds.variables.keys(): var["dev_present_only"].append(k)

    for k in prod_ds.variables.keys():
        if k not in dev_ds.variables.keys(): var["prod_present_only"].append(k)

    # Compare variables
    for k,v in dev_ds.variables.items():

        var["var_content"][k] = {}
        
        # Check if variable is present in production
        if not k in prod_ds.variables.keys():
            continue

        # Compare variable attributes
        dev_atts = v.__dict__.keys()
        prod_atts = prod_ds[k].__dict__.keys()
        var["var_content"][k]["atts_equal"] = reduce(lambda j, k: j and k, map(lambda i, j: i == j, dev_atts, prod_atts), True)

        # Compare variable arrays
        dev_v = v[:].filled(-9999)
        prod_v = prod_ds[k][:].filled(-9999)
        var["var_content"][k]["arrays_equal"] = np.array_equal(dev_v, prod_v, equal_nan=True)

    return var

def write_netcdf_report(data_dict, report_file, dataset):
    """Writes NetCDF file differences to disk."""

    granule_data = { "granules": {} }
    with open(report_file, 'a') as rf:
        rf.write(f"\n=================== NetCDF Reports for {dataset} =======================\n")
        nc_not_equal = []
        for nc_file in data_dict.keys():
            rf.write(f"\n\n<< Report for file: {nc_file} >>\n")    
            equal_dims = write_netcdf_dims(data_dict[nc_file]["dim_dict"], rf)
            equal_atts, ops_date, test_date = write_netcdf_atts(data_dict[nc_file]["att_dict"], rf)
            equal_vars = write_netcdf_var(data_dict[nc_file]["var_dict"], rf)
            if not equal_dims or not equal_atts or not equal_vars: nc_not_equal.append(nc_file)
            granule_data["granules"][nc_file] = {
                "equal_dims": equal_dims,
                "equal_atts": equal_atts,
                "equal_vars": equal_vars,
                "ops_date": ops_date,
                "uat_date": test_date
            }
            rf.write("--------------------------------------------------------------------------------------\n")

        if len(nc_not_equal) != 0:
            granule_data["nc_not_equal"] = nc_not_equal
            rf.write("\n<<<< NetCDF files that are different: >>>>\n")
            for nc_file in nc_not_equal:
                rf.write(f"\t{nc_file}\n")
        else:
            rf.write("\n<<<< All NetCDF files that were compared are equal. >>>>\n")
        
        if granule_data:
            granule_data["report_file"] = report_file.name    
        return granule_data

def write_netcdf_atts(att_dict, rf):
    """Write global attribute differences to the report.
    
    Attributes
    ----------
    att_dict: dict
        Dictionary of global attribute-level differences
    rf: _io.TextIOWrapper
        Report file handle to write to

    Returns boolean value indicating if datasets are equal
    """
        
    rf.write("\n<<<< Global Attribute-Level Differences >>>>\n")
    equal = True
        
    if len(att_dict["dev_present_only"]) != 0:
        equal = False
        rf.write("\t\tAttributes in development only:\n")
        for e in att_dict["dev_present_only"]:
            rf.write(f"\t\t{e}\n")
    else:
        rf.write("\t\tAttributes in development are accounted for.\n")

    if len(att_dict["prod_present_only"]) != 0:
        equal = False
        rf.write("\t\tAttributes in production only:\n")
        for e in att_dict["prod_present_only"]:
            rf.write(f"\t\t{e}\n")
    else:
        rf.write("\t\tAttributes in production are accounted for.\n")

    if len(att_dict["global_att"]) != 0:
        equal = False
        rf.write("\t\tAttribute names that are not equal:\n")
        for e in att_dict["global_att"]:
            rf.write(f"\t\tName: {e[0]}\n")
            rf.write(f"\t\t\tProduction: {e[1]}\n")
            rf.write(f"\t\t\tDevelopment: {e[2]}\n")
            if e[0] == "date_created": 
                prod_date = e[1]
                test_date = e[2]
                if len(att_dict["global_att"]) == 1: equal = True
    else:
        rf.write("\t\tAttributes are the same.\n")
    
    return equal, prod_date, test_date

def write_netcdf_dims(dim_dict, rf):
    """Write dimension differences to the report.
    
    Attributes
    ----------
    dim_dict: dict
        Dictionary of dimension-level differences
    rf: _io.TextIOWrapper
        Report file handle to write to

    Returns boolean value indicating if datasets are equal
    """

    rf.write("\n<<<< Dimension-Level Differences >>>>\n")
    equal = True

    if len(dim_dict["dev_present_only"]) != 0:
        equal = False
        rf.write("\t\tDimensions in development only:\n")
        for e in dim_dict["dev_present_only"]:
            rf.write(f"\t\t{e}\n")
    else:
        rf.write("\t\tDimensions in development are accounted for.\n")

    if len(dim_dict["prod_present_only"]) != 0:
        equal = False
        rf.write("\t\tDimensions in production only:\n")
        for e in dim_dict["prod_present_only"]:
            rf.write(f"\t\t{e}\n")
    else:
        rf.write("\t\tDimensions in production are accounted for.\n")

    if len(dim_dict["names_not_equal"]) != 0:
        equal = False
        rf.write("\t\tDimension names that are not equal:\n")
        for e in dim_dict["names_not_equal"]:
            rf.write(f"\t\tName: {e[0]}\n")
            rf.write(f"\t\t\tProduction: {e[1]}\n")
            rf.write(f"\t\t\tDevelopment: {e[2]}\n")
    else:
        rf.write("\t\tDimension names are the same.\n")

    if len(dim_dict["size_not_equal"]) != 0:
        equal = False
        rf.write("\t\tDimension sizes that are not equal:\n")
        for e in dim_dict["size_not_equal"]:
            rf.write(f"\t\tSize: {e[0]}\n")
            rf.write(f"\t\t\tProduction: {e[1]}\n")
            rf.write(f"\t\t\tDevelopment: {e[2]}\n")
    else:
        rf.write("\t\tDimension sizes are the same.\n")

    return equal

def write_netcdf_var(var_dict, rf):
    """Write variable differences to report.
    
    Attributes
    ----------
    var_dict: dict
        Dictionary of variable-level differences
    rf: _io.TextIOWrapper
        Report file handle to write to

    Returns boolean value indicating if datasets are equal
    """

    rf.write("\n<<<< Variable-Level Differences >>>>\n")
    equal = True

    if len(var_dict["dev_present_only"]) != 0:
        equal = False
        rf.write("\t\tVariables in development only:\n")
        for e in var_dict["dev_present_only"]:
            rf.write(f"\t\t\t{e}\n")
    else:
        rf.write("\t\tVariables in development are accounted for.\n")

    if len(var_dict["prod_present_only"]) != 0:
        equal = False
        rf.write("\t\tVariables in production only:\n")
        for e in var_dict["prod_present_only"]:
            rf.write(f"\t\t\t{e}\n")
    else:
        rf.write("\t\tVariables in production are accounted for.\n")

    rf.write("\t\tVariable attributes and data that are not equal:\n")
    for k in var_dict["var_content"].keys():
        try:
            atts_equal = var_dict["var_content"][k]["atts_equal"]
            data_equal = var_dict["var_content"][k]["arrays_equal"]
            if not atts_equal or not data_equal: 
                equal = False
                rf.write(f"\t\t\t{k}:\n")
                rf.write(f"\t\t\t\tAttributes equal: {atts_equal}\n")
                rf.write(f"\t\t\t\tData equal: {data_equal}\n")
        except KeyError:
            equal = False
            continue

    if equal == True: rf.write(f"\t\t\tAll variables have been accounted for.\n")
    return equal
