### catalog-query ###

A simple Python module to extract information about datasets in a CKAN catalog, via the CKAN API.
Not to be confused with [ckanapi](https://github.com/ckan/ckanapi) which is probably much better.

This was just created as a simple way to get some information from the [IOOS Data Catalog](https://data.ioos.us).

catalog-query includes some IOOS-specific add ons (or scientific data management add ons more precisely) including
the ability to run [Compliance Checker](https://github.com/ioos/compliance-checker) against OPeNDAP endpoints
using the -resource_cc_check action.

catalog-query can be easily extended by writing additional Action classes, better ones no doubt than the two already present...
See Usage section below for more details.

### Requirements ###
Currently this module works with Python 2.7 only due to the way the action classes are loaded via ```importlib.import_module```.
Looking for a way to do this in a Python 3-compatible way.

#### Installation: ####
```
git clone https://github.com/mwengren/catalog-query.git
cd catalog-query
python setup.py install
```

#### Usage: ####
```
# generate a list of datasets belonging to the 'NANOOS' Organization:
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:NANOOS

# run Compliance Checker against all resources of format 'OPeNDAP' belonging to NANOOS, and output to the file 'nanoos_opendap_compliance_results.csv'
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:NANOOS,resource_format:OPeNDAP -o nanoos_opendap_compliance_results.csv

# run Compliance Checker against all resources of format 'ERDDAP' with resource_name 'OPeNDAP' belonging to NANOOS,
# and output to the file 'nanoos_opendap_compliance_results.csv'.  This is how to extract only the ERDDAP OPeNDAP URLs from IOOS catalog.
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:NANOOS,resource_format:ERDDAP,resource_name:OPeNDAP -o nanoos_erddap_compliance_results.csv
```


Parameters:

```
-c | --catalog_api_url : The URL to the CKAN API endpoint (default: 'http://data.ioos.us/api/3')

-a | --action : The name of the catalog-query Action to execute.  

-o | --output : The name an output file to write results to (CSV format for all actions currently).  Will default to a
        CSV file in a subdirectory of the CKAN Organization name with the action name and a randomized string suffix.

-q | --query_params : Query parameter value(s) to pass to the query action.  Multiple query parameters needed for actions
        that expect multiple parameters can be passed as a comma separated string (eg. \'-q=name:AOOS,format:OPeNDAP or
        -q=name:NANOOS,resource_format:ERDDAP,resource_name:OPeNDAP)\' to run AOOS OPeNDAP services through the Compliance Checker test).

-t | --cc_tests : Compliance checker tests to run (by name, comma-separated) (eg -t=acdd:1.3,cf:1.6,ioos), for use with the
        'resource_cc_check' Action.  Consult the [Compliance Checker documentation](https://github.com/ioos/compliance-checker)
        for and explanation of the tests available.  
```
