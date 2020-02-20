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
Python 3 compatible.  Should work in Python 2.7, however untested at present.  Relies on dynamic module loading
via ```importlib.import_module``` to execute compatible 'Action' classes.

#### Installation: ####
```
git clone https://github.com/mwengren/catalog-query.git
cd catalog-query
python setup.py install
```

#### Usage: ####
Generate a list of datasets belonging to a CKAN Catalog Organization by name:
```
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:NANOOS
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:SCCOOS
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:NERACOOS -o neracoos_dataset_list.csv
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:MARACOOS -o maracoos_dataset_list.csv
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:SECOORA -o secoora_dataset_list.csv
catalog-query -c https://data.ioos.us/api/3 -a dataset_list -q=name:CeNCOOS -o cencoos_dataset_list.csv
```

Run Compliance Checker against all resources of format 'OPeNDAP' belonging to a few different IOOS Catalog Organizations, output Compliance Checker
scores and errors to named CSV files:
```
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:NANOOS,resource_format:OPeNDAP -o nanoos_opendap_compliance_results.csv -e nanoos_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:PacIOOS,resource_format:OPeNDAP -o pacioos_opendap_compliance_results.csv -e pacioos_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:CARICOOS,resource_format:OPeNDAP -o caricoos_opendap_compliance_results.csv -e caricoos_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:SCCOOS,resource_format:OPeNDAP,resource_name:OPeNDAP -o sccoos_opendap_compliance_results.csv -e sccoos_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:NERACOOS,resource_format:OPeNDAP,resource_name:OPeNDAP -o neracoos_opendap_compliance_results.csv -e neracoos_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:MARACOOS,resource_format:OPeNDAP,resource_name:OPeNDAP -o maracoos_opendap_compliance_results.csv -e maracoos_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:SECOORA,resource_format:OPeNDAP -o secoora_opendap_compliance_results.csv -e secoora_opendap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:CeNCOOS,resource_format:OPeNDAP -o cencoos_opendap_compliance_results.csv -e cencoos_opendap_compliance_errors.csv
```

Run Compliance Checker against all resources of format 'ERDDAP' with resource_name 'OPeNDAP' belonging to a few different IOOS Catalog Organizations, and output to named CSV files for Compliance Checker scores and errors.  These are a few examples of how to filter only the ERDDAP OPeNDAP and Tabledap URLs from IOOS catalog.  Because the metadata fed into the Catalog may differ, proper query parameters may vary.
```
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:NANOOS,resource_format:ERDDAP,resource_name:OPeNDAP -o nanoos_erddap_compliance_results.csv -e nanoos_erddap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:NERACOOS,resource_name:ERDDAP-tabledap -o neracoos_erddap_compliance_results.csv -e neracoos_erddap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:GCOOS,resource_name:ERDDAP-tabledap -o gcoos_erddap_compliance_results.csv -e gcoos_erddap_compliance_errors.csv
catalog-query -c https://data.ioos.us/api/3 -a resource_cc_check -q=name:CeNCOOS,resource_name:ERDDAP-tabledap -o cencoos_erddap_compliance_results.csv -e cencoos_erddap_compliance_errors.csv
```


Parameters:

```
-c | --catalog_api_url : The URL to the CKAN API endpoint (default: 'http://data.ioos.us/api/3')

-a | --action : The name of the catalog-query Action to execute.  

-o | --output : The name an output file to write results to (CSV format for all actions currently).  Will default to a
        CSV file in a subdirectory of the CKAN Organization name with the action name and a randomized string suffix.

-e | --error_output : The name of an output filename to write error information to (CSV format).  Will default to a
        subdirectory of the CKAN Organization name with the action name and a randomized string suffix  Only used in the
        'resource_cc_check' Action currently.

-q | --query_params : Query parameter value(s) to pass to the query action.  Multiple query parameters needed for actions
        that expect multiple parameters can be passed as a comma separated string (eg. \'-q=name:AOOS,format:OPeNDAP or
        -q=name:NANOOS,resource_format:ERDDAP,resource_name:OPeNDAP)\' to run AOOS OPeNDAP services through the Compliance Checker test).

-t | --cc_tests : Compliance checker tests to run (by name, comma-separated) (eg -t=acdd:1.3,cf:1.6,ioos), for use with the
        'resource_cc_check' Action.  Consult the [Compliance Checker documentation](https://github.com/ioos/compliance-checker)
        for and explanation of the tests available.  
```
