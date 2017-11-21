"""
Action class that obtains all DataSets belonging to a particular organization,
and dumps them in a .csv file to a subdirectory
"""
from __future__ import unicode_literals
try:
    from urllib.parse import urlencode, urlparse   # Python 3
    from io import StringIO
except ImportError:
    from urllib import urlencode  # Python 2
    from urlparse import urlparse
    from StringIO import StringIO
from builtins import str
import os
import errno
import io
import json
import sys
import subprocess
from datetime import datetime, timedelta
from dateutil import parser

import logging
import random
import string
import requests
import pandas
from compliance_checker.runner import ComplianceChecker, CheckSuite


# local:
from ..util import obtain_owner_org, package_search, create_output_dir
from ..catalog_query import ActionException

# logging:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log = logging.FileHandler('dataset_list.log', mode='w')
log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
logger.addHandler(log)

"""
Resource Compliance Checker Action:
 check a data provider's dataset's resources against Compliance Checker tests.
 This Action expects a Organization name to query for resources of a particular format (OPeNDAP for example) which
 can then be passed to Compliance Checker.
"""
class Action:
    """
    Attributes
    ----------
    catalog_api_url : str
        URL of CKAN API to sumbit queries to



    """

    #def __init__(self, *args, **kwargs):
    def __init__(self, **kwargs):
        # decode parameters:
        self.catalog_api_url = kwargs.get("catalog_api_url")

        # the query parameters for this action are all passed in list form in the 'query' parameter arg, and must be decoded:
        # this is a bit of a hack to extract query parameter keys into instance variables to use in the queries
        # expected values are along the lines of:
        #   -name:<organization_name>
        #   - more...?
        self.params_list = kwargs.get("query").split(",")
        if len(self.params_list) == 1 and len(self.params_list[0]) == 0:
            raise ActionException("Error running the Resource Compliance Checker action.  Query Params not passed to Action")
        self.query_params = dict(param.split(":") for param in self.params_list)

        # at a minimum, we need an organization name to filter resources by:
        if "name" not in self.query_params.keys():
            raise ActionException("Error running the Resource Compliance Checker action.  No 'name' parameter (CKAN Organization name) passed to the Resource Compliance Checker Action.  This is required.")

        # create output file in a directory of the Organization's name for general logging (create if not already existing):
        # get the Action file name to use in naming output file, using os.path.split:
        action_name = os.path.split(__file__)[1].split(".")[0]
        label = "".join(random.choice(string.ascii_lowercase) for i in range(5))
        filename = os.path.join(self.query_params.get("name"), action_name + ".out")

        if not os.path.exists(os.path.dirname(filename)):
            # will throw ActionException with error message if the output directory can't be created:
            create_output_dir(os.path.dirname(filename))
        self.out = io.open(filename, mode="wt", encoding="utf-8")

        # create the results_filename (path to results output file) depending on if an 'output' filename parameter was provided or not:
        if "output" in kwargs:
            self.results_filename = kwargs['output']
        # utf-8 issues resolved by just passing results_filename to DataFrame.to_csv, rather than opening filehandle here and echoing output to it:
        else:
            self.results_filename = os.path.join(self.query_params.get("name"), "_".join([self.query_params.get("name"), action_name, label]) + ".csv")


    def run(self):
        """
        Run the CKAN API queries and parse results, perform followup actions, if any
        # r = requests.post(url=url, headers=headers, data=data, files=files, auth=auth, verify=verify)
        # orgs:
        # https://data.ioos.us/api/3/action/organization_list?q=nanoos&all_fields=true
        # packages:
        # https://data.ioos.us/api/3/action/package_search?q=owner_org:e596892f-bf26-4020-addc-f60b78a39f41
        """

        # CKAN API interactions:
        # get the Organization:
        # org = self.obtain_owner_org(self.query_params.get("name"))
        org = obtain_owner_org(self.catalog_api_url, self.query_params.get("name"), logger=logger)
        # get the Resources:
        # must iterate results (default by 100 as defined in self.package_search)
        count = 0
        # find datasets belonging to Organization:
        dataset_results = []
        while True:
            package_results = package_search(self.catalog_api_url, org['id'], count, logger=logger, out=self.out)
            # obtain the total result count to iterate if necessary:
            result_count = package_results['result']['count']
            print ("result_count: " + str(result_count))
            for package in package_results['result']['results']:
                count += 1
                # print(count)
                # print(package['name'] + package['title'] + package['type'])

                # for this action, we just want to extract some attributes of the dataset and dump to .csv:
                # ['id']: dataset id
                # ['name']: used to contstruct a URL
                # ['dataset_url']: CKAN catalog URL for the dataset (contstructed from 'name')
                # ['title']: the real 'name'
                # ['harvest_object_url']: CKAN harvest object URL (stored ISO XML)
                # ['waf_location']: URL to the orignal harvested XML file
                # ['type']: usually 'dataset', but whatever
                # ['num_resources']: number of associated resources
                # ['num_tags']: number of associated tags
                # ['bbox']: the bounding box JSON (extracted from an 'extra' of the dataset with key='spatial')
                # ['']:
                parsed_url = urlparse(self.catalog_api_url, allow_fragments=False)
                try:
                    bbox = [ extra['value'] for extra in package['extras'] if extra['key'] == "spatial" ][0]
                except IndexError: bbox = ""
                try:
                    harvest_object_id = [ extra['value'] for extra in package['extras'] if extra['key'] == "harvest_object_id" ][0]
                    harvest_object_url = "{scheme}://{netloc}/harvest/object/{id}".format(scheme=parsed_url.scheme, netloc=parsed_url.netloc, id=harvest_object_id)
                except IndexError: harvest_object_url = ""
                try:
                    waf_location = [ extra['value'] for extra in package['extras'] if extra['key'] == "waf_location" ][0]
                except IndexError: waf_location = ""
                dataset_url = "{scheme}://{netloc}/dataset/{name}".format(scheme=parsed_url.scheme, netloc=parsed_url.netloc, name=package['name'])
                # you have to quote ("") and fields that may have commas for CSV output:
                title = "\"{title}\"".format(title=package['title']) if "," in package['title'] else package['title']
                dataset_results.append({
                    'id': package['id'],
                    'name': package['name'],
                    'dataset_url': dataset_url,
                    'title': package['title'],
                    'harvest_object_url': harvest_object_url,
                    'waf_location': waf_location,
                    'type': package['type'],
                    'num_resources': package['num_resources'],
                    'num_tags': package['num_tags'],
                    'bbox': bbox
                })

            if count == result_count:
                break

        # do something with results:
        for result in dataset_results:
            # print(json.dumps(result, indent=4, sort_keys=True))
            self.out.write(json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))
            pass
        print("Found {count} packages belonging to {org}".format(count=count, org=self.query_params.get("name"), res=len(dataset_results)))
        self.out.write(u"\nFound {count} packages belonging to {org}".format(count=count, org=self.query_params.get("name"), res=len(dataset_results)))

        # make a DataFrame:
        # for result in dataset_results:
        #    result = { k: str(v).encode("utf-8") for k,v in result.iteritems() }
        datasets_df = pandas.DataFrame.from_records(dataset_results, index="id", columns=dataset_results[0].keys())
        # debug:
        # for idx, dataset in datasets_df.iterrows():
        #    pass
        # print(datasets_df)

        # print(datasets_df.to_csv(encoding='utf-8'))
        # datasets_df.to_csv(self.results_filename, encoding='utf-8')
        datasets_df.reindex(columns=[ 'name', 'dataset_url', 'title', 'harvest_object_url', 'waf_location', 'type', 'num_resources', 'num_tags', 'bbox' ]).to_csv(self.results_filename, encoding='utf-8')
        print("should have printed results above....    ")
