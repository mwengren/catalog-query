"""
Action class that obtains all DataSets belonging to a particular organization,
and dumps them in a .csv file to a subdirectory
"""
from __future__ import unicode_literals
try:
    from urllib.parse import urlencode, urlparse   # Python 3
    from io import StringIO
    from builtins import str
except ImportError:
    from urllib import urlencode  # Python 2
    from urlparse import urlparse
    from StringIO import StringIO
import os
import errno
import io
import json
import traceback
from datetime import datetime, timedelta
from dateutil import parser

import logging
import random
import string
import requests
import pandas


# local:
from .action import ActionBase
from ..util import obtain_owner_org, package_search, dataset_query, create_output_dir
from ..catalog_query import ActionException

# logging:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log = logging.FileHandler('dataset_list.log', mode='w')
log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
logger.addHandler(log)


class Action(ActionBase):
    """
    dataset_list Action:

    Ouput a list of datasets belonging to a CKAN organization in CSV format.

    Requires a CKAN Organization name ('name') to be passed as a query parameter (-q|--query_params)
    """

    # def __init__(self, *args, **kwargs):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # decode parameters:
        # at a minimum, we need an organization name to filter resources by:
        if "name" not in self.query_params.keys():
            raise ActionException("Error running the '{}' action.  No 'name' parameter (CKAN Organization name) provided as a query parameter.  This is required for this action.".format(self.action_name))

        # call init_out:
        self.init_out(subdir=self.query_params.get("name"))

    def run(self):
        """
        Run the CKAN API queries and parse results, perform followup actions, if any
        # r = requests.post(url=url, headers=headers, data=data, files=files, auth=auth, verify=verify)
        # orgs API query:
        # https://data.ioos.us/api/3/action/organization_list?q=nanoos&all_fields=true
        # packages API query:
        # https://data.ioos.us/api/3/action/package_search?q=owner_org:e596892f-bf26-4020-addc-f60b78a39f41
        """

        # CKAN API interactions:
        # get the organization:
        org = obtain_owner_org(self.catalog_api_url, self.query_params.get("name"), logger=logger)

        # query packages for organization:
        results = dataset_query(self.catalog_api_url, org_id=org['id'], logger=logger, out=self.out)

        #handle output:
        datasets = self.parse_dataset_results(results)
        if len(datasets) > 0: self.write_dataset_results_to_csv(datasets)
