"""
ActionBase superclass
"""
try:
    from urllib.parse import urlencode, urlparse   # Python 3
    from io import StringIO
    from builtins import str
except ImportError:
    from urllib import urlencode  # Python 2
    from urlparse import urlparse
    from StringIO import StringIO

import importlib
import io
import json
import logging
import os
import random
import string

import requests
import pandas

from ..util import obtain_owner_org, package_search, dataset_query, create_output_dir
from ..catalog_query import ActionException


class ActionBase(object):
    """
    Attributes
    ----------
    catalog_api_url : str
        URL of CKAN API to sumbit queries to
    params_list: list
        list of query parameters specified by input param (use query_params instead)
    query_params : dict
        dict of query params passed (note: this dict by nature won't include reapeated keys, so beware.  Already used in various code locations so kept for simplicity.  Use params_list if repeated query keys necessary - e.g. for Solr filter queries)
    action_name: str
        name of the Action specified by input param
    label: str
        random 5 char string for labeling output
    results_filename: file
        output file to write results from the Action
    errors_filename: file
        output file to write errors encountered while running Action
    out: file
        output file for logging purposes
    """

    # def __init__(self, *args, **kwargs):
    def __init__(self, **kwargs):
        # logging:
        m = importlib.import_module(self.__module__)
        self.logger = logging.getLogger(m.__name__)
        self.logger.setLevel(logging.INFO)
        log = logging.FileHandler(m.__name__.split(".")[-1] + ".log", mode='w')
        log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
        self.logger.addHandler(log)


        # decode parameters:
        self.catalog_api_url = kwargs.get("catalog_api_url")

        # the query parameters for this action are all passed in list form in the 'query' parameter arg, and must be decoded:
        # this is a bit of a hack to extract query parameter keys into instance variables to use in the queries
        # expected values are along the lines of:
        #   -name:<organization_name>
        #   -resource_format: <format of a dataset's resource>
        #   -resource_name: <name of a dataset's resource>
        #   - more...?
        if kwargs.get("query") is not None:
            self.params_list = kwargs.get("query").split(",")
        else:
            self.params_list = []

        # split the params by ':' into self.query_params:
        # note: dicts do not accommodate repeating keys, so may lose repeated param keys passed (eg. res_format:, res_format:)
        # self.query_params was already incorporated in the code, so kept, but should use self.params_list instead
        if len(self.params_list) >= 1:
            self.query_params = dict(param.split(":") for param in self.params_list)
        else:
            self.query_params = {}

        # set the operator passed:
        self.operator = kwargs.get("operator")

        # get the Action file name to use in naming output file, using os.path.split and create a random string label:
        # first need a reference to subclass __module__ to obtain __file__:
        self.action_name = os.path.split(m.__file__)[1].split(".")[0]
        self.label = "".join(random.choice(string.ascii_lowercase) for i in range(5))

        # create the results_filename (path to results output file) depending on if an 'output' filename parameter was provided or not:
        if "output" in kwargs:
            self.results_filename = kwargs['output']
        else:
            # utf-8 issues resolved by just passing results_filename to DataFrame.to_csv, rather than opening filehandle here and echoing output to it:
            if "name" in self.query_params.keys():
                self.results_filename = os.path.join(self.query_params.get("name"), "_".join([self.query_params.get("name"), self.action_name, self.label]) + ".csv")
            else:
                self.results_filename = os.path.join(os.getcwd(), "_".join([self.action_name, self.label]) + ".csv")

        # create the errpr_output_filename (path to error output file) depending on if an 'error_output' filename parameter was provided or not:
        if "error_output" in kwargs:
            self.errors_filename = kwargs['error_output']
        else:
            if "name" in self.query_params.keys():
                self.errors_filename = os.path.join(self.query_params.get("name"), "_".join([self.query_params.get("name"), "error", self.action_name, self.label]) + ".csv")
            else:
                self.errors_filename = os.path.join(os.getcwd(), "_".join([self.action_name, "error", self.label]) + ".csv")


    def obtain_owner_org(self, org_name):
        """
        obtain_owner_org: return org info from the CKAN API via query by Org Name (self.query_org)
        obtain the organization id:
        https://data.ioos.us/api/3/action/organization_list?q=
        """
        action = "organization_list"
        payload = {'q': org_name, 'all_fields': 'true'}
        url = ("/").join([self.catalog_api_url, "action", action])
        if self.logger:
            self.logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
        r = requests.post(url=url, json=payload)

        result = json.loads(r.text)
        print(json.dumps(result, indent=4, sort_keys=True))

        # the step to loop through the 'result' array isn't really necessary since we expect the org name
        # to match what was passed in the query, but it is safer than assuming it will (API may return multiple)
        for org in result['result']:
            # could also use org['title'] it seems
            if org['display_name'] == org_name:
                org_result = org

        # check to make sure a valid Org name was passed:
        try:
            org_result
        except NameError:
            raise ActionException("Error: no Organization matching {org} exists in the Catalog.  Please try again (query is case sensitive).".format(org=org_name))

        print("Organization id: {id}".format(id=org_result['id']))
        return org_result


    def package_search(self, org_id=None, params=None, operator=None, start_index=0, rows=100):
        """
        package_search: run the package_search CKAN API query, filtering by org_id, iterating by 100, starting with 'start_index'
        perform package_search by owner_org:
        https://data.ioos.us/api/3/action/package_search?q=owner_org:
        """
        action = "package_search"
        payload = {'start': start_index, 'rows': rows}
        if org_id is not None:
            payload['owner_org'] = org_id

        if params is not None:
            query = " {} ".format(operator).join(params)
            payload['q'] = query

        print(payload)
        url = ("/").join([self.catalog_api_url, "action", action])
        if self.logger:
            self.logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
        #r = requests.get(url=url, headers = {'content-type': 'application/json'}, params=payload)
        #r = requests.post(url=url, headers = {'content-type': 'application/json'}, data=json.dumps(payload))
        r = requests.post(url=url, headers = {'content-type': 'application/json'}, json=payload)

        # either works:
        #result = json.loads(r.text)
        result = r.json()

        # this is the full package_search result:
        #print(r.text)
        #if out:
        #    out.write(json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))
        return result


    def dataset_query(self, org_id=None, params=None, operator=None, rows=100):
        """
        Wrapper function that queries CKAN package_search API endpoint via package_search function and collects results into list
        """

        count = 0
        dataset_results = []
        while True:
            package_results = self.package_search(org_id=org_id, params=params, operator=operator, start_index=count, rows=rows)
            # obtain the total result count to iterate if necessary:
            result_count = package_results['result']['count']
            if count == 0:
                print("result_count: " + str(result_count))

            # here we just append to dataset_results a nested dict with package['id'] and package JSON string
            for package in package_results['result']['results']:
                count += 1
                #print(package)
                """
                for resource in package['resources']:
                    # perform the resource filtering logic:
                    # this entails parsing out all the query_params that start with 'resource_', then parsing the
                    # remaining key string (after 'resource_') and using that as the attribute of the CKAN resource
                    # to filter by (ie 'resource_name' = resource['name'], resource_format = resource['format'], etc)
                    # NOTE: query parameters are ANDed together:
                    resource_query_keys = [key for key in self.query_params.keys() if key.startswith("resource_")]
                    for i, key in enumerate(resource_query_keys):
                        # this is the step where we filter out by resource['name'], resource['format'] etc, by taking
                        # the second part of the resource_query_key string after 'resource_' and filtering.
                        # break from loop if a query parameter check fails:
                        if resource[key.split("_", 1)[1]] != self.query_params[key]:
                            break
                        # if all checks pass, we add this to the resource_results list:
                        elif len(resource_query_keys) == i + 1:
                            resource_results.append(resource)
                """

                dataset_results.append({
                    'id': package['id'],
                    'package': package
                })
            if count == result_count:
                break

        return dataset_results


    def parse_dataset_results(self, results):
        """
        parse the results list, write output to self.out
        """

        # handle results (list of dicts):
        # [{'id': 'package_id', 'package': 'package_json'},]
        datasets = []
        for result in results:
            #print("id: {id}".format(id=result['id']))
            #print("package: {package}".format(package=result['package']))

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
            # ['resources']['format']: resource format
            # ['organization']['title']: the dataset's organization title
            parsed_url = urlparse(self.catalog_api_url, allow_fragments=False)
            try:
                bbox = [extra['value'] for extra in result['package']['extras'] if extra['key'] == "spatial"][0]
            except IndexError:
                bbox = ""
            try:
                harvest_object_id = [extra['value'] for extra in result['package']['extras'] if extra['key'] == "harvest_object_id"][0]
                harvest_object_url = "{scheme}://{netloc}/harvest/object/{id}".format(scheme=parsed_url.scheme, netloc=parsed_url.netloc, id=harvest_object_id)
            except IndexError:
                harvest_object_url = ""
            try:
                waf_location = [extra['value'] for extra in result['package']['extras'] if extra['key'] == "waf_location"][0]
            except IndexError:
                waf_location = ""
            dataset_url = "{scheme}://{netloc}/dataset/{name}".format(scheme=parsed_url.scheme, netloc=parsed_url.netloc, name=result['package']['name'])
            # necessary to quote ("") any fields that may have commas or semicolons for CSV output:
            if any(x in result['package']['title'] for x in [",",";"]):
                title = "\"{title}\"".format(title=result['package']['title'])
            else:
                title = result['package']['title']
            resource_formats = [resource['format'] for resource in result['package']['resources']]
            #formats_list = "\"{list}\"".format(list=",".join(resource_formats))
            formats_list = "-".join(resource_formats)
            organization = result['package']['organization']['title']
            datasets.append({
                'id': result['package']['id'],
                'name': result['package']['name'],
                'dataset_url': dataset_url,
                'title': title,
                'organization': organization,
                'harvest_object_url': harvest_object_url,
                'waf_location': waf_location,
                'type': result['package']['type'],
                'num_resources': result['package']['num_resources'],
                'num_tags': result['package']['num_tags'],
                'formats': formats_list,
                'bbox': bbox
            })

        # do something with results:
        for dataset in datasets:
            self.out.write(json.dumps(dataset, indent=2, sort_keys=True, ensure_ascii=False))

        if "name" in self.query_params.keys():
            print("Found {count} packages belonging to {org} from {action} query action".format(count=len(datasets), org=self.query_params.get("name"), action=self.action_name))
            self.out.write(u"\nFound {count} packages belonging to {org} from {action} query action".format(count=len(datasets), org=self.query_params.get("name"), action=self.action_name))
        else:
            print("Found {count} packages from {action} query action".format(count=len(datasets), action=self.action_name))
            self.out.write(u"\nFound {count} packages from {action} query action".format(count=len(datasets), action=self.action_name))

        return datasets

    def write_dataset_results_to_csv(self, datasets):
        """
        write dataset list to self.results_filename
        """
        # make a DataFrame:
        datasets_df = pandas.DataFrame.from_records(datasets, index="id", columns=datasets[0].keys())
        # debug:
        # for idx, dataset in datasets_df.iterrows():
        #    pass

        # datasets_df.to_csv(self.results_filename, encoding='utf-8')
        datasets_df.reindex(columns=['name', 'dataset_url', 'title', 'organization', 'harvest_object_url', 'waf_location', 'type', 'num_resources', 'num_tags','formats','bbox']).to_csv(self.results_filename, encoding='utf-8')


    def init_out(self, subdir=None):
        """
        init_out: create output file for general logging (create file if not already existing, including subdir if provided):
        """
        if subdir is not None:
            filename = os.path.join(subdir, self.action_name + ".out")
        else:
            filename = os.path.join(os.getcwd(), self.action_name + ".out")
        if not os.path.exists(os.path.dirname(filename)):
            # will throw ActionException with error message if the output directory can't be created:
            create_output_dir(os.path.dirname(filename))
        self.out = io.open(filename, mode="wt", encoding="utf-8")
        #print(filename)
