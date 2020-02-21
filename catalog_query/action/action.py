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
import os
import random
import string

import pandas

from ..util import obtain_owner_org, package_search, dataset_query, create_output_dir
from ..catalog_query import ActionException

class ActionBase:
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
            # split the params by ':' into self.query_params:
            # note: dicts do not accommodate repeating keys, so may lose some params passed
            self.query_params = dict(param.split(":") for param in self.params_list)

        # get the Action file name to use in naming output file, using os.path.split and create a random string label:
        # first need a reference to subclass __module__ to obtain __file__:
        m = importlib.import_module(self.__module__)
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
            # ['']:
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
            datasets.append({
                'id': result['package']['id'],
                'name': result['package']['name'],
                'dataset_url': dataset_url,
                'title': title,
                'harvest_object_url': harvest_object_url,
                'waf_location': waf_location,
                'type': result['package']['type'],
                'num_resources': result['package']['num_resources'],
                'num_tags': result['package']['num_tags'],
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
        datasets_df.reindex(columns=['name', 'dataset_url', 'title', 'harvest_object_url', 'waf_location', 'type', 'num_resources', 'num_tags', 'bbox']).to_csv(self.results_filename, encoding='utf-8')
