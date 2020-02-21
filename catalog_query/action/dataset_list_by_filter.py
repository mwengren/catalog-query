"""
dataset_list_by_filter Action: return a list of datasets that contain a set of resources.
"""

import logging


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
    dataset_list_by_filter Action:

    Output a list of CKAN datasets that include a particular resource or set of resources.  This is useful to filter a full CKAN catalog to determine how many datasets provide a certain type of access (OPenDAP, THREDDS, ERDDAP, WMS, WFS, etc).

    No required query parameters, however if omitted it will just dump the full CKAN dataset contents into the output CSV.
    """

    # def __init__(self, *args, **kwargs):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # decode parameters:

        # call init_out:
        self.init_out()


    def run(self):
        """
        Run the CKAN API queries and parse results.
        # packages API query:
        # https://data.ioos.us/api/3/action/package_search?q=owner_org:e596892f-bf26-4020-addc-f60b78a39f41
        """

        # query packages based on self.params_list list:
        results = dataset_query(self.catalog_api_url, params=self.params_list, logger=logger, out=self.out)

        #handle output:
        datasets = self.parse_dataset_results(results)
        if len(datasets) > 0: self.write_dataset_results_to_csv(datasets)
