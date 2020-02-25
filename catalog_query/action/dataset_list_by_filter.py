"""
dataset_list_by_filter Action: return a list of datasets that contain match the filter criteria passed (for 'package_search' API endpoint).
"""

# local:
from .action import ActionBase
from ..util import create_output_dir
from ..catalog_query import ActionException

class Action(ActionBase):
    """
    dataset_list_by_filter Action:

    Output a list of CKAN datasets that match filter criteria passed.  This can be useful to filter a full CKAN catalog to determine how many datasets provide a certain type of access (OPenDAP, THREDDS, ERDDAP, WMS, WFS, etc).

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
        results = self.dataset_query(params=self.params_list, operator=self.operator)

        #handle output:
        datasets = self.parse_dataset_results(results)
        if len(datasets) > 0: self.write_dataset_results_to_csv(datasets)
