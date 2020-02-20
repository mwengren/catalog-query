import argparse
import os
import errno
import io
import importlib
import sys
from datetime import datetime, timedelta
try:
    from urllib.parse import urlparse  # Python 3
except ImportError:
    from urlparse import urlparse  # Python 2
# import requests


IOOS_CATALOG_URL = "https://data.ioos.us/api/3"
VALID_QUERY_ACTIONS = ['resource_cc_check', 'dataset_list']


class ActionException(Exception):
    pass


def main():
    """
    Command line interface
    """
    kwargs = {
        'description': 'Query the CKAN API from IOOS Catalog (or other) to get stuff.',
        'formatter_class': argparse.RawDescriptionHelpFormatter,
    }
    parser = argparse.ArgumentParser(**kwargs)

    parser.add_argument('-c', '--catalog_api_url', type=str, default=IOOS_CATALOG_URL,
                        help='URL of CKAN Catalog to query.  Default: {cat_url}'.format(cat_url=IOOS_CATALOG_URL))

    parser.add_argument('-a', '--action', type=str, required=True,
                        help='Name of a defined Action (CKAN query plus any subsequent analysis) to run. Current provided actions: {valid}'.format(valid=", ".join(VALID_QUERY_ACTIONS)))

    parser.add_argument('-o', '--output', type=str, required=False,
                        help='Output filename (path to a file to output results to).  Will default to a subdirectory of the CKAN Organization name and a randomized output file name with the Action prefix.')

    parser.add_argument('-e', '--error_output', type=str, required=False,
                        help='Error output filename (path to a file to output results to).  Will default to a subdirectory of the CKAN Organization name and a randomized error output file name with the Action prefix.')

    parser.add_argument('-q', '--query_params', type=str, required=True,
                        help='Query parameter value(s) to pass to the query action.  Multiple query parameters needed for actions that expect multiple parameters can be passed as a comma separated string (eg. \'-q=name:AOOS,format:OPeNDAP or -q=name:NANOOS,resource_format:ERDDAP,resource_name:OPeNDAP)\' to run AOOS OPeNDAP services through the Compliance Checker test) ')

    parser.add_argument('-t', '--cc_tests', type=str, required=False,
                        help='Compliance checker tests to run (by name, comma-separated) (eg \'-t=acdd:1.3,cf:1.6,ioos\')')

    args = parser.parse_args()

    catalog_api_url = urlparse(args.catalog_api_url)
    if not catalog_api_url.scheme or not catalog_api_url.netloc:
        sys.exit("Error: '--catalog_api_url' parameter value must contain a valid URL.  Value passed: {param}".format(param=args.catalog_api_url))
    if catalog_api_url.params or catalog_api_url.query:
        sys.exit("Error: '--catalog_api_url' parameter should not contain query parameters ('{query}'). Please include only the service endpoint URL.  Value passed: {param}".format(query=catalog_api_url.query, param=args.catalog_api_url))

    # check to make sure the 'action' argument passed matches an expected query action type:
    if args.action not in VALID_QUERY_ACTIONS:
        sys.exit("Error: '--action' parameter value must contain a known query action.  Valid query actions: {valid}.  Value passed: {param}".format(valid=", ".join(VALID_QUERY_ACTIONS), param=args.action))

    # perform the query action (if value passed is known):
    for query_action in VALID_QUERY_ACTIONS:
        if args.action == query_action:
            print("query action: " + query_action)

            try:
                # try relative importlib import of action_module (Python 2.7?)
                # this should work in fact in both 2.7 and 3.x:
                action_module = importlib.import_module(".{module}".format(module=query_action), package="catalog_query.action")
                # for a same-level import (no submodule):
                # action_module = importlib.import_module(".%s" % query_action, package="catalog_query")

            # handle ImportError and instead try absolute module import (catalog_query.action.*) (Python 3?):
            except (SystemError, ImportError) as e:
                action_module = importlib.import_module("catalog_query.action.{module}".format(module=query_action))

            Action = action_module.Action

            # import failure attempts:
            # from .action.query_action import Action
            # module = "action." + query_action + ".Action"
            # __import__(module)
            # Action = importlib.import_module(".action." + query_action + ".Action")
            # Action = importlib.import_module("..Action.", "action." + query_action)
            # module = __import__(".action." + query_action, globals(), locals(), ['Action'])

            spec = {}
            if args.catalog_api_url:
                spec['catalog_api_url'] = args.catalog_api_url
            if args.output:
                spec['output'] = args.output
            if args.error_output:
                spec['error_output'] = args.error_output
            if args.query_params:
                spec['query'] = args.query_params
            if args.cc_tests:
                spec['cc_tests'] = args.cc_tests

            try:
                action = Action(**spec)
                action.run()
            except Exception as e:
                print(e)
