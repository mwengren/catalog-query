"""
Action class that obtains CKAN Resources belonging to a particular organization, runs Compliance Checker tests,
and writes out results in a .csv file to a subdirectory
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
import sys
import subprocess
import time
from datetime import datetime, timedelta
from dateutil import parser

import logging
import random
import string
import requests
import pandas
from compliance_checker.runner import ComplianceChecker, CheckSuite

# local:
from .action import ActionBase
from ..util import obtain_owner_org, package_search, dataset_query, create_output_dir
from ..catalog_query import ActionException

# logging:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log = logging.FileHandler('resource_cc_check.log', mode='w')
log.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
logger.addHandler(log)

# default CC tests:
CC_TESTS = ['cf', 'acdd', 'ioos']

class Action(ActionBase):
    """
    resource_cc_check Action:

    Check a data provider's dataset's resources against Compliance Checker tests.

    Requires a CKAN Organization name ('name') to be passed as a query parameter (-q|--query_params) to query for resources of a particular format (OPeNDAP for example) which can then be passed to Compliance Checker.
    """

    # def __init__(self, *args, **kwargs):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # at a minimum, we need an organization name to filter resources by:
        if "name" not in self.query_params.keys():
            raise ActionException("Error running the '{}' action.  No 'name' parameter (CKAN Organization name) provided as a query parameter. This is required for this action.".format(self.action_name))

        # call init_out:
        init_out(subdir=self.query_params.get("name"))

        # parse the Compliance Checker tests to run:
        try:
            self.cc_tests = [test for test in kwargs.get("cc_tests").split(",")]
        except AttributeError as e:
            self.cc_tests = CC_TESTS
            print("No Compliance Checker test name passed via the 'cc_test' parameter (-t|--cc_tests).  Running with the default tests: {tests}".format(tests=", ".join(CC_TESTS)))
            self.out.write(u"\nNo Compliance Checker test name passed via the 'cc_test' parameter (-t|--cc_tests).  Running with the default tests: {tests}".format(tests=", ".join(CC_TESTS)))

    def run(self):
        """
        # r = requests.post(url=url, headers=headers, data=data, files=files, auth=auth, verify=verify)
        # orgs API query:
        # https://data.ioos.us/api/3/action/organization_list?q=nanoos&all_fields=true
        # packages API query:
        # https://data.ioos.us/api/3/action/package_search?q=owner_org:e596892f-bf26-4020-addc-f60b78a39f41
        # resources API query:
        # https://data.ioos.us/api/3/action/resource_search?query=format:OPeNDAP
        """

        # CKAN API interactions:
        # get the Organization:
        org = obtain_owner_org(self.catalog_api_url, self.query_params.get("name"), logger=logger)
        # get the Resources:
        # must iterate results (default by 100 as defined in self.package_search)
        count = 0
        # find resources belonging to Organization with correct format:
        resource_results = []
        while True:
            package_results = package_search(self.catalog_api_url, org['id'], count, logger=logger, out=self.out)
            # obtain the total result count to iterate if necessary:
            result_count = package_results['result']['count']
            for package in package_results['result']['results']:
                count += 1
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

            if count == result_count:
                break

        # do something with results:
        for resource in resource_results:
            print(json.dumps(resource, indent=4, sort_keys=True))
            self.out.write(json.dumps(resource, indent=4, sort_keys=True, ensure_ascii=False))
        print("Found {count} packages belonging to {org}, with {res} resources meeting query criteria: {fmt}".format(count=count, org=self.query_params.get("name"), res=len(resource_results), fmt=", ".join([param for param in self.params_list if param.startswith("resource_")])))
        self.out.write(u"\nFound {count} packages belonging to {org}, with {res} resources meeting query criteria: {fmt}".format(count=count, org=self.query_params.get("name"), res=len(resource_results), fmt=", ".join([param for param in self.params_list if param.startswith("resource_")])))

        if resource_results:
            # make a DataFrame:
            resources_df = pandas.DataFrame.from_records(resource_results, index="id", columns=sorted(resource_results[0].keys()))
            # for idx, resource in resources_df.iterrows():
            #    pass
            print(resources_df.to_csv(encoding='utf-8'))
            self.out.write(u"\n" + str(resources_df.to_csv(encoding='utf-8')))
            # self.out.write(resources_df.to_csv())

            # obtain the results of Compliance Checker test in a DataFrame and add a 'score_percent' column:
            check_results_df, cc_failures_df = self.run_check(resources_df)
            check_results_df['score_percent'] = check_results_df['scored_points'] / check_results_df['possible_points']

            # calculate average scores by summing individual score_percent values per test type add add as extra rows with
            #   the score values in the 'score_percent' column (ie 'cf-average'):
            # filter by 'testname' for only the 'score_percent' column values, and calculate using mean()
            for test in self.cc_tests:
                score = check_results_df.loc[check_results_df['testname'] == test, 'score_percent'].mean()
                # debug:
                print("score for test '{test}' is: {score}".format(test=test, score=str(score)))

                check_results_df.loc[test + '-average'] = ["" for x in range(9)]
                check_results_df.set_value(test + '-average', 'score_percent', score)

            # write output to CSV:
            print("check_results_df contents:")
            print(check_results_df.to_csv(index=False, encoding='utf-8'))
            check_results_df.to_csv(self.results_filename, encoding='utf-8')

            # write errors to CSV (if any):
            if not cc_failures_df.empty:
                print("cc_failures_df contents:")
                print(cc_failures_df.to_csv(index=False, encoding='utf-8'))
                cc_failures_df.to_csv(self.errors_filename, encoding='utf-8')

            print("should have printed results above....")

    def run_check(self, df):
        """
        run Compliance Checker check(s):
        compliance-checker -t cf:1.6 -f json http://ona.coas.oregonstate.edu:8080/thredds/dodsC/NANOOS/OCOS
        """
        # create results and failures DataFrames (sometimes CC doesn't like certain DAP urls):
        # check_results_df = pandas.DataFrame(index= "id", columns=['url', 'testname', 'scored_points', 'possible_points', 'source_name', 'high_count', 'medium_count', 'low_count'])
        check_results_df = pandas.DataFrame(columns=['url', 'testname', 'scored_points', 'possible_points', 'source_name', 'high_count', 'medium_count', 'low_count'])
        check_results_df.index = [check_results_df['url'], check_results_df['testname']]
        failures_df = pandas.DataFrame(columns=['url', 'testname', 'cc_command', 'error_msg'])
        failures_df.index = [failures_df['url'], failures_df['testname']]

        # iterate URLs in the DataFrame to test:
        # for i, url in enumerate(reversed(df['url'].unique())):
        for i, url in enumerate(df['url'].unique()):
            print("Checking url: {url}".format(url=url))
            self.out.write(u"\nChecking url: {url}".format(url=url))

            # ToDo: Remove below:
            # temp debug: skip the Hyrax server .ncml URL:
            # if url.endswith(".ncml"):
            #    continue

            # command-line Compliance Checker:
            for test in self.cc_tests:

                # assemble a compliance-checker command we'll use to test the URL:
                cc_command = "compliance-checker -t {test} -f json {url}".format(test=test, url=url)

                # subprocess.call isn't what we're looking for here, but here's the equiv code:
                # cc = subprocess.call(cc_command, stdout=subprocess.PIPE)
                # cc_out = cc.stdout.read()

                # Popen/subprocess to call command line CC:
                cc = subprocess.Popen(cc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                #cc = subprocess.Popen(cc_command, stdout=subprocess.PIPE)

                # debug:
                print(cc_command)
                cc_out, cc_err = cc.communicate()
                # cc.terminate()

                # check the returncode from cc subprocess, handle:
                print(cc.returncode)
                if cc.returncode > 0:
                    print(cc_err)
                    # sys.exit(1)

                # workaround: until compliance-checker > 3.0.3 avaialble, remove the first line from 'JSON' output as it isn't JSON:
                # if test == "cf":
                    # print("cf: removing first line of debug from 'json'")
                    # cc_out_lines = cc_out.split("\n")
                    # cc_out = "\n".join(cc_out_lines[1:len(cc_out_lines) - 1])

                # print("result: " + cc_out)
                # self.out.write("\nresult: " + str(cc_out, "utf-8"))

                try:
                    cc_out_json = json.loads(cc_out)
                    # print(json.dumps(cc_out_json, indent=4, sort_keys=True))
                    # self.out.write(str(json.dumps(cc_out_json, indent=4, sort_keys=True)))

                    # testing with assigning results to list first:
                    result = [url, cc_out_json[test]['testname'], cc_out_json[test]['scored_points'],
                              cc_out_json[test]['possible_points'], cc_out_json[test]['source_name'],
                              cc_out_json[test]['high_count'], cc_out_json[test]['medium_count'],
                              cc_out_json[test]['low_count']]
                    print(result)
                    self.out.write(u"\n" + str(result))

                    # write an entry to the DataFrame (using index value set to the service url + testname - brittle, if columns in result list change order)
                    check_results_df.loc[result[0] + result[1]] = result

                    # alternatively, could use just this line instead of 'result' list:
                    # check_results_df.loc[url + cc_out_json[test]['testname']] = [ url, cc_out_json[test]['testname'], cc_out_json[test]['scored_points'], cc_out_json[test]['possible_points'],
                    #            cc_out_json[test]['source_name'], cc_out_json[test]['high_count'], cc_out_json[test]['medium_count'], cc_out_json[test]['low_count'] ]

                    """
                    # python API Compliance Checker:
                    # use a StringIO instance to store output instead of an actual file
                    # Unable to make this approach work with StringIO and deferred to using the subprocess.Popen approach instead
                    cc_out = StringIO()
                    check_suite = CheckSuite()
                    check_suite.load_all_available_checkers()
                    return_value, cc_err = ComplianceChecker.run_checker(
                        ds_loc=url,
                        checker_names = self.cc_tests,
                        verbose=1,
                        criteria="normal",
                        output_filename=cc_out,
                        output_format="json"
                    )
                    #print(cc_out.getvalue())
                    #cc_json = json.loads(cc_out.getvalue())
                    #cc_out.close()
                    #print(json.dumps(cc_json, indent=4, sort_keys=True))
                    #self.out.write(unicode(json.dumps(cc_json, indent=4, sort_keys=True)))
                    """

                except ValueError as e:
                    print("compliance-checker JSON parsing failed: {err}".format(err=e))
                    self.out.write(u"\ncompliance-checker JSON parsing failed: {err}".format(err=e))
                    # failures_df structure: ['url', 'testname', 'cc_command', 'error_msg']
                    failures_df.loc[url + test] = [url, test, cc_command, e]

            # pause:
            time.sleep(2)
            # debug, only check one url:
            #if i == 0:
            #    break

        return check_results_df, failures_df

    """ moved to util.py:
    def obtain_owner_org(self, org_name):

        #obtain_owner_org: return org info from the CKAN API via query by Org Name (self.query_org)
        #obtain the organization id:
        #https://data.ioos.us/api/3/action/organization_list?q=

        action = "organization_list"
        payload = {'q': org_name, 'all_fields': 'true'}
        url = ("/").join([self.catalog_api_url, "action", action])
        logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
        r = requests.post(url=url, json=payload)

        result = json.loads(r.text)
        print(json.dumps(result, indent=4, sort_keys=True))

        # the step to loop through the 'result' array isn't really necessary since we expect the org name
        # to match what was passed in the query, but it is safer than assuming it will (API may return multiple)
        for org in result['result']:
            # could also use org['title'] it seems
            if org['display_name'] == org_name: org_result = org

        print("Organization id: {id}".format(id=org_result['id']))
        return org_result
    """

    """ moved to util.py:
    def package_search(self, org_id, start_index):

        #package_search: run the package_search CKAN API query, filtering by org_id, iterating by 100, starting with 'start_index'
        #perform package_search by owner_org:
        #https://data.ioos.us/api/3/action/package_search?q=owner_org:

        action = "package_search"
        payload = {'q': "owner_org:{id}".format(id=org_id), 'start': start_index, 'rows':100}
        url = ("/").join([self.catalog_api_url, "action", action])
        logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
        r = requests.post(url=url, json=payload)
        result = json.loads(r.text)

        # this is the full package_search result:
        #print json.dumps(result, indent=4, sort_keys=True)
        self.out.write(json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))

        return result
        """
