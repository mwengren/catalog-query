"""
Action class that obtains CKAN Resources belonging to a particular organization, runs Compliance Checker tests,
and writes out results in a .csv file to a subdirectory
"""
import json
import subprocess
import time

import pandas
from compliance_checker.runner import ComplianceChecker, CheckSuite

# local:
from .action import ActionBase
from ..util import create_output_dir
from ..catalog_query import ActionException

# default CC tests and compatible formats:
CC_TESTS = ['cf', 'acdd', 'ioos']

# CC_RESOURCE_FORMATS, we only use ERDDAP-TableDAP for ERDDAP URLs, because we need to discard ERDDAP resources that are non-DAP-compliant (this is due to the way ERDDAP metadata calls many different 'types' of resources 'ERDDAP' format - eg. Make a Graph, Subset, HTML).  We only want to extract those named ERDDAP-TableDAP:
#CC_RESOURCE_FORMATS = ['ERDDAP', 'ERDDAP-TableDAP', 'OPeNDAP']
CC_RESOURCE_FORMATS = ['ERDDAP-TableDAP', 'OPeNDAP']

class Action(ActionBase):
    """
    resource_cc_check Action:

    Check dataset resources against Compliance Checker tests.  Output results and score information to .csv file. Output file contains summaries of CC results as well as commands run to obtain them.  Detailed test results are not produced with this tool, tests must be run individually from the outputs generated.

    Filters should be passed in the form of (-q|--query_params) to query for resources of a particular format that Compliance Checker can read (eg. ERDDAP, OPeNDAP)  Otherwise tests may fail when passed to the checker.  May be long running depending on the number of datasets, resources, and tests requested.
    """

    # def __init__(self, *args, **kwargs):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # at a minimum, we need an organization name to filter resources by:
        #if "name" not in self.query_params.keys():
        #    raise ActionException("Error running the '{}' action.  No 'name' parameter (CKAN Organization name) provided as a query parameter. This is required for this action to limit max number of datasets queried.".format(self.action_name))

        # call init_out:
        #self.init_out(subdir=self.query_params.get("name"))
        self.init_out()

        # parse the Compliance Checker tests to run:
        try:
            self.cc_tests = [test for test in kwargs.get("cc_tests").split(",")]
        except AttributeError as e:
            self.cc_tests = CC_TESTS
            print("No Compliance Checker test name passed via the 'cc_test' parameter (-t|--cc_tests).  Running with the default tests: {tests}".format(tests=", ".join(CC_TESTS)))
            self.out.write("\nNo Compliance Checker test name passed via the 'cc_test' parameter (-t|--cc_tests).  Running with the default tests: {tests}".format(tests=", ".join(CC_TESTS)))

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
        #org = self.obtain_owner_org(self.query_params.get("name"))

        # query packages based on self.params_list list:
        results = self.dataset_query(params=self.params_list, operator=self.operator)
        #self.out.write("\n" + json.dumps(results))


        # formats_to_test: list of Compliance Checker-compatible resource formats that match those passed as query_params filters.
        #   These will be used to filter resources in a package (because other formats are not suitable for use with Compliance Checker)
        #   Note: CC_RESOURCE_FORMATS includes only ERDDAP-TableDAP because many different non-DAP compliant resource types have format:ERDDAP in metadata
        #formats_to_test = [format for format in CC_RESOURCE_FORMATS if format in self.params_list]
        formats_to_test = []
        for format in CC_RESOURCE_FORMATS:
            for param in self.params_list:
                if format.lower() in param.lower():
                    formats_to_test.append(format)
        print("Checking formats: {}".format(formats_to_test))
        self.out.write("\nChecking formats: {}".format(formats_to_test))

        # handle results - format: [{'id': 'package_id', 'package': 'package_json'},]:
        # iterate through each package's resources and check if its 'format' value matches an item in formats_to_test and add to resources list:
        resources = []
        count = 0
        for result in results:
            #if count == 0:
            #    self.out.write("\n Example result: " + json.dumps(result['package']['resources'], indent=2, sort_keys=True, ensure_ascii=False) + "\n")

            # match any resources whose 'format' value matches one of the types to test:
            fmt_match = [resource for resource in result['package']['resources'] if resource['format'] in formats_to_test]
            count = count + len(fmt_match)
            resources.extend(fmt_match)

        # print matching resources to output:
        self.out.write("\nNum Resources Matched: " + str(len(resources)))
        # debug:
        #for resource in resources:
            #print(json.dumps(resource, indent=4, sort_keys=True))
            #self.out.write(json.dumps(resource, indent=4, sort_keys=True, ensure_ascii=False))

        #print("Found {count} packages with {res} resources meeting query criteria: {fmt}".format(count=len(results), res=len(resources), fmt=", ".join([param for param in self.params_list])))
        self.out.write("\nFound {count} packages with {res} resources meeting query criteria: {fmt}".format(count=len(results), res=len(resources), fmt=", ".join([param for param in self.params_list])))

        if resources:
            # make a DataFrame:
            resources_df = pandas.DataFrame.from_records(resources, index="id", columns=sorted(resources[0].keys()))
            # for idx, resource in resources_df.iterrows():
            #    pass
            #print(resources_df.to_csv(encoding='utf-8'))
            #self.out.write("\n" + str(resources_df.to_csv(encoding='utf-8')))

            # obtain the results of Compliance Checker test in a DataFrame and add a 'score_percent' column:
            check_results_df, cc_failures_df = self.run_check(resources_df)
            check_results_df['score_percent'] = check_results_df['scored_points'] / check_results_df['possible_points']

            # calculate average scores by summing individual score_percent values per test type add add as extra rows with
            #   the score values in the 'score_percent' column (ie 'cf-average'):
            # filter by 'testname' for only the 'score_percent' column values, and calculate using mean()
            for test in check_results_df['testname'].unique():
                score = check_results_df.loc[check_results_df['testname'] == test, 'score_percent'].mean()
                check_results_df.loc[test + '-average'] = ["" for x in range(11)]
                check_results_df.at[test + '-average', 'score_percent'] = score

            # write output to CSV:
            print("Writing Compliance Checker results to csv file: {}".format(self.results_filename))
            #print(check_results_df.to_csv(index=False, encoding='utf-8'))
            check_results_df.to_csv(self.results_filename, encoding='utf-8')

            # write errors to CSV (if any):
            if not cc_failures_df.empty:
                print("Writing Compliance Checker errors to csv file: {}".format(self.errors_filename))
                #print(cc_failures_df.to_csv(index=False, encoding='utf-8'))
                cc_failures_df.to_csv(self.errors_filename, encoding='utf-8')

    def run_check(self, df):
        """
        run Compliance Checker check(s):
        compliance-checker -t cf:1.6 -f json http://ona.coas.oregonstate.edu:8080/thredds/dodsC/NANOOS/OCOS
        """
        # create results and failures DataFrames (sometimes CC doesn't like certain DAP urls):
        # check_results_df = pandas.DataFrame(index= "id", columns=['url', 'testname', 'scored_points', 'possible_points', 'source_name', 'high_count', 'medium_count', 'low_count'])
        check_results_df = pandas.DataFrame(columns=['url', 'testname', 'scored_points', 'possible_points', 'high_count', 'medium_count', 'low_count', 'score_percent', 'cc_command', 'cc_spec_version', 'cc_url'])
        check_results_df.index = [check_results_df['url'], check_results_df['testname']]
        failures_df = pandas.DataFrame(columns=['url', 'testname', 'cc_command', 'error_msg'])
        failures_df.index = [failures_df['url'], failures_df['testname']]

        num_urls = len(df['url'].unique())
        # iterate unique URLs in the DataFrame to test:
        for i, url in enumerate(df['url'].unique()):
            print("Checking url: {url}".format(url=url))
            self.out.write("\nChecking url: {url}".format(url=url))

            # command-line Compliance Checker:
            for test in self.cc_tests:

                # assemble a compliance-checker command we'll use to test the URL:
                cc_command = "compliance-checker -t {test} -f json {url}".format(test=test, url=url)

                # debug:
                #print("Checker command: {}".format(cc_command))
                self.out.write("\nChecker command: {}".format(cc_command))

                # subprocess.call isn't what we're looking for here, but here's the equiv code:
                # cc = subprocess.call(cc_command, stdout=subprocess.PIPE)
                # cc_out = cc.stdout.read()

                # Popen/subprocess to call command line CC:
                cc = subprocess.Popen(cc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                cc_out, cc_err = cc.communicate()
                # cc.terminate()

                # check the returncode from cc subprocess, handle:
                print("Return code: {}".format(cc.returncode))
                self.out.write("\nReturn code: {}".format(cc.returncode))
                if cc.returncode > 0:
                    print("Error msg: {}".format(cc_err))
                    self.out.write("\nError msg: {}".format(cc_err))
                    # sys.exit(1)

                try:
                    cc_out_json = json.loads(cc_out)
                    # print(json.dumps(cc_out_json, indent=4, sort_keys=True))
                    # self.out.write(str(json.dumps(cc_out_json, indent=4, sort_keys=True)))

                    result = [url, cc_out_json[test]['testname'], cc_out_json[test]['scored_points'], cc_out_json[test]['possible_points'], cc_out_json[test]['high_count'], cc_out_json[test]['medium_count'], cc_out_json[test]['low_count'], '', cc_command, cc_out_json[test]['cc_spec_version'], cc_out_json[test]['cc_url']]
                    # debug:
                    #self.out.write("\n" + str(result))

                    # write an entry to the DataFrame (using index value set to the service url + testname - brittle, if columns in result list change order)
                    check_results_df.loc[result[0] + result[1]] = result

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
                    print("Results JSON parsing failed: {}".format(str(e)))
                    self.out.write("\nResults JSON parsing failed: {}".format(str(e)))
                    # failures_df structure: ['url', 'testname', 'cc_command', 'error_msg']
                    failures_df.loc[url + test] = [url, test, cc_command, str(e)]

            # record status:
            print("Check {} of {} completed".format(i, num_urls))
            self.out.write("\nCheck {} of {} completed".format(i, num_urls))

            # pause for a few seconds:
            time.sleep(2)
            # debug, only check a subset of results:
            #if i == 2:
            #    break

        return check_results_df, failures_df
