"""
General purpose CKAN API functions useful for various actions
"""
import os
import errno
import logging
import json
import requests

from .catalog_query import ActionException


def obtain_owner_org(api_url, org_name, logger=None):
    """
    obtain_owner_org: return org info from the CKAN API via query by Org Name (self.query_org)
    obtain the organization id:
    https://data.ioos.us/api/3/action/organization_list?q=
    """
    action = "organization_list"
    payload = {'q': org_name, 'all_fields': 'true'}
    url = ("/").join([api_url, "action", action])
    if logger:
        logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
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


def package_search(api_url, org_id=None, params=None, start_index=0, rows=100, logger=None, out=None):
    """
    package_search: run the package_search CKAN API query, filtering by org_id, iterating by 100, starting with 'start_index'
    perform package_search by owner_org:
    https://data.ioos.us/api/3/action/package_search?q=owner_org:
    """
    action = "package_search"
    if org_id is not None:
        if params is not None:
            payload = {'q': "owner_org:{id}+{params}".format(id=org_id, params="+".join(params)), 'start': start_index, 'rows': rows}
            print(payload)
        else:
            payload = {'q': "owner_org:{id}".format(id=org_id), 'start': start_index, 'rows': rows}
            print(payload)
    else:
        if params is not None:
            payload = {'q': "{params}".format(params=" ".join(params)), 'start': start_index, 'rows': rows}
            print(payload)
        else:
            payload = {'start': start_index, 'rows': rows}
            print(payload)
    url = ("/").join([api_url, "action", action])
    if logger:
        logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
    #r = requests.get(url=url, headers = {'content-type': 'application/json'}, params=payload)
    #r = requests.post(url=url, headers = {'content-type': 'application/json'}, data=json.dumps(payload))
    r = requests.post(url=url, headers = {'content-type': 'application/json'}, json=payload)
    print(json.dumps(payload))
    print(r.text)
    # either works:
    #result = json.loads(r.text)
    result = r.json()

    # this is the full package_search result:
    #if out:
    #    out.write(json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))
    return result


def dataset_query(api_url, org_id=None, params=None, rows=100, logger=None, out=None):
    """
    Wrapper function that queries CKAN package_search API endpoint via package_search function and collects results into list
    """

    count = 0
    dataset_results = []
    while True:
        package_results = package_search(api_url, org_id=org_id, params=params, start_index=count, rows=rows, logger=logger, out=out)
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


def create_output_dir(dir_name):
    """
    create an output directory(ies)
    """
    try:
        os.makedirs(dir_name, exist_ok=True)
    except OSError as ex:
        if ex.errno == errno.EEXIST:
            print("Warning: the configured output directory: {output_dir} already exists. Files may be overwritten from prior runs.".format(output_dir=os.path.abspath(dir_name)))
        else:
            raise ActionException("Error: the configured output directory: {output_dir} was not able to be created.".format(output_dir=os.path.abspath(dir_name)))
