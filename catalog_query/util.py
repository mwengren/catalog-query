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


def package_search(api_url, org_id, start_index, logger=None, out=None):
    """
    package_search: run the package_search CKAN API query, filtering by org_id, iterating by 100, starting with 'start_index'
    perform package_search by owner_org:
    https://data.ioos.us/api/3/action/package_search?q=owner_org:
    """
    action = "package_search"
    payload = {'q': "owner_org:{id}".format(id=org_id), 'start': start_index, 'rows': 100}
    url = ("/").join([api_url, "action", action])
    if logger:
        logger.info("Executing {action}.  URL: {url}. Parameters {params}".format(action=action, url=url, params=payload))
    r = requests.post(url=url, json=payload)
    result = json.loads(r.text)

    # this is the full package_search result:
    # print json.dumps(result, indent=4, sort_keys=True)
    if out:
        out.write(json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))

    return result


def create_output_dir(dir_name):
    """
    create an output directory(ies)
    """
    try:
        os.makedirs(dir_name)
        # test error handling:
        # raise OSError
    except OSError as ex:
        if ex.errno == errno.EEXIST:
            print("Warning: the configured output directory: {output_dir} already exists. Files may be overwritten from prior runs.".format(output_dir=os.path.abspath(dir_name)))
        else:
            raise ActionException("Error: the configured output directory: {output_dir} was not able to be created.".format(output_dir=os.path.abspath(dir_name)))
