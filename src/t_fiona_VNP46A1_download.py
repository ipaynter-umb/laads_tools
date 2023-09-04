import t_spinup
import t_misc
import t_requests
import t_stac
import t_vnp46a
import t_laads
import boto3
import t_earthdata
import requests
import base64
import json
import os
from os import environ
from pathlib import Path


def retrieve_credentials():
    """Makes the Oauth calls to authenticate with EDS and return a set of s3
    same-region, read-only credentials.
    """
    s3_endpoint = 'https://urs.earthdata.nasa.gov/s3credentials'

    login_resp = requests.get(
        s3_endpoint, allow_redirects=False
    )
    login_resp.raise_for_status()

    auth = f"{environ['earthdata_user']}:{environ['earthdata_pw']}"
    encoded_auth = base64.b64encode(auth.encode('ascii'))

    auth_redirect = requests.post(
        login_resp.headers['location'],
        data={"credentials": encoded_auth},
        headers={"Origin": s3_endpoint},
        allow_redirects=False
    )

    auth_redirect.raise_for_status()

    print(auth_redirect.status_code)

    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

    print(final.status_code)
    print(final.cookies)
    print(final.cookies['_urs-gui_session'])

    results = requests.get(s3_endpoint, cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()

    return json.loads(results.content)


username = os.getenv("EARTHDATA_USERNAME")
password = os.getenv("EARTHDATA_PASSWORD")

AUTH_HOST = "urs.earthdata.nasa.gov"
EDL_GENERATE_TOKENS_URL = "https://urs.earthdata.nasa.gov/api/users/token"
EDL_GET_TOKENS_URL = "https://urs.earthdata.nasa.gov/api/users/tokens"

s = requests.Session()
s.auth = (username, password)

#auth_resp = s.post(
#    EDL_GENERATE_TOKENS_URL,
#    headers={
#        "Accept": "application/json",
#    },
#    timeout=10,
#)

auth_resp = s.get(
    EDL_GET_TOKENS_URL,
    headers={
        "Accept": "application/json",
    },
    timeout=10,
)

curr_token = auth_resp.json()[0]

s = requests.Session()
s.trust_env = False

auth_token = {'Authorization': f'Bearer {curr_token}'}

s.headers.update(auth_token)

auth_url = "https://data.laadsdaac.earthdatacloud.nasa.gov/s3credentials"

cumulus_resp = s.get(
    auth_url, timeout=15, allow_redirects=True
)
auth_resp = s.get(
    cumulus_resp.url, allow_redirects=True, timeout=15
)

print(auth_resp.ok)
exit()

cumulus_resp = s.get(s3_endpoint, timeout=15, allow_redirects=True)
auth_resp = s.get(cumulus_resp.url, allow_redirects=True, timeout=15)

print(auth_resp.ok)
print(auth_resp.json())

exit()

client = boto3.client(
        's3',
        aws_access_key_id=environ["accessKeyId"],
        aws_secret_access_key=environ["secretAccessKey"],
        aws_session_token=environ["sessionToken"]
    )

print(client.__dict__)

response = client.Client.GetObject("prod-lads/VNP46A1/VNP46A1.A2023240.h04v16.001.2023241075046.h5")

print(json.dumps([r["Key"] for r in response['Contents']]))



#retrieve_credentials()

#exit()
#s3_endpoint = 'https://urs.earthdata.nasa.gov/s3credentials'

#results = requests.get(s3_endpoint, cookies={'accessToken': environ['earthdata_token']})

#print(results.status_code)
#print(results.content)
#exit()

#content = retrieve_credentials()
#print(content)
#exit()
#s = t_earthdata.get_laads_session()

#s3_redirect = 'https://urs.earthdata.nasa.gov/oauth/authorize?client_id=FtSFfbOeuxDcdf4px-elGw&redirect_uri=https%3A%2F%2Fdata.lpdaac.earthdatacloud.nasa.gov%2Fredirect&response_type=code&state=%2Fs3credentials'

#red_req = requests.get(s3_redirect)

#print(red_req.content)

#exit()
'https://data.lads.earthdatacloud.nasa.gov/s3credentials'
s3_cred_endpoint = 'https://urs.earthdata.nasa.gov/s3credentials'
temp_creds_req = requests.get(s3_cred_endpoint)

print(temp_creds_req.content)

#s3_creds = "https://data.laadsdaac.earthdatacloud.nasa.gov/s3credentials"

#r = t_requests.ask_nicely(s, s3_creds, max_attempts=1)
#print(r.code)

#print(r.json())

exit()



vnp46a1_file = 'VNP46A1.A2023240.h04v16.001.2023241075046.h5'

s3 = boto3.client('s3')
s3.download_file('prod-lads', f'VNP46A1/{vnp46a1_file}', Path(environ['outputs_dir'], vnp46a1_file))

exit()

filestr = "s3://prod-lads/VNP46A1/VNP46A1.A2023240.h04v16.001.2023241075046.h5"

cmr_cloudstac_ep = r'https://cmr.earthdata.nasa.gov/cloudstac/LAADS'



products = []

next_url = r'https://cmr.earthdata.nasa.gov/cloudstac/LAADS'

while next_url:

    r = t_requests.ask_nicely(t_stac.get_stac_session(),
                              next_url,
                              session_func=t_stac.get_stac_session())

    next_url = None

    for link in r.json()['links']:
        if link['rel'] == 'child':
            if 'VNP46' in link['href']:
                products.append(link['href'])
        elif link['rel'] == 'next':
            next_url = link['href']

for product in products:
    print(product)


#while year_urls:
#    year_url = year_urls.pop()

