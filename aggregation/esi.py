import requests
from . import redis_interface

refresh_uri = "https://login.eveonline.com/oauth/token"
refresh_headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Authorization': 'Basic NGE0Yzg4M2UwNmY5NDEyNTgxNWYyNTdjMTg2YTdlNjE6MkJLQjRkb1FXQjFTMVRFeFdlbndJTHNVd2xhQ2NSVDdLVFZlY201cA=='
}
cache = redis_interface.CacheInterface(None)

def get_esi_access_token():

    refresh_token = cache.redis.get('esi_refresh_token')

    if refresh_token is None:
        print("No refresh token available for ESI")
        return None

    try:
        res = requests.post(refresh_uri, headers=refresh_headers, data={'grant_type': 'refresh_token', 'refresh_token': refresh_token})

        data = res.json()
    except:
        return None

    if data is None or 'access_token' not in data:
        print("Failed to get access token from refresh token")
        return None

    return data['access_token']
