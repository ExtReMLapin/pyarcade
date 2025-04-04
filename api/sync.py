from .client import Client, LoginFailedException

import json
import logging
import requests


class SyncClient(Client):
    def __init__(self, host: str, port: str, protocol: str = "http", **kwargs):
        super().__init__(host, port, protocol, **kwargs)

    def subhandler(self, response: requests.Response, return_headers: bool=False):
        if response.status_code >= 400 :
            json_decoded_data = response.json()
            print(json_decoded_data)
            java_error_code = json_decoded_data['exception'] if 'exception' in json_decoded_data else "Unknown error"
            detail_error_code = None
            if 'detail' in json_decoded_data:
                detail_error_code = json_decoded_data['detail']
            elif 'exception' in json_decoded_data:
                detail_error_code = json_decoded_data['exception']
            else:
                detail_error_code = "Unknown error"
            
            if java_error_code == "com.arcadedb.server.security.ServerSecurityException":
                raise LoginFailedException(java_error_code, detail_error_code)
            else:
                raise Exception(java_error_code, detail_error_code)
        
        response.raise_for_status()
        logging.debug(f"response: {response.text}")
        if return_headers is False:
            if len(response.text) > 0:
                try:
                    return response.json()["result"]
                except:
                    return response.text
            else:
                return
        else:
            return  response.headers

    def post(self, endpoint: str, payload: dict, return_headers: bool=False, extra_headers: dict = {}) -> requests.Response:
        endpoint = self._get_endpoint(endpoint)
        logging.info(f"posting to {endpoint} with payload {payload}")
        response = requests.post(
            endpoint,
            data=json.dumps(payload),
            headers={**self.headers,**extra_headers},
            auth=(self.username, self.password),
        )
        return self.subhandler(response, return_headers=return_headers)

    def get(self, endpoint: str, return_headers: bool=False, extra_headers: dict = {}) -> requests.Response:
        endpoint = self._get_endpoint(endpoint)
        logging.info(f"submitting get request to {endpoint}")
        response = requests.get(
            endpoint,
            auth=(self.username, self.password),
            headers=extra_headers,
        )
        return self.subhandler(response, return_headers=return_headers)

