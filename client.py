import threading
import time
import schedule
import random
import functools
import requests
from typing import Any
from flask import Flask, request, jsonify


def retry_request(max_retries=5):
    """
    Decorator function that retries a request for a specified number of times.
    Args:
        max_retries (int): The maximum number of retries (default is 5).
    Returns:
        function: The decorated function.
    """
    def decorator_retry_request(func):
        @functools.wraps(func)
        def wrapper_retry_request(*args, **kwargs):
            for _ in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except requests.RequestException as e:
                    time.sleep(2)
            print(f"Failed after {max_retries} retries.")
            return None
        return wrapper_retry_request
    return decorator_retry_request



class Client:
    def __init__(self):
        self.coordinator_url = 'http://localhost:5000'
        self.init_api = '/init'
        self.pull_api = '/pull'
        self.push_api = '/write'
        
        
    def init(self):
        self.brokers_lock = threading.Lock()
        self.brokers = requests.get(self.coordinator_url + self.init_api)
        

    
    @retry_request()
    def pull(self):
            """
            Pulls data from the server using the specified API endpoint.

            Returns:
                dict: The response content as a JSON object if the request is successful.

            Raises:
                requests.HTTPError: If the response status code is 4xx or 5xx.
            """
            dest = self.route()
            url = dest + self.pull_api
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            json = response.json()
            return json['key'], json['value']   # Return the response content if request is successful
                                                # Todo convert to byte?
            
            
    @retry_request()    
    def push(self, key: str, value: Any):
            """
            Pushes a key-value pair to the server.

            Args:
                key (str): The key to be pushed.
                value (Any): The value associated with the key.

            Returns:
                dict: The response from the server in JSON format.
            """
            dest = self.route()
            url = dest + self.push_api
            response = requests.post(url, json={'key': key, 'value': value})
            response.raise_for_status()
            return response.json() 
    
    
    def update_brokers(self, brokers):
    #    with self.brokers_lock 
            self.brokers = brokers
        
    def register_subscription(self):
            """
            Sends a POST request to the coordinator URL to register a subscription.

            Returns:
                str: The ID of the registered subscription.
            """
            url = self.coordinator_url + '/subscribe'
            response = requests.post(url, json={'name': 'kir'})
            json = response.json()
            return json['id']
         
    def route(self):
        """
        Selects a random broker from the list of brokers.

        Returns:
            str: The selected broker.
        """
        # with self.brokers_lock:
        return random.choice(self.brokers)
    
app = Flask(__name__)
client = Client()


@app.route('/update', methods=['POST'])
def update():
    data = request.json
    client.update_brokers(data['brokers'])

def pull():
    return client.pull()

def push(key: str, value):
    return client.push(key, value)

    
def subscription_func_wrapper(f):
    """
    A decorator function that wraps a subscription function.

    Args:
        f (function): The subscription function to be wrapped.

    Returns:
        function: The wrapped function that extracts the 'key' and 'value' from the request JSON and passes them to the original function.

    """
    def f_caller():
        data = request.json
        return f(data['key'], data['value']) # convert to byte?
    return f_caller
    
def subscribe(f):
    """
    Subscribes a function to a route and registers a subscription.

    Args:
        f (function): The function to be subscribed.

    Returns:
        None
    """
    id = client.register_subscription()
    app.route('/subscribe-' + id, methods=['POST'])(subscription_func_wrapper(f))
    return

app.run(host='0.0.0.0', port=5000, debug=True)


