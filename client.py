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
        self.push_api = '/push'
        
        
    def init(self):
        self.brokers_lock = threading.Lock()
        self.brokers = requests.get(self.coordinator_url + self.init_api)
        

    
    @retry_request()
    def pull(self):
        dest = self.route()
        url = dest + self.pull_api
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()  # Return the response content if request is successful
 
    @retry_request()    
    def push(self, key: str, value: Any):
        dest = self.route()
        url = dest + self.push_api
        response = requests.post(url, json={'key': key, 'value': value})
        response.raise_for_status()
        return response.json()
    
    def subscribe(self, f):
        pass
    
    def update_brokers(self, brokers):
    #    with self.brokers_lock 
            self.brokers = brokers
        
        
    def route(self):
    #    with self.brokers_lock:
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

def subscribe(f):
    return client.subscribe(f)

app.run(host='0.0.0.0', port=5000, debug=True)


