import functools
import threading
import time
import random
import functools
import requests
import os
from dotenv import load_dotenv
from time import sleep
from flask import Flask, request, jsonify
import hashlib
import json as jsonlib

load_dotenv()

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
                    print('Request failed: \n', e)
            print(f"Failed after {max_retries} retries.")
            return None
        return wrapper_retry_request
    return decorator_retry_request


def hash_md5(key: str):
    return hashlib.md5(key.encode()).hexdigest()

class Client:
    def __init__(self):
        self.coordinator_url = os.getenv('COORDINATOR_URL')
        self.backup_coordinator_url = os.getenv('BACKUP_COORDINATOR_URL')
        self.init_api = os.getenv('INIT_API')
        self.pull_api = os.getenv('PULL_API')
        self.push_api = os.getenv('PUSH_API')
        self.ack_api = os.getenv('ACK_API')
        self.reg_subscribe_api = os.getenv('REG_SUBSCRIBE_API')
        self.my_port = os.getenv('MY_PORT')
        self.my_ip = os.getenv('MY_IP')
        self.health_check_api = os.getenv('HEALTH_CHECK_API')
        self.sleep_interval = int(os.getenv('SLEEP_INTERVAL'))
        
        self.init()

    def send_init_request(self, url):
        try:
            response = requests.post(url, data=jsonlib.dumps({'ip':self.my_ip, 'port':self.my_port}))
            if response.status_code == 200:
                brokers = response.json()
                print(f"response: {brokers}")
                return brokers
        except:
            return None
    
    def init(self):
        self.brokers_lock = threading.Lock()
        urls = [
            _ + self.init_api for _ in [self.coordinator_url, self.backup_coordinator_url]
        ]
        for url in urls:
            brokers = self.send_init_request(url)
            if brokers != None:
                self.brokers = brokers
                break


    @retry_request()
    def inner_pull(self, dest_broker):
        url = dest_broker + self.pull_api
        response = requests.get(url)
        if 400 <= response.status_code < 500:
            return None
        response.raise_for_status()
        json = response.json()
        return json['key'], json['value']   # Return the response content if request is successful
            
    
    def pull(self):
        """
        Pulls data from the server using the specified API endpoint.

        Returns:
            dict: The response content as a JSON object if the request is successful.

        Raises:
            requests.HTTPError: If the response status code is 4xx or 5xx.
        """
        brokers = dict(self.brokers)
        while True:
            print('brokers: ', brokers)
            dest_broker = self.route(brokers)
            print('dest_broker: ', dest_broker)
            result = self.inner_pull(dest_broker=dest_broker)
            if result is not None:
                url = dest_broker + self.ack_api
                requests.post(url)                
                return result
            brokers.remove(dest_broker)
            

            
            
    @retry_request()    
    def push(self, key: str, value):
        """
            Pushes a key-value pair to the server.

        Args:
            key (str): The key to be pushed.
            value (Any): The value associated with the key.

        Returns:
            dict: The response from the server in JSON format.
        """
        dest_broker = self.route_push(key)
        url = dest_broker + self.push_api
        print(f"dest_broker: {url}")
        response = requests.post(url, data=jsonlib.dumps({'key': key, 'value': value}), headers={"Content-Type": "application/json"})
        print("reponse: ",response.text, response.status_code)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.text
    
    
    def update_brokers(self, brokers):
        with self.brokers_lock:
            print(self.brokers)
            self.brokers = brokers
            print('updated brokers:', self.brokers)
    def send_register_request(self, url):
        try:
            response = requests.post(url, data=jsonlib.dumps({'ip':f'{self.my_ip}', 'port':f'{self.my_port}'}))
            print(response)
            if response.status_code == 200:
                json = response.json()
                print(json)
                return json['id']
        except Exception as e:
            print(e)
            
        return None
        
            
    def register_subscription(self):
        """
        Sends a POST request to the coordinator URL to register a subscription.

        Returns:
            str: The ID of the registered subscription.
        """
        urls = [
            _ + self.reg_subscribe_api for _ in [self.coordinator_url, self.backup_coordinator_url]
        ]
        for url in urls:
            print(url)
            broker_id = self.send_register_request(url)
            if broker_id is not None:
                return broker_id

            
    def route_push(self, key):
        with self.brokers_lock:
            partition_count = len(self.brokers.keys())
            print('partition count', partition_count)
            for item in self.brokers.keys():
                if int(hash_md5(key), 16) % partition_count == int(item) - 1:
                    print('id of dest broker', item)
                    return self.brokers[item]
            
    def route(self, brokers):
        """
        Selects a random broker from the list of brokers.

        Returns:
            str: The selected broker.
        """
        with self.brokers_lock:
            key = random.choice(list(brokers.keys()))
            return brokers[key]

app = Flask(__name__)
client = Client()


@app.route('/update-brokers', methods=['POST'])
def update():
    data = jsonlib.loads(request.data.decode('utf-8'))
    client.update_brokers(data['brokers'])
    return jsonify('Updated'), 200
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
        data = request.get_json()
        f(data['key'], data['value']) # convert to byte?
        return 'Awli'
    return f_caller

def healthcheck():
    url1 = client.coordinator_url + client.health_check_api
    url2 = client.backup_coordinator_url + client.health_check_api
    while True:
        try:
            res = requests.post(url1, data=jsonlib.dumps({'ip':client.my_ip, 'port':client.my_port}))    
            if res.status_code != 200:
                res = requests.post(url2, data=jsonlib.dumps({'ip':client.my_ip, 'port':client.my_port}))
        except:
            pass
        sleep(client.sleep_interval)
        
def subscribe(f):
    """
    Subscribes a function to a route and registers a subscription.

    Args:
        f (function): The function to be subscribed.

    Returns:
        None
    """
    sub_id = client.register_subscription()
    if sub_id == None:
        return 'Failed'
    app.route('/subscribe-' + str(sub_id), methods=['POST'])(subscription_func_wrapper(f))
    threading.Thread(
        target=healthcheck,
        daemon=True
    ).start()
    
    return

threading.Thread(
     target= lambda: app.run(
          host='0.0.0.0',
          port=client.my_port,
          debug=True,
          use_reloader=False,
        ),
        daemon=True
).start()
