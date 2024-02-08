import threading
import time
import schedule
import random
import functools
import requests
from typing import Any



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


def run_continuously(interval=10):
    """Continuously run, while executing pending jobs at each
    elapsed time interval.
    @return cease_continuous_run: threading. Event which can
    be set to cease continuous run. Please note that it is
    *intended behavior that run_continuously() does not run
    missed jobs*. For example, if you've registered a job that
    should run every minute and you set a continuous run
    interval of one hour then your job won't be run 60 times
    at each interval but only once.
    """
    cease_continuous_run = threading.Event()

    class ScheduleThread(threading.Thread):
        @classmethod
        def run(cls):
            while not cease_continuous_run.is_set():
                schedule.run_pending()
                time.sleep(interval)

    continuous_thread = ScheduleThread()
    continuous_thread.start()
    return cease_continuous_run

class Client:
    def __init__(self):
        
        self.coordinator_url = 'http://localhost:5000'
        self.data_api = '/data'
        self.pull_api = '/pull'
        self.push_api = '/push'
        self.init()
        
    
    def init(self):
        # Initialize the list of brokers and the lock
        self.brokers = self.get_brokers()
        self.brokers_lock = threading.Lock()
        # Initialize the destination broker
        self.route()
        
        # Set get brokers to run each 10 seconds
        schedule.every(10).seconds.do(self.get_brokers_sched)
        # Start the task
        self.scheduled_check = run_continuously()
    
    
    def get_brokers(self):
            """
            Retrieves a list of brokers from the coordinator server.

            Returns:
                A list of brokers in JSON format if the request is successful, None otherwise.
            """
            response = requests.get(self.coordinator_url + self.data_api)
            # Check if the response status code is not 200                
            if response.status_code != 200:
                # If the response status code is not 200, log the error and return
                print(f'Error: {response.status_code}')
                return None
            
            return response.json()
        
    def get_brokers_sched(self):
            """Get the list of brokers from the coordinator.

            This method makes a request to the coordinator's data API and retrieves the list of brokers.
            The retrieved data is then stored in the `self.brokers` attribute.

            Returns:
                None
            """            
            # Make request to self.coordinator/data and get response
            brokers = self.get_brokers()
            if brokers == None:
                return
              
            # Check if list is changed, update it
            if brokers != self.brokers:
                with self.brokers_lock:
                    self.brokers = brokers
                    self.route()

    def stop(self):
        """
        Stops the scheduled check.
        """
        self.scheduled_check.set()
        
    
    
    @retry_request()    
    def pull(self):
            """
            Sends a GET request to the destination broker's pull API and returns the response content as JSON.

            Returns:
                dict: The response content as a dictionary if the request is successful.

            Raises:
                requests.HTTPError: If the response status code is 4xx or 5xx.
            """
            url = ''
    #        with self.brokers_lock:
            url = self.dest_broker + self.pull_api
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()  # Return the response content if request is successful
 
    
    
    def push(self, key: str, value: Any):
        
        """
        Sends a POST request to the destination broker's push API with the given key and value.

        Args:
            key (str): The key to be pushed.
            value (Any): The value to be pushed.

        Returns:
            None

        Raises:
            requests.HTTPError: If the response status code is 4xx or 5xx.
        """
        url = ''
#        with self.brokers_lock:
        url = self.dest_broker + self.push_api
        response = requests.post(url, json={'key': key, 'value': value})
        response.raise_for_status()
        return response.json()
    
    def subscribe(self):
        pass
    
    def route(self):
        """
        Selects a random broker from the list of available brokers and returns it.

        Returns:
            str: The selected broker url.
        """
        # with self.brokers_lock:
        self.dest_broker = random.choice(self.brokers)
        return self.dest_broker

    def stop(self):
        self.scheduled_check.set()
        print('Client stopped')


