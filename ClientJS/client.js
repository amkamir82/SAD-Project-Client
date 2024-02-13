const { default: axios } = require('axios');
const express = require('express');
const request = require('sync-request')
const crypto = require('crypto');




const app = express();
app.use(express.json());
const port = 5001;
let brokers = []
let coordinatorURL = 'http://137.0.0.1:5000';
let backupCoordinatorURL = 'http://127.0.0.1:5000';
const initApi = '/init'
const pullApi = '/pull'
const pushApi = '/write'
const regSubscriptionApi = '/subscribe'
const health_check_api = '/client/healthcheck'
const ack_api = '/ack'
const myIp = '127.0.0.1'
const myPort = 5001
const sleepInterval = 3000
const TIME_OUT = 2000

axios.defaults.timeout = TIME_OUT;
function hash_md5(key) {
  const hash = crypto.createHash('md5');
  hash.update(key);
  return hash.digest('hex');
}

function randomChoice(list) {
  // Check if the list is empty
  if (list.length === 0) {
    return null;
  }

  // Generate a random index within the range of the list length
  const randomIndex = Math.floor(Math.random() * list.length);

  // Return the member at the random index
  return list[randomIndex];
}


app.post('/update', async (req, res) => {
  try {
    client.updateBrokers(req.body.brokers);
    res.status(200).send('Brokers updated');
  } catch (error) {
    console.error('Error updating brokers:', error);
    res.status(500).send('Error updating brokers');
  }
});

async function init() {
  app.listen(port, () => {
    console.log(`Client listening on port ${port}`);
  });
  try {
    const url = coordinatorURL + initApi;
    const res = await axios.get(url);
    if (res.status != 200) {
      const backup = backupCoordinatorURL + initApi
      const res = await axios.get(backup);
      brokers = res.data;

    } else {
      // console.log('body:' + res.getBody());
      brokers = res.data;
    }
  } catch (error) {
    try {
      const backup = backupCoordinatorURL + initApi
      const res = await axios.get(backup);
      brokers = res.data;
    } catch (err) {
      console.error(err)
    }
  }
}

async function pull() {

  let brokers_cp = [...brokers];
  while (true) {
    const destBroker = randomChoice(brokers_cp); // http://localhost:6000/
    const url = destBroker + pullApi;
    console.log("dest broker:" + destBroker);
    let data;
    try {
      const res = await axios.get(url)
      if (res.status_code != 200) {
        brokers_cp = brokers_cp.filter((item) => item !== destBroker);
        if(brokers_cp.length > 0)
          continue;
        else 
          return;
      }
      data = res.data;
    
    axios.post(destBroker + ack_api)
    return [data['key'], data['value']]
  } catch (error) {
    brokers_cp = brokers_cp.filter((item) => item !== destBroker);
        if(brokers_cp.length > 0)
          continue;
        else 
          return;
  }
  
}

}

function routeSend(key) {
  const partitionCount = brokers.length
  for (let i = 0; i < partitionCount; i++) {
    hashHex = hash_md5(key)
    if (parseInt(hashHex, 16) % partitionCount === i)
      return brokers[i];
  }

}

async function push(key, value) {
  try {
    const destBroker = routeSend(key) //
    console.log('push dest:', destBroker)
    const url = destBroker + pushApi
    const res = await axios.post(url, { key, value })
    const data = res.data
    return data
  } catch (error) {
    console.error(error)
  }
}

function subscriptionFuncWrapper(f) {
  return async (req, res) => {
    try {
      const { key, value } = req.body;
      await f(key, value);
      res.send('Awli');
    } catch (error) {
      console.error('Error in subscription function:', error);
      res.status(500).send('Error processing subscription');
    }
  };
}
async function registerSubscription() {
  try {
    let url = coordinatorURL + regSubscriptionApi;
    let res = await axios.post(url, { 'ip': myIp, 'port': myPort });
    if (res.status != 200) {
      url = backupCoordinatorURL + regSubscriptionApi;
      res = await axios.post(url, { 'ip': myIp, 'port': myPort });
      id = res.data['id'];
      return id;
    } else {
      id = res.data['id'];
      return id;
    }
  } catch (error) {
    url = backupCoordinatorURL + regSubscriptionApi;
    res = await axios.post(url, { 'ip': myIp, 'port': myPort });
    id = res.data['id'];
    return id;
  }
}

function healthcheck(id) {
  url1 = coordinatorURL + health_check_api
  url2 = backupCoordinatorURL + health_check_api
  res = axios.post(url1, { 'id': id }).then((res) => {
    if (res.status_code != 200) {
      axios.post(url2, { 'id': id })
    }
  }).catch((err) => {
    axios.post(url2, { 'id': id })
  })
}
async function subscribe(f) {
  const id = await registerSubscription();
  const route = `/subscribe-${id}`
  app.post(route, subscriptionFuncWrapper(f));
  setInterval(healthcheck, sleepInterval)

}

module.exports = {
  init,
  pull,
  push,
  subscribe
}