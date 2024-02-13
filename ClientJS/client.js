const { default: axios } = require('axios');
require('dotenv').config()
const express = require('express');
const request = require('sync-request')
const crypto = require('crypto');




const app = express();
app.use(express.json());
const port = 5001;
let brokers = []
let coordinatorURL = process.env.COORDINATOR_URL;
let backupCoordinatorURL = process.env.BACKUP_COORDINATOR_URL;
const INIT_API = process.env.INIT_API;
const PULL_API = process.env.PULL_API;
const PUSH_API = process.env.PUSH_API;
const REG_SUBSCRIPTION_API = process.env.REG_SUBSCRIPTION_API;
const HEALTH_CHECK_API = process.env.HEALTH_CHECK_API;
const ACK_API = process.env.ACK_API;
const myIp = '127.0.0.1';
const myPort = 5001;
const SLEEP_INTERVAL_SLEEP = parseInt(process.env.SLEEP_INTERVAL_SLEEP);
const TIME_OUT = process.env.TIME_OUT;

axios.defaults.timeout = TIME_OUT;
function hash_md5(key) {
  const hash = crypto.createHash('md5');
  hash.update(key);
  return hash.digest('hex');
}

function randomChoice(brokersObj) {
  let lst = Object.keys(brokersObj)
  // Check if the list is empty
  if (lst.length === 0) {
    return null;
  }

  // Generate a random index within the range of the list length
  const randomIndex = Math.floor(Math.random() * lst.length);

  // Return the member at the random index
  return brokersObj[lst[randomIndex]];
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
    const url = coordinatorURL + INIT_API;
    const res = await axios.post(url, { 'ip': myIp, 'port': myPort });
    if (res.status != 200) {
      const backup = backupCoordinatorURL + INIT_API
      const res = await axios.post(backup, { 'ip': myIp, 'port': myPort });
      brokers = res.data;

    } else {
      // console.log('body:' + res.getBody());
      brokers = res.data;
    }
  } catch (error) {
    try {
      const backup = backupCoordinatorURL + INIT_API
      const res = await axios.post(backup, { 'ip': myIp, 'port': myPort });
      brokers = res.data;
    } catch (err) {
      console.error(err)
    }
  }

}

async function pull() {

  let brokers_cp = JSON.parse(brokers)
  while (true) {
    const destBroker = randomChoice(brokers_cp); // http://localhost:6000/
    const url = destBroker + PULL_API;
    console.log("dest broker:" + destBroker);
    let data;
    try {
      const res = await axios.get(url)
      if (res.status_code != 200) {
        brokers_cp = brokers_cp.filter((item) => item !== destBroker);
        if (brokers_cp.length > 0)
          continue;
        else
          return;
      }
      data = res.data;

      axios.post(destBroker + ACK_API)
      return [data['key'], data['value']]
    } catch (error) {
      brokers_cp = brokers_cp.filter((item) => item !== destBroker);
      if (brokers_cp.length > 0)
        continue;
      else
        return;
    }

  }

}

function routeSend(key) {
  const partitionCount = Object.keys(brokers).length
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
    const url = destBroker + PUSH_API
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
    let url = coordinatorURL + REG_SUBSCRIPTION_API;
    let res = await axios.post(url, { 'ip': myIp, 'port': myPort });
    if (res.status != 200) {
      url = backupCoordinatorURL + REG_SUBSCRIPTION_API;
      res = await axios.post(url, { 'ip': myIp, 'port': myPort });
      id = res.data['id'];
      return id;
    } else {
      id = res.data['id'];
      return id;
    }
  } catch (error) {
    url = backupCoordinatorURL + REG_SUBSCRIPTION_API;
    res = await axios.post(url, { 'ip': myIp, 'port': myPort });
    id = res.data['id'];
    return id;
  }
}

function healthcheck(id) {
  try {
    url1 = coordinatorURL + HEALTH_CHECK_API
    url2 = backupCoordinatorURL + HEALTH_CHECK_API
    res = axios.post(url1, { 'ip': myIp, 'port': myPort }).then((res) => {
      if (res.status != 200) {
        console.log('bad health check')
        axios.post(url2, { 'ip': myIp, 'port': myPort })
      }
    }).catch((err) => {
      axios.post(url2, { 'ip': myIp, 'port': myPort })
    })
  } catch (e) {

  }
}
async function subscribe(f) {
  const id = await registerSubscription();
  const route = `/subscribe-${id}`
  app.post(route, subscriptionFuncWrapper(f));
  setInterval(healthcheck, SLEEP_INTERVAL_SLEEP)

}

module.exports = {
  init,
  pull,
  push,
  subscribe
}