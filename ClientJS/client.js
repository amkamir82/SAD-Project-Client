const  {default: axios} = require('axios');
const express = require('express');
const request = require('sync-request')



const app = express();
app.use(express.json());
const port = 5001;
let brokers = []
let coordinatorURL = 'http://127.0.0.1:5000';
let backupCoordinatorURL = 'http://127.0.0.1:5000';
const initApi = '/init'
const pullApi = '/pull'
const pushApi = '/write'
const regSubscriptionApi = '/subscribe'
const myIp = '127.0.0.1'
const myPort = 5001


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

function init(){
    try{
        const url = coordinatorURL + initApi;
        const res = request('GET', url);
        if (res.status != 200)
        {
          const backup = backupCoordinatorURL + initApi
          const res = request('GET', backupCoordinatorURL);
          brokers = JSON.parse(res.getBody('utf8'));

        } else{
        // console.log('body:' + res.getBody());
          brokers = JSON.parse(res.getBody('utf8'));
        }
      } catch(error){
        console.error(error)
    }
}

async function pull() {
    try{
        const destBroker = randomChoice(brokers) // http://localhost:6000/
        const url = destBroker + pullApi
        const res = await axios.get(url)
        const data = res.data
        return [data['key'], data['value']]
    } catch (error){
        console.error(error)
    }
}

async function push(key, value) {
    try{
        const destBroker = randomChoice(brokers) // http://localhost:6000/
        const url = destBroker + pushApi
        const res = await axios.post(url, {key, value})
        const data = res.data
        return data
    } catch (error){
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
async function registerSubscription(){
    try{
        let url = coordinatorURL + regSubscriptionApi;
        let res = await axios.post(url, {'ip':myIp, 'port':myPort});
        if(res.status != 200) {
          url = backupCoordinatorURL + regSubscriptionApi;
          res = await axios.post(url, {'ip':myIp, 'port':myPort});
          id = res.data['id'];
          return id;
        } else{
          id = res.data['id'];
          return id;
        }
    } catch(error) {
        console.log(error);
    }
}

async function subscribe(f) {
  const id = await registerSubscription();
  const route = `/subscribe-${id}`
  app.post(route, subscriptionFuncWrapper(f));
}

init();
app.listen(port, () => {
  //  console.log(`Client listening on port ${port}`);
});

module.exports = {
    pull,
    push,
    subscribe
}