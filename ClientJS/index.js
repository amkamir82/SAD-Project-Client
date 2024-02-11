const {pull, push, subscribe} = require('./client')
async function kar(){
    console.log('result: ' +  await pull());
    console.log('result: ' +  await push('arshia', 'akhavan'));
    await subscribe(console.log)
}
// push('arshia', 'akhavan').then(res => console.log(res))
// subscribe(console.log)

kar()