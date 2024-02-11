const {pull, push, subscribe} = require('./client')
async function kir(){
    console.log('result: ' +  await pull());
    console.log('result: ' +  await push('arshia', 'akhavan'));

}
// push('arshia', 'akhavan').then(res => console.log(res))
// subscribe(console.log)

kir()