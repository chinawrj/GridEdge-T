"use strict";
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const ack = require('../src/mqtt_ack.js');
const delivery = require('../src/outbox_delivery.js');
const event = { event_id: 'f'.repeat(64), source_sequence: 1,
  mqtt_topic: 'isolated-test/market', payload: JSON.stringify({source:{
    source_id: 'test-source', source_instance_id: '00000000-0000-4000-8000-000000000001'}}) };
const receipt = Buffer.from(JSON.stringify({spec:'gridedge.market.ack',schema_version:1,
  event_id:event.event_id,source_id:'test-source',
  source_instance_id:'00000000-0000-4000-8000-000000000001',source_sequence:1,result:'COMMITTED'}));

async function outcomeWithin(work, budget = 80) {
  let timeout;
  try {
    return await Promise.race([work.then(()=> 'RESOLVED', e=>String(e.message)),
      new Promise(resolve=>{timeout=setTimeout(()=>resolve('HUNG'),budget);})]);
  } finally { clearTimeout(timeout); }
}

test('lost PUBACK after exact DB ACK releases flush lane and retains PENDING', async () => {
  const client = new EventEmitter();
  const acknowledgements = [];
  const result = await outcomeWithin(delivery.flushPending({database:{},client,
    durable:{pendingEvents: async()=>[event],acknowledge:async(...args)=>acknowledgements.push(args)},
    mqttAck:ack,ackTimeoutMs:10,publishWithPuback:()=>{
      client.emit('message',`${ack.ACK_PREFIX}/${event.event_id}`,receipt);
      return new Promise(()=>{});
    }}));
  assert.match(result,/timed out/,'missing PUBACK must not own the global flush coordinator forever');
  assert.deepEqual(acknowledgements,[]);
  assert.equal(client.listenerCount('message'),0);
});

test('lost SUBACK has a bounded subscription deadline', async () => {
  const result = await outcomeWithin(ack.subscribe({subscribe(){}},ack.ACK_PREFIX,10));
  assert.match(result,/timed out/,'CONNACK alone cannot leave connection setup pending forever');
});

test('both acknowledgement orders succeed and absent COMMITTED retains PENDING', async () => {
  for (const order of ['commit-first','puback-first','no-commit']) {
    const client = new EventEmitter();
    let acknowledge = 0;
    const result = await outcomeWithin(delivery.flushPending({database:{},client,mqttAck:ack,
      ackTimeoutMs:15,durable:{pendingEvents:async()=>acknowledge?[]:[event],
        acknowledge:async()=>{acknowledge++;}},publishWithPuback:()=>{
        if(order==='commit-first')client.emit('message',`${ack.ACK_PREFIX}/${event.event_id}`,receipt);
        if(order==='puback-first')setTimeout(()=>client.emit('message',`${ack.ACK_PREFIX}/${event.event_id}`,receipt),1);
        return Promise.resolve();
      }}));
    assert.equal(acknowledge,order==='no-commit'?0:1);
    if(order==='no-commit')assert.match(result,/timed out/); else assert.equal(result,'RESOLVED');
    assert.equal(client.listenerCount('message'),0);
  }
});

test('late callbacks and publish rejection cannot acknowledge a timed out attempt', async () => {
  const client = new EventEmitter();
  let lateReject;
  const rejected=[];
  const handler = error=>rejected.push(error);
  process.on('unhandledRejection',handler);
  try {
    const result=await outcomeWithin(ack.waitForCommittedAck(client,event,
      ()=>new Promise((_,reject)=>{lateReject=reject;}),5));
    assert.match(result,/timed out/);
    client.emit('message',`${ack.ACK_PREFIX}/${event.event_id}`,receipt);
    lateReject(new Error('late transport error'));
    let suback;
    assert.match(await outcomeWithin(ack.subscribe({subscribe(_topic,_opts,cb){suback=cb;}},ack.ACK_PREFIX,5)),/timed out/);
    suback(null,[{qos:1}]);
    assert.match(await outcomeWithin(ack.waitForCommittedAck(client,event,()=>{throw new Error('sync publish');},5)),/sync publish/);
    await new Promise(resolve=>setTimeout(resolve,10));
    assert.deepEqual(rejected,[]);
    assert.equal(client.listenerCount('message'),0);
  } finally { process.removeListener('unhandledRejection',handler); }
});

test('timed out delivery retries exact topic and bytes and acknowledges only both receipts', async () => {
  const client=new EventEmitter();
  const sends=[];
  let count=0;
  const options={database:{},client,mqttAck:ack,ackTimeoutMs:5,
    durable:{pendingEvents:async()=>count?[]:[event],acknowledge:async()=>{count++;}},
    publishWithPuback:(_client,current)=>{
      sends.push([current.mqtt_topic,current.payload]);
      if(sends.length===2)client.emit('message',`${ack.ACK_PREFIX}/${event.event_id}`,receipt);
      return Promise.resolve();
    }};
  assert.match(await outcomeWithin(delivery.flushPending(options)),/timed out/);
  assert.equal(count,0);
  assert.equal(await outcomeWithin(delivery.flushPending(options)),'RESOLVED');
  assert.equal(count,1);
  assert.deepEqual(sends[0],sends[1]);
});
