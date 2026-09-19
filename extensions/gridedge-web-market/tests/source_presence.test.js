'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createSourcePresenceWatchdog}=require('../src/source_presence.js');
const url='https://quote.eastmoney.com/f1.html?newcode=0.002256';
test('missing previously reviewed source tab is restored with single-flight and cooldown',async()=>{
 const opened=[];let stored=null;let now=100000;
 const run=createSourcePresenceWatchdog({readSettings:async()=>({enabled:true}),
  readRemembered:async()=>[url],queryTabs:async()=>[],createTab:async x=>{opened.push(x);},
  readCooldown:async()=>stored,writeCooldown:async x=>{stored=x;},nowMs:()=>now});
 await Promise.all([run(),run(),run()]);assert.deepEqual(opened,[{url,active:false}]);
 await run();assert.equal(opened.length,1);now+=30000;await run();assert.equal(opened.length,2);
});
test('disabled, already present, duplicate, unknown URL and invalid remembered state cannot open tabs',async()=>{
 for(const [settings,remembered,tabs] of [[{enabled:false},[url],[]],
  [{enabled:true},[url],[{url}]], [{enabled:true},[url,url],[]],
  [{enabled:true},['https://evil.example/'],[]],[{enabled:true},null,[]]]){
  let count=0;const run=createSourcePresenceWatchdog({readSettings:async()=>settings,
   readRemembered:async()=>remembered,queryTabs:async()=>tabs,createTab:async()=>{count++;},
   readCooldown:async()=>null,writeCooldown:async()=>{},nowMs:()=>100000});
  await run();assert.equal(count,0);
 }
});


test('pause, revocation and another owner after reservation abort page creation',async()=>{
 for(const scenario of ['pause','revoke','other-owner']){
  let reserved=false,opened=0;
  const run=createSourcePresenceWatchdog({
   readSettings:async()=>({enabled:!(reserved&&scenario==='pause')}),
   readRemembered:async()=>reserved&&scenario==='revoke'?[]:[url],
   queryTabs:async()=>reserved&&scenario==='other-owner'?[{url}]:[],
   createTab:async()=>{opened++;},readCooldown:async()=>null,
   writeCooldown:async()=>{reserved=true;},nowMs:()=>100000});
  await run();assert.equal(opened,0,scenario);
 }
});
test('persistent cooldown survives a new worker; failed API releases the lane',async()=>{
 let stored=null,opened=0,hang=true;
 const options={readSettings:async()=>({enabled:true}),readRemembered:async()=>[url],
  queryTabs:async()=>[],createTab:async()=>{opened++;},readCooldown:async()=>stored,
  writeCooldown:async n=>{stored=n;},nowMs:()=>100000,timeoutMs:5};
 await createSourcePresenceWatchdog(options)();
 await createSourcePresenceWatchdog(options)();assert.equal(opened,1);
 const run=createSourcePresenceWatchdog({...options,readSettings:()=>hang?new Promise(()=>{}):Promise.resolve({enabled:false})});
 await assert.rejects(run,/timed out/);hang=false;await run();assert.equal(opened,1);
});
