(function install(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  root.GridEdgeSourcePresence=api;
})(typeof globalThis==='object'?globalThis:this,function(){
  'use strict';
  const reviewed=/^https:\/\/quote\.eastmoney\.com\/f1\.html\?newcode=[01]\.\d{6}$/;
  function validUrls(value){
    return Array.isArray(value)&&value.length>0&&value.length<=8&&
      new Set(value).size===value.length&&value.every(url=>typeof url==='string'&&reviewed.test(url));
  }
  function createSourcePresenceWatchdog({readSettings,readRemembered,queryTabs,createTab,
                                        readCooldown,writeCooldown,nowMs=Date.now,timeoutMs=2000}){
    let inFlight=null;
    async function bounded(fn){
      let timer;
      try{return await Promise.race([Promise.resolve().then(fn),new Promise((_,reject)=>{
        timer=setTimeout(()=>reject(new Error('source presence operation timed out')),timeoutMs);
      })]);}finally{clearTimeout(timer);}
    }
    async function run(){
      if((await bounded(readSettings)).enabled!==true)return;
      const urls=await bounded(readRemembered);
      if(!validUrls(urls))return;
      const tabs=await bounded(queryTabs);
      for(const url of urls){
        if(tabs.some(tab=>tab.url===url))continue;
        const now=nowMs();
        const previous=await bounded(()=>readCooldown(url));
        if(previous!==null&&(!Number.isSafeInteger(previous)||now-previous<30000))continue;
        if(!Number.isSafeInteger(now)||now<0)return;
        // A durable reservation precedes tab creation. Navigation/enablement are
        // rechecked to avoid opening after a paused collector or another owner.
        await bounded(()=>writeCooldown(now,url));
        if((await bounded(readSettings)).enabled!==true)return;
        const rechecked=await bounded(readRemembered);
        if(!validUrls(rechecked)||!rechecked.includes(url))return;
        if((await bounded(queryTabs)).some(tab=>tab.url===url))continue;
        await bounded(()=>createTab({url,active:false}));
      }
    }
    return function request(){
      if(!inFlight)inFlight=run().finally(()=>{inFlight=null;});
      return inFlight;
    };
  }
  return {validUrls,createSourcePresenceWatchdog};
});
