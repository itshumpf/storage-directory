const fs=require('fs'),crypto=require('crypto');
const out='analysis/storagemart_deep_dive_2026-10-17';fs.mkdirSync(out,{recursive:true});
const med=a=>{a=a.filter(Number.isFinite).sort((a,b)=>a-b);return a.length?(a[Math.floor((a.length-1)/2)]+a[Math.ceil((a.length-1)/2)])/2:null};
const sum=a=>a.reduce((x,y)=>x+y,0),av=u=>u.available&&Number.isFinite(u.price)&&u.price>0;
const data={},maps={},sources={},daily=[];
function cost(u,h){if(u.promo&&!u.promo_terms?.length)return null;let p=Array(h).fill(u.price);for(const t of u.promo_terms||[]){if(t.month<1||t.month>h)continue;if(t.type==='percent')p[t.month-1]*=1-t.amount/100;else if(['dollar','amount'].includes(t.type))p[t.month-1]=Math.max(0,p[t.month-1]-t.amount);else return null;}return sum(p)}
for(const f of fs.readdirSync('history/storagemart').filter(x=>/^2026-\d\d-\d\d.json$/.test(x)).sort()){
 const d=f.slice(0,10),path='history/storagemart/'+f,b=fs.readFileSync(path),ss=JSON.parse(b),m=new Map,dup=new Set;
 const units=ss.flatMap(s=>s.units),available=units.filter(av);data[d]=ss;
 for(const s of ss)for(const u of s.units){if(!u.sku)continue;let k=s.store_id+'|'+u.sku;if(m.has(k))dup.add(k);m.set(k,{...u,store_id:s.store_id,state:s.state,city:s.city,url:s.url})}for(const k of dup)m.delete(k);maps[d]=m;
 sources[d]={path,sha256:crypto.createHash('sha256').update(b).digest('hex'),stores:ss.length,rows:units.length,duplicateKeysExcluded:dup.size};
 const promos={};for(const u of available)promos[u.promo||'None']=(promos[u.promo||'None']||0)+1;
 daily.push({date:d,stores:ss.length,availableGroups:available.length,advertisedCount:sum(available.map(u=>u.count)),medianStorePrice:med(ss.map(s=>med(s.units.filter(av).map(u=>u.price)))),medianCheapest10x10:med(ss.map(s=>{let a=s.units.filter(u=>av(u)&&u.size==='10x10');return a.length?Math.min(...a.map(u=>u.price)):null})),promos,stores75:new Set(available.filter(u=>u.promo==='75% Off for 3 Months').map(u=>u)).size,storeCount75:ss.filter(s=>s.units.some(u=>av(u)&&u.promo==='75% Off for 3 Months')).length});
}
function compare(a,b){let pairs=[],identity=0;for(const[k,v]of maps[b]){let u=maps[a].get(k);if(!u)continue;if(['width','depth','height','attrs'].some(f=>JSON.stringify(u[f])!==JSON.stringify(v[f]))){identity++;continue}if(!av(u)||!av(v))continue;pairs.push([k,u,v])}
 const deeper=pairs.filter(([k,u,v])=>u.promo==='50% Off for 3 Months'&&v.promo==='75% Off for 3 Months');
 function stats(p){const horizons={};for(const h of [1,3,6,9,12]){let cc=p.map(([k,u,v])=>[cost(u,h),cost(v,h)]).filter(x=>x.every(Number.isFinite));horizons[h]={n:cc.length,before:sum(cc.map(x=>x[0])),after:sum(cc.map(x=>x[1])),aggregateChangePct:(sum(cc.map(x=>x[1]))/sum(cc.map(x=>x[0]))-1)*100,medianChangePct:med(cc.map(x=>(x[1]/x[0]-1)*100)),more:cc.filter(x=>x[1]>x[0]+1e-8).length}}
 return {n:p.length,stores:new Set(p.map(x=>x[2].store_id)).size,up:p.filter(x=>x[2].price>x[1].price).length,down:p.filter(x=>x[2].price<x[1].price).length,unchanged:p.filter(x=>x[2].price===x[1].price).length,medianRateChangePct:med(p.map(x=>(x[2].price/x[1].price-1)*100)),medianIncreasePct:med(p.filter(x=>x[2].price>x[1].price).map(x=>(x[2].price/x[1].price-1)*100)),aggregateRateChangePct:(sum(p.map(x=>x[2].price))/sum(p.map(x=>x[1].price))-1)*100,horizons};}
 const state={};for(const [k,u,v]of deeper){let s=state[v.state]??={groups:0,stores:new Set};s.groups++;s.stores.add(v.store_id)}
 const rows=pairs.map(([k,u,v])=>({key:k,state:v.state,city:v.city,url:v.url,oldPrice:u.price,newPrice:v.price,oldPromo:u.promo,newPromo:v.promo,oldPromoPrice:u.promo_price,newPromoPrice:v.promo_price}));
 fs.writeFileSync(out+'/'+a+'_'+b+'.json',JSON.stringify(rows,null,2));
 return {a,b,identityExcluded:identity,availableBoth:stats(pairs),deeper50to75:stats(deeper),deeperStates:Object.fromEntries(Object.entries(state).map(([k,v])=>[k,{groups:v.groups,stores:v.stores.size}]))};}
const comparisons=[['2026-09-28','2026-09-29'],['2026-09-30','2026-10-17'],['2026-10-13','2026-10-14'],['2026-10-13','2026-10-17']].map(x=>compare(...x));
const transitions=[];const ds=Object.keys(maps);for(let i=1;i<ds.length;i++){let a=ds[i-1],b=ds[i],n=0;for(const[k,v]of maps[b]){let u=maps[a].get(k);if(u&&av(u)&&av(v)&&['width','depth','height','attrs'].every(f=>u[f]===v[f])&&u.promo==='50% Off for 3 Months'&&v.promo==='75% Off for 3 Months')n++}if(n)transitions.push({a,b,deeper:n})}
const example=ds.filter(d=>d>='2026-10-13').map(d=>({date:d,...maps[d].get('smart_0106|smart_acb27f90-db0a-4f34-af8b-564669bd3749')}));
fs.writeFileSync(out+'/results.json',JSON.stringify({sources,daily,transitions,comparisons,example},null,2));
console.log(JSON.stringify({daily:daily.filter(x=>['2026-09-05','2026-09-28','2026-09-29','2026-09-30','2026-10-01','2026-10-13','2026-10-14','2026-10-17'].includes(x.date)),transitions,comparisons,example:example.map(({date,price,promo,promo_price,available})=>({date,price,promo,promo_price,available}))},null,2));
