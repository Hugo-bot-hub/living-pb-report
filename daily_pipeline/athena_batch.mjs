// LLM 없이 MCP(mcp-remote)로 Athena 쿼리 배치 병렬 실행.
// 사용: node athena_batch.mjs <queriesJson> <outDir> [concurrency]
import { spawn } from 'child_process';
import { readFileSync, writeFileSync, mkdirSync } from 'fs';
const [,, QJSON='daily_pipeline/_queries.json', OUT='daily_pipeline/tmp', CONC='6'] = process.argv;
const conc = parseInt(CONC);
const URL='https://query.datapl.datahou.se/mcp?username=yys';
const queries = JSON.parse(readFileSync(QJSON,'utf8')); // {name: sql}
mkdirSync(OUT,{recursive:true});
const p=spawn('npx.cmd',['-y','mcp-remote',URL],{stdio:['pipe','pipe','pipe'],shell:true});
let buf='';const pending=new Map();
p.stdout.on('data',d=>{buf+=d.toString();let i;while((i=buf.indexOf('\n'))>=0){const line=buf.slice(0,i).trim();buf=buf.slice(i+1);if(!line)continue;let m;try{m=JSON.parse(line)}catch{continue}if(m.id!==undefined&&pending.has(m.id)){pending.get(m.id)(m);pending.delete(m.id)}}});
p.stderr.on('data',()=>{});
let idc=0;
function call(method,params,timeoutMs=360000){return new Promise((res,rej)=>{const id=++idc;pending.set(id,res);p.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');setTimeout(()=>{if(pending.has(id)){pending.delete(id);rej(new Error('timeout'))}},timeoutMs)})}
async function exec(sql){
  const r=await call('tools/call',{name:'execute_athena_query',arguments:{query:sql}});
  const txt=r.result?.content?.[0]?.text; if(!txt) throw new Error('no content');
  const obj=JSON.parse(txt);
  if(obj.status!=='SUCCEEDED') throw new Error('status='+obj.status+' '+(obj.error||''));
  return obj;
}
const CAP=1000;
async function runQuery(name,sql){
  const first=await exec(sql);
  let data=first.data||[];
  // 1000행 캡에 걸리면 전체 컬럼 ORDER BY + OFFSET 페이지네이션으로 완전 추출
  if(data.length>=CAP){
    const ncol=(first.columns||[]).length||1;
    const ob=Array.from({length:ncol},(_,i)=>i+1).join(',');
    data=[]; let off=0, page;
    do{ const o=await exec(`SELECT * FROM (${sql}) _pg ORDER BY ${ob} OFFSET ${off} LIMIT ${CAP}`);
        page=o.data||[]; data=data.concat(page); off+=CAP;
    } while(page.length>=CAP && off<200000);
  }
  writeFileSync(`${OUT}/${name}.json`, JSON.stringify({data}), 'utf8');
  return {name, rows:data.length, paged:data.length>CAP||first.data.length>=CAP};
}
const t0=Date.now();
const results=[], errors=[];
await new Promise(r=>setTimeout(r,3000));
await call('initialize',{protocolVersion:'2024-11-05',capabilities:{},clientInfo:{name:'batch',version:'1'}});
p.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');
const entries=Object.entries(queries);
let idx=0;
async function worker(){ while(idx<entries.length){ const [name,sql]=entries[idx++]; try{ const r=await runQuery(name,sql); results.push(r); process.stderr.write(`OK ${name} ${r.rows}rows\n`);}catch(e){ errors.push({name,err:e.message}); process.stderr.write(`ERR ${name}: ${e.message}\n`);} } }
await Promise.all(Array.from({length:conc},()=>worker()));
const secs=((Date.now()-t0)/1000).toFixed(1);
console.log(JSON.stringify({elapsed_s:+secs, ok:results.length, err:errors.length, errors, results:results.map(r=>r.name)}));
p.kill(); process.exit(errors.length?1:0);
