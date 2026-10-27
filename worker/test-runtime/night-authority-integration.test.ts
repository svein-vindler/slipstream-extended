/** Explicit source-night zones outrank wall-clock/home fallbacks in both directions. */
import {env,createExecutionContext,waitOnExecutionContext} from 'cloudflare:test';
import {exportJWK,generateKeyPair,SignJWT} from 'jose';
import {PROTOCOL_VERSION_META_KEY,CLIENT_CAPABILITIES_META_KEY} from '@modelcontextprotocol/server';
import {beforeAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import worker from '../src/index';
import {clearSummaryCaches} from '../src/r2-storage';
const DAY='2026-10-26',START='2026-10-25T21:30:00Z',END='2026-10-26T04:30:00Z';
const HRV='health/hrv/2026/10/'+DAY+'.json',SLEEP='health/sleep/v1/2026/10/'+DAY+'.json';
const ISSUER='https://a-fallback-synthetic.cloudflareaccess.com',HOST='a-fallback.example';
const summary={sleep_seconds:25200,deep_sleep_seconds:null,light_sleep_seconds:null,rem_sleep_seconds:null,awake_sleep_seconds:null,unmeasurable_sleep_seconds:null,nap_seconds:null,sleep_score:80,average_spo2_percent:null,lowest_spo2_percent:null,average_respiration_brpm:null,lowest_respiration_brpm:null,highest_respiration_brpm:null,average_sleep_stress:null};
let key:CryptoKey,jwk:Record<string,unknown>,token:string,testEnv:Env;
const gets:string[]=[];
const bucket=new Proxy(env.SLIPSTREAM_DATA,{get(target,property){const value=Reflect.get(target,property);if(property!=='get')return typeof value==='function'?value.bind(target):value;
  return async(...args:unknown[])=>{gets.push(String(args[0]));
  return Reflect.apply(value,target,args);};}});
beforeAll(async()=>{const pair=await generateKeyPair('RS256',{extractable:true});
  key=pair.privateKey;
  jwk={...await exportJWK(pair.publicKey),kid:'a-fallback',alg:'RS256',use:'sig'};});
beforeEach(async()=>{gets.length=0;clearSummaryCaches();
  vi.spyOn(globalThis,'fetch').mockImplementation(async input=>{const url=input instanceof Request?input.url:String(input);if(url!==ISSUER+'/cdn-cgi/access/certs')throw Error('Unexpected external request');
  return Response.json({keys:[jwk]});});
  token=await new SignJWT({type:'app',email:'synthetic@example.invalid'}).setProtectedHeader({alg:'RS256',kid:'a-fallback'}).setIssuer(ISSUER).setAudience('a-fallback').setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime('5m').sign(key);
  testEnv={...env,SLIPSTREAM_DATA:bucket,ACCESS_TEAM_DOMAIN:ISSUER,ACCESS_AUD:'a-fallback',MCP_HOSTNAME:HOST,HEALTH_TIMEZONE:'Europe/Oslo',MCP_WRITES_ENABLED:'false',GITHUB_ACTIONS_TOKEN:'',GITHUB_REPOSITORY:'',MCP_RATE_LIMITER:{limit:async()=>({success:true})}};});
afterEach(async()=>{await env.SLIPSTREAM_DATA.delete([HRV,SLEEP,'summary/health_daily.csv']);
  const events=await env.SLIPSTREAM_DATA.list({prefix:`health/night-context/v1/2026/10/${DAY}/`,limit:101});
  for(const event of events.objects)await env.SLIPSTREAM_DATA.delete(event.key);
  clearSummaryCaches();
  vi.restoreAllMocks();});
async function history(protocol:string){const modern=protocol==='2026-07-28',ctx=createExecutionContext();
  const response=await worker.fetch(new Request('https://'+HOST+'/mcp',{method:'POST',headers:{host:HOST,'content-type':'application/json',accept:'application/json, text/event-stream','cf-access-jwt-assertion':token,'mcp-protocol-version':protocol,...(modern?{'mcp-method':'tools/call','mcp-name':'hrv_history'}:{})},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'hrv_history',arguments:{start_date:DAY,end_date:DAY,granularity:'daily',detail_level:'full'},...(modern?{_meta:{[PROTOCOL_VERSION_META_KEY]:protocol,[CLIENT_CAPABILITIES_META_KEY]:{}}}:{})}})}),testEnv,ctx);
  const raw=await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status).toBe(200);
  const line=raw.split('\n').find(item=>item.startsWith('data: '));
  const message=JSON.parse(line?line.slice(6):raw);
  expect(message.error).toBeUndefined();
  expect(message.result.isError).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent.days[0];}
const cases=[
 {name:'own HRV actual zone outranks matching sleep wall clocks',ownZone:'Asia/Tokyo',ownLocal:false,sleepZone:null,stream:'hrv',ownStart:START,ownEnd:END,ownWall:null},
 {name:'matching sleep actual zone supplies the missing local authority',ownZone:null,ownLocal:false,sleepZone:'Asia/Tokyo',stream:'sleep',ownStart:'2026-10-25T22:00:00Z',ownEnd:'2026-10-26T04:00:00Z',ownWall:null},
 {name:'matching sleep actual zone outranks an HRV wall-clock-only zone',ownZone:null,ownLocal:true,sleepZone:'Asia/Tokyo',stream:'sleep',ownStart:'2026-10-25T22:00:00Z',ownEnd:'2026-10-26T04:00:00Z',ownWall:'2026-10-25T23:00:00'},
] as const;
for(const protocol of ['2026-07-28','2025-03-26'])for(const sample of cases)it(protocol+': '+sample.name,async()=>{
 await env.SLIPSTREAM_DATA.put(HRV,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:sample.ownZone,sleep_start_gmt:sample.ownStart,sleep_end_gmt:sample.ownEnd,sleep_start_garmin_local:sample.ownWall,sleep_end_garmin_local:sample.ownLocal?'2026-10-26T05:00:00':null,summary:{lastNightAvg:50},readings:[{timestamp:'2026-10-26T00:30:00Z',hrv_ms:50,time_basis:'utc',timestamp_field:'readingTimeGMT',timestamp_unit:'iso8601'}]}));
 await env.SLIPSTREAM_DATA.put(SLEEP,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:sample.sleepZone,sleep_start_gmt:START,sleep_end_gmt:END,sleep_start_garmin_local:'2026-10-25T22:30:00',sleep_end_garmin_local:'2026-10-26T05:30:00',summary,stages:[]}));
 const result=await history(protocol);
 expect(result).toMatchObject({actual_night_timezone:'Asia/Tokyo',timezone:'Asia/Tokyo',local_time_source:'actual_night_timezone',night_of:'2026-10-26',sleep_start_local:'2026-10-26T06:30:00+09:00',sleep_end_local:'2026-10-26T13:30:00+09:00',night_context_stream:sample.stream});
 expect(result.source_time).toMatchObject({sleep_start_gmt:sample.ownStart,sleep_end_gmt:sample.ownEnd,sleep_start_garmin_local:sample.ownWall});
 if(sample.stream==='sleep')expect(result.night_context_source_time).toMatchObject({sleep_start_gmt:START,sleep_end_gmt:END,sleep_start_garmin_local:'2026-10-25T22:30:00'});
 else expect(gets.filter(key=>key===SLEEP)).toEqual([]);
 expect(result.time_quality_flags).not.toContain('actual_timezone_unknown');
});
async function publicTool(name:string,args:Record<string,unknown>,protocol:string){
 const modern=protocol==='2026-07-28',ctx=createExecutionContext();
 const response=await worker.fetch(new Request('https://'+HOST+'/mcp',{method:'POST',headers:{host:HOST,'content-type':'application/json',accept:'application/json, text/event-stream','cf-access-jwt-assertion':token,'mcp-protocol-version':protocol,...(modern?{'mcp-method':'tools/call','mcp-name':name}:{})},body:JSON.stringify({jsonrpc:'2.0',id:2,method:'tools/call',params:{name,arguments:args,...(modern?{_meta:{[PROTOCOL_VERSION_META_KEY]:protocol,[CLIENT_CAPABILITIES_META_KEY]:{}}}:{})}})}),testEnv,ctx);
 const raw=await response.text();await waitOnExecutionContext(ctx);expect(response.status).toBe(200);
 const line=raw.split('\n').find(item=>item.startsWith('data: '));const message=JSON.parse(line?line.slice(6):raw);
 expect(message.error).toBeUndefined();expect(message.result.isError).not.toBe(true);expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
 return message.result.structuredContent;
}
async function sourcePair(ownZone:string|null,ownWall:string|null,sleepZone:string|null){
 await env.SLIPSTREAM_DATA.put(HRV,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:ownZone,sleep_start_gmt:'2026-10-25T22:00:00Z',sleep_end_gmt:'2026-10-26T04:00:00Z',sleep_start_garmin_local:ownWall,sleep_end_garmin_local:ownWall?'2026-10-26T05:00:00':null,summary:{lastNightAvg:50},readings:[{timestamp:'2026-10-26T00:30:00Z',hrv_ms:50,time_basis:'utc',timestamp_field:'readingTimeGMT',timestamp_unit:'iso8601'}]}));
 await env.SLIPSTREAM_DATA.put(SLEEP,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:sleepZone,sleep_start_gmt:START,sleep_end_gmt:END,sleep_start_garmin_local:'2026-10-25T22:30:00',sleep_end_garmin_local:'2026-10-26T05:30:00',summary,stages:[]}));
}
for(const protocol of ['2026-07-28','2025-03-26']){
 it(protocol+': explicit user timezone overlays selected sleep clock without losing original HRV provenance',async()=>{
  await sourcePair(null,'2026-10-25T23:00:00','Asia/Tokyo');
  testEnv.MCP_WRITES_ENABLED='true';
  await publicTool('add_night_context',{wake_date:DAY,actual_night_timezone:'America/New_York'},protocol);
  const result=await history(protocol);
  expect(result).toMatchObject({actual_night_timezone:'America/New_York',sleep_start_local:'2026-10-25T17:30:00-04:00',sleep_end_local:'2026-10-26T00:30:00-04:00',night_context_stream:'sleep',source_time:{sleep_start_gmt:'2026-10-25T22:00:00Z'},night_context_source_time:{sleep_start_gmt:START}});
  expect(result.user_context).toMatchObject({actual_night_timezone:'America/New_York',source:'user'});
  await env.SLIPSTREAM_DATA.put('summary/health_daily.csv','Date,Source,Sleep Seconds\n'+DAY+',garmin,25200\n');
  const daily=await publicTool('daily_health',{start_date:DAY,end_date:DAY},protocol);
  expect(daily.days[0].hrv_night).toMatchObject({sleep_start_local:'2026-10-25T17:30:00-04:00',source_time:{sleep_start_gmt:'2026-10-25T22:00:00Z'},night_context_source_time:{sleep_start_gmt:START}});
 });
 for(const sample of [
  {name:'HRVactual outranks sleepGarmin',ownZone:'Asia/Tokyo',ownWall:null,sleepZone:null,start:'2026-10-26T07:00:00+09:00'},
  {name:'sleepActual outranks HRVGarmin',ownZone:null,ownWall:'2026-10-25T23:00:00',sleepZone:'Asia/Tokyo',start:'2026-10-26T06:30:00+09:00'},
  {name:'invalid HRVactual is not authoritative',ownZone:'Invalid/Zone',ownWall:'2026-10-25T23:00:00',sleepZone:'Asia/Tokyo',start:'2026-10-26T06:30:00+09:00'},
 ])it(protocol+': daily_health '+sample.name,async()=>{
  await sourcePair(sample.ownZone,sample.ownWall,sample.sleepZone);
  await env.SLIPSTREAM_DATA.put('summary/health_daily.csv','Date,Source,Sleep Seconds\n'+DAY+',garmin,25200\n');
  const result=await publicTool('daily_health',{start_date:DAY,end_date:DAY},protocol);
  expect(result.days[0].hrv_night).toMatchObject({actual_night_timezone:'Asia/Tokyo',local_time_source:'actual_night_timezone',sleep_start_local:sample.start,night_of:DAY,source_time:{sleep_start_gmt:'2026-10-25T22:00:00Z'}});
  if(sample.sleepZone)expect(result.days[0].hrv_night.night_context_source_time).toMatchObject({sleep_start_gmt:START,sleep_end_gmt:END});
 });
}

for (const protocol of ['2026-07-28','2025-03-26']) for (const userOverride of [false,true]) it(protocol+': known HRV zone with missing window and user override='+userOverride, async()=>{
  await sourcePair('Asia/Tokyo',null,null);
  await env.SLIPSTREAM_DATA.put(HRV,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:'Asia/Tokyo',summary:{lastNightAvg:50},readings:[{timestamp:'2026-10-26T00:30:00Z',hrv_ms:50,time_basis:'utc',timestamp_field:'readingTimeGMT',timestamp_unit:'iso8601'}]}));
  if(userOverride){testEnv.MCP_WRITES_ENABLED='true';await publicTool('add_night_context',{wake_date:DAY,actual_night_timezone:'America/New_York'},protocol);}
  await env.SLIPSTREAM_DATA.put('summary/health_daily.csv','Date,Source,Sleep Seconds\n'+DAY+',garmin,25200\n');
  const result=await publicTool('daily_health',{start_date:DAY,end_date:DAY},protocol);
  expect(result.days[0].hrv_night).toMatchObject({actual_night_timezone:userOverride?'America/New_York':'Asia/Tokyo',night_of:userOverride?'2026-10-25':DAY,sleep_start_local:userOverride?'2026-10-25T17:30:00-04:00':'2026-10-26T06:30:00+09:00',source_time:{sleep_start_gmt:null,sleep_end_gmt:null},night_context_source_time:{sleep_start_gmt:START,sleep_end_gmt:END}});
});

// Public CSV summaries stay authoritative for metrics; canonical sources supply
// night-time provenance even when the corresponding CSV sleep metric is absent.
for (const protocol of ['2026-07-28','2025-03-26']) it(protocol+': keeps public CSV metrics while resolving canonical night context', async()=>{
  await sourcePair(null,'2026-10-25T23:00:00','Asia/Tokyo');
  await env.SLIPSTREAM_DATA.put('summary/health_daily.csv','Date,Source,HRV Last Night Average\n'+DAY+',garmin,49\n');
  const result=await publicTool('daily_health',{start_date:DAY,end_date:DAY},protocol);
  expect(result.days[0]).toMatchObject({hrv_last_night_avg_ms:49,sleep_hours:null,sleep_night:null});
  expect(result.days[0].hrv_night).toMatchObject({actual_night_timezone:'Asia/Tokyo',
    sleep_start_local:'2026-10-26T06:30:00+09:00',night_of:DAY,
    source_time:{sleep_start_gmt:'2026-10-25T22:00:00Z'},
    night_context_source_time:{sleep_start_gmt:START,sleep_end_gmt:END}});
});