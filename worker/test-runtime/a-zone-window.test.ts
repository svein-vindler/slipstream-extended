/** Explicit source-night zones outrank wall-clock/home fallbacks in both directions. */
import {env,createExecutionContext,waitOnExecutionContext} from 'cloudflare:test';
import {exportJWK,generateKeyPair,SignJWT} from 'jose';
import {PROTOCOL_VERSION_META_KEY,CLIENT_CAPABILITIES_META_KEY} from '@modelcontextprotocol/server';
import {beforeAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import worker from '../src/index';
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
beforeEach(async()=>{gets.length=0;
  vi.spyOn(globalThis,'fetch').mockImplementation(async input=>{const url=input instanceof Request?input.url:String(input);if(url!==ISSUER+'/cdn-cgi/access/certs')throw Error('Unexpected external request');
  return Response.json({keys:[jwk]});});
  token=await new SignJWT({type:'app',email:'synthetic@example.invalid'}).setProtectedHeader({alg:'RS256',kid:'a-fallback'}).setIssuer(ISSUER).setAudience('a-fallback').setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime('5m').sign(key);
  testEnv={...env,SLIPSTREAM_DATA:bucket,ACCESS_TEAM_DOMAIN:ISSUER,ACCESS_AUD:'a-fallback',MCP_HOSTNAME:HOST,HEALTH_TIMEZONE:'Europe/Oslo',MCP_WRITES_ENABLED:'false',GITHUB_ACTIONS_TOKEN:'',GITHUB_REPOSITORY:'',MCP_RATE_LIMITER:{limit:async()=>({success:true})}};});
afterEach(async()=>{await env.SLIPSTREAM_DATA.delete([HRV,SLEEP]);
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
const missingWindows = [
 {name:'both own window clocks absent', start:null, end:null, sleepZone:null},
 {name:'own end clock absent', start:START, end:null, sleepZone:null},
 {name:'own start clock absent', start:null, end:END, sleepZone:null},
 {name:'known HRV zone outranks a conflicting sleep zone while sleep supplies the window', start:null, end:null, sleepZone:'America/New_York'},
] as const;
for(const protocol of ['2026-07-28','2025-03-26'])for(const sample of missingWindows)it(protocol+': '+sample.name,async()=>{
 await env.SLIPSTREAM_DATA.put(HRV,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:'Asia/Tokyo',sleep_start_gmt:sample.start,sleep_end_gmt:sample.end,summary:{lastNightAvg:50},readings:[{timestamp:'2026-10-26T00:30:00Z',hrv_ms:50,time_basis:'utc',timestamp_field:'readingTimeGMT',timestamp_unit:'iso8601'}]}));
 await env.SLIPSTREAM_DATA.put(SLEEP,JSON.stringify({schema_version:1,source:'garmin',date:DAY,actual_night_timezone:sample.sleepZone,sleep_start_gmt:START,sleep_end_gmt:END,sleep_start_garmin_local:'2026-10-25T22:30:00',sleep_end_garmin_local:'2026-10-26T05:30:00',summary,stages:[]}));
 const result=await history(protocol);
 expect(result).toMatchObject({actual_night_timezone:'Asia/Tokyo',timezone:'Asia/Tokyo',local_time_source:'actual_night_timezone',night_of:DAY,sleep_start_local:'2026-10-26T06:30:00+09:00',sleep_end_local:'2026-10-26T13:30:00+09:00',night_context_stream:'sleep'});
 expect(result.source_time).toMatchObject({sleep_start_gmt:sample.start,sleep_end_gmt:sample.end,sleep_start_garmin_local:null});
 expect(result.night_context_source_time).toMatchObject({sleep_start_gmt:START,sleep_end_gmt:END,sleep_start_garmin_local:'2026-10-25T22:30:00'});
 expect(gets.filter(key=>key===SLEEP)).toHaveLength(1);
 expect(result.time_quality_flags).not.toContain('actual_timezone_unknown');
});
