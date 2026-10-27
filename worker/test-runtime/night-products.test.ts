/** Actual offline CLI products -> authenticated MCP JSON/SSE reference reader. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { beforeAll, beforeEach, afterEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import fixture from "./generated/night-products-contract.json";

type Row = Record<string, unknown>;
const ISSUER="https://night-products.cloudflareaccess.com", HOST="night-products.example";
const stored=new Set<string>(), calls:Array<{operation:string;key:string}>=[];
const responseSamples:Row[]=[];
let token:string,privateKey:CryptoKey,jwk:Row,testEnv:Env,id=0,failedGet:string|undefined;
const bucket=new Proxy(env.SLIPSTREAM_DATA,{get(target,property){
  const value=Reflect.get(target,property);
  if(typeof value!=="function")return value;
  return async(...args:unknown[])=>{
    calls.push({operation:String(property),key:String(args[0])});
    if(property==="get"&&args[0]===failedGet)throw new Error("SYNTHETIC_PRIVATE_PRODUCT_FAILURE");
    return Reflect.apply(value,target,args);
  };
}});
async function put(key:string,value:unknown){stored.add(key);await env.SLIPSTREAM_DATA.put(key,JSON.stringify(value));}
beforeAll(async()=>{
  const pair=await generateKeyPair("RS256",{extractable:true});privateKey=pair.privateKey;
  jwk={...await exportJWK(pair.publicKey),kid:"night-products",alg:"RS256",use:"sig"};
});
beforeEach(async()=>{
  calls.length=0;responseSamples.length=0;id=0;failedGet=undefined;
  vi.spyOn(globalThis,"fetch").mockImplementation(async input=>{
    const url=input instanceof Request?input.url:String(input);
    if(url===`${ISSUER}/cdn-cgi/access/certs`)return Response.json({keys:[jwk]});
    throw new Error("Unexpected external request in baseline contract");
  });
  token=await new SignJWT({type:"app",email:"synthetic@example.invalid"})
    .setProtectedHeader({alg:"RS256",kid:"night-products"}).setIssuer(ISSUER).setAudience("night-products")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  testEnv={...env,ACCESS_TEAM_DOMAIN:ISSUER,ACCESS_AUD:"night-products",MCP_HOSTNAME:HOST,HEALTH_TIMEZONE:"Europe/Oslo",
    MCP_WRITES_ENABLED:"false",GITHUB_ACTIONS_TOKEN:"",GITHUB_REPOSITORY:"",SLIPSTREAM_DATA:bucket,
    MCP_RATE_LIMITER:{limit:async()=>({success:true})}};
  for(const product of fixture.products)await put(product.storage_key,product.product);
});
afterEach(async context=>{
  context.task.meta.nightProductResources={responses:[...responseSamples],operations:Object.fromEntries(["get","head","list","put","delete"].map(operation=>[operation,calls.filter(call=>call.operation===operation).length]))};
  try{expect(calls.filter(call=>["put","delete","list","head"].includes(call.operation))).toEqual([]);}
  finally{await env.SLIPSTREAM_DATA.delete([...stored]);stored.clear();vi.restoreAllMocks();}
});
async function rpc(protocol:string,method:string,params:Row){
  const modern=protocol==="2026-07-28",requestId=++id,context=createExecutionContext(),started=performance.now();
  const response=await worker.fetch(new Request(`https://${HOST}/mcp`,{method:"POST",headers:{host:HOST,"content-type":"application/json",
    accept:"application/json, text/event-stream","cf-access-jwt-assertion":token,"mcp-protocol-version":protocol,
    ...(modern?{"mcp-method":method,...(method==="tools/call"?{"mcp-name":String(params.name)}:{})}:{})},
    body:JSON.stringify({jsonrpc:"2.0",id:requestId,method,params:{...params,...(modern?{_meta:{[PROTOCOL_VERSION_META_KEY]:protocol,[CLIENT_CAPABILITIES_META_KEY]:{}}}:{})}})}),testEnv,context);
  const text=await response.text();await waitOnExecutionContext(context);
  expect(response.status).toBe(200);expect(response.headers.get("content-type")).toContain(modern?"application/json":"text/event-stream");
  expect(response.headers.get("cache-control")).toContain("no-store");
  expect(new TextEncoder().encode(text).byteLength).toBeLessThanOrEqual(512*1024);
  const line=text.split("\n").find(part=>part.startsWith("data: "));
  const message=JSON.parse(line?line.slice(6):text);expect(message.id).toBe(requestId);
  if(method==="tools/call")responseSamples.push({protocol,elapsed_ms:performance.now()-started,raw_response_bytes:new TextEncoder().encode(text).byteLength,status:message.result?.structuredContent?.status,resources:message.result?.structuredContent?.resources});
  return message;
}
async function call(protocol:string,args:Row){
  if(protocol!=="2026-07-28")await rpc(protocol,"initialize",{protocolVersion:protocol,capabilities:{},clientInfo:{name:"night-products",version:"1"}});
  return rpc(protocol,"tools/call",{name:"night_baseline",arguments:args});
}
function success(message:Row){
  expect(message.error).toBeUndefined();const result=message.result as {isError?:boolean;content:Array<{text:string}>;structuredContent:Row};
  expect(result.isError).not.toBe(true);expect(JSON.parse(result.content[0].text)).toEqual(result.structuredContent);return result.structuredContent;
}
const args={end_date:"2026-10-25",period:"7",alignment:"sleep_start",bin_minutes:15,channel:"hrv"};
it.each(["2026-07-28","2025-03-26"])("%s reads exact CLI product with one GET and known nightly-weighted quantiles",async protocol=>{
  const value=success(await call(protocol,args));
  expect(value).toMatchObject({status:"available",method_version:"night-baseline-v1",current:{status:"available",window:{start_date:"2026-10-19",end_date:"2026-10-25"},
    contributing_nights:4}});
  expect((value.current as {bins:unknown[]}).bins[0]).toMatchObject({bin_index:0,p10:46,p25:55,p50:70,p75:85,p90:94,nights:4,valid_points:16});
  expect(calls).toEqual([{operation:"get",key:fixture.products[0].storage_key}]);
  expect(JSON.stringify(value)).not.toContain("snapshot_sha256");
});
it.each(["2026-07-28","2025-03-26"])("%s compares previous or explicit precomputed periods with exactly two GETs",async protocol=>{
  const value=success(await call(protocol,{...args,compare:"previous"}));
  expect(value).toMatchObject({status:"available",current:{window:{start_date:"2026-10-19",end_date:"2026-10-25"}},reference:{status:"available",window:{start_date:"2026-10-12",end_date:"2026-10-18"}}});
  expect((value.reference as {bins:Row[]}).bins[0]).toMatchObject({p10:23,p25:27.5,p50:35,p75:42.5,p90:47,nights:4});
  expect(calls.map(call=>call.key)).toEqual(fixture.products.slice(0,2).map(product=>product.storage_key));
  calls.length=0;
  const explicit=success(await call(protocol,{...args,compare:{start_date:"2026-10-12",end_date:"2026-10-18"}}));
  expect((explicit.reference as Row).source_identity).toBe((value.reference as Row).source_identity);
  expect(calls).toHaveLength(2);
});
it.each(["2026-07-28","2025-03-26"])("%s uses identical CLI filter canonicalization and reports clock-fold reference provenance",async protocol=>{
  const filtered=success(await call(protocol,{...args,context_filter:{travel:false,illness:false},exclude_dates:["2026-10-25","2026-10-25"]}));
  expect((filtered.current as {bins:Row[]}).bins[0]).toMatchObject({p50:50,nights:2,valid_points:4});
  expect(calls.at(-1)?.key).toBe(fixture.products.find(product=>product.label==="filtered")!.storage_key);
  expect(JSON.stringify(filtered)).not.toContain("PRIVATE_NOTE_NOT_IN_PRODUCT");
  const clock=success(await call(protocol,{...args,end_date:"2026-10-26",alignment:"clock"}));
  const current=clock.current as {bins:Row[];quality_notes:unknown[]};
  expect(current.bins.find(bin=>bin.bin_index===10)).toMatchObject({p10:64,p50:80,p90:96,nights:2,valid_points:3});
  expect(current.quality_notes).toContainEqual({wake_date:"2026-10-25",flags:["dst_fold_combined"]});
});
it.each(["2026-07-28","2025-03-26"])("%s distinguishes missing, failed, invalid and partial-reference products",async protocol=>{
  const key=fixture.products[0].storage_key;
  await env.SLIPSTREAM_DATA.delete(key);
  expect(success(await call(protocol,args))).toMatchObject({status:"not_stored",current:{status:"not_stored",bins:[]}});
  await put(key,fixture.products[0].product);failedGet=key;
  const failed=success(await call(protocol,args));
  expect(failed).toMatchObject({status:"read_error",current:{status:"read_error",bins:[]}});
  expect(JSON.stringify(failed)).not.toContain("SYNTHETIC_PRIVATE_PRODUCT_FAILURE");
  failedGet=undefined;
  await put(key,{...fixture.products[0].product,source_identity:"0".repeat(64)});
  expect(success(await call(protocol,args))).toMatchObject({status:"invalid_schema",current:{status:"invalid_schema",bins:[]}});
  await put(key,{...fixture.products[0].product,unit:"bpm"});
  expect(success(await call(protocol,args))).toMatchObject({status:"invalid_schema"});
  await put(key,fixture.products[0].product);failedGet=fixture.products[1].storage_key;
  expect(success(await call(protocol,{...args,compare:"previous"}))).toMatchObject({status:"partial",current:{status:"available"},reference:{status:"read_error"}});
});
it.each(["2026-07-28","2025-03-26"])("%s derives the same key for equivalent custom periods and rejects unbounded or arbitrary-key requests",async protocol=>{
  const value=success(await call(protocol,{...args,period:"custom",start_date:"2026-10-19"}));
  expect(value).toMatchObject({status:"available"});
  expect(calls.at(-1)?.key).toBe(fixture.products[0].storage_key);
  calls.length=0;
  const tooLong=success(await call(protocol,{...args,period:"custom",start_date:"2025-10-24"}));
  expect(tooLong).toMatchObject({status:"invalid_request",current:null});expect(calls).toEqual([]);
  const arbitrary=await call(protocol,{...args,key:"private/arbitrary.json"});
  expect(arbitrary.result.isError).toBe(true);expect(calls).toEqual([]);
  const noteFilter=await call(protocol,{...args,context_filter:{note:"SYNTHETIC_PRIVATE_FILTER"}});
  expect(noteFilter.result.isError).toBe(true);expect(JSON.stringify(noteFilter)).not.toContain("SYNTHETIC_PRIVATE_FILTER");expect(calls).toEqual([]);
});
it.each(["2026-07-28","2025-03-26"])("%s rejects bin positions that contradict the stored alignment method",async protocol=>{
  const source=structuredClone(fixture.products[0].product);
  source.bins[0].bin_index=-1;source.bins[0].offset_minutes=-15;
  await put(fixture.products[0].storage_key,source);
  expect(success(await call(protocol,args))).toMatchObject({status:"invalid_schema",current:{status:"invalid_schema",bins:[]}});
});
it.each(["2026-07-28","2025-03-26"])("%s reads a365-night actual CLI reference using one product GET and no source scan",async protocol=>{
  const value=success(await call(protocol,{...args,period:"365"}));
  expect(value).toMatchObject({status:"available",current:{nights_requested:365,source_nights:365,contributing_nights:365}});
  expect((value.current as {bins:Row[]}).bins[0]).toMatchObject({p50:50,nights:365,valid_points:365});
  expect(calls).toEqual([{operation:"get",key:fixture.products.find(product=>product.label==="annual")!.storage_key}]);
});
