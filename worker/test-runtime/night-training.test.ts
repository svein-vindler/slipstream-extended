import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import fixtures from "./generated/night-training-contract.json";

const issuer = "https://night-training-synthetic.cloudflareaccess.com", hostname = "night-training.example";
let privateKey: CryptoKey, jwk: Record<string, unknown>, token: string, testEnv: Env;
const counts = { get: 0, head: 0, list: 0, put: 0, delete: 0 };
let lastHttpBytes=0;
const tracked = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return (...args: unknown[]) => {
    if (property === "get") counts.get++; if (property === "head") counts.head++;
    if (property === "list") counts.list++; if (property === "put") counts.put++; if (property === "delete") counts.delete++;
    return Reflect.apply(value, target, args);
  };
} });
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "night-training", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  Object.assign(counts, {get:0,head:0,list:0,put:0,delete:0});
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "night-training" }).setIssuer(issuer).setAudience("night-training")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden");
    return Response.json({ keys: [jwk] });
  });
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "night-training", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: tracked, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "" };
  await env.SLIPSTREAM_DATA.put(fixtures.basic.storage_key, JSON.stringify(fixtures.basic.product));
});
afterEach(async () => {
  await env.SLIPSTREAM_DATA.delete(["health/sleep/v1/2026/10/2026-10-25.json","health/hrv/2026/10/2026-10-25.json"]);
  vi.restoreAllMocks();
  for (const fixture of Object.values(fixtures)) await env.SLIPSTREAM_DATA.delete(fixture.storage_key);
});
async function call(args: Record<string, unknown>, protocol: string, allowError=false, toolName="night_training_history") {
  const modern = protocol === "2026-07-28", context = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, { method: "POST", headers: {
    host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
    "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
    ...(modern ? {"mcp-method":"tools/call","mcp-name":toolName} : {}),
  }, body: JSON.stringify({jsonrpc:"2.0",id:1,method:"tools/call",params:{name:toolName,arguments:args,
    ...(modern ? {_meta:{[PROTOCOL_VERSION_META_KEY]:protocol,[CLIENT_CAPABILITIES_META_KEY]:{}}} : {})}}) }), testEnv, context);
  const text = await response.text(); lastHttpBytes=new TextEncoder().encode(text).byteLength; await waitOnExecutionContext(context);
  expect(response.status,text).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const data = text.split("\n").find(line => line.startsWith("data: "));
  const message = JSON.parse(data ? data.slice(6) : text);
  if(allowError)return message.error?{isError:true}:message.result;
  expect(message.error,text).toBeUndefined();
  expect(message.result.isError,text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}
describe("fixed offline training products through authenticated public HTTP", () => {
  for (const protocol of ["2025-03-26","2026-07-28"]) {
    it(`shows temporal associations and snapshot context with exactly one stored-product GET via ${protocol}`, async () => {
      const result = await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
      expect(result.status).toBe("available");
      expect(result.method_version).toBe("night-training-v1");
      expect(result.association).toBe("temporal_only_not_causal");
      expect(result.nights.map((row:{training:{activities:Array<{activity_id:string}>}}) =>
        row.training.activities.map(activity => activity.activity_id))).toEqual([["garmin-1"],["garmin-3"]]);
      expect(result.nights[0].channels.hrv.median).toBe(60);
      expect(result.nights[0].training.training_load).toBeNull();
      expect(result.nights[0].context).toMatchObject({status:"available",basis:"export_snapshot",values:{source:"user",alcohol:false,sleep_quality:4}});
      expect(JSON.stringify(result)).not.toContain("PRIVATE_NOTE_NEVER_PROJECT");
      expect(JSON.stringify(result)).not.toContain("PRIVATE_ACTIVITY_NAME");
      expect(counts).toEqual({get:1,head:0,list:0,put:0,delete:0});
      expect(result.resources.r2_get_attempts).toBe(1);
      if(protocol==="2026-07-28")console.info(JSON.stringify({synthetic_training_basic_resources:{...result.resources,http_body_bytes:lastHttpBytes}}));
    });
  }
});
for (const protocol of ["2025-03-26","2026-07-28"]) {
  it(`distinguishes absent product, missing activity export and zero recorded activities via ${protocol}`, async () => {
    const range={start_date:"2026-10-25",end_date:"2026-10-26"};
    await env.SLIPSTREAM_DATA.delete(fixtures.basic.storage_key);
    const absent=await call(range,protocol);
    expect(absent).toMatchObject({status:"not_stored",nights:[],activity_dataset_status:null});
    await env.SLIPSTREAM_DATA.put(fixtures.missing_activities.storage_key,JSON.stringify(fixtures.missing_activities.product));
    const missing=await call(range,protocol);
    expect(missing.activity_dataset_status).toBe("not_stored");
    expect(missing.nights[0].training.confirmed_activity_count).toBeNull();
    expect(missing.nights[0].training.recorded_duration_seconds).toBeNull();
    await env.SLIPSTREAM_DATA.put(fixtures.empty_activities.storage_key,JSON.stringify(fixtures.empty_activities.product));
    const empty=await call(range,protocol);
    expect(empty.activity_dataset_status).toBe("no_data");
    expect(empty.nights[0].training.confirmed_activity_count).toBe(0);
    expect(empty.nights[0].training.recorded_duration_seconds).toBe(0);
    expect(empty.coverage).toBe("provided_export_only_provider_completeness_unknown");
    expect(empty.nights[0].training.training_load).toBeNull();
  });
  it(`verifies scientific, signed-zero and round-tie numeric product identities from the actual CLI via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.floats.storage_key,fixtures.floats.product_json);
    const result=await call({start_date:"2026-10-25",end_date:"2026-10-25"},protocol);
    expect(result.status).toBe("available");
    expect(result.nights[0].channels.hrv.median).toBeCloseTo(0.6172838,12);
    expect(result.nights[0].channels.body_battery.median).toBe(0);
    expect(result.product_identity).toBe(fixtures.floats.product.product_identity);
  });
  it(`pages a40-night offline product without scanning source nights via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.pages.storage_key,JSON.stringify(fixtures.pages.product));
    const range={start_date:"2026-08-01",end_date:"2026-09-09"};
    const first=await call(range,protocol);
    expect(first.status).toBe("available");expect(first.nights).toHaveLength(7);
    expect(first.next_page_start_date).toBe("2026-08-08");
    const large=await call({...range,page_size:31},protocol);
    expect(large.nights).toHaveLength(31);expect(large.next_page_start_date).toBe("2026-09-01");
    const last=await call({...range,page_start_date:"2026-09-01",page_size:31},protocol);
    expect(last.nights).toHaveLength(9);expect(last.next_page_start_date).toBeNull();
    expect(counts).toEqual({get:3,head:0,list:0,put:0,delete:0});
  });
  it.each([
    {start_date:"2026-02-30",end_date:"2026-03-02"},
    {start_date:"2026-10-26",end_date:"2026-10-25"},
    {start_date:"2025-01-01",end_date:"2026-10-25"},
    {start_date:"2026-10-25",end_date:"2026-10-26",page_start_date:"2026-10-27"},
  ])(`rejects invalid product/page date bounds before storage access via ${protocol}: %j`,async args=>{
    expect((await call(args,protocol)).status).toBe("invalid_request");
    expect(counts.get).toBe(0);expect(counts.list).toBe(0);
  });
  it(`rejects tampered derived values and raw-field injection rather than exposing unverified product data via ${protocol}`,async()=>{
    const product=structuredClone(fixtures.basic.product);
    product.nights[0].channels.hrv.median=999;
    await env.SLIPSTREAM_DATA.put(fixtures.basic.storage_key,JSON.stringify(product));
    const bad=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    expect(bad.status).toBe("invalid_schema");expect(bad.nights).toEqual([]);
    const injected={...fixtures.basic.product,private_note:"RAW_FIELD_MUST_NOT_APPEAR"};
    await env.SLIPSTREAM_DATA.put(fixtures.basic.storage_key,JSON.stringify(injected));
    const unsafe=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    expect(unsafe.status).toBe("invalid_schema");expect(JSON.stringify(unsafe)).not.toContain("RAW_FIELD_MUST_NOT_APPEAR");
  });
  it(`keeps transport read failure distinct from missing product via ${protocol}`,async()=>{
    vi.spyOn(env.SLIPSTREAM_DATA,"get").mockRejectedValue(new Error("synthetic read outage"));
    const result=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    expect(result.status).toBe("read_error");expect(result.nights).toEqual([]);expect(result.resources.r2_get_attempts).toBe(1);
  });
  it(`reduces dense pages after an output limit without changing the product identity via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.dense.storage_key,JSON.stringify(fixtures.dense.product));
    const range={start_date:"2026-10-01",end_date:"2026-10-08"};
    const tooLarge=await call({...range,page_size:31},protocol);
    expect(tooLarge.status).toBe("output_limit");expect(tooLarge.nights).toEqual([]);
    const small=await call({...range,page_size:1},protocol);
    expect(small.status).toBe("available");expect(small.nights).toHaveLength(1);
    expect(small.product_identity).toBe(fixtures.dense.product.product_identity);
    expect(small.nights[0].training.activities).toHaveLength(100);
    expect(small.resources.structured_bytes).toBeLessThanOrEqual(256*1024);
    expect(small.resources.envelope_budget_bytes).toBeLessThanOrEqual(512*1024);
    if(protocol==="2026-07-28")console.info(JSON.stringify({synthetic_training_dense_page_resources:{...small.resources,http_body_bytes:lastHttpBytes}}));
  });
}
for (const protocol of ["2025-03-26","2026-07-28"]) {
  it(`accepts ordinary JSON reserialization of numeric signed zero from the actual offline CLI via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.floats.storage_key,JSON.stringify(fixtures.floats.product));
    const result=await call({start_date:"2026-10-25",end_date:"2026-10-25"},protocol);
    expect(result.status).toBe("available");
    expect(result.product_identity).toBe(fixtures.floats.product.product_identity);
    expect(result.nights[0].channels.body_battery.median).toBe(0);
  });
}
async function semanticDigest(value:unknown):Promise<string>{
  const typed=(item:unknown):unknown=>{
    if(typeof item==="number"){
      const bytes=new ArrayBuffer(8);new DataView(bytes).setFloat64(0,item===0?0:item,false);
      return "f64:"+[...new Uint8Array(bytes)].map(value=>value.toString(16).padStart(2,"0")).join("");
    }
    if(Array.isArray(item))return item.map(typed);
    if(item!==null&&typeof item==="object")return Object.fromEntries(Object.entries(item).sort(([a],[b])=>a<b?-1:a>b?1:0).map(([name,child])=>[name,typed(child)]));
    return item;
  };
  const bytes=await crypto.subtle.digest("SHA-256",new TextEncoder().encode(JSON.stringify(typed(value))));
  return [...new Uint8Array(bytes)].map(value=>value.toString(16).padStart(2,"0")).join("");
}
for(const protocol of ["2025-03-26","2026-07-28"]){
  it(`rejects a reidentified product whose recorded activity ends inside sleep via ${protocol}`,async()=>{
    const product=structuredClone(fixtures.basic.product);
    product.nights[0].training.activities[0].end_utc="2026-10-24T23:00:00Z";
    product.nights[0].training.activities[0].duration_seconds=10800;
    product.nights[0].training.known_recorded_duration_seconds=10800;
    const {product_identity:_old,...core}=product;
    product.product_identity=await semanticDigest(core);
    await env.SLIPSTREAM_DATA.put(fixtures.basic.storage_key,JSON.stringify(product));
    const result=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    expect(result.status).toBe("invalid_schema");expect(result.nights).toEqual([]);
  });
  it(`rejects a late read result instead of reporting a successful product via ${protocol}`,async()=>{
    const clock=vi.spyOn(performance,"now").mockReturnValue(0);
    const get=env.SLIPSTREAM_DATA.get.bind(env.SLIPSTREAM_DATA);
    vi.spyOn(env.SLIPSTREAM_DATA,"get").mockImplementation(async(name,options)=>{
      const result=await get(name,options);clock.mockReturnValue(10001);return result;
    });
    const result=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    expect(result.status).toBe("budget_exhausted");expect(result.nights).toEqual([]);
    expect(result.quality_flags).toContain("deadline_exceeded");
  });
  it(`stops oversized stored product before decoding it via ${protocol}`,async()=>{
    const get=env.SLIPSTREAM_DATA.get.bind(env.SLIPSTREAM_DATA);
    vi.spyOn(env.SLIPSTREAM_DATA,"get").mockImplementation(async(name,options)=>{
      const result=await get(name,options);
      return result?new Proxy(result,{get(target,property){return property==="size"?1024*1024+1:Reflect.get(target,property,target);}}):null;
    });
    const result=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    expect(result.status).toBe("budget_exhausted");expect(result.nights).toEqual([]);
    expect(result.resources).toMatchObject({r2_get_attempts:1,decoded_bytes:0,decoded_budget_bytes:1024*1024});
  });
  it(`rejects unsupported page size and arbitrary object selection before storage access via ${protocol}`,async()=>{
    const range={start_date:"2026-10-25",end_date:"2026-10-26"};
    expect((await call({...range,page_size:32},protocol,true)).isError).toBe(true);
    expect((await call({...range,key:"arbitrary/private.json"},protocol,true)).isError).toBe(true);
    expect(counts.get).toBe(0);
  });
}

it("does not read a training product for an unauthenticated caller",async()=>{
  const context=createExecutionContext();
  const response=await worker.fetch(new Request(`https://${hostname}/mcp`,{method:"POST",headers:{"content-type":"application/json"},
    body:JSON.stringify({jsonrpc:"2.0",id:1,method:"tools/call",params:{name:"night_training_history",arguments:{start_date:"2026-10-25",end_date:"2026-10-26"}}})}),testEnv,context);
  await waitOnExecutionContext(context);
  expect(response.status).toBe(401);expect(counts.get).toBe(0);
});
for(const protocol of ["2025-03-26","2026-07-28"]){
  for(const mode of ["ambiguous","documented_empty"]){
    it(`preserves normal exported source quality through the exact offline CLI product: ${mode} via ${protocol}`,async()=>{
      const day="2026-10-25";
      await env.SLIPSTREAM_DATA.put("health/sleep/v1/2026/10/2026-10-25.json",JSON.stringify({schema_version:1,source:"garmin",date:day,sleep_start_gmt:day+"T00:00:00Z",sleep_end_gmt:day+"T01:00:00Z"}));
      await env.SLIPSTREAM_DATA.put("health/hrv/2026/10/2026-10-25.json",JSON.stringify({schema_version:1,source:"garmin",date:day,readings:mode==="ambiguous"?[{timestamp:day+"T00:10:00",hrv_ms:40},{timestamp:day+"T00:20:00",hrv_ms:50}]:[]}));
      const exported=await call({date:day,channels:["hrv"]},protocol,false,"night_signals");
      const fixture=mode==="ambiguous"?fixtures.source_ambiguous:fixtures.source_empty;
      expect(exported.channels.hrv.points).toEqual([]);
      expect(exported.channels.hrv.status).toBe(fixture.source_export.channels.hrv.status);
      expect(exported.channels.hrv.quality).toEqual(fixture.source_export.channels.hrv.quality);
      expect(exported.channels.hrv.quality_flags).toEqual(fixture.source_export.channels.hrv.quality_flags);
      expect(fixture.product.source_manifest.export_files[0].sha256).toBe(fixture.source_export_sha256);
      await env.SLIPSTREAM_DATA.put(fixture.storage_key,JSON.stringify(fixture.product));
      Object.assign(counts,{get:0,head:0,list:0,put:0,delete:0});
      const result=await call({start_date:day,end_date:day},protocol);
      expect(result.status).toBe("available");
      const hrv=result.nights[0].channels.hrv;
      expect(hrv).toMatchObject({status:mode==="ambiguous"?"ambiguous_time":"no_data",valid_points:0,invalid_points:0,median:null,coverage:null,quality_flags:[]});
      expect(hrv.source_quality).toMatchObject({metadata_status:"provided",channel_status:exported.channels.hrv.status,counts:exported.channels.hrv.quality,quality_flags:exported.channels.hrv.quality_flags});
      expect(counts).toEqual({get:1,head:0,list:0,put:0,delete:0});
      if(protocol==="2026-07-28")console.info(JSON.stringify({synthetic_source_quality_roundtrip:{mode,exact_cli_input_sha256:fixture.source_export_sha256,...result.resources,http_body_bytes:lastHttpBytes}}));
    });
  }
  it(`does not reassign an activity when only the output period narrows via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.association_full.storage_key,JSON.stringify(fixtures.association_full.product));
    await env.SLIPSTREAM_DATA.put(fixtures.association_narrow.storage_key,JSON.stringify(fixtures.association_narrow.product));
    const wide=await call({start_date:"2026-10-25",end_date:"2026-10-26"},protocol);
    const narrow=await call({start_date:"2026-10-26",end_date:"2026-10-26"},protocol);
    expect(wide.status).toBe("available");expect(wide.nights[0].training.confirmed_activity_count).toBe(1);
    expect(wide.nights[1].training.confirmed_activity_count).toBe(0);
    expect(narrow.status).toBe("available");expect(narrow.nights[0].training).toMatchObject({confirmed_activity_count:0,activities:[],known_recorded_duration_seconds:0});
    expect(narrow.unlinked_counts.outside_window).toBe(1);expect(counts).toEqual({get:2,head:0,list:0,put:0,delete:0});
  });
  it(`retains a recorded sleep overlap outside the returned period via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.association_overlap.storage_key,JSON.stringify(fixtures.association_overlap.product));
    const result=await call({start_date:"2026-10-26",end_date:"2026-10-26"},protocol);
    expect(result.status).toBe("available");expect(result.unlinked_counts.overlaps_recorded_sleep).toBe(1);
    expect(result.nights[0].training).toMatchObject({status:"partial",confirmed_activity_count:0,uncertain_activity_count:1,known_recorded_duration_seconds:0,recorded_duration_seconds:null});
    expect(counts.get).toBe(1);expect(counts.list).toBe(0);
  });
  it(`accepts an ambiguous window whose other recorded overlap lies outside the output period via ${protocol}`,async()=>{
    await env.SLIPSTREAM_DATA.put(fixtures.overlapping_windows_narrow.storage_key,JSON.stringify(fixtures.overlapping_windows_narrow.product));
    const result=await call({start_date:"2026-10-26",end_date:"2026-10-26"},protocol);
    expect(result.status).toBe("available");expect(result.nights[0]).toMatchObject({status:"ambiguous_sleep_window",channels:{},training:{confirmed_activity_count:null}});
  });
  it(`rejects reidentified false empty source quality via ${protocol}`,async()=>{
    const product=structuredClone(fixtures.source_ambiguous.product);product.nights[0].channels.hrv.status="no_data";
    const {product_identity:_old,...core}=product;product.product_identity=await semanticDigest(core);
    await env.SLIPSTREAM_DATA.put(fixtures.source_ambiguous.storage_key,JSON.stringify(product));
    expect((await call({start_date:"2026-10-25",end_date:"2026-10-25"},protocol)).status).toBe("invalid_schema");
  });
  it(`rejects reidentified reassignment past an earlier exported night via ${protocol}`,async()=>{
    const product=structuredClone(fixtures.association_narrow.product);
    const activity=fixtures.association_full.product.nights[0].training.activities[0];
    product.nights[0].training.activities=[activity];product.nights[0].training.confirmed_activity_count=1;
    product.nights[0].training.known_recorded_duration_seconds=3600;product.nights[0].training.recorded_duration_seconds=3600;product.unlinked_counts.outside_window=0;
    const {product_identity:_old,...core}=product;product.product_identity=await semanticDigest(core);
    await env.SLIPSTREAM_DATA.put(fixtures.association_narrow.storage_key,JSON.stringify(product));
    expect((await call({start_date:"2026-10-26",end_date:"2026-10-26"},protocol)).status).toBe("invalid_schema");
  });
}

for(const protocol of ["2025-03-26","2026-07-28"])it(`retains a documented outside window with unavailable measurements via ${protocol}`,async()=>{
  await env.SLIPSTREAM_DATA.put(fixtures.unavailable_window_narrow.storage_key,JSON.stringify(fixtures.unavailable_window_narrow.product));
  const result=await call({start_date:"2026-10-26",end_date:"2026-10-26"},protocol);
  expect(result.status).toBe("available");expect(result.unlinked_counts.ambiguous_night_window).toBe(1);
  expect(result.nights[0].training).toMatchObject({status:"partial",confirmed_activity_count:0,known_recorded_duration_seconds:0,recorded_duration_seconds:null});
  expect(counts.get).toBe(1);expect(counts.list).toBe(0);
});
