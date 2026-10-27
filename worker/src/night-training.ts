/** One bounded read of a versioned offline activity -> following-night product. */
import type { McpServer, RequestId } from "@modelcontextprotocol/server";
import { z } from "zod";
import { R2Storage } from "./r2-storage";
import { PayloadTooLargeError } from "./security";
import { exactDate, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";

const METHOD = "night-training-v1";
const CSV_TIME = "slipstream-activities-csv-utc-v1";
const COVERAGE = "provided_export_only_provider_completeness_unknown";
const LINK_METHOD = "earliest-documented-sleep-after-recorded-end-within48h-overlap-excluded-v1";
const encoder = new TextEncoder();
const integer = z.number().int().nonnegative();
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const number = z.number().finite().nonnegative();
const instant = z.iso.datetime();
const argsSchema = z.object({start_date:exactDate,end_date:exactDate}).strict();
const datasetStatus = z.enum(["available","no_data","not_stored","read_error"]);
const contextFields = ["alcohol","late_bedtime","travel","illness","sleep_quality","morning_condition","actual_night_timezone"] as const;
const contextValues = z.object({source:z.literal("user"),alcohol:z.boolean().nullable(),late_bedtime:z.boolean().nullable(),
  travel:z.boolean().nullable(),illness:z.boolean().nullable(),sleep_quality:integer.min(1).max(5).nullable(),
  morning_condition:integer.min(1).max(5).nullable(),actual_night_timezone:z.string().min(1).max(100).nullable(),
  declared_fields:z.array(z.enum(contextFields)).max(7)}).strict();
const contextSchema = z.object({status:z.enum(["available","not_stored","not_provided","unavailable"]),
  basis:z.literal("export_snapshot"),revision:z.string().min(1).max(128).nullable(),values:contextValues.nullable()}).strict();
const activitySchema = z.object({activity_id:z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/),
  activity_type:z.string().max(64),source:z.string().regex(/^[a-z0-9_-]{1,32}$/),
  start_utc:instant,end_utc:instant,duration_seconds:integer.max(7*86400),average_hr_bpm:number.nullable(),
  time_basis:z.enum([CSV_TIME,"explicit_offset"]),intensity:z.null(),training_load:z.null()}).strict();
const trainingSchema = z.object({status:z.enum(["available","no_data","not_stored","read_error","partial"]),
  confirmed_activity_count:integer.max(100).nullable(),uncertain_activity_count:integer.max(5000).nullable(),
  activities:z.array(activitySchema).max(100),recorded_duration_seconds:number.nullable(),known_recorded_duration_seconds:number.nullable(),
  intensity:z.null(),training_load:z.null(),coverage:z.literal(COVERAGE)}).strict();
const exportedStatuses = ["available","partial","no_data","not_stored","read_error","invalid_schema","invalid_response","ambiguous_time"] as const;
const exportedFlags = ["invalid_observations","ambiguous_time_excluded","conflicting_instants_excluded","observed_gaps","coverage_unknown","minimum_window_not_covered","source_quality_metadata_invalid","source_retained_revision_unknown","source_available","source_partial","source_no_data","source_not_stored","source_read_error","source_invalid_schema","source_invalid_response","source_ambiguous_time"] as const;
const observationFlags = ["missing_values","partial_source","retained_previous_observations","source_completeness_unknown","retained_revision_unknown","invalid_source_shape","missing_or_conflicting_source_date","invalid_or_oversize_curve","invalid_point","invalid_timestamp","invalid_timestamp_unit_or_range","invalid_value","conflicting_source_observations","duplicate_source_observations","unverified_source_observations"] as const;
const sourceCount = integer.max(200000).nullable();
const sourceQualitySchema = z.object({metadata_status:z.enum(["provided","not_provided","invalid"]),
  channel_status:z.enum(exportedStatuses),counts:z.object({invalid:sourceCount,ambiguous_time:sourceCount,outside_interval:sourceCount,duplicates:sourceCount,conflicting_timestamps:sourceCount,gaps:sourceCount}).strict().nullable(),
  quality_flags:z.array(z.enum(exportedFlags)).max(32),observation_flags:z.array(z.enum(observationFlags)).max(16),missing_value_count:integer.max(100000).nullable()}).strict();
const channelSchema = z.object({status:z.enum(["available","partial","no_data","not_stored","read_error","invalid_schema","invalid_response","ambiguous_time"]),
  source_quality:sourceQualitySchema.optional(),
  unit:z.enum(["bpm","ms","points"]),valid_points:integer.max(200000),invalid_points:integer.max(200000),median:number.nullable(),
  observed_cadence_seconds:z.number().finite().positive().nullable(),coverage:number.max(1).nullable(),
  coverage_basis:z.literal("occupied-observed-cadence-slots-v1"),largest_gap_seconds:number.nullable(),
  quality_flags:z.array(z.enum(["conflicting_timestamps_excluded","invalid_or_ambiguous_or_outside_points_excluded"])).max(2),
  source_revisions:z.array(z.object({date:exactDate,revision:z.string().min(1).max(256)}).strict()).max(16)}).strict();
const channelsSchema = z.object({heart_rate:channelSchema.optional(),hrv:channelSchema.optional(),stress:channelSchema.optional(),body_battery:channelSchema.optional()}).strict();
const nightSchema = z.object({wake_date:exactDate,status:z.enum(["available","not_in_export","duplicate_conflict","unsupported_source_method","invalid_sleep_interval","source_unavailable","ambiguous_sleep_window"]),
  sleep_start_utc:instant.nullable(),sleep_end_utc:instant.nullable(),actual_night_timezone:z.string().min(1).max(100).nullable(),
  quality_flags:z.array(z.enum(["actual_timezone_unknown","invalid_actual_timezone","source_partial","context_unavailable","ambiguous_sleep_windows"])).max(5),
  context:contextSchema,channels:channelsSchema,training:trainingSchema}).strict();
const activitySourceSchema = z.object({status:datasetStatus,sha256:hash.nullable(),bytes:integer.max(16*1024*1024),rows:integer.max(5000),duplicates:integer.max(5000),
  time_contract:z.literal(CSV_TIME),coverage:z.literal(COVERAGE)}).strict();
const associationWindowSchema = z.object({wake_date:exactDate,sleep_start_utc:instant,sleep_end_utc:instant,
  status:z.enum(["available","ambiguous_sleep_window","duplicate_conflict","source_unavailable","unsupported_source_method"]),snapshot_sha256:hash}).strict();
const sourceManifestSchema = z.object({association_windows:z.array(associationWindowSchema).max(366).optional(),activity_source:activitySourceSchema,export_files:z.array(z.object({sha256:hash,bytes:integer.min(1).max(16*1024*1024)}).strict()).min(1).max(64),nights:z.array(z.object({wake_date:exactDate,
  snapshot_sha256:hash.nullable(),context_revision:z.string().min(1).max(128).nullable()}).strict()).max(366)}).strict();
const unlinkedSchema = z.object({unknown_start:integer,unknown_end:integer,overlaps_recorded_sleep:integer,ambiguous_night_window:integer,outside_window:integer,
  duplicate_conflict:integer,invalid_identity:integer}).strict();
const productSchema = z.object({schema_version:z.literal(1),kind:z.literal("night-training"),method_version:z.literal(METHOD),
  args:argsSchema,args_sha256:hash,source_manifest:sourceManifestSchema,source_identity:hash,product_identity:hash,
  association:z.literal("temporal_only_not_causal"),link_method:z.literal(LINK_METHOD),activity_time_contract:z.literal(CSV_TIME),
  activity_dataset_status:datasetStatus,nights_requested:integer.min(1).max(366),nights:z.array(nightSchema).max(366),unlinked_counts:unlinkedSchema,
  resources:z.object({input_bytes:integer.max(16*1024*1024),input_points:integer.max(200000),activity_rows:integer.max(5000)}).strict()}).strict();
const resourcesSchema = z.object({r2_get_attempts:integer,decoded_bytes:integer,decoded_budget_bytes:integer,
  elapsed_ms:number,structured_bytes:integer,envelope_budget_bytes:integer});
export const nightTrainingHistorySchema = z.object({status:z.enum(["available","not_stored","read_error","invalid_schema","invalid_request","budget_exhausted","output_limit"]),
  method_version:z.literal(METHOD),association:z.literal("temporal_only_not_causal"),coverage:z.literal(COVERAGE),
  window:argsSchema.nullable(),page_start_date:exactDate.nullable(),next_page_start_date:exactDate.nullable(),page_size:integer.min(1).max(31),
  source_identity:hash.nullable(),product_identity:hash.nullable(),activity_dataset_status:datasetStatus.nullable(),
  nights_requested:integer,nights_returned:integer.max(31),nights:z.array(nightSchema).max(31),unlinked_counts:unlinkedSchema.nullable(),
  quality_flags:z.array(z.string()),resources:resourcesSchema});
type Product = z.infer<typeof productSchema>;
type Result = z.infer<typeof nightTrainingHistorySchema>;
class Deadline extends Error {}
function check(started:number) {if(performance.now()-started>10000)throw new Deadline();}
function dayNumber(value:string) {
  const time=Date.parse(value+"T00:00:00Z");
  if(!Number.isFinite(time)||value<"1970-01-01"||new Date(time).toISOString().slice(0,10)!==value)throw new Error("invalid_date");
  return time/86400000;
}
function validZone(value:string|null) {
  if(value===null)return true;
  try{new Intl.DateTimeFormat("en",{timeZone:value});return true;}catch{return false;}
}
/** Finite IEEE754 semantic material: signed zero normalized, other bits match Python struct.pack('>d'). */
function material(value:unknown):unknown {
  if(typeof value==="number"){
    if(!Number.isFinite(value))throw new Error("nonfinite");
    const bytes=new ArrayBuffer(8);new DataView(bytes).setFloat64(0,value===0?0:value,false);
    return "f64:"+[...new Uint8Array(bytes)].map(byte=>byte.toString(16).padStart(2,"0")).join("");
  }
  if(Array.isArray(value))return value.map(material);
  if(value!==null&&typeof value==="object")return Object.fromEntries(Object.entries(value)
    .sort(([left],[right])=>left<right?-1:left>right?1:0).map(([key,item])=>[key,material(item)]));
  return value;
}
async function digest(value:unknown) {
  const buffer=await crypto.subtle.digest("SHA-256",encoder.encode(JSON.stringify(material(value))));
  return [...new Uint8Array(buffer)].map(byte=>byte.toString(16).padStart(2,"0")).join("");
}
function consistent(product:Product):boolean {
  const count=dayNumber(product.args.end_date)-dayNumber(product.args.start_date)+1;
  const source=product.source_manifest.activity_source;
  if(count<1||count>366||count!==product.nights_requested||product.nights.length!==count
    ||product.source_manifest.nights.length!==count||product.activity_dataset_status!==source.status
    ||product.resources.activity_rows!==source.rows||source.duplicates>source.rows
    ||source.bytes+product.source_manifest.export_files.reduce((sum,file)=>sum+file.bytes,0)!==product.resources.input_bytes)return false;
  const absent=source.status==="not_stored"||source.status==="read_error";
  if(absent ? source.sha256!==null||source.rows!==0||source.bytes!==0 : source.sha256===null||source.status==="no_data"&&source.rows!==0||source.status==="available"&&source.rows===0)return false;
  const associationContext=product.source_manifest.association_windows;
  const documented=associationContext??product.nights.filter(night=>night.status==="available"||night.status==="ambiguous_sleep_window")
    .map(night=>({wake_date:night.wake_date,status:night.status,sleep_start_utc:night.sleep_start_utc,sleep_end_utc:night.sleep_end_utc,snapshot_sha256:null}));
  const windows=documented.map(window=>({...window,start:Date.parse(window.sleep_start_utc??""),end:Date.parse(window.sleep_end_utc??"")})).sort((a,b)=>a.start-b.start);
  if(windows.some(window=>{try{dayNumber(window.wake_date);return false;}catch{return true;}}))return false;
  if(windows.some(window=>!Number.isFinite(window.start)||!Number.isFinite(window.end)||window.end<=window.start||window.end-window.start>36*3600000))return false;
  if(windows.some(window=>(window.status==="available"||window.status==="ambiguous_sleep_window")
    &&(window.status==="ambiguous_sleep_window")!==windows.some(other=>other!==window&&window.start<other.end&&window.end>other.start)))return false;
  const ids=new Set<string>();let linked=0,points=0;
  for(let index=0;index<product.nights.length;index++){
    const night=product.nights[index],manifest=product.source_manifest.nights[index];
    const expected=new Date((dayNumber(product.args.start_date)+index)*86400000).toISOString().slice(0,10);
    if(night.wake_date!==expected||manifest.wake_date!==expected||manifest.context_revision!==night.context.revision
      ||(night.status==="not_in_export")!==(manifest.snapshot_sha256===null)||!validZone(night.actual_night_timezone)||(night.actual_night_timezone===null)!==night.quality_flags.includes("actual_timezone_unknown"))return false;
    if(night.context.status==="available" ? night.context.values===null||night.context.revision===null : night.context.values!==null||night.context.revision!==null)return false;
    if(night.context.values){
      if(!validZone(night.context.values.actual_night_timezone)||new Set(night.context.values.declared_fields).size!==night.context.values.declared_fields.length)return false;
    }
    if(associationContext){
      if(night.status==="not_in_export"&&windows.some(window=>window.wake_date===night.wake_date))return false;
      if((night.status==="available"||night.status==="ambiguous_sleep_window")
        &&windows.filter(window=>window.wake_date===night.wake_date&&window.status===night.status
          &&window.sleep_start_utc===night.sleep_start_utc&&window.sleep_end_utc===night.sleep_end_utc
          &&window.snapshot_sha256===manifest.snapshot_sha256).length!==1)return false;
    }
    const training=night.training;
    if(night.status!=="available"){
      if(training.activities.length||training.confirmed_activity_count!==null||training.uncertain_activity_count!==null
        ||training.recorded_duration_seconds!==null||training.known_recorded_duration_seconds!==null||Object.keys(night.channels).length)return false;
      continue;
    }
    const start=Date.parse(night.sleep_start_utc??""),end=Date.parse(night.sleep_end_utc??"");
    if(!Number.isFinite(start)||!Number.isFinite(end)||end<=start||end-start>36*3600000)return false;
    for(const [name,unit] of Object.entries({heart_rate:"bpm",hrv:"ms",stress:"points",body_battery:"points"}as const)){
      const channel=night.channels[name as keyof typeof night.channels];
      if(!channel||channel.unit!==unit||(channel.valid_points===0)!==(channel.median===null))return false;
      if(channel.median!==null&&(name==="heart_rate"||name==="hrv"?channel.median<=0:channel.median>100))return false;
      const quality=channel.source_quality;
      if(channel.status==="no_data"&&((associationContext&&!quality)||(quality&&(quality.channel_status!=="no_data"
        ||quality.metadata_status==="invalid"||Object.values(quality.counts??{}).some(value=>(value??0)>0)
        ||quality.observation_flags.length>0||(quality.missing_value_count??0)>0
        ||quality.quality_flags.some(flag=>!["coverage_unknown","minimum_window_not_covered","source_no_data"].includes(flag))))))return false;
      for(const revision of channel.source_revisions)dayNumber(revision.date);
      points+=channel.valid_points;
    }
    if(absent){
      if(training.status!==source.status||training.activities.length||training.confirmed_activity_count!==null
        ||training.uncertain_activity_count!==null||training.recorded_duration_seconds!==null||training.known_recorded_duration_seconds!==null)return false;
      continue;
    }
    if(training.confirmed_activity_count!==training.activities.length||training.uncertain_activity_count===null)return false;
    const expectedStatus=training.uncertain_activity_count?"partial":source.status;
    if(training.status!==expectedStatus||source.status==="no_data"&&training.activities.length)return false;
    let sum=0;
    for(const activity of training.activities){
      const activityStart=Date.parse(activity.start_utc),activityEnd=Date.parse(activity.end_utc);
      if(ids.has(activity.activity_id)||!Number.isFinite(activityStart)||!Number.isFinite(activityEnd)||activityEnd<activityStart
        ||Math.abs((activityEnd-activityStart)/1000-activity.duration_seconds)>0.001||activityEnd>start||start-activityEnd>48*3600000)return false;
      if(windows.some(window=>activityStart<window.end&&activityEnd>window.start))return false;
      if(windows.find(window=>activityEnd<=window.start&&window.start-activityEnd<=48*3600000)?.wake_date!==night.wake_date)return false;
      ids.add(activity.activity_id);linked++;sum+=activity.duration_seconds;
    }
    if(training.known_recorded_duration_seconds!==sum||training.recorded_duration_seconds!==(training.uncertain_activity_count?null:sum))return false;
  }
  return points<=product.resources.input_points&&linked+Object.values(product.unlinked_counts).reduce((sum,value)=>sum+value,0)+source.duplicates<=source.rows;
}

export class NightTrainingTools {
  constructor(private readonly env:Env){}
  private result(value:Result,id:RequestId,started:number){
    const response=()=>({content:[{type:"text"as const,text:JSON.stringify(value)}],structuredContent:value});
    const measure=()=>{for(let index=0;index<5;index++){
      value.resources.structured_bytes=encoder.encode(JSON.stringify(value)).byteLength;
      value.resources.envelope_budget_bytes=encoder.encode(JSON.stringify({jsonrpc:"2.0",id,result:response()})).byteLength+1024;
    }};
    value.resources.elapsed_ms=Math.max(0,performance.now()-started);measure();
    if(value.resources.structured_bytes>256*1024||value.resources.envelope_budget_bytes>512*1024){
      value.status="output_limit";value.nights=[];value.nights_returned=0;value.next_page_start_date=null;
      value.quality_flags.push("reduce_page_size");measure();
    }
    if(performance.now()-started>10000){value.status="budget_exhausted";value.nights=[];value.nights_returned=0;
      value.next_page_start_date=null;value.quality_flags.push("deadline_exceeded");measure();}
    return response();
  }
  registerTools(server:McpServer){
    server.registerTool("night_training_history",{
      description:"Read one fixed offline product linking recorded activity ends to the earliest following documented sleep within48hours. Returns at most31 nights per page(default7). Missing data, uncertain ends, export coverage and snapshot context remain explicit; no causal/medical score, source scan, provider request or write.",
      inputSchema:z.object({start_date:exactDate,end_date:exactDate,page_start_date:exactDate.optional(),page_size:integer.min(1).max(31).default(7)}).strict(),
      outputSchema:nightTrainingHistorySchema,annotations:PRIVATE_READ_TOOL_ANNOTATIONS,
    },async(args,context)=>{
      const started=performance.now();
      const value:Result={status:"invalid_request",method_version:METHOD,association:"temporal_only_not_causal",coverage:COVERAGE,
        window:null,page_start_date:null,next_page_start_date:null,page_size:args.page_size,source_identity:null,product_identity:null,
        activity_dataset_status:null,nights_requested:0,nights_returned:0,nights:[],unlinked_counts:null,quality_flags:[],
        resources:{r2_get_attempts:0,decoded_bytes:0,decoded_budget_bytes:0,elapsed_ms:0,structured_bytes:0,envelope_budget_bytes:0}};
      try{
        const start=dayNumber(args.start_date),end=dayNumber(args.end_date),page=dayNumber(args.page_start_date??args.start_date);
        if(end<start||end-start+1>366||page<start||page>end)throw new Error("invalid_range");
        value.window={start_date:args.start_date,end_date:args.end_date};value.page_start_date=args.page_start_date??args.start_date;value.nights_requested=end-start+1;
      }catch{value.quality_flags.push("invalid_date_range");return this.result(value,context.mcpReq.id,started);}
      try{
        check(started);
        const productArgs={start_date:args.start_date,end_date:args.end_date};
        const argsHash=await digest(productArgs);check(started);
        const end=args.end_date;
        const key=`health/night-products/v1/training/${end.slice(0,4)}/${end.slice(5,7)}/${end.slice(8,10)}/${argsHash}.json`;
        let stored:{text:string}|null;
        try{
          value.resources.r2_get_attempts++;value.resources.decoded_budget_bytes=1024*1024;
          stored=await new R2Storage(this.env.SLIPSTREAM_DATA).getR2Text([key],{stored:1024*1024,decoded:1024*1024});
        }catch(error){
          check(started);value.status=error instanceof PayloadTooLargeError?"budget_exhausted":"read_error";
          value.quality_flags.push(error instanceof PayloadTooLargeError?"product_size_limit":"product_read_error");
          return this.result(value,context.mcpReq.id,started);
        }
        check(started);
        if(!stored){value.status="not_stored";return this.result(value,context.mcpReq.id,started);}
        value.resources.decoded_bytes=encoder.encode(stored.text).byteLength;
        try{
          const product=productSchema.parse(JSON.parse(stored.text));
          const {product_identity,...core}=product;
          if(product.args.start_date!==args.start_date||product.args.end_date!==args.end_date||product.args_sha256!==argsHash
            ||!consistent(product)||product.source_identity!==await digest(product.source_manifest)||product_identity!==await digest(core))throw new Error("invalid_product");
          check(started);
          const offset=dayNumber(value.page_start_date!)-dayNumber(args.start_date);
          value.nights=product.nights.slice(offset,offset+args.page_size);value.nights_returned=value.nights.length;
          value.next_page_start_date=offset+value.nights.length<product.nights.length?product.nights[offset+value.nights.length].wake_date:null;
          value.source_identity=product.source_identity;value.product_identity=product.product_identity;
          value.activity_dataset_status=product.activity_dataset_status;value.unlinked_counts=product.unlinked_counts;value.status="available";
          if(value.nights.some(night=>night.status!=="available"||!["available","no_data"].includes(night.training.status)))value.quality_flags.push("incomplete_training_or_night_export");
          value.quality_flags.push("context_is_export_snapshot");
        }catch(error){if(error instanceof Deadline)throw error;value.status="invalid_schema";value.quality_flags.push("product_identity_or_contract_invalid");}
      }catch(error){if(!(error instanceof Deadline))throw error;value.status="budget_exhausted";value.quality_flags.push("deadline_exceeded");}
      return this.result(value,context.mcpReq.id,started);
    });
  }
}