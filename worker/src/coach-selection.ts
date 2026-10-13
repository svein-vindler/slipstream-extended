/** Shared current-selection contract; immutable object timestamps are legacy only. */
type Json = Record<string, unknown>;
export type CoachSelection =
  | { status: "available"; analysis: Json }
  | { status: "absent" | "unverified" };
type ReadJson = (keys: string[], limits?: { stored: number; decoded: number }) =>
  Promise<{ key: string; data: unknown; uploaded?: Date } | null>;
const POINTER_LIMITS = { stored: 64 * 1024, decoded: 64 * 1024 };
function record(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function scopedKey(key: string, prefix: string): boolean {
  return key.startsWith(prefix) && /^[a-zA-Z0-9_-]{1,128}\.json(?:\.gz)?$/.test(key.slice(prefix.length));
}

function uploadTime(value: Date | undefined): number | null {
  return value instanceof Date && Number.isFinite(value.getTime()) ? value.getTime() : null;
}
function latestLegacy(versions: R2Object[], prefix: string):
  { status: "selected"; object: R2Object } | { status: "absent" | "unverified" } {
  if (!versions.length) return { status: "absent" };
  const scoped = versions.filter(object => scopedKey(object.key, prefix));
  // Missing clocks and object-key tie-breakers cannot prove a current selection.
  if (!scoped.length || scoped.some(object => uploadTime(object.uploaded) === null)) return { status: "unverified" };
  scoped.sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime());
  if (scoped.some(object => object.key !== scoped[0].key
    && object.uploaded.getTime() === scoped[0].uploaded.getTime())) return { status: "unverified" };
  return { status: "selected", object: scoped[0] };
}

export async function readCurrentCoachInput(options: {
  canonicalPrefix: string; activityId: string; date?: string;
  read: ReadJson; versions: () => Promise<R2Object[]>;
  allowLegacy?: boolean; pointerUploaded?: Date; acceptPointer?: (pointer: Json) => boolean;
}): Promise<CoachSelection> {
  const { canonicalPrefix: prefix, activityId, read } = options;
  if (!prefix) return { status: "unverified" };
  const pointerKey = prefix.slice(0, -"canonical/".length) + "latest-ready.json";
  const storedPointer = await read([pointerKey], POINTER_LIMITS);
  let key: string | undefined;
  let strictIdentity = false;
  if (storedPointer) {
    const pointer = record(storedPointer.data);
    key = typeof pointer.analysis_key === "string" ? pointer.analysis_key : undefined;
    if (pointer.schema_version !== 1 || !key || !scopedKey(key, prefix)
      || (pointer.selection_version !== undefined && pointer.selection_version !== 1)
      || (options.acceptPointer && !options.acceptPointer(pointer))) return { status: "unverified" };
    strictIdentity = pointer.selection_version === 1;
    if (strictIdentity && !/^[0-9a-f]{24}\.json$/.test(key.slice(prefix.length))) return { status: "unverified" };
    if (!strictIdentity) {
      // Old bulk rewrote canonical objects; old targeted writers advanced the
      // pointer. Respect whichever legacy commit was newer. Marked selections
      // never use these upload clocks, including a return to an older A.
      const versions = await options.versions();
      // A broken original reference cannot be replaced by other legacy history.
      if (!versions.some(object => object.key === key)) return { status: "unverified" };
      const legacy = latestLegacy(versions, prefix);
      if (legacy.status === "unverified") return legacy;
      if (legacy.status === "selected" && legacy.object.key !== key) {
        const pointerTime = uploadTime(storedPointer.uploaded ?? options.pointerUploaded);
        const newestTime = legacy.object.uploaded.getTime();
        if (pointerTime === null || newestTime === pointerTime) return { status: "unverified" };
        if (newestTime > pointerTime) {
          if (options.allowLegacy === false) return { status: "unverified" };
          key = legacy.object.key;
        }
      }
    }
  } else {
    // Only a missing pointer permits legacy discovery. Corrupt/provider errors
    // propagate, and an invalid/dangling pointer never silently selects history.
    if (options.allowLegacy === false) return { status: "unverified" };
    const legacy = latestLegacy(await options.versions(), prefix);
    if (legacy.status !== "selected") return legacy;
    key = legacy.object.key;
  }
  if (!key) return { status: "unverified" };
  const stored = await read([key]);
  const analysis = record(stored?.data);
  if (analysis.schema_version !== 1 || String(analysis.activity_id) !== activityId
    || typeof analysis.analysis_id !== "string"
    || (options.date && record(analysis.activity).date !== options.date)
    || (strictIdentity && analysis.analysis_id !== key.slice(prefix.length, -5))) return { status: "unverified" };
  return { status: "available", analysis };
}
