import { R2Storage } from "./r2-storage";
/** Current explicit user context is independent of source/analysis freshness. */
import { readNightUserContext, type NightContextReadHooks, type NightUserContextRead } from "./night-user-context";
import { nightContext, validatedHealthTimezone, type NightFormatting } from "./night-context";

type Row = Record<string, unknown>;
function record(value: unknown): Row | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : null;
}

export function applyNightUserContext(env: Env, wakeDate: string, value: Row, current: NightUserContextRead, formatting?: NightFormatting): Row {
  const nested = record(value.night_context);
  const prior = nested ?? value;
  const originalSource = record(prior.source_time);
  const selectedSource = record(prior.night_context_source_time);
  const source = selectedSource ?? originalSource ?? prior;
  const start = source.sleep_start_gmt ?? value.sleep_start_utc;
  const end = source.sleep_end_gmt ?? value.sleep_end_utc;
  if (start === undefined && end === undefined) return { ...value, ...current };
  const user = current.user_context;
  const declared = user && record(user)?.declared_fields;
  const zoneWasDeclared = Array.isArray(declared) && declared.includes("actual_night_timezone");
  const actual = current.view.context_status === "unavailable" ? null
    : zoneWasDeclared || typeof user?.actual_night_timezone === "string"
      ? user?.actual_night_timezone ?? null
      : typeof prior.actual_night_timezone === "string" ? prior.actual_night_timezone : null;
  const context = nightContext(wakeDate, start, end, validatedHealthTimezone(env.HEALTH_TIMEZONE, formatting),
    source.sleep_start_garmin_local, source.sleep_end_garmin_local, actual, formatting);
  // Display follows the selected sleep clock, while the HRV's own raw clocks
  // remain separately available even after a user timezone override.
  const projected: Row = {
    ...context,
    source_time: originalSource ? nightContext(wakeDate, originalSource.sleep_start_gmt, originalSource.sleep_end_gmt,
      null, originalSource.sleep_start_garmin_local, originalSource.sleep_end_garmin_local).source_time : context.source_time,
    ...(selectedSource ? { night_context_source_time: context.source_time } : {}),
  };
  const replacedZoneFlags = new Set(["actual_timezone_unknown",
    "source_local_conflicts_actual_timezone", "user_context_unavailable", "user_cleared_actual_timezone"]);
  if (zoneWasDeclared) replacedZoneFlags.add("invalid_actual_night_timezone");
  const priorFlags = Array.isArray(prior.time_quality_flags)
    ? prior.time_quality_flags.filter((flag): flag is string => typeof flag === "string" && !replacedZoneFlags.has(flag)) : [];
  context.time_quality_flags = [...new Set([...priorFlags, ...context.time_quality_flags,
    ...(current.view.context_status === "unavailable" ? ["user_context_unavailable"] : []),
    ...(zoneWasDeclared && actual === null ? ["user_cleared_actual_timezone"] : []),
  ])];
  projected.time_quality_flags = context.time_quality_flags;
  const signalFlags = Array.isArray(value.quality_flags)
    ? value.quality_flags.filter((flag): flag is string => typeof flag === "string" && !replacedZoneFlags.has(flag)) : [];
  return nested ? { ...value, ...current, quality_flags: [...new Set([...signalFlags, ...context.time_quality_flags])], night_context: projected, night_of: context.night_of,
    timezone: context.timezone, actual_night_timezone: context.actual_night_timezone }
    : { ...value, ...projected, ...current };
}

export async function readCurrentNightContext(env: Env, wakeDate: string,
  hooks: NightContextReadHooks = {}) {
  const resources = { r2_get_attempts: 0, r2_list_attempts: 0 };
  const storage = new R2Storage(env.SLIPSTREAM_DATA);
  const current = await readNightUserContext(env, wakeDate, {
    beforeList: () => { resources.r2_list_attempts++; hooks.beforeList?.(); },
    readJson: (keys, limits) => {
      // The context reader resolves one exact selected event key; no aliases.
      resources.r2_get_attempts++;
      return hooks.readJson ? hooks.readJson(keys, limits) : storage.getR2Json(keys, limits);
    },
    onError: hooks.onError,
  });
  return { ...current, user_context_resources: resources };
}

export async function currentNightView(env: Env, wakeDate: string, value: Row,
  hooks: NightContextReadHooks = {}): Promise<Row> {
  const current = await readCurrentNightContext(env, wakeDate, hooks);
  return applyNightUserContext(env, wakeDate, value, current);
}
