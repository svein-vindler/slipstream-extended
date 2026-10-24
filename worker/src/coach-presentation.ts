type Json = Record<string, unknown>;

function record(value: unknown): Json {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Json : {};
}

function storedText(value: unknown): string {
  return typeof value === "string" ? value : "not_stored";
}

const nativeMeasurements = [
  "average_power", "average_step_length", "average_ground_contact_time",
  "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance",
] as const;
const nativeUnits = ["W", "mm", "ms", "mm", "%", "%"] as const;

function data(value: unknown): string {
  return JSON.stringify(value) ?? "not_stored";
}

function fields(value: unknown, names: readonly string[]): Json {
  const source = record(value), selected: Json = {};
  for (const name of names) if (source[name] !== undefined) selected[name] = source[name];
  return selected;
}

function measuredValue(value: unknown): string {
  const metric = record(value);
  const numeric = typeof metric.value === "number" && Number.isFinite(metric.value);
  const unit = typeof metric.unit === "string" ? metric.unit : undefined;
  return (numeric ? String(metric.value) : "null")
    + (unit === undefined ? "" : ` ${["W", "mm", "ms", "%"].includes(unit) ? unit : data(unit)}`);
}

/** Scalars apply to all six columns; arrays/indexed objects retain differences. */
function columnMetadata(value: unknown): Json {
  const block = record(value), result: Json = {};
  // Exact field names and calculation-policy diagnostics remain in format=full.
  for (const key of ["status", "side_semantics"]) {
    const values = nativeMeasurements.map(name => record(block[name])[key]);
    const present = values.map((item, index) => item === undefined ? -1 : index).filter(index => index >= 0);
    if (!present.length) continue;
    result[key] = present.length === nativeMeasurements.length
      ? (values.every(item => data(item) === data(values[0])) ? values[0] : values)
      : Object.fromEntries(present.map(index => [index + 1, values[index]]));
  }
  return result;
}

function tableCell(value: unknown): string {
  return data(value).replace(/\|/g, "\\|");
}

function measurement(name: string, value: unknown): string {
  const metric = record(value);
  const numeric = typeof metric.value === "number" && Number.isFinite(metric.value);
  const unit = typeof metric.unit === "string" ? metric.unit : undefined;
  const displayUnit = unit === undefined ? "" : ` ${["W", "mm", "ms", "%"].includes(unit) ? unit : data(unit)}`;
  return `  ${name}: ${numeric ? `${metric.value}${displayUnit}` : "null"}`
    + `; status: ${data(storedText(metric.status))}`
    + (!numeric && unit !== undefined ? `; unit: ${data(unit)}` : "")
    + (metric.side_semantics === undefined ? "" : `; side_semantics: ${data(metric.side_semantics)}`)
    + (metric.coverage === undefined ? "" : `; coverage: ${data(metric.coverage)}`);
}

/** Selected frozen context facts, never a fresh-data or temporal inference. */
function storedContextLines(value: unknown): string[] {
  if (value == null) return ["Stored context: not_stored"];
  const snapshot = record(value);
  if (!Object.keys(snapshot).length) return [`Stored context: ${data(value)}`];
  const lines = [`Stored context: ${data(fields(snapshot,
    ["schema_version", "snapshot_at", "reference_time_utc", "semantics"]))}`];
  for (const kind of ["sleep", "hrv"] as const) {
    const source = record(snapshot[kind]);
    if (!Object.keys(source).length) {
      lines.push(`  ${kind}: ${snapshot[kind] == null ? "not_stored" : data(snapshot[kind])}`);
      continue;
    }
    lines.push(`  ${kind}: ${data(fields(source, ["status", "source", "source_checked_at", "wake_date",
      "night_of", "local_time_source", "observation_start_utc", "observation_end_utc"]))}`);
    const metrics = record(source.metrics);
    for (const name of kind === "sleep" ? ["sleep_seconds", "sleep_score"] : ["last_night_avg_ms", "weekly_avg_ms"]) {
      lines.push(measurement(name, metrics[name]));
    }
  }
  const training = record(snapshot.training_day);
  if (!Object.keys(training).length) {
    lines.push(`  training_day: ${snapshot.training_day == null ? "not_stored" : data(snapshot.training_day)}`);
  } else {
    lines.push(`  training_day: ${data(fields(training,
      ["status", "selection_status", "selected_slot", "temporal_status", "last_attempt"]))}`);
    for (const name of ["readiness", "recovery"]) {
      const metric = record(training[name]);
      lines.push(measurement(name, metric)
        + ["observation_at", "source_checked_at", "retained_previous"]
          .filter(field => metric[field] !== undefined).map(field => `; ${field}: ${data(metric[field])}`).join(""));
    }
  }
  return lines;
}

/** Native session/lap cadence only; factor metadata without computing new values. */
function nativeCadenceLines(value: unknown): string[] {
  if (value == null) return [];
  const block = record(value), laps = record(block.laps);
  const rows = [{ scope: "session", lap: null, metrics: record(block.session) },
    ...(Array.isArray(laps.items) ? laps.items.map(value => {
      const item = record(value);
      return { scope: "lap", lap: item.lap, metrics: item };
    }) : [])];
  const lines = [`Native recorded cadence: ${data(fields(block,
    ["schema_version", "method", "association", "source_fit_sha256"]))}`,
  `Cadence laps: ${data(fields(laps, ["status", "reason"]))}`,
  "| Scope | Lap | Average (steps/min) | Maximum (steps/min) |",
  "| --- | --- | --- | --- |"];
  for (const row of rows) lines.push(`| ${[row.scope, row.lap,
    record(row.metrics.average).value, record(row.metrics.maximum).value].map(tableCell).join(" | ")} |`);
  const metadata: Json = {};
  for (const name of ["status", "unit", "source_fields", "precision", "reason"]) {
    const average = rows.map(row => record(row.metrics.average)[name]),
      maximum = rows.map(row => record(row.metrics.maximum)[name]),
      cells = rows.flatMap((_row, index) => [average[index], maximum[index]]);
    const same = (values: unknown[]) => values.every(value => data(value) === data(values[0]));
    metadata[name] = same(cells) ? cells[0] : same(average) && same(maximum)
      ? { average: average[0], maximum: maximum[0] } : { cells };
  }
  lines.push(`Cadence metadata (scalar=all; average/maximum=all rows; cells=row order average then maximum): ${data(metadata)}`);
  return lines;
}

/** One explicit row per stored record scope; no joining by coincident values. */
function recordCadenceLines(value: unknown, describe: (value: unknown) => string): string[] {
  const block = record(value), rawGroups = record(block.record_aggregates);
  const groups = ["whole_distance_halves", "main_distance_halves", "executed_intervals", "recovery_bouts", "kilometer_splits"]
    .filter(name => rawGroups[name] !== undefined).map(name => ({ name, value: record(rawGroups[name]) }));
  if (!groups.length) return [];
  const names = ["status", "reason", "method", "distance_basis", "clock_basis", "interval_policy", "distance_allocation",
    "source_tcx_sha256", "source_fields", "input_unit", "unit", "conversion", "valid_record_precision",
    "session_start_time_utc", "session_end_time_utc", "session_summary_timestamp_utc", "source_window", "source_laps", "source_windows"];
  const common: Json = {};
  for (const name of names) {
    const first = groups[0].value[name];
    if (first !== undefined && groups.every(group => data(group.value[name]) === data(first))) common[name] = first;
  }
  const lines = [`Record-derived cadence: ${data(fields(block,
    ["schema_version", "activity_id", "association", "source_fit_sha256"]))}`,
  `Shared cadence scope metadata: ${data(common)}`,
  "| Scope | Reference | Source laps (row) | Average (steps/min) |",
  "| --- | --- | --- | --- |"];
  const notes: string[] = [];
  for (const group of groups) {
    const metadata = fields(group.value, names.filter(name => common[name] === undefined));
    if (Object.keys(metadata).length) notes.push(`Cadence scope ${data(group.name)}: ${data(metadata)}`);
    for (const rawRow of Array.isArray(group.value.items) ? group.value.items : []) {
      const row = record(rawRow), metric = record(row.average_cadence);
      const reference = group.name === "executed_intervals" ? row.interval
        : group.name === "recovery_bouts" ? row.bout : group.name === "kilometer_splits" ? row.split : row.segment;
      lines.push(`| ${[group.name, reference, row.source_laps ?? null, metric.value ?? null].map(tableCell).join(" | ")} |`);
      if (group.name === "kilometer_splits") notes.push(`Cadence ${data(group.name)}/${data(reference)} geometry: ${data(fields(row,
        ["start_distance_m", "end_distance_m", "distance_m"]))}`);
      notes.push(`Cadence ${data(group.name)}/${data(reference)} metadata: ${describe(fields(metric,
        ["status", "unit", "source_fields", "coverage"]))}`);
    }
  }
  return [...lines, ...notes];
}

/** Display only already validated side rows; never infer a side or new mean. */
function groundContactSideLines(value: unknown, describe: (value: unknown) => string): string[] | null {
  const block = record(value);
  if (block.schema_version !== 2 || block.status !== "available") return null;
  const laps = record(block.laps), groups = record(block.record_aggregates);
  const rows: Array<{ scope: string; reference: unknown; sourceLaps: unknown; value: Json }> = [
    { scope: "native_session", reference: null, sourceLaps: null, value: record(block.session) },
    ...(Array.isArray(laps.items) ? laps.items.map(raw => {
      const row = record(raw);
      return { scope: "native_lap", reference: row.lap, sourceLaps: null, value: row };
    }) : [])];
  const lines = [`Declared ground-contact sides: ${data(fields(block,
    ["schema_version", "activity_id", "source_fit_sha256", "status", "reason", "declaration"]))}`,
  `Native side laps: ${data(fields(laps, ["status", "reason"]))}`,
  "Contact-balance side view:",
  "| Scope | Reference | Source laps (row) | Left (%) | Right (%) |",
  "| --- | --- | --- | --- | --- |"];
  const notes: string[] = [];
  for (const scope of ["executed_intervals", "whole_distance_halves", "main_distance_halves"]) {
    const group = record(groups[scope]);
    notes.push(`Contact side scope ${data(scope)}: ${data(fields(group, ["status", "reason", "source_metric"]))}`);
    for (const raw of Array.isArray(group.items) ? group.items : []) {
      const row = record(raw);
      rows.push({ scope, reference: scope === "executed_intervals" ? row.interval : row.segment,
        sourceLaps: row.source_laps ?? null, value: row });
    }
  }
  if (rows.some(row => row.value.both_sides_derived === true)) lines.push(
    "Derived L/R use the declared-side record mean and its complement; not two measured contact channels or a total-contact-time ratio.",
  );
  for (const row of rows) {
    lines.push(`| ${[row.scope, row.reference, row.sourceLaps,
      row.value.left_percent ?? null, row.value.right_percent ?? null].map(tableCell).join(" | ")} |`);
    notes.push(`Contact side ${data(row.scope)}/${data(row.reference)} metadata: ${describe(fields(row.value,
      ["status", "method", "recorded_side", "raw_side", "calculated_side", "both_sides_derived", "coverage"]))}`);
  }
  return [...lines, ...notes];
}

/** Readable typed plan; the unscaled original plan remains in format=full. */
function plannedTargetLines(value: unknown, structure: Json): string[] | null {
  const block = record(value);
  if (block.status !== "available" || !Array.isArray(block.items)) return null;
  const planned = uniqueRows(structure.planned_steps, "step_index");
  const lines = [`Verified planned targets: ${data(fields(block,
    ["schema_version", "method", "activity_id", "source_fit_sha256", "status", "reason"]))}`,
  "Planned structure only; not a completion claim. Original raw plan: format=full.",
  "| Step | Name | Role | Planned duration | Target | Raw target code |",
  "| --- | --- | --- | --- | --- | --- |"];
  const notes = new Map<string, unknown[]>();
  for (const raw of block.items) {
    const item = record(raw), plan = typeof item.step_index === "number" ? planned.get(item.step_index) : undefined;
    const duration = record(item.duration), target = record(item.target), span = record(target.range);
    const durationText = duration.status === "available" && typeof duration.value === "number"
      ? `${duration.value} ${duration.unit}` : null;
    const targetText = span.status === "available" ? `${target.type} ${target.mode} ${span.low}..${span.high} ${span.unit}`
      : target.mode === "open" ? "open" : "unverified";
    lines.push(`| ${[item.step_index, plan?.name ?? null, plan?.intensity ?? null,
      durationText, targetText, target.raw_value].map(tableCell).join(" | ")} |`);
    for (const [kind, detail] of [["duration", duration], ["target", target]] as const) {
      if (kind === "duration" ? duration.status === "available" : span.status === "available" || target.mode === "open") continue;
      const key = data({ kind, ...detail }), steps = notes.get(key) ?? [];
      steps.push(item.step_index); notes.set(key, steps);
    }
  }
  for (const [detail, steps] of notes) lines.push(`Unverified plan fields for steps ${data(steps)}: ${detail}`);
  return lines;
}

/** Stored group identities and existing record references, without new aggregates. */
function logicalGroupLines(value: unknown): string[] {
  if (value == null) return [];
  const group = record(value);
  const lines = [`Logical groups: ${data(fields(group,
    ["schema_version", "status", "reason", "method", "clock_basis", "grouping_basis", "occurrence_basis",
      "source_fit_sha256", "unassigned_laps", "source_coverage"]))}`];
  if ((group.status !== "available" && !(group.schema_version === 2 && group.status === "partial"))
    || !Array.isArray(group.items) || !group.items.length) return lines;
  lines.push(
    "| Group | Role | Step | Observed occurrence (derived) | Planned repeat iteration | Source laps | Source messages | Start UTC | End UTC | Record interval |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  );
  const absentReferences = new Set<string>();
  const timingNotes = new Map<string, unknown[]>();
  for (const value of group.items) {
    const item = record(value), reference = record(item.record_aggregate);
    const timing = group.schema_version === 2 ? record(item.timing) : item;
    const values = [item.bout, item.role, item.workout_step_index, item.observed_step_occurrence,
      item.planned_repeat_iteration, item.source_laps, item.source_message_indices,
      timing.start_time_utc, timing.end_time_utc, reference.status === "available" ? reference.interval : null];
    lines.push(`| ${values.map(tableCell).join(" | ")} |`);
    if (group.schema_version === 2) {
      const note = data(fields(timing, ["status", "basis", "reason"]));
      const bouts = timingNotes.get(note) ?? [];
      bouts.push(item.bout); timingNotes.set(note, bouts);
    }
    if (reference.status !== "available") absentReferences.add(data(fields(reference, ["status", "reason"])));
  }
  for (const [note, bouts] of timingNotes) lines.push(`Group timing ${data(bouts)}: ${note}`);
  for (const reference of absentReferences) lines.push(`Record references: null => ${reference}`);
  return lines;
}

/** Duplicate or absent identities never select a row. No positional matching. */
function uniqueRows(value: unknown, field: string): Map<number, Json | null> {
  const rows = new Map<number, Json | null>();
  for (const item of Array.isArray(value) ? value : []) {
    const row = record(item), key = row[field];
    if (typeof key === "number" && Number.isSafeInteger(key) && key >= 0) {
      rows.set(key, rows.has(key) ? null : row);
    }
  }
  return rows;
}

/** A neutral excerpt of an analysis already verified by the current selector. No I/O or mutation. */
export function coachPresentation(analysis: Json) {
  const sourceId = storedText(analysis.analysis_id);
  const sourceVersion = storedText(analysis.analyzer_version);
  const lines = [
    `Analysis: ${data(sourceId)}`,
    `Analyzer: ${data(sourceVersion)}`,
    "Scope: selected_fields. Full data: format=full.",
    `Activity: ${data({ activity_id: analysis.activity_id, ...record(analysis.activity) })}`,
    "Summary:",
  ];
  // Request-local exact-content references consolidate metadata, not segments
  // or measurements. Different coverage/status/basis objects stay distinct.
  const details = new Map<string, string>();
  function describe(value: unknown): string {
    const serialized = data(value);
    let id = details.get(serialized);
    if (id === undefined) { id = `D${details.size + 1}`; details.set(serialized, id); }
    return id;
  }
  function measurementDetails(block: Json): string {
    const coverage: Json = {};
    nativeMeasurements.forEach((name, index) => {
      const metric = record(block[name]);
      if (metric.coverage !== undefined) coverage[index + 1] = describe(metric.coverage);
    });
    const refs = Object.values(coverage);
    const coverageText = refs.length === nativeMeasurements.length
      ? (refs.every(value => value === refs[0]) ? refs[0] : data(refs)) : data(coverage);
    return `metadata: ${describe(columnMetadata(block))}` + (refs.length ? `; coverage: ${coverageText}` : "");
  }
  function measurementGroup(title: string, value: unknown, prefix: string) {
    const group = record(value);
    lines.push(`${title}: ${data(storedText(group.status))}`
      + (group.reason == null ? "" : `; reason: ${data(group.reason)}`));
    const basis = fields(group, ["clock_basis"]);
    if (Object.keys(basis).length || group.distance_basis !== undefined) lines.push(
      `  basis: ${Object.keys(basis).length ? describe(basis) : "not_stored"}`
      + (group.distance_basis === undefined ? "" : `; distance_basis: ${data(group.distance_basis)}`));
    for (const value of Array.isArray(group.items) ? group.items : []) {
      const item = record(value);
      const label = prefix === "executed_intervals" ? `interval ${data(item.interval)}`
        : ["first", "second"].includes(storedText(item.segment)) ? storedText(item.segment) : data(item.segment);
      lines.push(`${prefix} ${label}:`, `  scope: ${data(fields(item,
        ["source_laps", "workout_step_index", "start_time_utc", "end_time_utc", "start_distance_m", "end_distance_m", "distance_m"]))}`,
      `  measurements: ${nativeMeasurements.map(name => measuredValue(item[name])).join("; ")}; ${measurementDetails(item)}`);
    }
    if (prefix !== "executed_intervals" && group.status !== "available") {
      const status = storedText(group.status);
      const label = ["missing", "unavailable", "not_stored"].includes(status) ? status : data(status);
      for (const half of ["first", "second"]) lines.push(`${prefix} ${half}: ${label}`
        + (group.reason == null ? "" : `; reason: ${data(group.reason)}`));
    }
  }
  const summary = record(analysis.summary);
  for (const field of ["elapsed_seconds", "moving_seconds", "distance_m", "average_speed_mps",
    "average_heart_rate_bpm", "maximum_heart_rate_bpm", "trackpoint_count", "sampled_trackpoint_count"]) {
    lines.push(`  ${field}: ${data(summary[field])}`);
  }
  lines.push(`Profile/references: ${analysis.profile == null ? "not_stored" : data(fields(analysis.profile,
    ["profile_id", "name", "effective_from", "zones", "references"]))}`);
  lines.push(`User context: ${analysis.user_context == null ? "not_stored" : data(analysis.user_context)}`);
  const effect = record(analysis.training_effect);
  lines.push(`Training Effect: ${analysis.training_effect == null ? "not_stored" : data(fields(effect,
    ["source", "association", "local_date", "measured_at_utc"]))}`);
  if (analysis.training_effect != null) {
    lines.push(`  scale: ${data(effect.scale)}`, measurement("aerobic", effect.aerobic),
      measurement("anaerobic", effect.anaerobic));
  }
  lines.push(...storedContextLines(analysis.stored_context_snapshot),
    `Sections: ${data(Array.isArray(analysis.sections) ? analysis.sections.map(section => fields(section,
      ["section", "laps", "elapsed_seconds", "distance_m", "average_heart_rate_bpm", "heart_rate_coverage"])) : analysis.sections)}`,
    `Time basis: ${data(analysis.time_basis)}`,
    `Heart rate: ${data(analysis.heart_rate)}`);
  const tcxHalves = record(analysis.distance_halves);
  const tcxFirst = record(tcxHalves.first), tcxSecond = record(tcxHalves.second);
  const tcxColumns = Object.keys(tcxFirst);
  // A labelled column layout only factors identical field names. Both source
  // rows and every value remain separate, including null and genuine zero.
  if (Object.keys(tcxHalves).length === 2 && tcxColumns.length
    && Object.keys(tcxSecond).length === tcxColumns.length && tcxColumns.every(key => key in tcxSecond)) {
    lines.push("TCX distance halves:", `  columns: ${data(tcxColumns)}`,
      `  first: ${data(tcxColumns.map(key => tcxFirst[key]))}`,
      `  second: ${data(tcxColumns.map(key => tcxSecond[key]))}`);
  } else lines.push(`TCX distance halves: ${data(analysis.distance_halves)}`);
  const laps = record(record(analysis.recorded_running_metrics).laps);
  const native = uniqueRows(laps.items, "lap");
  const binding = record(analysis.native_lap_workout_binding);
  const references = uniqueRows(binding.items, "lap");
  const messages = uniqueRows(binding.items, "source_message_index");
  const structure = record(analysis.workout_structure);
  const planned = uniqueRows(structure.planned_steps, "step_index");
  const executed = uniqueRows(structure.executed_laps, "lap");
  const executedRows = Array.isArray(structure.executed_laps) ? structure.executed_laps : [];
  const displayedExecution = new Set<Json>();
  const executionColumns = [...new Set(executedRows.flatMap(value => Object.keys(record(value))))]
    .filter(name => !["lap", "workout_step_index", "elapsed_seconds"].includes(name)
      && executedRows.some(value => record(value)[name] != null));
  lines.push(...nativeCadenceLines(analysis.recorded_cadence));
  lines.push("Recorded session measurements:");
  for (const field of nativeMeasurements) {
    lines.push(measurement(field, record(analysis.recorded_running_metrics)[field]));
  }
  const sides = record(analysis.ground_contact_sides);
  lines.push(...(groundContactSideLines(analysis.ground_contact_sides, describe)
    ?? [`Declared ground-contact sides: ${analysis.ground_contact_sides == null ? "not_stored"
      : data(sides.status === "available" ? sides : fields(sides, ["status", "reason"]))}`]));
  lines.push("Measurement order: power, step length, GCT, VO, VR, balance. Metadata arrays/keys=columns; scalar=all.");
  measurementGroup("Recorded FIT distance halves", record(analysis.recorded_running_metrics).distance_halves, "fit_distance_halves");
  lines.push(...(plannedTargetLines(analysis.workout_targets, structure)
    ?? [`Workout plan: ${data(structure.planned_steps)}`]));
  lines.push(...logicalGroupLines(analysis.logical_bouts));
  lines.push(`Native recorded laps: ${JSON.stringify(storedText(laps.status))}`);
  if (laps.reason !== null && laps.reason !== undefined) {
    lines.push(`Reason: ${JSON.stringify(laps.reason)}`);
  }
  const nativeRows = Array.isArray(laps.items) ? laps.items : [];
  const lapNotes: string[] = [];
  if (nativeRows.length) lines.push(
    "| Lap | Source message | Step | Distance (m) | Executed elapsed (s) | Power (W) | Step length (mm) | GCT (ms) | VO (mm) | VR (%) | GCT balance (%) |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  );
  for (const value of nativeRows) {
    const lap = record(value);
    const reference = typeof lap.lap === "number" ? references.get(lap.lap) : undefined;
    const execution = typeof lap.lap === "number" ? executed.get(lap.lap) : undefined;
    const step = typeof reference?.workout_step_index === "number"
      ? planned.get(reference.workout_step_index) : undefined;
    const verified = binding.method === "fit_lap_wkt_step_index" && reference?.status === "bound"
      && typeof lap.lap === "number" && native.get(lap.lap) === lap
      && typeof reference.source_message_index === "number" && messages.get(reference.source_message_index) === reference
      && step && execution && execution.workout_step_index === reference.workout_step_index;
    const values: unknown[] = [lap.lap, reference?.source_message_index,
      verified ? reference?.workout_step_index : undefined, lap.distance_m, verified ? execution?.elapsed_seconds : undefined];
    nativeMeasurements.forEach((name, index) => {
      const metric = record(lap[name]);
      values.push(metric.unit === nativeUnits[index]
        ? (typeof metric.value === "number" && Number.isFinite(metric.value) ? metric.value : null)
        : measuredValue(metric));
    });
    lines.push(`| ${values.map(tableCell).join(" | ")} |`);
    if (verified && execution) displayedExecution.add(execution);
    lapNotes.push(`Native lap ${data(lap.lap)}: ${measurementDetails(lap)}; recorded: ${data(fields(lap, ["start_time_utc", "end_time_utc"]))}`
      + `; native binding: ${data(reference?.status ?? "not_stored")}; link: ${verified ? "verified" : "unverified"}`
      + (reference?.reason == null ? "" : `; reason: ${data(reference.reason)}`)
      + (verified && execution ? `; execution: ${data(executionColumns.map(name => execution[name] === undefined
        ? { status: "not_stored" } : execution[name]))}` : ""));
  }
  if (displayedExecution.size) lines.push(`Verified execution detail columns: ${data(executionColumns)}`);
  lines.push(...lapNotes);
  const unlinked = executedRows.filter(value => !displayedExecution.has(record(value)));
  if (unlinked.length) lines.push(`Unlinked executed laps (separate source): ${data(unlinked)}`);
  const segments = record(analysis.segment_running_metrics);
  for (const field of ["executed_intervals", "whole_distance_halves", "main_distance_halves"] as const) {
    measurementGroup(`Record-derived ${field}`, segments[field], field);
  }
  lines.push(...recordCadenceLines(analysis.recorded_cadence, describe));
  lines.push("Coverage/warnings:");
  for (const [serialized, id] of details) lines.push(`  ${id}: ${serialized}`);
  return {
    version: "1.0.0" as const,
    format: "coach_text" as const,
    source_analysis_id: sourceId,
    source_analyzer_version: sourceVersion,
    scope: "selected_fields" as const,
    text: lines.join("\n"),
    full_data_hint: "format=full" as const,
  };
}
