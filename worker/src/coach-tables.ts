/** Selected stored rows only. No FIT reads, record reaggregation or history mutation. */
type Json = Record<string, unknown>;
function object(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function rows(value: unknown): Json[] { return Array.isArray(value) ? value.map(object) : []; }
function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
function cell(value: unknown): string { return JSON.stringify(value ?? null).replace(/\|/g, "\\|"); }
function unique(items: Json[], key: string, id: unknown): Json {
  const matched = items.filter(item => item[key] === id);
  return matched.length === 1 ? matched[0] : {};
}
function measurement(value: unknown, unit: string): Json {
  const metric = object(value);
  return metric.status === "available" && metric.unit === unit && number(metric.value) !== null
    ? metric : { status: "unavailable", value: null, unit, coverage: metric.coverage ?? null };
}
function partition(row: Json, start: number, distance: number | null): boolean {
  return distance !== null && distance > 0 && number(row.start_distance_m) === start
    && number(row.end_distance_m) !== null && number(row.distance_m) !== null
    && Math.abs(Number(row.end_distance_m) - start - distance) <= 0.001
    && Math.abs(Number(row.distance_m) - distance) <= 0.001;
}
function balanceCell(metric: Json, side: Json): Json {
  const rawSide = side.recorded_side ?? side.raw_side;
  const known = side.status === "available" && ["left", "right"].includes(String(rawSide))
    && side[`${rawSide}_percent`] === metric.value;
  return { value: metric.value, side_semantics: known ? rawSide : "unknown",
    ...(known ? { left_percent: side.left_percent, right_percent: side.right_percent,
      calculated_side: side.calculated_side } : {}) };
}

/** A display guard, not a physiological validation or a replacement statistic. */
export function decouplingInterpretation(analysis: Json) {
  const value = number(object(analysis.heart_rate).aerobic_decoupling_percent);
  const structured = object(analysis.workout_structure).has_structured_workout === true;
  const halves = object(analysis.distance_halves), first = object(halves.first), second = object(halves.second);
  const d1 = number(first.distance_m), d2 = number(second.distance_m), total = number(object(analysis.summary).distance_m),
    e1 = number(first.elevation_change_m), e2 = number(second.elevation_change_m);
  const terrain = d1 !== null && d2 !== null && total !== null && d1 > 0 && d2 > 0
    && Math.abs(d1 - d2) <= 0.001 && Math.abs(d1 + d2 - total) <= 0.001 && e1 !== null && e2 !== null
    ? Math.abs(e1 / d1 - e2 / d2) * 100 : null;
  // Conservative UI policy, explicitly not a clinically validated threshold.
  const threshold = 1, imbalance = terrain !== null && terrain >= threshold;
  const reasons = [...(structured ? ["structured_workout"] : []),
    ...(imbalance ? ["terrain_net_grade_imbalance"] : terrain === null ? ["terrain_not_assessed"] : [])];
  return { aerobic_decoupling_percent: value,
    interpretability: value === null ? "unavailable" : structured || imbalance ? "low" : terrain === null ? "unknown" : "not_flagged",
    reasons, net_grade_difference_percentage_points: terrain,
    display_threshold_percentage_points: threshold,
    terrain_scope: "net_grade_only_not_total_climbing_or_conditions",
    semantics: "display_guard_only_not_physiological_validation" };
}

export function kilometerTable(analysis: Json, sideView: unknown, describe: (value: unknown) => string): string[] {
  const source = rows(analysis.kilometer_splits);
  if (!source.length) return ["Kilometer table: not_stored"];
  const recorded = object(object(analysis.recorded_running_metrics).kilometer_splits);
  const cadence = object(object(object(analysis.recorded_cadence).record_aggregates).kilometer_splits);
  const sideGroup = object(object(object(sideView).distance_aggregates).kilometer_splits);
  const lines = ["Kilometer table:",
    "| Km | Pace (s/km) | HR avg/max (bpm) | Ascent/descent (m) | Cadence (steps/min) | Step length (mm) | GCT (ms) | VR (%) | GCT balance (%) |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"];
  const notes: string[] = [];
  let start = 0;
  for (const [index, row] of source.entries()) {
    const split = index + 1, distance = number(row.distance_m);
    const measured = unique(rows(recorded.items), "split", split);
    const cadenceRow = unique(rows(cadence.items), "split", split);
    const metrics = recorded.status === "available" && row.split === split && partition(measured, start, distance) ? measured : {};
    const cad = measurement(cadence.status === "available" && row.split === split && partition(cadenceRow, start, distance)
      ? cadenceRow.average_cadence : null, "steps_per_minute");
    const step = measurement(metrics.average_step_length, "mm"), gct = measurement(metrics.average_ground_contact_time, "ms"),
      vr = measurement(metrics.average_vertical_ratio, "%"), balance = measurement(metrics.average_ground_contact_balance, "%");
    const side = unique(rows(sideGroup.items), "split", split);
    const balanceValue = balanceCell(balance, partition(side, start, distance) && side.raw_percent === balance.value ? side : {});
    lines.push(`| ${[split, number(row.pace_seconds_per_km),
      [number(row.average_heart_rate_bpm), number(row.maximum_heart_rate_bpm)],
      [number(row.ascent_m), number(row.descent_m)], cad.value, step.value, gct.value, vr.value, balanceValue].map(cell).join(" | ")} |`);
    notes.push(`Km ${split}: ${JSON.stringify({ distance_m: distance, partial: row.partial ?? null,
      net_elevation_change_m: number(row.elevation_change_m),
      coverage: Object.fromEntries(Object.entries({ cadence: cad, step_length: step, gct, vertical_ratio: vr, balance })
        .map(([name, metric]) => [name, describe({ status: metric.status, coverage: metric.coverage ?? null })])) })}`);
    start = distance !== null && distance > 0 ? start + distance : NaN;
  }
  return [...lines, ...notes, "Ascent/descent null means not stored; net elevation change is not ascent/descent."];
}

export function logicalRepTable(analysis: Json, sideView: unknown, describe: (value: unknown) => string): string[] {
  const logical = object(analysis.logical_bouts);
  if (!["available", "partial"].includes(String(logical.status))) {
    return [`Logical rep table: ${cell({ status: logical.status ?? "not_stored", reason: logical.reason ?? null })}`];
  }
  const execution = rows(object(analysis.workout_structure).executed_laps);
  const active = object(object(analysis.segment_running_metrics).executed_intervals);
  const cadenceGroups = object(object(analysis.recorded_cadence).record_aggregates);
  const sideGroups = object(object(sideView).record_aggregates);
  const lines = ["Logical rep table (observed groups, not planned completion):",
    "| Rep | Role | Duration (s) | Distance (m) / pace (s/km) | HR avg/max (bpm) | Cadence (steps/min) | Step length (mm) | GCT (ms) | VR (%) | GCT balance (%) |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"];
  const notes: string[] = [];
  let rep = 0;
  for (const bout of rows(logical.items).filter(row => ["work", "recovery"].includes(String(row.role)))) {
    if (bout.role === "work") rep++;
    const sources = Array.isArray(bout.source_laps) ? bout.source_laps : [];
    const timing = logical.schema_version === 1 ? bout : object(bout.timing);
    const selected = sources.map(lap => unique(execution, "lap", lap));
    const timed = (logical.schema_version === 1 || timing.status === "available") && selected.length > 0
      && selected.every(row => number(row.elapsed_seconds) !== null && Number(row.elapsed_seconds) > 0);
    const duration = timed ? selected.reduce((total, row) => total + Number(row.elapsed_seconds), 0) : null;
    const distance = selected.length && selected.every(row => number(row.distance_m) !== null && Number(row.distance_m) >= 0)
      ? selected.reduce((total, row) => total + Number(row.distance_m), 0) : null;
    const avgHr = duration !== null && selected.every(row => number(row.average_heart_rate_bpm) !== null)
      ? selected.reduce((total, row) => total + Number(row.average_heart_rate_bpm) * Number(row.elapsed_seconds), 0) / duration : null;
    const maxHr = selected.length && selected.every(row => number(row.maximum_heart_rate_bpm) !== null)
      ? Math.max(...selected.map(row => Number(row.maximum_heart_rate_bpm))) : null;
    const reference = object(bout.record_aggregate);
    const linked = reference.status === "available" && active.status === "available"
      ? unique(rows(active.items), "interval", reference.interval) : {};
    const same = (row: Json) => JSON.stringify(row.source_laps) === JSON.stringify(sources)
      && row.workout_step_index === bout.workout_step_index && row.start_time_utc === timing.start_time_utc
      && row.end_time_utc === timing.end_time_utc;
    const recovery = object(analysis.recovery_running_metrics);
    const recoveryRow = recovery.status === "available" ? unique(rows(recovery.items), "bout", bout.bout) : {};
    const metrics = timed && same(linked) ? linked
      : timed && bout.role === "recovery" && same(recoveryRow) ? recoveryRow : {};
    const scope = bout.role === "work" ? "executed_intervals" : "recovery_bouts";
    const cadenceGroup = object(cadenceGroups[scope]);
    const matchingCadence = rows(cadenceGroup.items).filter(same);
    const cad = measurement(timed && cadenceGroup.status === "available" && matchingCadence.length === 1
      ? matchingCadence[0].average_cadence : null, "steps_per_minute");
    const step = measurement(metrics.average_step_length, "mm"), gct = measurement(metrics.average_ground_contact_time, "ms"),
      vr = measurement(metrics.average_vertical_ratio, "%"), balance = measurement(metrics.average_ground_contact_balance, "%");
    const sides = object(sideGroups[scope]);
    const side = unique(rows(sides.items), scope === "executed_intervals" ? "interval" : "bout",
      scope === "executed_intervals" ? reference.interval : bout.bout);
    lines.push(`| ${[rep || null, bout.role, duration, [distance, duration !== null && distance !== null && distance > 0
      ? duration * 1000 / distance : null], [avgHr, maxHr], cad.value, step.value, gct.value, vr.value,
    balanceCell(balance, JSON.stringify(side.source_laps) === JSON.stringify(sources) ? side : {})].map(cell).join(" | ")} |`);
    notes.push(`Logical rep ${rep || "unknown"}/${bout.role}: ${JSON.stringify({ source_laps: sources,
      observed_step_occurrence: bout.observed_step_occurrence ?? null,
      planned_repeat_iteration: bout.planned_repeat_iteration ?? null, completion_status: "unknown",
      hr_method: "elapsed_weighted_native_lap_means_complete_only",
      coverage: Object.fromEntries(Object.entries({ cadence: cad, step_length: step, gct, vertical_ratio: vr, balance })
        .map(([name, metric]) => [name, describe({ status: metric.status, coverage: metric.coverage ?? null })])) })}`);
  }
  return [...lines, ...notes, `Logical source coverage: ${cell({ status: logical.status, unassigned_laps: logical.unassigned_laps })}`,
    "Rep numbers are observed work order; repeat iteration/completed/shortened/skipped are unknown without explicit source proof."];
}
