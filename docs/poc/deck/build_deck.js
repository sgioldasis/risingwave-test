// Builds REVERSE_ETL_PRESENTATION.pptx. Run from docs/poc/deck: node build_deck.js
const pptxgen = require("pptxgenjs");
const SKILL = "/Users/s.gioldasis-si/Library/Application Support/Claude/local-agent-mode-sessions/skills-plugin/7f3a4412-a9fe-49e6-9ac2-951a3547ae53/46ad2bcc-a4be-4861-8072-6b29683651d9/skills/pptx";
const { applyTheme } = require(SKILL + "/scripts/apply_theme.js");

const INK = "0F1B2D", PANEL = "1B2B45", TEAL = "14B8A6", AMBER = "F59E0B", ROSE = "E5484D";
const PAPER = "FFFFFF", MIST = "EEF3F8", TEXT = "1F2A3C", MUTED = "5B6B80", SOFT = "A9B8CC";
const HEAD = "Cambria", BODY = "Calibri";
const THEME = { name: "Reverse ETL", headFontFace: HEAD, bodyFontFace: BODY,
  colors: { dk1: TEXT, lt1: PAPER, dk2: INK, lt2: MIST, accent1: TEAL, accent2: AMBER, accent3: ROSE,
            accent4: PANEL, accent5: MUTED, accent6: SOFT, hlink: TEAL, folHlink: MUTED } };

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9"; // 10 x 5.625
pres.theme = { headFontFace: HEAD, bodyFontFace: BODY };
pres.title = "Reverse ETL: from business case to a released zero-DBU component";

const T = (s, text, o) => s.addText(text, Object.assign({ isTextBox: true, fontFace: BODY, margin: 0, valign: "top" }, o));

function content(title, sub, notes) {
  const s = pres.addSlide(); s.background = { color: PAPER };
  T(s, title, { x: 0.5, y: 0.35, w: 9, h: 0.6, fontFace: HEAD, fontSize: 30, bold: true, color: INK, valign: "middle" });
  if (sub) T(s, sub, { x: 0.5, y: 0.98, w: 9, h: 0.35, fontSize: 15, color: MUTED });
  if (notes) s.addNotes(notes);
  return s;
}
function badge(s, n, x, y, d = 0.4, fill = TEAL, col = INK) {
  s.addShape(pres.shapes.OVAL, { x, y, w: d, h: d, fill: { color: fill }, line: { color: fill } });
  T(s, String(n), { x, y, w: d, h: d, align: "center", valign: "middle", fontSize: 14, bold: true, color: col });
}
function card(s, x, y, w, h, fill = MIST) {
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.08, fill: { color: fill }, line: { color: fill } });
}
function box(s, x, y, w, h, label, sub, fill, col) {
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.08, fill: { color: fill }, line: { color: fill } });
  const runs = [{ text: label, options: { bold: true, fontSize: 14, color: col, breakLine: !!sub } }];
  if (sub) runs.push({ text: sub, options: { fontSize: 11, color: col } });
  T(s, runs, { x: x + 0.1, y, w: w - 0.2, h, align: "center", valign: "middle" });
}
function arrow(s, x, y, w, color = MUTED) { s.addShape(pres.shapes.LINE, { x, y, w, h: 0, line: { color, width: 2, endArrowType: "triangle" } }); }
function varrow(s, x, y, h, color = MUTED) { s.addShape(pres.shapes.LINE, { x, y, w: 0, h, line: { color, width: 2, endArrowType: "triangle" } }); }
const bullets = (arr) => arr.map((t, i, a) => ({ text: t, options: { bullet: true, breakLine: i < a.length - 1 } }));

// 1 Title
{
  const s = pres.addSlide(); s.background = { color: INK };
  T(s, "REVERSE ETL  ·  EPIC APR-233  ·  COMPONENT APR-302", { x: 0.7, y: 0.9, w: 8.6, h: 0.3, fontSize: 12, color: TEAL, bold: true, charSpacing: 3 });
  T(s, "From warehouse table to Kafka topic, at zero DBU", { x: 0.7, y: 1.4, w: 8.2, h: 1.6, fontFace: HEAD, fontSize: 40, bold: true, color: PAPER });
  T(s, "The business case, the choices we made, and the component now in dagster-workspace", { x: 0.7, y: 3.2, w: 7.5, h: 0.7, fontSize: 18, color: SOFT });
  box(s, 0.7, 4.4, 1.7, 0.65, "Delta", null, PANEL, PAPER); arrow(s, 2.45, 4.72, 0.6, TEAL);
  box(s, 3.1, 4.4, 1.7, 0.65, "Kafka", null, TEAL, INK); arrow(s, 4.85, 4.72, 0.6, TEAL);
  box(s, 5.5, 4.4, 1.7, 0.65, "Consumers", null, PANEL, PAPER);
  s.addNotes("Opening: one sentence on the problem. Delta is the system of record; consumers live elsewhere; getting data out should not cost Databricks compute.");
}

// 2 Business case
{
  const s = content("The business case: data has to leave the lakehouse", "Curated Delta tables are the system of record. Their consumers live elsewhere",
    "Consumers: operational Postgres, dashboards on other engines, downstream event pipelines. Every naive way out costs compute or labour.");
  const items = [
    ["Scheduled warehouse query", "Pays DBUs on every run", AMBER],
    ["Spark job reading the table", "Cluster cost for what is, at heart, a diff", AMBER],
    ["readChangeFeed (CDF)", "Needs the feature enabled on the source table, which governed tables rarely allow", ROSE],
    ["Ad-hoc scripts per team", "No replay safety, no standard, no reuse", ROSE]];
  items.forEach((it, i) => {
    const x = 0.5 + (i % 2) * 4.6, y = 1.65 + Math.floor(i / 2) * 1.75;
    card(s, x, y, 4.4, 1.55, MIST); badge(s, i + 1, x + 0.2, y + 0.2, 0.4, it[2], INK);
    T(s, it[0], { x: x + 0.75, y: y + 0.2, w: 3.45, h: 0.4, fontSize: 18, bold: true, color: INK, valign: "middle" });
    T(s, it[1], { x: x + 0.75, y: y + 0.7, w: 3.45, h: 0.75, fontSize: 14, color: TEXT });
  });
}

// 3 Goal
{
  const s = pres.addSlide(); s.background = { color: INK };
  T(s, "The goal: a standard component, not another script", { x: 0.5, y: 0.35, w: 9, h: 0.6, fontFace: HEAD, fontSize: 30, bold: true, color: PAPER, valign: "middle" });
  const st = [["0", "Databricks compute in the read path", "No cluster, no warehouse, no statement execution"],
              ["1", "YAML file to adopt it", "No Python, no Dagster resources, same file for stg and prd"],
              ["∞", "safe re-runs", "A failed run moves nothing; duplicates are harmless upserts"]];
  st.forEach((a, i) => {
    const x = 0.5 + i * 3.05;
    card(s, x, 1.5, 2.85, 3.2, PANEL);
    T(s, a[0], { x: x + 0.25, y: 1.7, w: 2.35, h: 1.2, fontFace: HEAD, fontSize: 72, bold: true, color: i === 0 ? AMBER : TEAL, valign: "middle" });
    T(s, a[1], { x: x + 0.25, y: 3.0, w: 2.35, h: 0.7, fontSize: 16, bold: true, color: PAPER });
    T(s, a[2], { x: x + 0.25, y: 3.75, w: 2.35, h: 0.8, fontSize: 12, color: SOFT });
  });
  s.addNotes("Zero DBU became the acceptance bar, not a nice-to-have.");
}

// 4 Tool evaluation
{
  const s = content("We looked at the tools first, and none fit", "Tool evaluation for APR-233, 2026-10-15",
    "drt: no Kafka destination (ADR 0004), no CDC extraction. dlt: no Kafka destination and none will be merged; row-level merge semantics, no change-op concept. SaaS: excluded by the epic on cost. Airbyte: Kafka destination exists but the Databricks source reads via JDBC (warehouse DBUs) and syncs are snapshots.");
  const Y = "✓", N = "✗", G = "D6F5F0";
  const hdr = (t, left) => ({ text: t, options: { bold: true, color: PAPER, fill: { color: INK }, align: left ? "left" : "center" } });
  const c = (t, good) => ({ text: t, options: { align: "center", bold: true, color: good ? "0E8F82" : ROSE } });
  const n = (t) => ({ text: t, options: { bold: true, color: INK } });
  const g = (t, center) => ({ text: t, options: { bold: true, color: center ? "0E8F82" : INK, fill: { color: G }, align: center ? "center" : "left" } });
  const rows = [
    [hdr("Tool", true), hdr("Kafka output"), hdr("Change events"), hdr("Reads without DBUs"), hdr("Verdict", true)],
    [n("drt"), c(N, 0), c(N, 0), c(N, 0), "No Kafka, no CDC by design"],
    [n("dlt"), c(N, 0), c(N, 0), c(N, 0), "Row merge, no change-op concept"],
    [n("Airbyte"), c(Y, 1), c(N, 0), c(N, 0), "JDBC source, snapshot syncs"],
    [n("Hightouch / Census"), c("n/a", 0), c(N, 0), c(N, 0), "SaaS cost, same extraction work"],
    [g("Custom Dagster component"), g(Y, 1), g(Y, 1), g(Y, 1), g("Debezium events, direct reads")]];
  s.addTable(rows, { x: 0.5, y: 1.55, w: 9, colW: [2.3, 1.2, 1.3, 1.6, 2.6], rowH: 0.5, fontFace: BODY, fontSize: 14, color: TEXT, valign: "middle", border: { type: "solid", pt: 0.75, color: "D5DEE9" } });
  T(s, "The three non-generic properties (Debezium events to Kafka, change shape from Delta versions, direct-storage reads) are absent from every tool scanned.", { x: 0.5, y: 4.75, w: 9, h: 0.5, fontSize: 13, color: MUTED });
}

// 5 Engine choice
{
  const s = content("The decisive experiment: read the Delta log directly", "delta-rs over ADLS reconstructs changes by diffing table versions",
    "Three read paths measured in REVERSE_ETL_DBU_COST_NOTES. table_changes() always runs on Databricks compute. Spark readChangeFeed needs CDF enabled. delta-rs version-diff needs nothing on the source.");
  const rows = [["SQL warehouse  table_changes()", "DBUs every run; warehouse start-up dominates"],
                ["Spark  readChangeFeed", "Cluster cost, and CDF must be enabled on the table"],
                ["delta-rs version-diff over ADLS", "Zero DBU, plain Delta, no source prerequisites"]];
  rows.forEach((r, i) => {
    const y = 1.65 + i * 0.78;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.5, y, w: 9, h: 0.65, rectRadius: 0.08, fill: { color: i === 2 ? "D6F5F0" : MIST }, line: { color: i === 2 ? TEAL : MIST } });
    T(s, r[0], { x: 0.7, y, w: 3.7, h: 0.65, fontSize: 15, bold: true, color: INK, valign: "middle" });
    T(s, r[1], { x: 4.5, y, w: 4.8, h: 0.65, fontSize: 14, color: TEXT, valign: "middle" });
  });
  box(s, 0.5, 4.2, 2.3, 0.85, "Delta log on ADLS", "transaction log, over storage", INK, PAPER); arrow(s, 2.85, 4.62, 0.5, TEAL);
  box(s, 3.4, 4.2, 2.3, 0.85, "delta-rs diff", "version N → version M", TEAL, INK); arrow(s, 5.75, 4.62, 0.5, TEAL);
  box(s, 6.3, 4.2, 3.2, 0.85, "Change events", "inserts, updates, deletes, schema changes", INK, PAPER);
}

// 6 Journey timeline
{
  const s = content("The road: four steps from PoC to released component", null,
    "PoC in risingwave-test; engine choice; release design; port to dagster-workspace as APR-302 following the Jobotron APR-293 pattern. Review-phase simplification follows on the next slide.");
  const steps = [["PoC", "Component family plus Postgres, RisingWave and DuckDB targets; Debezium JDBC sink drains the topic"],
                 ["Engine", "delta-rs version-diff chosen; zero DBU becomes the acceptance bar"],
                 ["Release design", "One sync, slim targets, minted names, env contract"],
                 ["Port APR-302", "Library in dagster-workspace, instance in inverness, CI wiring, import-hygiene test"]];
  s.addShape(pres.shapes.LINE, { x: 1.2, y: 2.05, w: 7.6, h: 0, line: { color: SOFT, width: 2 } });
  steps.forEach((a, i) => {
    const x = 0.5 + i * 2.3;
    badge(s, i + 1, x + 0.85, 1.85, 0.4, i === 3 ? AMBER : TEAL, INK);
    T(s, a[0], { x, y: 2.5, w: 2.1, h: 0.4, fontSize: 17, bold: true, color: INK, align: "center" });
    T(s, a[1], { x: x + 0.05, y: 3.0, w: 2.0, h: 1.9, fontSize: 13, color: TEXT });
  });
}

// 7 Release design
{
  const s = content("Release design: correct by construction", "The sync/target seam, naming policy and env contract");
  const cols = [["Minted names", "reverse_etl_<label>[_<role>] is derived in one module, so both sides of a seam compute byte-identical values.", "delta → reverse_etl_delta_topic"],
                ["The seam", "Targets see the topic, never the source table. Kafka is the contract, not any one target.", "Delta → sync → topic → any consumer"],
                ["Env contract", "No Dagster resources. Connections read os.environ at call time. Names in files, values in the environment.", "one defs.yaml: stg and prd"]];
  cols.forEach((c, i) => {
    const x = 0.5 + i * 3.05;
    card(s, x, 1.6, 2.85, 3.5, MIST);
    T(s, c[0], { x: x + 0.25, y: 1.8, w: 2.35, h: 0.4, fontSize: 18, bold: true, color: INK });
    T(s, c[1], { x: x + 0.25, y: 2.35, w: 2.35, h: 1.8, fontSize: 14, color: TEXT });
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: x + 0.2, y: 4.3, w: 2.45, h: 0.6, rectRadius: 0.06, fill: { color: INK }, line: { color: INK } });
    T(s, c[2], { x: x + 0.3, y: 4.3, w: 2.25, h: 0.6, fontFace: "Courier New", fontSize: 10, color: TEAL, valign: "middle" });
  });
}

// 8 Simplification
{
  const s = content("Review made it smaller, and the contract sharper", "Five steps turned the component into a pure Kafka producer",
    "Each strip made the contract sharper: deleted functionality lives on as the consumer project's own code.");
  const st = ["Instance repointed at the real table, key rid", "asset_key_prefix added (Jobotron parity)", "Topic asset and bootstrap job removed: no infrastructure", "Target components deleted: the only target is Kafka", "Every trace of the downstream side swept out"];
  st.forEach((t, i) => {
    const y = 1.6 + i * 0.66;
    badge(s, i + 1, 0.5, y + 0.05, 0.4, i >= 2 ? AMBER : TEAL, INK);
    T(s, t, { x: 1.1, y, w: 4.6, h: 0.5, fontSize: 15, color: TEXT, valign: "middle" });
  });
  card(s, 6.0, 1.6, 3.5, 3.2, INK);
  T(s, "Now it knows only", { x: 6.25, y: 1.8, w: 3.0, h: 0.4, fontSize: 14, color: SOFT });
  T(s, "one topic", { x: 6.25, y: 2.25, w: 3.0, h: 0.7, fontFace: HEAD, fontSize: 32, bold: true, color: PAPER });
  T(s, "one watermark", { x: 6.25, y: 3.0, w: 3.0, h: 0.7, fontFace: HEAD, fontSize: 32, bold: true, color: TEAL });
  T(s, "The blast radius shrank; nothing downstream of the topic is the library's business.", { x: 6.25, y: 3.85, w: 3.0, h: 0.8, fontSize: 12, color: SOFT });
}

// 9 Key decisions
{
  const s = content("Seven decisions that shaped it", null);
  const d = [["delta-rs, not CDF or Spark", "Zero DBU; plain Delta, no source prerequisites"],
             ["Debezium envelopes on Kafka", "Any CDC consumer can drain the topic"],
             ["JSON first", "No schema registry to run or grant; Avro stays available"],
             ["Watermark after the flush", "Append-only; a failed run moves nothing, replays are safe"],
             ["No infrastructure created", "Topic and state table belong to the deployment; a missing one fails loudly"],
             ["Targets are the consumer's code", "The library stays small and permission-free"],
             ["Aged-out history baselines, never backfills", "Documented trade-off; keep retention ahead of the schedule"]];
  d.forEach((a, i) => {
    const col = i % 2, row = Math.floor(i / 2);
    const x = i === 6 ? 0.5 : 0.5 + col * 4.6, y = 1.2 + row * 1.0, w = i === 6 ? 9 : 4.4;
    card(s, x, y, w, 0.88, MIST);
    T(s, a[0], { x: x + 0.2, y: y + 0.1, w: w - 0.4, h: 0.3, fontSize: 15, bold: true, color: INK });
    T(s, a[1], { x: x + 0.2, y: y + 0.45, w: w - 0.4, h: 0.38, fontSize: 12, color: TEXT });
  });
}

// 10 Final architecture
{
  const s = pres.addSlide(); s.background = { color: INK };
  T(s, "The end solution", { x: 0.5, y: 0.35, w: 9, h: 0.6, fontFace: HEAD, fontSize: 30, bold: true, color: PAPER, valign: "middle" });
  box(s, 0.5, 1.2, 3.4, 0.75, "Databricks Delta", "reverse_etl_delta_source", PANEL, PAPER);
  varrow(s, 2.2, 1.98, 0.3, TEAL);
  T(s, "delta-rs over ADLS, zero DBU", { x: 2.35, y: 2.03, w: 2.2, h: 0.2, fontSize: 10, color: SOFT });
  box(s, 0.5, 2.5, 3.4, 0.75, "inverness sync asset", "kaizen-dagster-reverse-etl, key = rid", TEAL, INK);
  varrow(s, 2.2, 3.28, 0.3, TEAL);
  T(s, "watermark in Postgres", { x: 2.35, y: 3.33, w: 2.2, h: 0.2, fontSize: 10, color: SOFT });
  box(s, 0.5, 3.8, 3.4, 0.75, "STG Kafka topic", "reverse_etl_delta_topic, compacted", PANEL, PAPER);
  arrow(s, 3.95, 4.17, 0.4, TEAL);
  box(s, 4.4, 3.8, 2.2, 0.75, "Consumer job", "risingwave-test", PANEL, PAPER);
  arrow(s, 6.65, 4.17, 0.4, TEAL);
  box(s, 7.1, 3.8, 2.4, 0.75, "Postgres target", "upsert by rid, tombstones", AMBER, INK);
  card(s, 4.4, 1.2, 5.1, 1.05, PANEL);
  T(s, [{ text: "dagster-workspace", options: { bold: true, color: TEAL, breakLine: true } }, { text: "One component, one sync asset, no infrastructure, no targets. The instance is one YAML.", options: { color: PAPER } }], { x: 4.6, y: 1.3, w: 4.7, h: 0.85, fontSize: 13 });
  card(s, 4.4, 2.45, 5.1, 1.1, PANEL);
  T(s, [{ text: "Owned by the deployment", options: { bold: true, color: AMBER, breakLine: true } }, { text: "The compacted topic and the reverse_etl_sync_state table. A missing one fails the run with the DDL to create it.", options: { color: PAPER } }], { x: 4.6, y: 2.55, w: 4.7, h: 0.9, fontSize: 13 });
  T(s, "Consumer side, built from the PoC's own builders: topic setup, target table, JSON connector, report", { x: 0.5, y: 4.8, w: 9, h: 0.4, fontSize: 12, color: SOFT });
}

// 11 Verified + replay safety
{
  const s = content("Verified live, and replay-safe by construction", "End to end against the STG topic and a Postgres target",
    "A change reached Postgres as an upsert, a deletion as a tombstone, and re-running the sync replayed safely.");
  const v = [["Change", "a Delta row changed", "upsert in Postgres"], ["Deletion", "a Delta row was deleted", "tombstone removes it"], ["Re-run", "the sync ran again", "versions replayed, no harm"]];
  v.forEach((a, i) => {
    const y = 1.6 + i * 0.75;
    card(s, 0.5, y, 4.6, 0.62, MIST);
    T(s, a[0], { x: 0.7, y, w: 1.0, h: 0.62, fontSize: 14, bold: true, color: INK, valign: "middle" });
    T(s, a[1] + "  →  " + a[2], { x: 1.75, y, w: 3.25, h: 0.62, fontSize: 12, color: TEXT, valign: "middle" });
  });
  T(s, "Renaming the watermark key fails loudly instead of silently restarting the sync.", { x: 0.5, y: 3.95, w: 4.6, h: 0.6, fontSize: 13, color: TEXT });
  T(s, "Why a failed run is safe", { x: 5.6, y: 1.5, w: 3.9, h: 0.35, fontSize: 16, bold: true, color: INK });
  ["Read versions", "Produce to Kafka", "Final flush succeeds", "Append watermark"].forEach((t, i) => {
    const y = 1.95 + i * 0.7;
    box(s, 5.6, y, 3.0, 0.5, t, null, i === 3 ? TEAL : INK, i === 3 ? INK : PAPER);
    if (i < 3) varrow(s, 7.1, y + 0.5, 0.2, MUTED);
  });
  T(s, "A failure before the last step leaves the watermark where it was.", { x: 5.6, y: 4.75, w: 3.9, h: 0.5, fontSize: 12, color: MUTED });
}

// 12 Pros / cons
{
  const s = content("What it gives us, and what it costs", null);
  card(s, 0.5, 1.2, 4.4, 3.9, "D6F5F0"); card(s, 5.1, 1.2, 4.4, 3.9, "FDECDC");
  T(s, "Pros", { x: 0.75, y: 1.35, w: 3.9, h: 0.4, fontSize: 20, bold: true, color: INK });
  T(s, bullets(["Zero Databricks compute in the read path", "No source-table prerequisites", "Replay-safe: duplicates are harmless upserts", "Adoption is one YAML", "Small blast radius", "Industry-neutral Debezium output"]),
    { x: 0.75, y: 1.9, w: 3.9, h: 3.0, fontSize: 14, color: TEXT, paraSpaceAfter: 6 });
  T(s, "Cons", { x: 5.35, y: 1.35, w: 3.9, h: 0.4, fontSize: 20, bold: true, color: INK });
  T(s, bullets(["Batch: latency is schedule plus diff run", "Topic and state table set up per deployment", "JSON carries its schema in every message", "Expired log retention baselines silently", "Consumers build and monitor their own target", "Encoding must match across the topic"]),
    { x: 5.35, y: 1.9, w: 3.9, h: 3.0, fontSize: 14, color: TEXT, paraSpaceAfter: 6 });
}

// 13 Next steps
{
  const s = pres.addSlide(); s.background = { color: INK };
  T(s, "Takeaways and next steps", { x: 0.5, y: 0.35, w: 9, h: 0.6, fontFace: HEAD, fontSize: 30, bold: true, color: PAPER, valign: "middle" });
  const t = [["Takeaway", "A Delta table's changes reach Kafka as standard events with no Databricks compute, adopted with one YAML.", TEAL],
             ["Next: first live instance", "Needs UC storage-credential grants for the sync's service principal, the same blocker the Jobotron work hit.", AMBER],
             ["Next: per deployment", "Provision the compacted topic and the reverse_etl_sync_state table, then match the consumer's connector to the sync's encoding.", AMBER]];
  t.forEach((a, i) => {
    const y = 1.3 + i * 1.3;
    card(s, 0.5, y, 9, 1.1, PANEL); badge(s, i + 1, 0.75, y + 0.35, 0.4, a[2], INK);
    T(s, a[0], { x: 1.4, y: y + 0.12, w: 7.8, h: 0.35, fontSize: 17, bold: true, color: PAPER });
    T(s, a[1], { x: 1.4, y: y + 0.5, w: 7.8, h: 0.55, fontSize: 14, color: SOFT });
  });
}

(async () => {
  await pres.writeFile({ fileName: "../REVERSE_ETL_PRESENTATION.pptx" });
  await applyTheme("../REVERSE_ETL_PRESENTATION.pptx", THEME);
  console.log("ok");
})();
