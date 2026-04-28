const fs = require("fs");
const path = require("path");
const vm = require("vm");

function createLocalStorage() {
  const store = new Map();
  return {
    getItem(key) {
      return store.has(key) ? store.get(key) : null;
    },
    setItem(key, value) {
      store.set(key, String(value));
    },
    removeItem(key) {
      store.delete(key);
    },
    clear() {
      store.clear();
    },
  };
}

function buildTable({
  id,
  datasetId,
  filename,
  rows,
  columns,
  transformConfig = {},
  charts = [],
  activeChartId = null,
  dashboardMode = "single",
  dashboardColumns = 2,
}) {
  return {
    id,
    datasetId,
    datasetState: null,
    filename,
    shape: { rows, columns },
    schema: { region: { type: "categorical", role: "geography" } },
    preview: [{ region: "north" }],
    analysis: { key_insights: [`Loaded ${filename}`] },
    transformConfig,
    transformHistory: [{ label: "Original cleaned view", config: {} }],
    transformHistoryIndex: 0,
    chartConfig: charts[0] || null,
    charts,
    activeChartId,
    dashboardMode,
    dashboardColumns,
  };
}

function expectEqual(label, actual, expected, failures) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    failures.push(`${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function expect(label, condition, failures) {
  if (!condition) {
    failures.push(label);
  }
}

function loadDataTool() {
  const sharedJsPath = path.join(__dirname, "..", "frontend", "shared.js");
  const code = fs.readFileSync(sharedJsPath, "utf8");
  const localStorage = createLocalStorage();
  const promptQueue = [];

  const context = {
    console,
    JSON,
    Math,
    Date,
    localStorage,
    window: {
      prompt: () => promptQueue.shift() ?? null,
      location: {
        reloaded: false,
        reload() {
          this.reloaded = true;
        },
      },
    },
  };
  context.window.window = context.window;
  context.globalThis = context.window;
  vm.createContext(context);
  vm.runInContext(code, context);

  return {
    dataTool: context.window.dataTool,
    localStorage,
    window: context.window,
    queuePrompt(value) {
      promptQueue.push(value);
    },
  };
}

async function run() {
  const failures = [];
  const harness = loadDataTool();
  const { dataTool, window } = harness;

  const chartA = {
    id: "chart-a",
    chart_type: "bar",
    x_column: "region",
    y_column: "revenue",
    label: "Revenue by region",
    chart_data_url: "data:image/png;base64,abc",
    mime_type: "image/png",
    chart_options: { aggregation: "sum", top_n: 5 },
  };

  const chartB = {
    id: "chart-b",
    chart_type: "line",
    x_column: "Year",
    y_column: "cases",
    label: "Cases over time",
    chart_options: { aggregation: "latest", top_n: 12 },
  };

  const tableA = buildTable({
    id: "table-sales",
    datasetId: "dataset-sales",
    filename: "sales.csv",
    rows: 8,
    columns: 4,
    transformConfig: { filters: [{ column: "region", operator: "==", value: "north" }] },
    charts: [chartA],
    activeChartId: "chart-a",
    dashboardMode: "grid",
    dashboardColumns: 3,
  });

  const tableB = buildTable({
    id: "table-health",
    datasetId: "dataset-health",
    filename: "health.csv",
    rows: 12,
    columns: 5,
    transformConfig: { group_by: ["country"] },
    charts: [chartB],
    activeChartId: "chart-b",
    dashboardMode: "single",
    dashboardColumns: 2,
  });

  const session = dataTool.createMultiTableSession([tableA, tableB], { lastPage: "/prepare" });
  expectEqual("initial active table", session.activeTableId, "table-sales", failures);
  expectEqual("initial filename mirrors first table", session.filename, "sales.csv", failures);
  expectEqual("initial transform mirrors first table", session.transformConfig, tableA.transformConfig, failures);

  session.transformConfig = { filters: [{ column: "region", operator: "==", value: "south" }] };
  session.charts = [
    {
      ...chartA,
      label: "Updated revenue by region",
      chart_data_url: "data:image/png;base64,updated",
      preview_filter: { label: "North only", conditions: [{ column: "region", type: "equals", value: "north" }] },
    },
  ];
  session.activeChartId = "chart-a";
  session.chartConfig = session.charts[0];
  session.dashboardMode = "grid";
  session.dashboardColumns = 4;

  const switched = dataTool.setActiveTable(session, "table-health");
  expectEqual("switched active table", switched.activeTableId, "table-health", failures);
  expectEqual("session filename after switch", switched.filename, "health.csv", failures);
  expectEqual("table A config preserved on switch", switched.tables[0].transformConfig, session.transformConfig, failures);
  expectEqual("table A dashboardColumns preserved on switch", switched.tables[0].dashboardColumns, 4, failures);

  switched.transformConfig = { group_by: ["country"], aggregations: [{ column: "cases", operation: "sum" }] };
  switched.dashboardMode = "single";
  switched.dashboardColumns = 2;
  switched.charts = [
    {
      ...chartB,
      note: "Health table note",
      filtered_preview: [{ country: "Kenya" }],
      filtered_preview_shape: { rows: 1, columns: 5 },
    },
  ];
  switched.activeChartId = "chart-b";
  switched.chartConfig = switched.charts[0];

  dataTool.saveSession(switched);
  const storedSessionRaw = JSON.parse(harness.localStorage.getItem("data_visualisation_tool_session"));
  const reloadedSession = dataTool.loadSession();
  expectEqual("reloaded active table id", reloadedSession.activeTableId, "table-health", failures);
  expectEqual("reloaded active filename", reloadedSession.filename, "health.csv", failures);
  expectEqual("reloaded first table preserved", reloadedSession.tables[0].filename, "sales.csv", failures);
  expectEqual("reloaded second table transform", reloadedSession.transformConfig, switched.transformConfig, failures);
  expect("session storage strips chart binary", !storedSessionRaw.tables[0].charts[0].chart_data_url, failures);

  const workspace = dataTool.storeWorkspace(switched, "My Workspace");
  expectEqual("workspace saved name", workspace.name, "My Workspace", failures);
  const reopened = dataTool.getWorkspace(workspace.id);
  expectEqual("reopened workspace active table", reopened.session.activeTableId, "table-health", failures);
  expectEqual("reopened workspace second chart note", reopened.session.tables[1].charts[0].note, "Health table note", failures);
  expectEqual("reopened workspace first table dashboard columns", reopened.session.tables[0].dashboardColumns, 4, failures);

  dataTool.saveSession({ ...reopened.session, lastPage: reopened.lastPage || "/visualize" });
  const reopenedSession = dataTool.loadSession();
  expectEqual("session restored from workspace keeps active table", reopenedSession.activeTableId, "table-health", failures);
  expectEqual("session restored from workspace keeps first table transform", reopenedSession.tables[0].transformConfig, session.transformConfig, failures);

  const transientState = {
    datasetId: "dataset-fresh",
    datasetState: null,
    dataset_state: null,
    filename: "fresh.csv",
    transformConfig: { filters: [] },
  };
  harness.queuePrompt("Transient workspace");
  let fetchCalls = 0;
  const savedViaFlow = await dataTool.saveWorkspaceFlow({
    state: transientState,
    lastPage: "/visualize",
    fetchJson: async () => {
      fetchCalls += 1;
      return { dataset_state: { clean_data: "{}", filename: "fresh.csv" } };
    },
    setStatus: () => {},
  });
  expectEqual("saveWorkspaceFlow fetches dataset state once", fetchCalls, 1, failures);
  expect("saveWorkspaceFlow stores workspace", Boolean(savedViaFlow && savedViaFlow.id), failures);
  expect("saveWorkspaceFlow fills datasetState", Boolean(transientState.datasetState), failures);

  dataTool.switchWorkspaceTable(reopenedSession, "table-sales", { reload: false });
  expectEqual("switchWorkspaceTable without reload changes active id", reopenedSession.activeTableId, "table-sales", failures);
  expect("switchWorkspaceTable without reload avoids reload call", window.location.reloaded === false, failures);

  console.log("Workspace regression checks complete.");
  if (failures.length) {
    console.log("\nFailures:");
    failures.forEach((failure) => console.log(`- ${failure}`));
    process.exitCode = 1;
    return;
  }

  console.log("All checks passed.");
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
