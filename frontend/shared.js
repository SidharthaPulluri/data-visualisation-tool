(function () {
  const STORAGE_KEY = "data_visualisation_tool_session";
  const WORKSPACES_KEY = "data_visualisation_tool_workspaces";
  const MEMORY_GRAPH_KEY = "data_visualisation_tool_memory_events";
  const ACTIVE_TABLE_FIELDS = [
    "datasetId",
    "datasetState",
    "dataset_state",
    "filename",
    "shape",
    "schema",
    "preview",
    "analysis",
    "transformConfig",
    "transformHistory",
    "transformHistoryIndex",
    "chartConfig",
    "charts",
    "activeChartId",
    "dashboardMode",
    "dashboardColumns",
  ];

  function cloneJson(value, fallback) {
    if (value === undefined) return fallback;
    try {
      return JSON.parse(JSON.stringify(value));
    } catch {
      return fallback;
    }
  }

  function createMemoryEvent(type, payload = {}) {
    return {
      id: `memory_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      type,
      createdAt: new Date().toISOString(),
      payload: cloneJson(payload, payload) || {},
    };
  }

  function loadMemoryEvents() {
    try {
      const parsed = JSON.parse(localStorage.getItem(MEMORY_GRAPH_KEY) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  function saveMemoryEvents(events) {
    localStorage.setItem(MEMORY_GRAPH_KEY, JSON.stringify((Array.isArray(events) ? events : []).slice(0, 120)));
  }

  function recordMemoryEvent(type, payload = {}) {
    const events = loadMemoryEvents();
    const event = createMemoryEvent(type, payload);
    events.unshift(event);
    saveMemoryEvents(events);
    return event;
  }

  function clearMemoryEvents() {
    localStorage.removeItem(MEMORY_GRAPH_KEY);
  }

  function buildTableRecord(record = {}) {
    const datasetState = record?.datasetState || record?.dataset_state || null;
    return {
      id: record.id || `table_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      datasetId: record?.datasetId || null,
      datasetState,
      dataset_state: datasetState,
      filename: record?.filename || "Dataset",
      shape: cloneJson(record?.shape, null),
      schema: cloneJson(record?.schema, {}),
      preview: cloneJson(record?.preview, []),
      analysis: cloneJson(record?.analysis, null),
      transformConfig: cloneJson(record?.transformConfig, {}) || {},
      transformHistory: cloneJson(record?.transformHistory, [{ label: "Original cleaned view", config: {} }]) || [{ label: "Original cleaned view", config: {} }],
      transformHistoryIndex: typeof record?.transformHistoryIndex === "number" ? record.transformHistoryIndex : 0,
      chartConfig: cloneJson(record?.chartConfig, null),
      charts: cloneJson(record?.charts, []) || [],
      activeChartId: record?.activeChartId || null,
      dashboardMode: record?.dashboardMode || "single",
      dashboardColumns: Number(record?.dashboardColumns || 2),
      sourcePath: record?.sourcePath || null,
      sourceContext: cloneJson(record?.sourceContext, null),
    };
  }

  function mirrorActiveTableIntoSession(session, preferredTableId) {
    if (!session || !Array.isArray(session.tables) || !session.tables.length) {
      return session;
    }

    const tables = session.tables.map((table) => buildTableRecord(table));
    const activeTableId = preferredTableId || session.activeTableId || tables[0]?.id;
    const activeTable = tables.find((table) => table.id === activeTableId) || tables[0];
    const nextSession = { ...session, tables, activeTableId: activeTable?.id || null };
    if (activeTable) {
      ACTIVE_TABLE_FIELDS.forEach((field) => {
        if (field === "dataset_state") return;
        nextSession[field] = cloneJson(activeTable[field], activeTable[field]);
      });
      nextSession.dataset_state = nextSession.datasetState;
      nextSession.filename = activeTable.filename || nextSession.filename;
      nextSession.shape = activeTable.shape || nextSession.shape;
      nextSession.schema = activeTable.schema || nextSession.schema;
      nextSession.preview = activeTable.preview || nextSession.preview;
      nextSession.analysis = activeTable.analysis || nextSession.analysis;
    }
    return nextSession;
  }

  function syncActiveTableIntoSession(session) {
    if (!session || !Array.isArray(session.tables) || !session.tables.length) {
      return session;
    }

    const activeTableId = session.activeTableId || session.tables[0]?.id;
    const tables = session.tables.map((table, index) => {
      const nextTable = buildTableRecord(table);
      if ((nextTable.id || index) !== activeTableId) {
        return nextTable;
      }
      ACTIVE_TABLE_FIELDS.forEach((field) => {
        if (field === "dataset_state") return;
        if (field === "datasetState") {
          const datasetState = session?.datasetState || session?.dataset_state || null;
          nextTable.datasetState = cloneJson(datasetState, datasetState);
          nextTable.dataset_state = nextTable.datasetState;
          return;
        }
        if (field in session) {
          nextTable[field] = cloneJson(session[field], session[field]);
        }
      });
      nextTable.dataset_state = nextTable.datasetState;
      return nextTable;
    });

    return mirrorActiveTableIntoSession({ ...session, tables, activeTableId }, activeTableId);
  }

  function createMultiTableSession(tables, options = {}) {
    const workspaceTables = (Array.isArray(tables) ? tables : []).map((table) => buildTableRecord(table));
    if (!workspaceTables.length) {
      return null;
    }
    const session = {
      tables: workspaceTables,
      activeTableId: workspaceTables[0].id,
      lastPage: options.lastPage || "/prepare",
      sourcePath: options.sourcePath || null,
      sourceContext: cloneJson(options.sourceContext, null),
    };
    const nextSession = mirrorActiveTableIntoSession(session, workspaceTables[0].id);
    recordMemoryEvent("session_initialized", {
      sourcePath: nextSession.sourcePath || "file",
      tableCount: workspaceTables.length,
      activeTableId: nextSession.activeTableId,
      filename: nextSession.filename || null,
    });
    return nextSession;
  }

  function getWorkspaceTables(session) {
    return Array.isArray(session?.tables) ? session.tables : [];
  }

  function getActiveTableRecord(session) {
    const tables = getWorkspaceTables(session);
    if (!tables.length) return null;
    const activeTableId = session?.activeTableId || tables[0]?.id;
    return tables.find((table) => table.id === activeTableId) || tables[0] || null;
  }

  function setActiveTable(session, tableId) {
    if (!session || !Array.isArray(session.tables) || !session.tables.length) {
      return session;
    }
    const currentActiveId = session.activeTableId || session.tables[0]?.id;
    const syncedCurrent = syncActiveTableIntoSession({ ...session, activeTableId: currentActiveId });
    return mirrorActiveTableIntoSession({ ...syncedCurrent, activeTableId: tableId }, tableId);
  }

  function replaceSessionState(targetState, nextSession) {
    if (!targetState || !nextSession) {
      return nextSession;
    }
    Object.keys(targetState).forEach((key) => {
      delete targetState[key];
    });
    Object.assign(targetState, nextSession);
    return targetState;
  }

  function setStatusText(target, message) {
    if (target) {
      target.textContent = message;
    }
  }

  function renderWorkspaceTablePanel({ session, panel, meta, tabs, onSwitch }) {
    const tables = getWorkspaceTables(session);
    if (!panel || !tabs) {
      return tables;
    }

    if (tables.length <= 1) {
      panel.hidden = true;
      tabs.innerHTML = "";
      return tables;
    }

    panel.hidden = false;
    if (meta) {
      meta.textContent = `${formatNumber(tables.length, 0)} uploaded files are available in this workspace.`;
    }
    tabs.innerHTML = tables.map((table, index) => `
      <button
        type="button"
        class="table-switch-button${table.id === session.activeTableId ? " active" : ""}"
        data-table-id="${escapeHtml(table.id)}"
      >
        <strong>${escapeHtml(table.filename || `Table ${index + 1}`)}</strong>
        <span>${formatNumber(table.shape?.rows || 0, 0)} rows • ${formatNumber(table.shape?.columns || 0, 0)} columns</span>
      </button>
    `).join("");

    tabs.querySelectorAll("[data-table-id]").forEach((button) => {
      button.addEventListener("click", () => {
        if (typeof onSwitch === "function") {
          onSwitch(button.dataset.tableId);
        }
      });
    });
    return tables;
  }

  function switchWorkspaceTable(state, tableId, { reload = true } = {}) {
    const nextSession = setActiveTable(state, tableId);
    replaceSessionState(state, nextSession);
    saveSession(state);
    recordMemoryEvent("table_switched", {
      activeTableId: state.activeTableId,
      filename: state.filename || null,
      lastPage: state.lastPage || null,
    });
    if (reload && typeof window !== "undefined") {
      window.location.reload();
    }
    return state;
  }

  async function ensureDatasetState({ state, config, fetchJson: fetcher }) {
    if (state?.datasetState || state?.dataset_state) {
      return state.datasetState || state.dataset_state;
    }
    if (typeof fetcher !== "function") {
      throw new Error("A fetchJson helper is required to refresh dataset state.");
    }

    const payload = await fetcher("/api/transform", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset_id: state?.datasetId,
        dataset_state: state?.datasetState || state?.dataset_state || null,
        config: config || state?.transformConfig || {}
      })
    });
    state.datasetState = payload.dataset_state;
    state.dataset_state = payload.dataset_state;
    return payload.dataset_state;
  }

  async function saveWorkspaceFlow({ state, lastPage = "/prepare", fetchJson: fetcher, setStatus }) {
    const workspaceName = window.prompt("Name this workspace", `${state?.filename || "dataset"} workspace`);
    if (!workspaceName) {
      return null;
    }

    await ensureDatasetState({ state, config: state?.transformConfig || {}, fetchJson: fetcher });
    const workspace = storeWorkspace({ ...state, lastPage }, workspaceName.trim());
    if (typeof setStatus === "function") {
      setStatus(`Saved workspace as ${workspace.name}.`);
    }
    return workspace;
  }

  function stripChartBinary(chart) {
    if (!chart) return chart;
    const clone = { ...chart };
    delete clone.chart_data_url;
    delete clone.mime_type;
    return clone;
  }

  function createStorageSafeSession(session) {
    const syncedSession = syncActiveTableIntoSession(session);
    const charts = Array.isArray(syncedSession?.charts)
      ? syncedSession.charts.map(stripChartBinary)
      : [];

    const chartConfig = syncedSession?.chartConfig
      ? stripChartBinary(syncedSession.chartConfig)
      : null;

    const datasetState = syncedSession?.datasetId ? null : (syncedSession?.datasetState || syncedSession?.dataset_state || null);
    const tables = Array.isArray(syncedSession?.tables)
      ? syncedSession.tables.map((table) => {
          const nextTable = buildTableRecord(table);
          nextTable.charts = Array.isArray(nextTable.charts) ? nextTable.charts.map(stripChartBinary) : [];
          nextTable.chartConfig = nextTable.chartConfig ? stripChartBinary(nextTable.chartConfig) : null;
          if (nextTable.datasetId) {
            nextTable.datasetState = null;
            nextTable.dataset_state = null;
          }
          return nextTable;
        })
      : undefined;

    return {
      ...syncedSession,
      charts,
      chartConfig,
      datasetState,
      dataset_state: datasetState,
      ...(tables ? { tables } : {}),
    };
  }

  function createMinimalSession(session) {
    const syncedSession = syncActiveTableIntoSession(session);
    return {
      datasetId: syncedSession?.datasetId || null,
      filename: syncedSession?.filename || null,
      shape: syncedSession?.shape || null,
      schema: syncedSession?.schema || null,
      datasetState: null,
      dataset_state: null,
      transformConfig: syncedSession?.transformConfig || {},
      transformHistory: syncedSession?.transformHistory || [],
      transformHistoryIndex: syncedSession?.transformHistoryIndex ?? 0,
      charts: Array.isArray(syncedSession?.charts)
        ? syncedSession.charts.map((chart) => ({
            id: chart.id,
            label: chart.label,
            title: chart.title,
            chart_type: chart.chart_type,
            x_column: chart.x_column,
            y_column: chart.y_column,
            format: chart.format,
            chart_file: chart.chart_file,
            note: chart.note || "",
            chart_options: chart.chart_options || {},
            plot_data: chart.plot_data || null,
            preview_filter: chart.preview_filter || null,
            filtered_preview: chart.filtered_preview || null,
            filtered_preview_shape: chart.filtered_preview_shape || null,
          }))
        : [],
      activeChartId: syncedSession?.activeChartId || null,
      dashboardMode: syncedSession?.dashboardMode || "single",
      dashboardColumns: syncedSession?.dashboardColumns || 2,
      chartConfig: syncedSession?.chartConfig
        ? {
            id: syncedSession.chartConfig.id,
            title: syncedSession.chartConfig.title,
            chart_type: syncedSession.chartConfig.chart_type,
            x_column: syncedSession.chartConfig.x_column,
            y_column: syncedSession.chartConfig.y_column,
            format: syncedSession.chartConfig.format,
            chart_file: syncedSession.chartConfig.chart_file,
            note: syncedSession.chartConfig.note || "",
            chart_options: syncedSession.chartConfig.chart_options || {},
          }
        : null,
      tables: Array.isArray(syncedSession?.tables)
        ? syncedSession.tables.map((table) => ({
            id: table.id,
            datasetId: table.datasetId || null,
            filename: table.filename || null,
            shape: table.shape || null,
            schema: table.schema || null,
            datasetState: null,
            dataset_state: null,
            preview: table.preview || [],
            analysis: table.analysis || null,
            transformConfig: table.transformConfig || {},
            transformHistory: table.transformHistory || [],
            transformHistoryIndex: table.transformHistoryIndex ?? 0,
            charts: Array.isArray(table.charts) ? table.charts.map(stripChartBinary) : [],
            activeChartId: table.activeChartId || null,
            dashboardMode: table.dashboardMode || "single",
            dashboardColumns: table.dashboardColumns || 2,
            chartConfig: table.chartConfig ? stripChartBinary(table.chartConfig) : null,
            sourcePath: table.sourcePath || null,
            sourceContext: table.sourceContext || null,
          }))
        : undefined,
      activeTableId: syncedSession?.activeTableId || null,
      lastPage: syncedSession?.lastPage || "/prepare",
      sourcePath: syncedSession?.sourcePath || null,
      sourceContext: syncedSession?.sourceContext || null,
    };
  }

  function normalizeSessionShape(session) {
    if (!session || typeof session !== "object") {
      return session;
    }

    const nextSession = {
      ...session,
      lastPage: session.lastPage || "/prepare",
      dashboardMode: session.dashboardMode || "single",
      dashboardColumns: Number(session.dashboardColumns || 2),
    };

    if (Array.isArray(session.tables) && session.tables.length) {
      nextSession.tables = session.tables.map((table) => buildTableRecord(table));
      return mirrorActiveTableIntoSession(nextSession, nextSession.activeTableId);
    }

    return nextSession;
  }

  function saveSession(session) {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(createStorageSafeSession(session)));
    } catch (error) {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(createMinimalSession(session)));
      } catch {
        console.warn("Unable to persist the full session in browser storage.", error);
      }
    }
  }

  function loadSession() {
    try {
      return normalizeSessionShape(JSON.parse(localStorage.getItem(STORAGE_KEY) || "null"));
    } catch {
      return null;
    }
  }

  function clearSession() {
    localStorage.removeItem(STORAGE_KEY);
  }

  function loadWorkspaces() {
    try {
      const parsed = JSON.parse(localStorage.getItem(WORKSPACES_KEY) || "[]");
      if (!Array.isArray(parsed)) return [];
      return parsed.map((workspace) => ({
        ...workspace,
        session: normalizeSessionShape(workspace?.session || null),
        lastPage: workspace?.lastPage || workspace?.session?.lastPage || "/prepare",
      }));
    } catch {
      return [];
    }
  }

  function saveWorkspaces(workspaces) {
    localStorage.setItem(WORKSPACES_KEY, JSON.stringify(workspaces));
  }

  function createWorkspaceSnapshot(session, name) {
    const syncedSession = syncActiveTableIntoSession(session);
    const datasetState = syncedSession?.datasetState || syncedSession?.dataset_state || null;
    const charts = Array.isArray(syncedSession?.charts)
      ? syncedSession.charts.map(stripChartBinary)
      : [];
    const chartConfig = syncedSession?.chartConfig
      ? stripChartBinary(syncedSession.chartConfig)
      : null;

    const snapshot = {
      datasetId: syncedSession?.datasetId || null,
      datasetState,
      dataset_state: datasetState,
      filename: syncedSession?.filename || null,
      shape: syncedSession?.shape || null,
      schema: syncedSession?.schema || null,
      preview: syncedSession?.preview || [],
      analysis: syncedSession?.analysis || null,
      transformConfig: syncedSession?.transformConfig || {},
      transformHistory: syncedSession?.transformHistory || [],
      transformHistoryIndex: syncedSession?.transformHistoryIndex ?? 0,
      charts,
      activeChartId: syncedSession?.activeChartId || null,
      dashboardMode: syncedSession?.dashboardMode || "single",
      dashboardColumns: syncedSession?.dashboardColumns || 2,
      chartConfig,
      lastPage: syncedSession?.lastPage || "/prepare",
      tables: Array.isArray(syncedSession?.tables)
        ? syncedSession.tables.map((table) => {
            const nextTable = buildTableRecord(table);
            nextTable.charts = Array.isArray(nextTable.charts) ? nextTable.charts.map(stripChartBinary) : [];
            nextTable.chartConfig = nextTable.chartConfig ? stripChartBinary(nextTable.chartConfig) : null;
            return nextTable;
          })
        : undefined,
      activeTableId: syncedSession?.activeTableId || null,
      sourcePath: syncedSession?.sourcePath || null,
      sourceContext: syncedSession?.sourceContext || null,
    };
    return {
      id: `workspace_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      name: name || syncedSession?.filename || "Saved workspace",
      filename: syncedSession?.filename || null,
      shape: syncedSession?.shape || null,
      savedAt: new Date().toISOString(),
      lastPage: syncedSession?.lastPage || "/prepare",
      session: snapshot,
    };
  }

  function storeWorkspace(session, name) {
    const workspaces = loadWorkspaces();
    const snapshot = createWorkspaceSnapshot(session, name);
    workspaces.unshift(snapshot);
    saveWorkspaces(workspaces.slice(0, 8));
    recordMemoryEvent("workspace_saved", {
      workspaceId: snapshot.id,
      name: snapshot.name,
      filename: snapshot.filename || null,
      tableCount: Array.isArray(snapshot.session?.tables) ? snapshot.session.tables.length : 0,
      lastPage: snapshot.lastPage || "/prepare",
    });
    return snapshot;
  }

  function deleteWorkspace(workspaceId) {
    saveWorkspaces(loadWorkspaces().filter((workspace) => workspace.id !== workspaceId));
  }

  function getWorkspace(workspaceId) {
    return loadWorkspaces().find((workspace) => workspace.id === workspaceId) || null;
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatNumber(value, digits = 2) {
    const numericValue = Number(value);
    if (value === null || value === undefined || value === "") return "n/a";
    if (!Number.isFinite(numericValue)) return String(value);
    return numericValue.toLocaleString(undefined, { maximumFractionDigits: digits });
  }

  function truncateValue(value, limit = 46) {
    const compact = String(value ?? "-").replace(/\s+/g, " ").trim();
    if (compact.length <= limit) return compact || "-";
    return `${compact.slice(0, limit - 3)}...`;
  }

  function humanizeFieldName(value) {
    const base = String(value || "")
      .replace(/__col_\d+\b/gi, "")
      .replace(/_col[\d_]+/gi, "")
      .replace(/StateUT/g, "State/UT")
      .replace(/Invsgnat/g, "Investigation at")
      .replace(/Invsgn/g, "Investigation")
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return base || String(value || "");
  }

  function createGraphNode(id, kind, title, details = [], meta = {}) {
    return {
      id,
      kind,
      title,
      details: Array.isArray(details) ? details.filter(Boolean) : [],
      meta: cloneJson(meta, meta) || {},
    };
  }

  function createGraphEdge(from, to, label) {
    return { from, to, label };
  }

  function buildWorkspaceMemoryGraph(session, options = {}) {
    const currentSession = normalizeSessionShape(session || loadSession() || null);
    const workspaces = options.includeWorkspaces === false ? [] : loadWorkspaces();
    const events = options.includeEvents === false ? [] : loadMemoryEvents();
    const nodes = [];
    const edges = [];

    if (currentSession) {
      const sessionId = `session:${currentSession.activeTableId || currentSession.datasetId || currentSession.filename || "current"}`;
      nodes.push(
        createGraphNode(
          sessionId,
          "session",
          currentSession.filename || "Current session",
          [
            `${formatNumber(currentSession.shape?.rows || 0, 0)} rows`,
            `${formatNumber(currentSession.shape?.columns || 0, 0)} columns`,
            `${formatNumber(getWorkspaceTables(currentSession).length || 0, 0)} tables`,
          ],
          {
            activeTableId: currentSession.activeTableId || null,
            sourcePath: currentSession.sourcePath || "file",
            lastPage: currentSession.lastPage || "/prepare",
          }
        )
      );

      if (currentSession.sourcePath || currentSession.sourceContext) {
        const sourceId = `source:${currentSession.sourcePath || "file"}:${currentSession.activeTableId || "current"}`;
        const sourceContext = currentSession.sourceContext || {};
        nodes.push(
          createGraphNode(
            sourceId,
            "source",
            currentSession.sourcePath === "database" ? "Database-origin source" : "Structured file source",
            [
              sourceContext.sourceType ? `Type: ${humanizeFieldName(sourceContext.sourceType)}` : null,
              sourceContext.sourceObject ? `Object: ${sourceContext.sourceObject}` : null,
              sourceContext.sourceNotes ? truncateValue(sourceContext.sourceNotes, 56) : null,
            ],
            sourceContext
          )
        );
        edges.push(createGraphEdge(sourceId, sessionId, "feeds"));
      }

      getWorkspaceTables(currentSession).forEach((table, index) => {
        const tableId = `table:${table.id}`;
        nodes.push(
          createGraphNode(
            tableId,
            "table",
            table.filename || `Table ${index + 1}`,
            [
              `${formatNumber(table.shape?.rows || 0, 0)} rows`,
              `${formatNumber(table.shape?.columns || 0, 0)} columns`,
              table.id === currentSession.activeTableId ? "Active table" : null,
            ],
            {
              datasetId: table.datasetId || null,
              active: table.id === currentSession.activeTableId,
            }
          )
        );
        edges.push(createGraphEdge(sessionId, tableId, "contains"));

        (table.transformHistory || []).forEach((entry, historyIndex) => {
          const transformId = `${tableId}:transform:${historyIndex}`;
          nodes.push(
            createGraphNode(
              transformId,
              "transform",
              entry.label || `Transform ${historyIndex + 1}`,
              [
                historyIndex === (table.transformHistoryIndex ?? 0) ? "Current prepared view" : null,
                Object.keys(entry.config || {}).length ? `${Object.keys(entry.config || {}).length} config groups` : "No transform options",
              ],
              {
                tableId: table.id,
                index: historyIndex,
                config: entry.config || {},
              }
            )
          );
          edges.push(createGraphEdge(tableId, transformId, historyIndex === 0 ? "starts with" : "evolves to"));
        });

        (table.charts || []).forEach((chart, chartIndex) => {
          const chartId = `chart:${table.id}:${chart.id || chartIndex}`;
          nodes.push(
            createGraphNode(
              chartId,
              "chart",
              chart.label || chart.title || `${humanizeFieldName(chart.chart_type || "chart")} chart`,
              [
                chart.chart_type ? `Type: ${humanizeFieldName(chart.chart_type)}` : null,
                chart.x_column ? `X: ${humanizeFieldName(chart.x_column)}` : null,
                chart.y_column ? `Y: ${humanizeFieldName(chart.y_column)}` : null,
                chart.id === table.activeChartId ? "Active chart" : null,
              ],
              {
                tableId: table.id,
                active: chart.id === table.activeChartId,
                chartType: chart.chart_type || null,
              }
            )
          );
          edges.push(createGraphEdge(tableId, chartId, "visualized as"));
        });
      });
    }

    workspaces.forEach((workspace, index) => {
      const workspaceId = `workspace:${workspace.id || index}`;
      const tableCount = Array.isArray(workspace.session?.tables) ? workspace.session.tables.length : 0;
      nodes.push(
        createGraphNode(
          workspaceId,
          "workspace",
          workspace.name || "Saved workspace",
          [
            workspace.filename || null,
            `${formatNumber(tableCount, 0)} tables`,
            workspace.savedAt ? `Saved ${new Date(workspace.savedAt).toLocaleString()}` : null,
          ],
          {
            workspaceId: workspace.id || null,
            lastPage: workspace.lastPage || "/prepare",
          }
        )
      );
      if (currentSession) {
        edges.push(createGraphEdge(`session:${currentSession.activeTableId || currentSession.datasetId || currentSession.filename || "current"}`, workspaceId, "saved as"));
      }
    });

    events.slice(0, 18).forEach((event, index) => {
      const eventId = `event:${event.id || index}`;
      const payload = event.payload || {};
      nodes.push(
        createGraphNode(
          eventId,
          "event",
          humanizeFieldName(event.type || "event"),
          [
            payload.filename || payload.name || payload.endpoint || null,
            event.createdAt ? new Date(event.createdAt).toLocaleString() : null,
          ],
          payload
        )
      );

      if (payload.workspaceId) {
        edges.push(createGraphEdge(`workspace:${payload.workspaceId}`, eventId, "recorded"));
      } else if (payload.activeTableId) {
        edges.push(createGraphEdge(`table:${payload.activeTableId}`, eventId, "recorded"));
      } else if (currentSession) {
        edges.push(createGraphEdge(`session:${currentSession.activeTableId || currentSession.datasetId || currentSession.filename || "current"}`, eventId, "recorded"));
      }
    });

    return {
      currentSession,
      workspaces,
      events,
      nodes,
      edges,
      summary: {
        tables: currentSession ? getWorkspaceTables(currentSession).length : 0,
        transforms: currentSession
          ? getWorkspaceTables(currentSession).reduce((total, table) => total + (table.transformHistory || []).length, 0)
          : 0,
        charts: currentSession
          ? getWorkspaceTables(currentSession).reduce((total, table) => total + (table.charts || []).length, 0)
          : 0,
        workspaces: workspaces.length,
        events: events.length,
      },
    };
  }

  function getPreviewColumns(rows) {
    return rows.length ? Object.keys(rows[0]) : [];
  }

  function filterPreviewRows(rows, options = {}) {
    const searchTerm = String(options.searchTerm || "").trim().toLowerCase();
    const sortColumn = options.sortColumn || "";
    const sortDirection = options.sortDirection === "desc" ? "desc" : "asc";

    let filtered = [...rows];
    if (searchTerm) {
      filtered = filtered.filter((row) =>
        Object.values(row).some((value) => String(value ?? "").toLowerCase().includes(searchTerm))
      );
    }

    if (sortColumn) {
      filtered.sort((left, right) => {
        const leftValue = left?.[sortColumn];
        const rightValue = right?.[sortColumn];
        const leftNumber = Number(leftValue);
        const rightNumber = Number(rightValue);
        let comparison = 0;

        if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
          comparison = leftNumber - rightNumber;
        } else {
          comparison = String(leftValue ?? "").localeCompare(String(rightValue ?? ""), undefined, { numeric: true, sensitivity: "base" });
        }

        return sortDirection === "desc" ? comparison * -1 : comparison;
      });
    }

    return filtered;
  }

  function summarizePreviewColumns(rows, limit = 6) {
    const columns = getPreviewColumns(rows);
    return columns.slice(0, limit).map((column) => {
      const values = rows.map((row) => row?.[column]).filter((value) => value !== null && value !== undefined && value !== "");
      if (!values.length) {
        return { column, label: `${column}: no values` };
      }

      const numericValues = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
      if (numericValues.length === values.length) {
        const min = Math.min(...numericValues);
        const max = Math.max(...numericValues);
        return { column, label: `${column}: ${formatNumber(min, 2)} to ${formatNumber(max, 2)}` };
      }

      const unique = new Set(values.map((value) => String(value))).size;
      return { column, label: `${column}: ${formatNumber(unique, 0)} unique value${unique === 1 ? "" : "s"}` };
    });
  }

  function populateSelect(select, columns, includeEmpty = true) {
    const previous = select.value;
    select.innerHTML = includeEmpty ? '<option value="">None</option>' : "";
    columns.forEach((column) => {
      const option = document.createElement("option");
      option.value = column;
      option.textContent = column;
      select.appendChild(option);
    });
    if (columns.includes(previous)) select.value = previous;
  }

  function populateMultiSelect(select, columns) {
    const previous = new Set(Array.from(select.selectedOptions).map((option) => option.value));
    select.innerHTML = "";
    columns.forEach((column) => {
      const option = document.createElement("option");
      option.value = column;
      option.textContent = column;
      option.selected = previous.has(column);
      select.appendChild(option);
    });
  }

  function renderSchema(tbody, schema) {
    tbody.innerHTML = "";
    Object.entries(schema).forEach(([column, meta]) => {
      const row = document.createElement("tr");
      const warnings = (meta.warnings || []).join(" ");
      row.innerHTML = `
        <td title="${escapeHtml(column)}">${column}</td>
        <td>${meta.type}</td>
        <td>${meta.role || "-"}</td>
        <td>${meta.missing}</td>
        <td>${meta.unique}</td>
        <td title="${escapeHtml(warnings || meta.aggregation_hint || "")}">${meta.aggregation_hint || "-"}</td>
        <td>${(meta.allowed_charts || []).join(", ") || "none"}</td>
      `;
      tbody.appendChild(row);
    });
  }

  function renderPreview(head, body, metaEl, rows) {
    head.innerHTML = "";
    body.innerHTML = "";
    if (!rows.length) {
      if (metaEl) metaEl.textContent = "No rows are available in the current dataset view.";
      body.innerHTML = '<tr><td>No rows available.</td></tr>';
      return;
    }

    if (metaEl) {
      metaEl.textContent = `Showing the first ${rows.length} rows from the current processed dataset.`;
    }

    const columns = Object.keys(rows[0]);
    const headRow = document.createElement("tr");
    columns.forEach((column) => {
      const th = document.createElement("th");
      th.textContent = column;
      headRow.appendChild(th);
    });
    head.appendChild(headRow);

    rows.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        const fullValue = row[column] ?? "-";
        td.textContent = truncateValue(fullValue);
        td.title = String(fullValue);
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
  }

  function renderDatasetExplanation(container, context) {
    const schema = context?.schema || {};
    const analysis = context?.analysis || {};
    const shape = context?.shape || {};
    const story = analysis.dataset_story || {};
    const intent = story.intent || {};
    const consistencyChecks = story.consistency_checks || [];
    const anomalyFlags = story.anomaly_flags || [];
    const schemaEntries = Object.entries(schema);
    const countFields = schemaEntries.filter(([, meta]) => meta.role === "count").map(([name]) => name);
    const rateFields = schemaEntries.filter(([, meta]) => meta.role === "rate").map(([name]) => name);
    const timeFields = schemaEntries.filter(([, meta]) => meta.role === "time" || meta.type === "datetime").map(([name]) => name);
    const categoryFields = schemaEntries.filter(([, meta]) => ["category", "geography"].includes(meta.role)).map(([name]) => name);

    const dimension = story.focus_dimension ? humanizeFieldName(story.focus_dimension) : null;
    const metricChoices = story.metric_choices || [];
    const primaryCountChoice = metricChoices.find((item) => item.slot === "primary_count");
    const primaryRateChoice = metricChoices.find((item) => item.slot === "primary_rate");
    const pendingCountChoice = metricChoices.find((item) => item.slot === "pending_count");

    const whatItTracks = [];
    if (intent.label) {
      whatItTracks.push(`This looks like a ${intent.label.toLowerCase()}.`);
    }
    if (dimension) {
      whatItTracks.push(`This dataset is mainly organized by ${dimension}.`);
    }
    if (shape.rows || shape.columns) {
      whatItTracks.push(`It currently contains ${formatNumber(shape.rows || 0, 0)} rows and ${formatNumber(shape.columns || 0, 0)} columns in the working view.`);
    }
    if (countFields.length) {
      whatItTracks.push(`It includes count-style fields such as ${countFields.slice(0, 3).map(humanizeFieldName).join(", ")}.`);
    }
    if (rateFields.length) {
      whatItTracks.push(`It also includes rate or percentage fields such as ${rateFields.slice(0, 2).map(humanizeFieldName).join(", ")}.`);
    }

    const compareItems = [];
    if (dimension && primaryCountChoice) {
      compareItems.push(`Compare ${dimension} values using ${humanizeFieldName(primaryCountChoice.column)}.`);
    }
    if (pendingCountChoice) {
      compareItems.push("Check which groups have the largest backlog and highest pendency.");
    }
    if (primaryRateChoice) {
      compareItems.push(`Compare groups using ${humanizeFieldName(primaryRateChoice.column)} rather than raw volume alone.`);
    }
    if (timeFields.length) {
      compareItems.push(`Track changes over time with ${humanizeFieldName(timeFields[0])}.`);
    }

    const goodFirstCharts = [];
    if (dimension && primaryCountChoice) {
      const volumeLabel = humanizeFieldName(primaryCountChoice.column);
      const needsTopN = Number(schema[story.focus_dimension || ""]?.unique || 0) > 20;
      goodFirstCharts.push(
        needsTopN
          ? `Bar chart: top ${dimension} values by ${volumeLabel}.`
          : `Bar chart: ${dimension} vs ${volumeLabel}.`
      );
    }
    if (dimension && pendingCountChoice) {
      goodFirstCharts.push(`Bar chart: ${dimension} vs ${humanizeFieldName(pendingCountChoice.column)}.`);
    }
    if (timeFields.length && primaryCountChoice) {
      goodFirstCharts.push(`Line chart: ${humanizeFieldName(timeFields[0])} over ${humanizeFieldName(primaryCountChoice.column)}.`);
    }
    if (!goodFirstCharts.length && categoryFields.length && countFields.length) {
      goodFirstCharts.push(`Bar chart: ${humanizeFieldName(categoryFields[0])} vs ${humanizeFieldName(countFields[0])}.`);
    }

    const plainEnglish = story.takeaways?.length
      ? story.takeaways.slice(0, 2)
      : analysis.key_insights?.slice(0, 2) || [];
    const whyThisReading = intent.reasons || [];
    const chosenMetrics = (story.metric_choices || []).slice(0, 3);
    const validationItems = consistencyChecks.map((item) => `
      <li><strong>${escapeHtml(item.title)}:</strong> ${escapeHtml(item.message)}</li>
    `).join("");
    const anomalyItems = anomalyFlags.map((item) => `<li>${escapeHtml(item)}</li>`).join("");

    container.innerHTML = `
      <div class="analysis-card">
        <h3>What this dataset tracks</h3>
        ${whatItTracks.length
          ? `<ul class="insight-list">${whatItTracks.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
          : '<p class="empty-copy">Upload a dataset to see a plain-English explanation here.</p>'}
      </div>
      <div class="analysis-card">
        <h3>What you can compare</h3>
        ${compareItems.length
          ? `<ul class="insight-list">${compareItems.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
          : '<p class="empty-copy">The app will list the most useful comparisons after it profiles the dataset structure.</p>'}
      </div>
      <div class="analysis-card">
        <h3>Why the app reads it this way</h3>
        ${whyThisReading.length
          ? `
              <p class="empty-copy">Confidence: ${escapeHtml(formatNumber((intent.confidence || 0) * 100, 0))}%</p>
              <ul class="insight-list">${whyThisReading.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
            `
          : '<p class="empty-copy">The app will explain its dataset reading here after it profiles the table shape.</p>'}
        ${chosenMetrics.length
          ? `
              <div class="analysis-subsection">
                <h4>Chosen focus metrics</h4>
                <ul class="insight-list">
                  ${chosenMetrics.map((item) => `<li><strong>${escapeHtml(item.slot_label)}:</strong> ${escapeHtml(humanizeFieldName(item.column))}</li>`).join("")}
                </ul>
              </div>
            `
          : ""}
      </div>
      <div class="analysis-card">
        <h3>Good first charts</h3>
        ${goodFirstCharts.length
          ? `<ul class="insight-list">${goodFirstCharts.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
          : '<p class="empty-copy">Once the app finds clear dimensions and measures, this section will suggest chart starting points.</p>'}
      </div>
      <div class="analysis-card">
        <h3>Plain-English takeaway</h3>
        ${plainEnglish.length
          ? `<ul class="insight-list">${plainEnglish.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
          : '<p class="empty-copy">No clear dataset takeaway is available yet for this view.</p>'}
      </div>
      <div class="analysis-card">
        <h3>Checks and anomalies</h3>
        ${validationItems
          ? `<ul class="insight-list">${validationItems}</ul>`
          : '<p class="empty-copy">No built-in totals or formula rows were available, so consistency checks are limited for this dataset view.</p>'}
        ${anomalyItems
          ? `
              <div class="analysis-subsection">
                <h4>Things to watch</h4>
                <ul class="insight-list">${anomalyItems}</ul>
              </div>
            `
          : ""}
      </div>
    `;
  }

function renderAnalysis(container, analysis, shape) {
    const typeCounts = analysis.type_counts || {};
    const datasetStory = analysis.dataset_story || {};
    const storyIntent = datasetStory.intent || {};
    function summarizeWarnings(warnings) {
      const items = Array.isArray(warnings) ? warnings.filter(Boolean) : [];
      if (!items.length) {
        return [];
      }

      const summaries = [];
      const used = new Set();
      const labelWarnings = items.filter((item) => item.includes(": Many unique labels; prefer bar charts with filters over pie charts."));

      if (labelWarnings.length) {
        const labels = labelWarnings.map((item) => item.split(":")[0]).filter(Boolean);
        const sample = labels.slice(0, 3);
        const extra = Math.max(labels.length - sample.length, 0);
        summaries.push(
          `${sample.join(", ")}${extra ? ` and ${extra} more` : ""}: High-cardinality labels are easier to read with top-N bar charts than full pie charts.`
        );
        labelWarnings.forEach((item) => used.add(item));
      }

      const geographyWarning = items.find((item) => item.startsWith("Geography-like columns such as "));
      if (geographyWarning) {
        summaries.push(geographyWarning);
        used.add(geographyWarning);
      }

      const timelineWarning = items.find((item) => item.includes("Very low numeric variety") && item.startsWith("Year:"));
      if (timelineWarning) {
        summaries.push("Year has a limited set of distinct values here, so it behaves more like a timeline or category than a continuous measure.");
        used.add(timelineWarning);
      }

      items.forEach((item) => {
        if (!used.has(item) && summaries.length < 4) {
          summaries.push(item);
        }
      });

      return summaries.slice(0, 4);
    }

    const summaryCards = [
      { label: "Rows analyzed", value: formatNumber(analysis.row_count ?? shape?.rows, 0) },
      { label: "Columns analyzed", value: formatNumber(analysis.column_count ?? shape?.columns, 0) },
      { label: "Numeric columns", value: formatNumber(typeCounts.numeric ?? 0, 0) },
      { label: "Categorical columns", value: formatNumber(typeCounts.categorical ?? 0, 0) },
    ];

    const insightItems = (analysis.key_insights || []).length
      ? analysis.key_insights.map((item) => `<li>${escapeHtml(item)}</li>`).join("")
      : "<li>No standout insights were generated for the current dataset view.</li>";

    const relationshipRows = (analysis.top_correlations || []).slice(0, 5).map((item) => `
      <tr>
        <td>${escapeHtml(item.left)}</td>
        <td>${escapeHtml(item.right)}</td>
        <td>${escapeHtml(formatNumber(item.value, 4))}</td>
      </tr>
    `).join("");

    const groupComparison = analysis.group_comparison;
    const groupRows = groupComparison && (groupComparison.largest_mean_gaps || []).length
      ? groupComparison.largest_mean_gaps.slice(0, 5).map((item) => `
          <tr>
            <td>${escapeHtml(item.metric)}</td>
            <td>${escapeHtml(item.highest_group)} (${escapeHtml(formatNumber(item.highest_mean, 4))})</td>
            <td>${escapeHtml(item.lowest_group)} (${escapeHtml(formatNumber(item.lowest_mean, 4))})</td>
            <td>${escapeHtml(formatNumber(item.difference, 4))}</td>
          </tr>
        `).join("")
      : "";

    const numericCards = Object.entries(analysis.numeric_summary || {}).slice(0, 6).map(([field, stats]) => `
      <div class="stats-card">
        <strong>${escapeHtml(field)}</strong>
        <div class="metric-pair"><span>Mean</span><span>${escapeHtml(formatNumber(stats.mean, 4))}</span></div>
        <div class="metric-pair"><span>Median</span><span>${escapeHtml(formatNumber(stats["50%"], 4))}</span></div>
        <div class="metric-pair"><span>Min</span><span>${escapeHtml(formatNumber(stats.min, 4))}</span></div>
        <div class="metric-pair"><span>Max</span><span>${escapeHtml(formatNumber(stats.max, 4))}</span></div>
      </div>
    `).join("");

    const categoryCards = Object.entries(analysis.categorical_breakdown || {}).map(([field, values]) => `
      <div class="stats-card">
        <strong>${escapeHtml(field)}</strong>
        <div class="chip-list">
          ${Object.entries(values).slice(0, 8).map(([label, count]) => `<span class="chip">${escapeHtml(label)}: ${escapeHtml(formatNumber(count, 0))}</span>`).join("") || '<span class="chip">No values</span>'}
        </div>
      </div>
    `).join("");

    const summaryLineChips = (analysis.summary_lines || "")
      .split("\n")
      .filter(Boolean)
      .map((line) => `<span class="chip">${escapeHtml(line)}</span>`)
      .join("");

    const warningItems = summarizeWarnings(analysis.warnings || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
    const storyOverview = (datasetStory.overview || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
    const storyTakeaways = (datasetStory.takeaways || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
    const storyChecks = (datasetStory.consistency_checks || []).map((item) => `
      <li><strong>${escapeHtml(item.title)}:</strong> ${escapeHtml(item.message)}</li>
    `).join("");
    const storyFlags = (datasetStory.anomaly_flags || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
    const headlineMetricCards = (datasetStory.headline_metrics || []).map((item) => `
      <div class="summary-card">
        <strong>${escapeHtml(item.formatted_value || formatNumber(item.value, 1))}</strong>
        <span>${escapeHtml(item.label)}</span>
      </div>
    `).join("");
    const rankingSections = (datasetStory.ranking_sections || []).map((section) => {
      const items = (section.items || []).map((item, index) => `
        <li>
          <strong>${index + 1}. ${escapeHtml(item.label)}</strong>
          <span>${escapeHtml(item.formatted_value || formatNumber(item.value, 1))}${item.formatted_secondary ? ` | ${escapeHtml(item.formatted_secondary)}` : ""}</span>
        </li>
      `).join("");
      return `
        <div class="analysis-card">
          <h3>${escapeHtml(section.title)}</h3>
          ${items
            ? `<ol class="insight-list ranked-list">${items}</ol>`
            : '<p class="empty-copy">No ranked values are available for this section.</p>'}
        </div>
      `;
    }).join("");

    container.innerHTML = `
      <div class="summary-grid">
        ${summaryCards.map((item) => `
          <div class="summary-card">
            <strong>${escapeHtml(item.value)}</strong>
            <span>${escapeHtml(item.label)}</span>
          </div>
        `).join("")}
      </div>
      ${headlineMetricCards
        ? `<div class="summary-grid story-grid">${headlineMetricCards}</div>`
        : ""}
      <div class="analysis-stack">
        ${storyOverview || storyTakeaways || rankingSections
          ? `
            <div class="analysis-card">
              <h3>Dataset story</h3>
              ${storyIntent.label
                ? `<p class="empty-copy">Detected dataset shape: <strong>${escapeHtml(storyIntent.label)}</strong>.</p>`
                : ""}
              ${datasetStory.focus_dimension
                ? `<p class="empty-copy">The app is reading this as a comparison dataset organized by <strong>${escapeHtml(humanizeFieldName(datasetStory.focus_dimension))}</strong>.</p>`
                : ""}
              ${storyOverview ? `<ul class="insight-list">${storyOverview}</ul>` : ""}
              ${storyTakeaways
                ? `
                  <div class="analysis-subsection">
                    <h4>What stands out</h4>
                    <ul class="insight-list">${storyTakeaways}</ul>
                  </div>
                `
                : ""}
            </div>
            ${rankingSections}
          `
          : ""}
        <div class="analysis-card">
          <h3>Quick read</h3>
          <ul class="insight-list">${insightItems}</ul>
          <div class="chip-list">${summaryLineChips || '<span class="chip">No summary available yet</span>'}</div>
        </div>
        <div class="analysis-card">
          <h3>Data cautions</h3>
          ${warningItems
            ? `<ul class="insight-list">${warningItems}</ul>`
            : '<p class="empty-copy">No major rule-based cautions were detected for the current dataset view.</p>'}
        </div>
        <div class="analysis-card">
          <h3>Validation and anomalies</h3>
          ${storyChecks
            ? `<ul class="insight-list">${storyChecks}</ul>`
            : '<p class="empty-copy">No built-in totals or formula rows were available, so consistency checks are limited for this dataset view.</p>'}
          ${storyFlags
            ? `
                <div class="analysis-subsection">
                  <h4>Standout flags</h4>
                  <ul class="insight-list">${storyFlags}</ul>
                </div>
              `
            : ""}
        </div>
        <div class="analysis-card">
          <h3>Strongest relationships</h3>
          ${relationshipRows
            ? `<div class="table-wrap"><table><thead><tr><th>Field A</th><th>Field B</th><th>Correlation</th></tr></thead><tbody>${relationshipRows}</tbody></table></div>`
            : '<p class="empty-copy">Not enough numeric fields were available to compare relationships.</p>'}
        </div>
        <div class="analysis-card">
          <h3>Group comparison${groupComparison ? ` for ${escapeHtml(groupComparison.group_column)}` : ""}</h3>
          ${groupRows
            ? `<div class="table-wrap"><table><thead><tr><th>Metric</th><th>Highest group</th><th>Lowest group</th><th>Difference</th></tr></thead><tbody>${groupRows}</tbody></table></div>`
            : '<p class="empty-copy">No categorical split was available for a group comparison in this dataset.</p>'}
        </div>
        <div class="analysis-card">
          <h3>Sample numeric summary</h3>
          ${numericCards
            ? `<div class="stats-grid">${numericCards}</div>`
            : '<p class="empty-copy">No numeric summary was available for the current dataset.</p>'}
        </div>
        <div class="analysis-card">
          <h3>Category counts</h3>
          ${categoryCards
            ? `<div class="stats-grid">${categoryCards}</div>`
            : '<p class="empty-copy">No categorical breakdown was available for the current dataset.</p>'}
        </div>
      </div>
    `;
  }

  function getColumnsForTypes(schema, types) {
    return Object.entries(schema)
      .filter(([, meta]) => types.includes(meta.type))
      .map(([name]) => name);
  }

  function getColumnsForRoles(schema, roles, allowedTypes = []) {
    return Object.entries(schema)
      .filter(([, meta]) => roles.includes(meta.role) && (!allowedTypes.length || allowedTypes.includes(meta.type)))
      .map(([name]) => name);
  }

  function uniqueColumns(columns) {
    return [...new Set(columns)];
  }

  function getChartCompatibility(chartType, schema) {
    const identifierLike = new Set(getColumnsForRoles(schema, ["identifier"]));
    const categorical = uniqueColumns(
      getColumnsForRoles(schema, ["category", "geography"], ["categorical", "text"])
        .concat(getColumnsForTypes(schema, ["categorical", "text"]).filter((name) => !identifierLike.has(name)))
    );
    const datetime = uniqueColumns(
      getColumnsForTypes(schema, ["datetime"]).concat(getColumnsForRoles(schema, ["time"]))
    );
    const numeric = Object.entries(schema)
      .filter(([, meta]) => meta.type === "numeric" && !["identifier", "constant"].includes(meta.role))
      .map(([name]) => name);
    const lineMeasures = Object.entries(schema)
      .filter(([, meta]) => meta.type === "numeric" && !["identifier", "constant", "time"].includes(meta.role))
      .map(([name]) => name);
    const rates = getColumnsForRoles(schema, ["rate"], ["numeric"]);
    const counts = getColumnsForRoles(schema, ["count"], ["numeric"]);
    const measures = getColumnsForRoles(schema, ["measure"], ["numeric"]);
    const allColumns = Object.entries(schema)
      .filter(([, meta]) => meta.role !== "identifier")
      .map(([name]) => name);
    const preferredNumeric = uniqueColumns(counts.concat(rates).concat(measures).concat(numeric));

    switch (chartType) {
      case "histogram":
      case "box":
        return {
          xColumns: uniqueColumns(rates.concat(measures).concat(counts).concat(numeric)),
          yColumns: [],
          rowColumns: [],
          yOptional: true,
          rowOptional: true,
          hint: numeric.length
            ? `This ${chartType} chart needs one numeric measure, rate, or count column.`
            : `No numeric columns are available for a ${chartType} chart.`,
        };
      case "scatter":
        return {
          xColumns: numeric,
          yColumns: numeric,
          rowColumns: [],
          yOptional: false,
          rowOptional: true,
          hint: numeric.length >= 2
            ? "Scatter plots need two numeric columns."
            : "You need at least two numeric columns for a scatter plot.",
        };
      case "line":
      case "area":
        return {
          xColumns: datetime,
          yColumns: lineMeasures,
          rowColumns: [],
          yOptional: false,
          rowOptional: true,
          hint: datetime.length && lineMeasures.length
            ? `${chartType === "area" ? "Area" : "Line"} charts need a date/time column on X and a numeric column on Y.`
            : `You need one datetime column and one numeric column for an ${chartType} chart.`,
        };
      case "pie":
        return {
          xColumns: categorical,
          yColumns: preferredNumeric,
          rowColumns: [],
          yOptional: true,
          rowOptional: true,
          hint: categorical.length
            ? "Pie charts work best with a few categories and a meaningful part-to-whole measure. Geography fields often need filtering first."
            : "You need a categorical column for a pie chart.",
        };
      case "heatmap":
        return {
          xColumns: uniqueColumns(datetime.concat(categorical)),
          yColumns: preferredNumeric,
          rowColumns: categorical.length ? categorical.concat(datetime) : allColumns,
          yOptional: true,
          rowOptional: false,
          hint: "Heatmaps work best with two grouping fields, such as year and country, plus an optional numeric measure to color each cell.",
        };
      case "bar":
      default:
        return {
          xColumns: categorical.length ? categorical.concat(datetime) : allColumns,
          yColumns: preferredNumeric,
          rowColumns: [],
          yOptional: true,
          rowOptional: true,
          hint: "Bar charts work best with a category, geography, or time field on X and a count, rate, or measure on Y.",
        };
    }
  }

  function recommendChartConfig(schema) {
    const entries = Object.entries(schema);
    const numeric = entries
      .filter(([, meta]) => meta.type === "numeric" && !["identifier", "constant"].includes(meta.role))
      .map(([name]) => name);
    const countColumns = getColumnsForRoles(schema, ["count"], ["numeric"]);
    const rateColumns = getColumnsForRoles(schema, ["rate"], ["numeric"]);
    const measureColumns = getColumnsForRoles(schema, ["measure"], ["numeric"]);
    const categorical = uniqueColumns(
      getColumnsForRoles(schema, ["geography", "category"], ["categorical", "text"])
        .concat(entries.filter(([, meta]) => ["categorical", "text"].includes(meta.type) && meta.role !== "identifier").map(([name]) => name))
    );
    const datetime = entries.filter(([, meta]) => meta.type === "datetime" || meta.role === "time").map(([name]) => name);
    const primaryMeasure = countColumns[0] || rateColumns[0] || measureColumns[0] || numeric[0];

    if (datetime.length && primaryMeasure) {
      return {
        chartType: "line",
        xColumn: datetime[0],
        yColumn: primaryMeasure,
        message: `Suggested chart: line chart using ${datetime[0]} over ${primaryMeasure}.`,
      };
    }

    if (categorical.length && primaryMeasure) {
      const xMeta = schema[categorical[0]];
      return {
        chartType: "bar",
        xColumn: categorical[0],
        yColumn: primaryMeasure,
        message: xMeta?.role === "geography"
          ? `Suggested chart: bar chart comparing ${primaryMeasure} across ${categorical[0]}. Pie charts may need top-N filtering first.`
          : `Suggested chart: bar chart using ${categorical[0]} and ${primaryMeasure}.`,
      };
    }

    if (numeric.length >= 2) {
      return {
        chartType: "scatter",
        xColumn: numeric[0],
        yColumn: numeric[1],
        message: `Suggested chart: scatter plot using ${numeric[0]} and ${numeric[1]}.`,
      };
    }

    if (numeric.length) {
      const xColumn = rateColumns[0] || measureColumns[0] || countColumns[0] || numeric[0];
      return {
        chartType: "histogram",
        xColumn,
        yColumn: "",
        message: `Suggested chart: histogram of ${xColumn}.`,
      };
    }

    if (categorical.length) {
      return {
        chartType: "bar",
        xColumn: categorical[0],
        yColumn: "",
        message: `Suggested chart: bar chart of ${categorical[0]}.`,
      };
    }

    return {
      chartType: "bar",
      xColumn: entries[0]?.[0] || "",
      yColumn: "",
      message: "Upload a dataset to get a chart suggestion.",
    };
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || "Request failed.");
    return data;
  }

  async function downloadBinary(endpoint, payload, fallbackName) {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const responsePayload = await response.json().catch(() => ({}));
      throw new Error(responsePayload.error || "Download failed.");
    }
    const blob = await response.blob();
    const disposition = response.headers.get("Content-Disposition") || "";
    const filename = disposition.split("filename=")[1]?.replace(/"/g, "") || fallbackName;
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
    recordMemoryEvent("export_downloaded", {
      endpoint,
      filename,
      datasetId: payload?.dataset_id || null,
      chartType: payload?.chart_type || null,
      dashboardColumns: payload?.dashboard_columns || null,
    });
    return filename;
  }

  function requireSession(redirectTo = "/") {
    const session = loadSession();
    if (!session) {
      window.location.href = redirectTo;
      return null;
    }
    return session;
  }

  window.dataTool = {
    saveSession,
    loadSession,
    clearSession,
    createMultiTableSession,
    getWorkspaceTables,
    getActiveTableRecord,
    setActiveTable,
    replaceSessionState,
    setStatusText,
    renderWorkspaceTablePanel,
    switchWorkspaceTable,
    ensureDatasetState,
    saveWorkspaceFlow,
    escapeHtml,
    formatNumber,
    populateSelect,
    populateMultiSelect,
    renderSchema,
    renderPreview,
    filterPreviewRows,
    summarizePreviewColumns,
    getPreviewColumns,
    renderDatasetExplanation,
    renderAnalysis,
    getChartCompatibility,
    recommendChartConfig,
    fetchJson,
    downloadBinary,
    requireSession,
    loadWorkspaces,
    storeWorkspace,
    deleteWorkspace,
    getWorkspace,
    loadMemoryEvents,
    clearMemoryEvents,
    recordMemoryEvent,
    buildWorkspaceMemoryGraph,
  };
})();
