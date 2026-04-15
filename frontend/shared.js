(function () {
  const STORAGE_KEY = "data_visualisation_tool_session";

  function createStorageSafeSession(session) {
    const charts = Array.isArray(session?.charts)
      ? session.charts.map((chart) => {
          if (!chart) return chart;
          const clone = { ...chart };
          delete clone.chart_data_url;
          delete clone.mime_type;
          return clone;
        })
      : [];

    const chartConfig = session?.chartConfig
      ? (() => {
          const clone = { ...session.chartConfig };
          delete clone.chart_data_url;
          delete clone.mime_type;
          return clone;
        })()
      : null;

    const datasetState = session?.datasetId ? null : (session?.datasetState || session?.dataset_state || null);

    return {
      ...session,
      charts,
      chartConfig,
      datasetState,
      dataset_state: datasetState,
    };
  }

  function createMinimalSession(session) {
    return {
      datasetId: session?.datasetId || null,
      filename: session?.filename || null,
      shape: session?.shape || null,
      schema: session?.schema || null,
      datasetState: null,
      dataset_state: null,
      transformConfig: session?.transformConfig || {},
      charts: Array.isArray(session?.charts)
        ? session.charts.map((chart) => ({
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
          }))
        : [],
      activeChartId: session?.activeChartId || null,
      dashboardMode: session?.dashboardMode || "single",
      dashboardColumns: session?.dashboardColumns || 2,
      chartConfig: session?.chartConfig
        ? {
            id: session.chartConfig.id,
            title: session.chartConfig.title,
            chart_type: session.chartConfig.chart_type,
            x_column: session.chartConfig.x_column,
            y_column: session.chartConfig.y_column,
            format: session.chartConfig.format,
            chart_file: session.chartConfig.chart_file,
            note: session.chartConfig.note || "",
            chart_options: session.chartConfig.chart_options || {},
          }
        : null,
    };
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
      return JSON.parse(localStorage.getItem(STORAGE_KEY) || "null");
    } catch {
      return null;
    }
  }

  function clearSession() {
    localStorage.removeItem(STORAGE_KEY);
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

  function renderAnalysis(container, analysis, shape) {
    const typeCounts = analysis.type_counts || {};
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

    const warningItems = (analysis.warnings || []).slice(0, 5).map((item) => `<li>${escapeHtml(item)}</li>`).join("");

    container.innerHTML = `
      <div class="summary-grid">
        ${summaryCards.map((item) => `
          <div class="summary-card">
            <strong>${escapeHtml(item.value)}</strong>
            <span>${escapeHtml(item.label)}</span>
          </div>
        `).join("")}
      </div>
      <div class="analysis-stack">
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
        return {
          xColumns: datetime,
          yColumns: lineMeasures,
          rowColumns: [],
          yOptional: false,
          rowOptional: true,
          hint: datetime.length && lineMeasures.length
            ? "Line charts need a date/time column on X and a numeric column on Y."
            : "You need one datetime column and one numeric column for a line chart.",
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
    escapeHtml,
    formatNumber,
    populateSelect,
    populateMultiSelect,
    renderSchema,
    renderPreview,
    renderAnalysis,
    getChartCompatibility,
    recommendChartConfig,
    fetchJson,
    downloadBinary,
    requireSession,
  };
})();
