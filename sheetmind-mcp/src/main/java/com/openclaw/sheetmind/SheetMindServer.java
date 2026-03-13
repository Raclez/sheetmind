package com.openclaw.sheetmind;

import com.github.pjfanning.xlsx.StreamingReader;
import com.github.thought2code.mcp.annotated.McpServers;
import com.github.thought2code.mcp.annotated.annotation.McpServerApplication;
import com.github.thought2code.mcp.annotated.annotation.McpTool;

import com.github.thought2code.mcp.annotated.annotation.McpToolParam;
import com.github.thought2code.mcp.annotated.configuration.McpServerConfiguration;
import org.apache.commons.jexl3.introspection.JexlSandbox;
import java.util.regex.Pattern;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * SheetMind MCP Server - Excel processing service for AI agents
 * 
 * Handles large Excel files (millions of rows) with streaming I/O
 * to prevent OOM and provide accurate data filtering for AI.
 */
@McpServerApplication
public class SheetMindServer {

    private static final Logger logger = LoggerFactory.getLogger(SheetMindServer.class);

    // ========== 核心安全与性能配置 ==========
    // 强制限制大模型的读写范围，防止读取系统密码或敏感配置
    private static final String WORKSPACE_DIR = System.getProperty("user.home") + File.separator + "SheetMind_Workspace";
    private static final List<String> ALLOWED_DIRECTORIES = new ArrayList<>();
    private static final int UNIQUE_VALUE_LIMIT = 10000;
    private static final int DEFAULT_SEARCH_LIMIT = 20;
    private static final int STREAMING_ROW_CACHE_SIZE = 100;
    private static final int STREAMING_BUFFER_SIZE = 4096;
    private static final DataFormatter DATA_FORMATTER = new DataFormatter();

    // 🔒 JEXL 安全沙箱配置：只允许基础字符串和数学操作，彻底阻断 RCE 注入
    // 支持操作：> < == != >= <= && || 以及字符串方法调用 (name.contains("x"), name.toUpperCase() 等)
    private static final JexlEngine JEXL_ENGINE;
    static {
        JexlSandbox sandbox = new JexlSandbox(false);
        sandbox.allow(String.class.getName());
        sandbox.allow(Math.class.getName());
        sandbox.allow(Integer.class.getName());
        sandbox.allow(Double.class.getName());
        sandbox.allow(Boolean.class.getName());
        sandbox.allow(Pattern.class.getName());

        JEXL_ENGINE = new JexlBuilder()
                .sandbox(sandbox)
                .strict(true)
                .silent(false)
                .cache(512)
                .create();
    }

    private Sheet getSheet(Workbook workbook, String sheetName, Integer sheetIndex) {
        if (sheetName != null && !sheetName.isBlank()) {
            Sheet sheet = workbook.getSheet(sheetName);
            if (sheet == null) {
                throw new IllegalArgumentException("Sheet not found: " + sheetName);
            }
            return sheet;
        } else if (sheetIndex != null) {
            int maxIndex = workbook.getNumberOfSheets() - 1;
            if (sheetIndex < 0 || sheetIndex > maxIndex) {
                throw new IllegalArgumentException("Sheet index out of range. Available: 0-" + maxIndex);
            }
            return workbook.getSheetAt(sheetIndex);
        }
        return workbook.getSheetAt(0);
    }

    // ========== 工具 0: 目录扫描 ==========
    @McpTool(name = "scan_directory", description = "扫描目录下所有Excel文件，支持递归扫描和文件过滤。\n" +
            "【参数】：\n" +
            "  - directoryPath: 目录路径\n" +
            "  - recursive: 是否递归扫描子目录（默认false）\n" +
            "  - pattern: 文件名过滤（支持*通配符，如 *.xlsx）\n" +
            "【返回】：目录下的文件列表，包含每个文件的基本信息")
    public Map<String, Object> scanDirectory(
            @McpToolParam(name = "directoryPath", description = "目录路径") String directoryPath,
            @McpToolParam(name = "recursive", description = "是否递归扫描子目录") Boolean recursive,
            @McpToolParam(name = "pattern", description = "文件名过滤模式") String pattern) {
        try {
            File dir = getSafeFile(directoryPath);
            if (!dir.exists() || !dir.isDirectory()) {
                throw new IllegalArgumentException("目录不存在或不是有效目录: " + directoryPath);
            }

            boolean recursiveScan = recursive != null && recursive;
            String filePattern = pattern != null && !pattern.isBlank() ? pattern : "*.xlsx";

            String regex = filePattern.replace("*", ".*").replace("?", ".");
            Pattern compiledPattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

            List<Map<String, Object>> files = Collections.synchronizedList(new ArrayList<>());

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                scanFilesVirtual(dir, recursiveScan, compiledPattern, files, executor);
            }

            return successResponse(Map.of(
                    "directory", dir.getAbsolutePath(),
                    "recursive", recursiveScan,
                    "pattern", filePattern,
                    "fileCount", files.size(),
                    "files", files
            ));
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private void scanFilesVirtual(File dir, boolean recursive, Pattern pattern,
                                   List<Map<String, Object>> files, ExecutorService executor) throws ExecutionException, InterruptedException {
        File[] fileList = dir.listFiles();
        if (fileList == null) return;

        List<Future<?>> subDirFutures = new ArrayList<>();

        for (File file : fileList) {
            if (file.isDirectory() && recursive) {
                Future<?> future = executor.submit(() -> {
                    try {
                        scanFilesVirtual(file, recursive, pattern, files, executor);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                subDirFutures.add(future);
            } else if (file.isFile() && pattern.matcher(file.getName()).matches()) {
                Map<String, Object> fileInfo = new LinkedHashMap<>();
                fileInfo.put("name", file.getName());
                fileInfo.put("path", file.getAbsolutePath());
                fileInfo.put("size", file.length());
                fileInfo.put("lastModified", java.time.Instant.ofEpochMilli(file.lastModified()).toString());
                files.add(fileInfo);
            }
        }

        for (Future<?> future : subDirFutures) {
            future.get();
        }
    }

    // ========== 工具 0: 列出所有 Sheet ==========
    @McpTool(name = "list_sheets", description = "列出Excel文件中的所有Sheet名称。\n" +
            "【用途】：快速了解文件结构，确认目标Sheet名称后再调用 inspect_spreadsheet 查看表结构。")
    public Map<String, Object> listSheets(@McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath) {
        try {
            File file = getSafeFile(filePath);
            try (Workbook workbook = StreamingReader.builder().rowCacheSize(100).open(file)) {
                List<String> sheetNames = new ArrayList<>();
                for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                    sheetNames.add(workbook.getSheetAt(i).getSheetName());
                }
                return successResponse(Map.of(
                        "fileName", file.getName(),
                        "sheetCount", workbook.getNumberOfSheets(),
                        "sheetNames", sheetNames
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 1: 结构探测 ==========
    @McpTool(name = "inspect_spreadsheet", description = "获取 Excel 工作表元数据和前 5 行预览数据。\n" +
            "【AI 调用时机】：在进行任何搜索或修改前，必须先调用此工具了解该表有哪些精确的列名（headers）！\n" +
            "【提示】：它能帮你确认日期是否带有特殊格式，以及列名的确切叫法（如是否有空格）。")

    public Map<String, Object> inspectSpreadsheet(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName) {
        try {
            File file = getSafeFile(filePath);
            try (Workbook workbook = StreamingReader.builder().rowCacheSize(100).open(file)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();
                List<Map<String, Object>> previewRows = new ArrayList<>();
                List<String> headers = new ArrayList<>();

                int rowCount = 0;
                if (rowIterator.hasNext()) {
                    Row headerRow = rowIterator.next();
                    rowCount++;
                    for (Cell cell : headerRow) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                while (rowIterator.hasNext() && previewRows.size() < 5) {
                    Row row = rowIterator.next();
                    rowCount++;
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < headers.size(); i++) {
                        Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        rowData.put(headers.get(i), getCellValue(cell));
                    }
                    previewRows.add(rowData);
                }

        // 构建列字母映射表，方便 AI 写 JEXL
                Map<String, String> columnMapping = new LinkedHashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    columnMapping.put("col_" + convertIndexToColumnLetter(i), headers.get(i));
                }
                return Map.of(
                        "success", true,
                        "fileName", file.getName(),
                        "sheetName", sheet.getSheetName(),
                        "previewRowCount", rowCount,
                        "columnMapping", columnMapping, // 新增这一行：把映射表发给 AI
                        "headers", headers,
                        "preview", previewRows,
                        "note", "流式读取，此为前置预览。若需查找完整数据请调用 smart_search_rows。"
                );
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 2: 智能流式检索 ==========
    @McpTool(name = "smart_search_rows", description = "使用 JEXL 引擎流式检索 Excel 数据行。\n" +
            "【🚨 语法红线：仅限纯 Java 表达式，禁用 SQL/Python 及中文变量】\n" +
            "1. 列名引用：必须先调用 inspect_spreadsheet 获取 columnMapping，强制使用 `col_字母`（如 col_A），严禁写入中文。\n" +
            "2. 逻辑运算：仅支持 && (与) 和 || (或)，严禁 and / or。\n" +
            "3. 文本搜索：强制使用原生 Java 字符串方法，如 col_A.toString().matches('.*关键字.*')。\n" +
            "4. 数值比较：col_B > 1000, col_C == '精确值'。\n" +
            "【完美示例】：col_C.toString().matches('.*黄金.*') && col_E > 3000")
    public Map<String, Object> smartSearchRows(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "query", description = "JEXL查询表达式") String query,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName) {
        try {
            File file = getSafeFile(filePath);
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            JexlExpression expression = (query != null && !query.isBlank()) ? JEXL_ENGINE.createExpression(query) : null;

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(100).open(file)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                List<Map<String, Object>> results = new ArrayList<>();
                int totalFiltered = 0, totalProcessed = 0, skipCount = 0;

                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    totalProcessed++;
                    MapContext context = createJexlContext(headers, row);

                    if (expression == null || evaluateExpression(expression, context)) {
                        if (skipCount < offset) {
                            skipCount++;
                        } else if (results.size() < limit) {
                            Map<String, Object> rowData = new LinkedHashMap<>();
                            for (int i = 0; i < headers.size(); i++) {
                                rowData.put(headers.get(i), getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)));
                            }
                            results.add(rowData);
                            totalFiltered++;
                        } else {
                            // Hit limit, check if has more
                            boolean hasMore = true;
                            return successResponse(Map.of(
                                    "rowsProcessed", totalProcessed,
                                    "returnedCount", results.size(),
                                    "results", results,
                                    "pagination", Map.of("limit", limit, "offset", offset, "hasMore", hasMore)
                            ));
                        }
                    }
                }
                return successResponse(Map.of(
                        "rowsProcessed", totalProcessed, "returnedCount", results.size(), "results", results,
                        "pagination", Map.of("limit", limit, "offset", offset, "hasMore", false)
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 3: 原子无内存泄漏修改 (Stream-Copy-Append) ==========
    // 定义大文件阈值：30MB (约等于四五十万行，再大极易 OOM)
    private static final long LARGE_FILE_THRESHOLD = 30 * 1024 * 1024L;
    private static final int FORMULA_MAX_ROWS = 10000;
    private static final int FORMULA_ROW_ESTIMATE_SAMPLE = 100;

    @McpTool(name = "update_cell", description = "精准更新特定单元格。注意：为保护系统内存，不支持修改大于 30MB 的文件。\n" +
            "【⚠️ 致命警告：索引机制】\n" +
            "传入的 row 和 col 必须是基于 0 的程序索引 (0-based index)！\n" +
            " - row 索引：Excel 的第 1 行（通常是表头）row=0；第 2 行 row=1。\n" +
            " - col 索引：Excel 的 A 列 col=0；B 列 col=1；C 列 col=2。\n" +
            "【举例】：如果要修改 Excel 中第 2 行、C 列的数据，必须传入 row: 1, col: 2。请在内部计算好再调用。")    public Map<String, Object> updateCell(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "row", description = "行索引") int row,
            @McpToolParam(name = "col", description = "列索引") int col,
            @McpToolParam(name = "value", description = "新值") String value,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName) {
        try {
            File safeFile = getSafeFile(filePath);
            File backupPath = new File(safeFile.getAbsolutePath() + ".bak");
            File tempPath = new File(safeFile.getAbsolutePath() + ".tmp");
            boolean backupExists = false;

            try {
                if (safeFile.length() > LARGE_FILE_THRESHOLD) {
                    return Map.of(
                            "success", false,
                            "error", String.format("文件过大 (%.1f MB)。为防止服务器内存溢出，拒绝执行 update_cell。请手动修改或使用 Python 脚本处理。",
                                    safeFile.length() / (1024.0 * 1024.0))
                    );
                }

                Files.copy(safeFile.toPath(), backupPath.toPath(), StandardCopyOption.REPLACE_EXISTING);
                backupExists = true;

                try (XSSFWorkbook writeWb = new XSSFWorkbook(safeFile);
                     FileOutputStream fos = new FileOutputStream(tempPath)) {

                    Sheet targetSheet = getSheet(writeWb, sheetName, null);
                    Row targetRow = targetSheet.getRow(row);
                    if (targetRow == null) {
                        targetRow = targetSheet.createRow(row);
                    }
                    Cell cell = targetRow.getCell(col, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    cell.setCellValue(value);

                    writeWb.write(fos);
                }

                Files.move(tempPath.toPath(), safeFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                Files.deleteIfExists(backupPath.toPath());

                return successResponse(Map.of("message", String.format("成功更新单元格 [%d,%d] 为 '%s'", row, col, value)));

            } catch (Exception e) {
                try {
                    Files.deleteIfExists(tempPath.toPath());
                    if (backupExists) {
                        Files.move(backupPath.toPath(), safeFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException ignored) {}
                return errorResponse(e);
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 4: 数据统计分析 ==========
    @McpTool(name = "summarize_column", description = "计算指定数值列的统计信息（总和、平均值、最大最小值等）。\n" +
            "【参数建议】：column 参数直接传入汉字/英文列名（如 '交易金额'、'Amount'），不要传数字索引。\n" +
            "【特性】：自动跳过文本或脏数据，只对有效数字计算。")
    public Map<String, Object> summarizeColumn(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "column", description = "列标识") String column,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName) {
        try {
            File safeFile = getSafeFile(filePath);
            try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                int colIndex = -1;
                if (column.matches("\\d+")) {
                    colIndex = Integer.parseInt(column); // 支持传数字 "1"
                } else if (column.matches("[A-Za-z]+")) {
                    colIndex = convertColumnLetterToIndex(column.toUpperCase()); // 支持传字母 "B"
                } else {
                    colIndex = headers.indexOf(column); // 支持传列名 "价格"
                }

                if (colIndex < 0 || colIndex >= headers.size()) {
                    throw new IllegalArgumentException("无法识别的列标识，或列索引超出范围: " + column);
                }

                double sum = 0.0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                int count = 0;
                Set<Double> uniqueValues = new HashSet<>();
                boolean uniqueLimitReached = false;

                while (rowIterator.hasNext()) {
                    Cell cell = rowIterator.next().getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    if (cell.getCellType() == CellType.NUMERIC) {
                        double val = cell.getNumericCellValue();
                        sum += val;
                        min = Math.min(min, val);
                        max = Math.max(max, val);
                        count++;
                        if (!uniqueLimitReached && uniqueValues.add(val) && uniqueValues.size() >= UNIQUE_VALUE_LIMIT) {
                            uniqueLimitReached = true;
                        }
                    }
                }

                Map<String, Object> res = new LinkedHashMap<>();
                res.put("columnName", headers.get(colIndex));
                res.put("totalNumericRows", count);
                if (count > 0) {
                    res.put("sum", sum); res.put("average", sum / count);
                    res.put("min", min); res.put("max", max);
                    res.put("uniqueCount", uniqueValues.size());
                    if (uniqueLimitReached) {
                        res.put("note", "Unique count capped at " + UNIQUE_VALUE_LIMIT);
                    }
                }
                return successResponse(res);
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 5: 分组聚合 ==========
    @McpTool(name = "aggregate_table", description = "分组聚合统计，类似SQL GROUP BY。\n" +
            "【功能】：按一个或多个列分组，对数值列进行聚合计算。\n" +
            "【参数】：\n" +
            "  - groupBy: 分组列名（数组，如 [\"客户ID\", \"城市\"]）\n" +
            "  - aggregations: 聚合配置数组，每个包含 {column: 列名, func: 函数名, alias: 结果别名}\n" +
            "  - func支持: sum(求和), avg(平均值), count(计数), min(最小值), max(最大值)\n" +
            "【示例】：按客户ID分组，计算每个客户的消费总额和订单数\n" +
            "  groupBy=[\"客户ID\"], aggregations=[{column:\"金额\", func:\"sum\", alias:\"总消费\"}, {column:\"订单ID\", func:\"count\", alias:\"订单数\"}]")
    public Map<String, Object> aggregateTable(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName,
            @McpToolParam(name = "groupBy", description = "分组列名数组") List<String> groupBy,
            @McpToolParam(name = "aggregations", description = "聚合配置数组，每个包含 column, func, alias") List<Map<String, String>> aggregations,
            @McpToolParam(name = "filters", description = "可选的过滤条件 (JEXL表达式)") String filters,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            File safeFile = getSafeFile(filePath);
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            JexlExpression filterExpr = null;
            if (filters != null && !filters.isBlank()) {
                try {
                    filterExpr = JEXL_ENGINE.createExpression(filters);
                } catch (Exception e) {
                    throw new IllegalArgumentException("filters JEXL表达式语法错误: " + filters);
                }
            }

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                if (groupBy == null || groupBy.isEmpty()) {
                    throw new IllegalArgumentException("必须指定 groupBy 分组列");
                }
                if (aggregations == null || aggregations.isEmpty()) {
                    throw new IllegalArgumentException("必须指定 aggregations 聚合配置");
                }

                for (String gbCol : groupBy) {
                    if (!headers.contains(gbCol)) {
                        throw new IllegalArgumentException("分组列不存在: " + gbCol + "，可用列: " + headers);
                    }
                }
                for (Map<String, String> agg : aggregations) {
                    String col = agg.get("column");
                    if (col != null && !headers.contains(col) && !col.matches("\\d+") && !col.matches("[A-Za-z]+")) {
                        throw new IllegalArgumentException("聚合列不存在: " + col + "，可用列: " + headers);
                    }
                }

                Map<List<String>, GroupAggregate> groupData = new LinkedHashMap<>();

                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    MapContext context = createJexlContext(headers, row);

                    if (filterExpr != null && !evaluateExpression(filterExpr, context)) {
                        continue;
                    }

                    List<String> groupKey = new ArrayList<>();
                    for (String gbCol : groupBy) {
                        Object val = context.get(gbCol.replaceAll("\\s+", "_"));
                        groupKey.add(val != null ? val.toString() : "");
                    }

                    GroupAggregate agg = groupData.computeIfAbsent(groupKey, k -> new GroupAggregate(groupBy, aggregations));
                    agg.aggregate(headers, row);
                }

                List<Map<String, Object>> results = new ArrayList<>();
                for (GroupAggregate agg : groupData.values()) {
                    results.add(agg.toResult());
                }

                int total = results.size();
                int start = Math.min(offset, total);
                int end = Math.min(offset + limit, total);
                List<Map<String, Object>> pagedResults = results.subList(start, end);

                return successResponse(Map.of(
                        "groupBy", groupBy,
                        "aggregations", aggregations,
                        "returnedCount", pagedResults.size(),
                        "totalGroups", total,
                        "results", pagedResults,
                        "pagination", Map.of("limit", limit, "offset", offset, "hasMore", end < total)
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private static class GroupAggregate {
        List<String> groupByColumns;
        List<Map<String, String>> aggregations;
        Map<String, AggregateStats> statsMap = new LinkedHashMap<>();

        GroupAggregate(List<String> groupByColumns, List<Map<String, String>> aggregations) {
            this.groupByColumns = groupByColumns;
            this.aggregations = aggregations;
            for (Map<String, String> agg : aggregations) {
                String alias = agg.getOrDefault("alias", agg.get("column"));
                statsMap.put(alias, new AggregateStats());
            }
        }

        void aggregate(List<String> headers, Row row) {
            for (Map<String, String> agg : aggregations) {
                String col = agg.get("column");
                String func = agg.getOrDefault("func", "sum");
                String alias = agg.getOrDefault("alias", col);

                int colIndex = -1;
                if (col.matches("\\d+")) {
                    colIndex = Integer.parseInt(col);
                } else if (col.matches("[A-Za-z]+")) {
                    colIndex = convertColumnLetterToIndex(col.toUpperCase());
                } else {
                    colIndex = headers.indexOf(col);
                }

                Object value = "";
                if (colIndex >= 0 && colIndex < headers.size()) {
                    value = getCellValue(row.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                }

                AggregateStats stats = statsMap.get(alias);
                if (stats == null) continue;

                stats.count++;

                if ("count".equalsIgnoreCase(func)) {
                    if (value != null && !value.toString().isBlank()) {
                        stats.countResult++;
                    }
                } else if (value instanceof Number) {
                    double numVal = ((Number) value).doubleValue();
                    if ("sum".equalsIgnoreCase(func) || "avg".equalsIgnoreCase(func)) {
                        stats.sum += numVal;
                    }
                    if ("min".equalsIgnoreCase(func)) {
                        stats.min = Math.min(stats.min, numVal);
                    }
                    if ("max".equalsIgnoreCase(func)) {
                        stats.max = Math.max(stats.max, numVal);
                    }
                    stats.hasNumeric = true;
                }
            }
        }

        Map<String, Object> toResult() {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map<String, String> agg : aggregations) {
                String func = agg.getOrDefault("func", "sum");
                String alias = agg.getOrDefault("alias", agg.get("column"));
                AggregateStats stats = statsMap.get(alias);

                if (stats == null) {
                    result.put(alias, null);
                    continue;
                }

                switch (func.toLowerCase()) {
                    case "sum" -> result.put(alias, stats.hasNumeric ? stats.sum : 0);
                    case "avg" -> result.put(alias, stats.hasNumeric ? stats.sum / stats.count : 0);
                    case "count" -> result.put(alias, stats.countResult);
                    case "min" -> result.put(alias, stats.hasNumeric ? stats.min : null);
                    case "max" -> result.put(alias, stats.hasNumeric ? stats.max : null);
                    default -> result.put(alias, null);
                }
            }
            return result;
        }
    }

    private static class AggregateStats {
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        int count = 0;
        int countResult = 0;
        boolean hasNumeric = false;
    }

    // ========== 工具 6: 多变联查 (Multi-File Join) ==========
    @McpTool(name = "join_tables", description = "多Excel文件关联查询（支持 INNER/LEFT/RIGHT/FULL JOIN）。\n" +
            "【设计目标】：类似 SQL JOIN 的多表联查能力，支持多个Excel文件之间通过关联键进行数据合并。\n" +
            "【语法说明】：\n" +
            "1. files: 文件列表，每个包含 filePath(必填), sheetName(可选), alias(必填别名)\n" +
            "2. joinOn: 关联条件数组，支持多条件 AND 组合。格式: {left: '别名.列名', right: '别名.列名', operator: '='}\n" +
            "3. joinType: JOIN类型 (INNER/LEFT/RIGHT/FULL)，默认 INNER\n" +
            "4. select: 要选择的列，格式: '别名.列名' 或 '别名.*'\n" +
            "5. filters: 可选的过滤条件 (JEXL表达式)，基于 select 的结果进行过滤\n" +
            "【示例】：关联客户表和交易表，查询客户的交易记录")
    public Map<String, Object> joinTables(
            @McpToolParam(name = "files", description = "文件列表，每个包含 filePath, sheetName, alias") List<Map<String, String>> files,
            @McpToolParam(name = "joinOn", description = "关联条件数组") List<Map<String, String>> joinOn,
            @McpToolParam(name = "joinType", description = "JOIN类型: INNER/LEFT/RIGHT/FULL，默认 INNER") String joinType,
            @McpToolParam(name = "select", description = "要选择的列，格式: '别名.列名' 或 '别名.*'") List<String> select,
            @McpToolParam(name = "filters", description = "可选的过滤条件 (JEXL表达式)") String filters,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            if (files == null || files.size() < 2) {
                throw new IllegalArgumentException("联查至少需要2个文件");
            }
            if (joinOn == null || joinOn.isEmpty()) {
                throw new IllegalArgumentException("必须指定关联条件 joinOn");
            }

            String actualJoinType = (joinType != null && !joinType.isBlank()) ? joinType.toUpperCase() : "INNER";
            if (!List.of("INNER", "LEFT", "RIGHT", "FULL").contains(actualJoinType)) {
                throw new IllegalArgumentException("不支持的JOIN类型: " + joinType + "，仅支持 INNER/LEFT/RIGHT/FULL");
            }

            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            Map<String, JoinFileInfo> fileInfoMap = new LinkedHashMap<>();
            List<JoinCondition> conditions = new ArrayList<>();
            List<String> selectColumns = select != null ? select : new ArrayList<>();

            for (Map<String, String> file : files) {
                String filePath = file.get("filePath");
                String alias = file.get("alias");
                String sheetName = file.get("sheetName");

                if (filePath == null || filePath.isBlank()) {
                    throw new IllegalArgumentException("每个文件必须指定 filePath");
                }
                if (alias == null || alias.isBlank()) {
                    throw new IllegalArgumentException("每个文件必须指定 alias (别名)");
                }

                File safeFile = getSafeFile(filePath);
                List<String> headers = loadHeaders(safeFile, sheetName);

                JoinFileInfo info = new JoinFileInfo();
                info.filePath = safeFile;
                info.sheetName = sheetName;
                info.alias = alias;
                info.headers = headers;
                fileInfoMap.put(alias, info);
            }

            for (Map<String, String> cond : joinOn) {
                String left = cond.get("left");
                String right = cond.get("right");
                String op = cond.getOrDefault("operator", "=");

                if (left == null || right == null) {
                    throw new IllegalArgumentException("关联条件必须指定 left 和 right");
                }

                String[] leftParts = parseColumnRef(left);
                String[] rightParts = parseColumnRef(right);

                JoinCondition jc = new JoinCondition();
                jc.leftAlias = leftParts[0];
                jc.leftColumn = leftParts[1];
                jc.rightAlias = rightParts[0];
                jc.rightColumn = rightParts[1];
                jc.operator = op;
                conditions.add(jc);
            }

            if (selectColumns.isEmpty()) {
                for (JoinFileInfo info : fileInfoMap.values()) {
                    for (String header : info.headers) {
                        selectColumns.add(info.alias + "." + header);
                    }
                }
            }

            JexlExpression filterExpr = null;
            if (filters != null && !filters.isBlank()) {
                try {
                    filterExpr = JEXL_ENGINE.createExpression(filters);
                } catch (Exception e) {
                    throw new IllegalArgumentException("filters JEXL表达式语法错误: " + filters + "，错误原因: " + e.getMessage());
                }
            }

            List<Map<String, Object>> results = executeJoin(fileInfoMap, conditions, actualJoinType,
                    selectColumns, filterExpr, limit, offset);

            return successResponse(Map.of(
                    "returnedCount", results.size(),
                    "results", results,
                    "joinType", actualJoinType,
                    "joinedFiles", fileInfoMap.keySet(),
                    "selectColumns", selectColumns,
                    "pagination", Map.of("limit", limit, "offset", offset)
            ));

        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private static class JoinFileInfo {
        File filePath;
        String sheetName;
        String alias;
        List<String> headers;
    }

    private static class JoinCondition {
        String leftAlias;
        String leftColumn;
        String rightAlias;
        String rightColumn;
        String operator;
    }

    private List<String> loadHeaders(File file, String sheetName) throws IOException {
        try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(file)) {
            Sheet sheet = getSheet(workbook, sheetName, null);
            Iterator<Row> iterator = sheet.iterator();
            if (!iterator.hasNext()) {
                return Collections.emptyList();
            }
            Row headerRow = iterator.next();
            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(getCellValueAsString(cell));
            }
            return headers;
        }
    }

    private String[] parseColumnRef(String ref) {
        int dotIndex = ref.indexOf('.');
        if (dotIndex <= 0) {
            throw new IllegalArgumentException("列引用格式错误: " + ref + "，应为 '别名.列名'");
        }
        return new String[]{ref.substring(0, dotIndex), ref.substring(dotIndex + 1)};
    }

    private List<Map<String, Object>> executeJoin(
            Map<String, JoinFileInfo> fileInfoMap,
            List<JoinCondition> conditions,
            String joinType,
            List<String> selectColumns,
            JexlExpression filterExpr,
            int limit,
            int offset) {

        JoinFileInfo leftInfo = fileInfoMap.values().iterator().next();
        JoinFileInfo rightInfo = null;
        for (JoinFileInfo info : fileInfoMap.values()) {
            if (!info.equals(leftInfo)) {
                rightInfo = info;
                break;
            }
        }

        if (rightInfo == null) {
            throw new IllegalArgumentException("需要至少两个不同的文件进行联查");
        }

        Map<String, List<Map<String, Object>>> rightIndex = new LinkedHashMap<>();

        try (Workbook rightWb = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(rightInfo.filePath)) {
            Sheet rightSheet = getSheet(rightWb, rightInfo.sheetName, null);
            Iterator<Row> rightIter = rightSheet.iterator();

            if (rightIter.hasNext()) {
                rightIter.next();
            }

            while (rightIter.hasNext()) {
                Row row = rightIter.next();
                Map<String, Object> rowData = new LinkedHashMap<>();
                for (int i = 0; i < rightInfo.headers.size(); i++) {
                    rowData.put(rightInfo.headers.get(i),
                            getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)));
                }

                String joinKey = buildJoinKey(rowData, rightInfo, conditions, rightInfo.alias);
                rightIndex.computeIfAbsent(joinKey, k -> new ArrayList<>()).add(rowData);
            }
        } catch (IOException e) {
            throw new RuntimeException("读取右表失败: " + e.getMessage(), e);
        }

        List<Map<String, Object>> results = new ArrayList<>();
        Set<String> matchedRightKeys = new HashSet<>();

        try (Workbook leftWb = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(leftInfo.filePath)) {
            Sheet leftSheet = getSheet(leftWb, leftInfo.sheetName, null);
            Iterator<Row> leftIter = leftSheet.iterator();

            if (leftIter.hasNext()) {
                leftIter.next();
            }

            while (leftIter.hasNext()) {
                Row row = leftIter.next();
                Map<String, Object> leftRowData = new LinkedHashMap<>();
                for (int i = 0; i < leftInfo.headers.size(); i++) {
                    leftRowData.put(leftInfo.headers.get(i),
                            getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)));
                }

                String joinKey = buildJoinKey(leftRowData, leftInfo, conditions, leftInfo.alias);
                List<Map<String, Object>> rightMatches = rightIndex.get(joinKey);

                if (rightMatches != null) {
                    matchedRightKeys.add(joinKey);
                    for (Map<String, Object> rightRow : rightMatches) {
                        Map<String, Object> merged = mergeRows(leftInfo, leftRowData, rightInfo, rightRow, selectColumns);
                        if (filterExpr == null || evaluateJoinFilter(filterExpr, merged)) {
                            results.add(merged);
                        }
                    }
                } else if ("LEFT".equals(joinType) || "FULL".equals(joinType)) {
                    Map<String, Object> merged = mergeRows(leftInfo, leftRowData, rightInfo, null, selectColumns);
                    if (filterExpr == null || evaluateJoinFilter(filterExpr, merged)) {
                        results.add(merged);
                    }
                }

                if (results.size() > limit + offset) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("读取左表失败: " + e.getMessage(), e);
        }

        if ("RIGHT".equals(joinType) || "FULL".equals(joinType)) {
            for (Map.Entry<String, List<Map<String, Object>>> entry : rightIndex.entrySet()) {
                if (!matchedRightKeys.contains(entry.getKey())) {
                    for (Map<String, Object> rightRow : entry.getValue()) {
                        Map<String, Object> merged = mergeRows(leftInfo, null, rightInfo, rightRow, selectColumns);
                        if (filterExpr == null || evaluateJoinFilter(filterExpr, merged)) {
                            results.add(merged);
                        }
                    }
                }
            }
        }

        int start = Math.min(offset, results.size());
        int end = Math.min(offset + limit, results.size());
        return results.subList(start, end);
    }

    private String buildJoinKey(Map<String, Object> rowData, JoinFileInfo info,
                                List<JoinCondition> conditions, String alias) {
        StringBuilder key = new StringBuilder();
        for (JoinCondition cond : conditions) {
            String col = alias.equals(cond.leftAlias) ? cond.leftColumn : cond.rightColumn;
            Object val = rowData.get(col);
            key.append(val != null ? val.toString() : "").append("|");
        }
        return key.toString();
    }

    private Map<String, Object> mergeRows(JoinFileInfo leftInfo, Map<String, Object> leftRow,
                                           JoinFileInfo rightInfo, Map<String, Object> rightRow,
                                           List<String> selectColumns) {
        Map<String, Object> result = new LinkedHashMap<>();

        for (String col : selectColumns) {
            String[] parts = parseColumnRef(col);
            String alias = parts[0];
            String column = parts[1];

            if (alias.equals(leftInfo.alias) && leftRow != null) {
                result.put(col, leftRow.get(column));
            } else if (alias.equals(rightInfo.alias) && rightRow != null) {
                result.put(col, rightRow.get(column));
            } else {
                result.put(col, "");
            }
        }

        return result;
    }

    private boolean evaluateJoinFilter(JexlExpression filterExpr, Map<String, Object> rowData) {
        if (filterExpr == null || rowData == null) {
            return true;
        }
        MapContext context = new MapContext();
        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                continue;
            }
            String key = entry.getKey().replace('.', '_');
            if (!key.isBlank()) {
                context.set(key, entry.getValue());
            }
        }
        return evaluateExpression(filterExpr, context);
    }

    // ========== 工具 7: 数据清洗 ==========
    @McpTool(name = "clean_data", description = "数据清洗工具，支持去重、填充空值、值替换、去除空格等操作。\n" +
            "【操作类型】：\n" +
            "  1. removeDuplicates: 去重（基于指定列或全部列），参数: columns\n" +
            "  2. fillNull: 填充空值（固定值/前值/0/平均值），参数: column, value, mode(fixed/previous/zero/mean)\n" +
            "  3. replace: 值替换，参数: column, oldValue, newValue\n" +
            "  4. trim: 去除首尾空格，参数: column\n" +
            "  5. removeRows: 按条件删除行，参数: column, condition (JEXL表达式)\n" +
            "【输出】：返回清洗后的数据（不修改原文件）\n" +
            "【完整示例】：去除客户ID重复行，填充金额空值为0，去除姓名空格\n" +
            "  operations=[\n" +
            "    {type:\"removeDuplicates\", columns:[\"客户ID\"]},\n" +
            "    {type:\"fillNull\", column:\"金额\", value:\"0\", mode:\"fixed\"},\n" +
            "    {type:\"trim\", column:\"姓名\"}\n" +
            "  ]")
    public Map<String, Object> cleanData(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName,
            @McpToolParam(name = "operations", description = "清洗操作数组，每个包含 type 和相关参数") List<Map<String, Object>> operations,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            File safeFile = getSafeFile(filePath);
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            if (operations == null || operations.isEmpty()) {
                throw new IllegalArgumentException("必须指定至少一个清洗操作");
            }

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                List<Map<String, Object>> dataRows = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < headers.size(); i++) {
                        Object value = getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                        rowData.put(headers.get(i), value);
                    }
                    rowData.put("_rowIndex", dataRows.size());
                    dataRows.add(rowData);
                }

                Map<String, Object> summary = new LinkedHashMap<>();
                int originalCount = dataRows.size();

                for (Map<String, Object> op : operations) {
                    String opType = (String) op.get("type");
                    if (opType == null) continue;

                    switch (opType) {
                        case "removeDuplicates" -> {
                            @SuppressWarnings("unchecked")
                            List<String> dupCols = (List<String>) op.get("columns");
                            Set<String> seen = new HashSet<>();
                            int dupRemoved = 0;
                            Iterator<Map<String, Object>> iter = dataRows.iterator();
                            while (iter.hasNext()) {
                                Map<String, Object> row = iter.next();
                                String key;
                                if (dupCols == null || dupCols.isEmpty()) {
                                    key = row.values().toString();
                                } else {
                                    StringBuilder sb = new StringBuilder();
                                    for (String col : dupCols) {
                                        sb.append(row.getOrDefault(col, "")).append("|");
                                    }
                                    key = sb.toString();
                                }
                                if (seen.contains(key)) {
                                    iter.remove();
                                    dupRemoved++;
                                } else {
                                    seen.add(key);
                                }
                            }
                            summary.put("duplicatesRemoved", dupRemoved);
                        }
                        case "fillNull" -> {
                            String fillCol = (String) op.get("column");
                            String fillValue = (String) op.get("value");
                            String fillType = (String) op.get("fillType");
                            int filled = 0;

                            if ("previous".equalsIgnoreCase(fillType)) {
                                Object prevValue = null;
                                for (Map<String, Object> row : dataRows) {
                                    Object current = row.get(fillCol);
                                    if (current == null || current.toString().isBlank()) {
                                        if (prevValue != null) {
                                            row.put(fillCol, prevValue);
                                            filled++;
                                        }
                                    } else {
                                        prevValue = current;
                                    }
                                }
                            } else if ("mean".equalsIgnoreCase(fillType)) {
                                double sum = 0;
                                int count = 0;
                                for (Map<String, Object> row : dataRows) {
                                    Object val = row.get(fillCol);
                                    if (val instanceof Number) {
                                        sum += ((Number) val).doubleValue();
                                        count++;
                                    }
                                }
                                double mean = count > 0 ? sum / count : 0;
                                for (Map<String, Object> row : dataRows) {
                                    Object val = row.get(fillCol);
                                    if (val == null || val.toString().isBlank()) {
                                        row.put(fillCol, mean);
                                        filled++;
                                    }
                                }
                            } else {
                                for (Map<String, Object> row : dataRows) {
                                    Object val = row.get(fillCol);
                                    if (val == null || val.toString().isBlank()) {
                                        row.put(fillCol, fillValue != null ? fillValue : "");
                                        filled++;
                                    }
                                }
                            }
                            summary.put("nullsFilled_" + fillCol, filled);
                        }
                        case "replace" -> {
                            String replaceCol = (String) op.get("column");
                            String oldValue = (String) op.get("oldValue");
                            String newValue = (String) op.get("newValue");
                            int replaced = 0;
                            for (Map<String, Object> row : dataRows) {
                                Object val = row.get(replaceCol);
                                if (val != null && val.toString().equals(oldValue)) {
                                    row.put(replaceCol, newValue != null ? newValue : "");
                                    replaced++;
                                }
                            }
                            summary.put("valuesReplaced_" + replaceCol, replaced);
                        }
                        case "trim" -> {
                            @SuppressWarnings("unchecked")
                            List<String> trimCols = (List<String>) op.get("columns");
                            if (trimCols == null || trimCols.isEmpty()) {
                                trimCols = headers;
                            }
                            int trimmed = 0;
                            for (Map<String, Object> row : dataRows) {
                                for (String col : trimCols) {
                                    Object val = row.get(col);
                                    if (val instanceof String) {
                                        String trimmedVal = ((String) val).trim();
                                        if (!trimmedVal.equals(val)) {
                                            row.put(col, trimmedVal);
                                            trimmed++;
                                        }
                                    }
                                }
                            }
                            summary.put("trimmedCells", trimmed);
                        }
                        case "removeRows" -> {
                            String condition = (String) op.get("condition");
                            int removed = 0;
                            if (condition != null && !condition.isBlank()) {
                                try {
                                    JexlExpression expr = JEXL_ENGINE.createExpression(condition);
                                    Iterator<Map<String, Object>> iter = dataRows.iterator();
                                    while (iter.hasNext()) {
                                        Map<String, Object> row = iter.next();
                                        MapContext context = new MapContext();
                                        for (Map.Entry<String, Object> entry : row.entrySet()) {
                                            String key = entry.getKey().replaceAll("\\s+", "_");
                                            context.set(key, entry.getValue());
                                        }
                                        if (evaluateExpression(expr, context)) {
                                            iter.remove();
                                            removed++;
                                        }
                                    }
                                } catch (Exception e) {
                                    throw new IllegalArgumentException("removeRows条件表达式错误: " + condition);
                                }
                            }
                            summary.put("rowsRemoved", removed);
                        }
                    }
                }

                int total = dataRows.size();
                int start = Math.min(offset, total);
                int end = Math.min(offset + limit, total);
                List<Map<String, Object>> pagedResults = dataRows.subList(start, end);

                for (Map<String, Object> row : pagedResults) {
                    row.remove("_rowIndex");
                }

                summary.put("originalRows", originalCount);
                summary.put("finalRows", total);

                return successResponse(Map.of(
                        "operations", operations,
                        "summary", summary,
                        "returnedCount", pagedResults.size(),
                        "totalRows", total,
                        "results", pagedResults,
                        "pagination", Map.of("limit", limit, "offset", offset, "hasMore", end < total)
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 8: 排序功能 ==========
    @McpTool(name = "sort_data", description = "数据排序工具，支持按单列或多列排序。\n" +
            "【参数】：\n" +
            "  - sortBy: 排序配置数组，每个包含 {column: 列名, order: asc/desc}\n" +
            "  - 默认升序(asc)，可指定降序(desc)\n" +
            "【示例】：按金额降序排列\n" +
            "  sortBy=[{column: \"金额\", order: \"desc\"}]\n" +
            "【示例】：先按城市升序，再按金额降序\n" +
            "  sortBy=[{column: \"城市\", order: \"asc\"}, {column: \"金额\", order: \"desc\"}]")
    public Map<String, Object> sortData(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName,
            @McpToolParam(name = "sortBy", description = "排序配置数组") List<Map<String, String>> sortBy,
            @McpToolParam(name = "filters", description = "可选的过滤条件 (JEXL表达式)") String filters,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            File safeFile = getSafeFile(filePath);
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            if (sortBy == null || sortBy.isEmpty()) {
                throw new IllegalArgumentException("必须指定 sortBy 排序配置");
            }

            JexlExpression filterExpr = null;
            if (filters != null && !filters.isBlank()) {
                try {
                    filterExpr = JEXL_ENGINE.createExpression(filters);
                } catch (Exception e) {
                    throw new IllegalArgumentException("filters JEXL表达式语法错误: " + filters);
                }
            }

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                for (Map<String, String> sortConfig : sortBy) {
                    String col = sortConfig.get("column");
                    if (col != null && !headers.contains(col)) {
                        throw new IllegalArgumentException("排序列不存在: " + col + "，可用列: " + headers);
                    }
                }

                List<Map<String, Object>> dataRows = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    if (filterExpr != null) {
                        MapContext context = createJexlContext(headers, row);
                        if (!evaluateExpression(filterExpr, context)) {
                            continue;
                        }
                    }
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < headers.size(); i++) {
                        Object value = getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                        rowData.put(headers.get(i), value);
                    }
                    dataRows.add(rowData);
                }

                dataRows.sort((a, b) -> {
                    for (Map<String, String> sortConfig : sortBy) {
                        String col = sortConfig.get("column");
                        String order = sortConfig.getOrDefault("order", "asc");
                        boolean descending = "desc".equalsIgnoreCase(order);

                        Object valA = a.get(col);
                        Object valB = b.get(col);

                        int cmp = 0;
                        if (valA == null && valB == null) {
                            cmp = 0;
                        } else if (valA == null) {
                            cmp = -1;
                        } else if (valB == null) {
                            cmp = 1;
                        } else if (valA instanceof Number && valB instanceof Number) {
                            cmp = Double.compare(((Number) valA).doubleValue(), ((Number) valB).doubleValue());
                        } else {
                            cmp = valA.toString().compareTo(valB.toString());
                        }

                        if (cmp != 0) {
                            return descending ? -cmp : cmp;
                        }
                    }
                    return 0;
                });

                int total = dataRows.size();
                int start = Math.min(offset, total);
                int end = Math.min(offset + limit, total);
                List<Map<String, Object>> pagedResults = dataRows.subList(start, end);

                return successResponse(Map.of(
                        "sortBy", sortBy,
                        "returnedCount", pagedResults.size(),
                        "totalRows", total,
                        "results", pagedResults,
                        "pagination", Map.of("limit", limit, "offset", offset, "hasMore", end < total)
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 9: 导出功能 ==========
    @McpTool(name = "export_data", description = "导出数据到新Excel文件，支持筛选和列选择。\n" +
            "【功能】：从原始Excel中筛选数据，指定导出列，保存为新文件。\n" +
            "【参数】：\n" +
            "  - filters: 可选的过滤条件\n" +
            "  - columns: 要导出的列（为空则导出所有列）\n" +
            "  - outputPath: 导出文件路径（必须指定）\n" +
            "【注意】：导出文件大小限制30MB，超过则拒绝执行\n" +
            "【示例】：导出金额大于5000的记录\n" +
            "  filters=\"金额 > 5000\", outputPath=\"/path/to/output.xlsx\"")
    public Map<String, Object> exportData(
            @McpToolParam(name = "filePath", description = "源Excel文件绝对路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName,
            @McpToolParam(name = "filters", description = "可选的过滤条件 (JEXL表达式)") String filters,
            @McpToolParam(name = "columns", description = "要导出的列数组") List<String> columns,
            @McpToolParam(name = "outputPath", description = "导出文件路径（必须指定）") String outputPath) {
        try {
            File safeFile = getSafeFile(filePath);
            File outputFile = getSafeFile(outputPath);

            if (outputFile.exists() && outputFile.length() > LARGE_FILE_THRESHOLD) {
                return Map.of("success", false, "error", "导出文件过大，超过30MB限制");
            }

            JexlExpression filterExpr = null;
            if (filters != null && !filters.isBlank()) {
                try {
                    filterExpr = JEXL_ENGINE.createExpression(filters);
                } catch (Exception e) {
                    throw new IllegalArgumentException("filters JEXL表达式语法错误: " + filters);
                }
            }

            try (Workbook readWb = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile);
                 XSSFWorkbook writeWb = new XSSFWorkbook()) {

                Sheet readSheet = getSheet(readWb, sheetName, null);
                Iterator<Row> rowIterator = readSheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                List<Integer> columnIndices = new ArrayList<>();
                if (columns != null && !columns.isEmpty()) {
                    for (String col : columns) {
                        int idx = headers.indexOf(col);
                        if (idx < 0) {
                            throw new IllegalArgumentException("导出列不存在: " + col + "，可用列: " + headers);
                        }
                        columnIndices.add(idx);
                    }
                } else {
                    for (int i = 0; i < headers.size(); i++) {
                        columnIndices.add(i);
                    }
                }

                Sheet writeSheet = writeWb.createSheet(readSheet.getSheetName());
                Row headerRow = writeSheet.createRow(0);
                for (int i = 0; i < columnIndices.size(); i++) {
                    headerRow.createCell(i).setCellValue(headers.get(columnIndices.get(i)));
                }

                int writeRowNum = 1;
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    if (filterExpr != null) {
                        MapContext context = createJexlContext(headers, row);
                        if (!evaluateExpression(filterExpr, context)) {
                            continue;
                        }
                    }

                    Row writeRow = writeSheet.createRow(writeRowNum++);
                    for (int i = 0; i < columnIndices.size(); i++) {
                        Cell cell = row.getCell(columnIndices.get(i), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        Object value = getCellValue(cell);
                        if (value instanceof Number) {
                            writeRow.createCell(i).setCellValue(((Number) value).doubleValue());
                        } else if (value != null) {
                            writeRow.createCell(i).setCellValue(value.toString());
                        }
                    }
                }

                try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                    writeWb.write(fos);
                }

                return successResponse(Map.of(
                        "sourceFile", safeFile.getName(),
                        "outputFile", outputFile.getName(),
                        "exportedRows", writeRowNum - 1,
                        "exportedColumns", columnIndices.size(),
                        "message", "成功导出 " + (writeRowNum - 1) + " 行数据到 " + outputPath
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 10: 联邦查询（多文件并行） ==========
    @McpTool(name = "federated_query", description = "跨目录/多文件联邦查询，使用虚拟线程并行处理（Java 21+）。\n" +
            "【功能】：在多个Excel文件或目录下执行查询，并支持分组聚合。\n" +
            "【参数】：\n" +
            "  - sources: 文件/目录列表，每个包含 path, sheetName(可选), alias\n" +
            "  - query: JEXL查询条件\n" +
            "  - groupBy: 可选的分组聚合列\n" +
            "  - aggregations: 可选的聚合配置（当groupBy存在时）\n" +
            "【特点】：\n" +
            "  - 使用虚拟线程并行查询，充分利用IO等待时间\n" +
            "  - 单文件失败不影响整体查询\n" +
            "  - 支持按文件别名区分结果来源\n" +
            "【示例】：查询张三在所有打卡表的记录\n" +
            "  sources=[{path:\"/data/打卡表1.xlsx\", alias:\"d1\"}, {path:\"/data/打卡表2.xlsx\", alias:\"d2\"}]\n" +
            "  query=\"姓名 == '张三'\"\n" +
            "  groupBy=\"姓名\"")
    public Map<String, Object> federatedQuery(
            @McpToolParam(name = "sources", description = "文件/目录列表，每个包含 path, sheetName, alias") List<Map<String, String>> sources,
            @McpToolParam(name = "query", description = "JEXL查询条件") String query,
            @McpToolParam(name = "groupBy", description = "可选的分组聚合列") String groupBy,
            @McpToolParam(name = "aggregations", description = "可选的聚合配置数组") List<Map<String, String>> aggregations,
            @McpToolParam(name = "filters", description = "全局过滤条件（对聚合后结果）") String filters,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            if (sources == null || sources.isEmpty()) {
                throw new IllegalArgumentException("必须指定至少一个数据源");
            }

            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            JexlExpression filterExpr = null;
            if (query != null && !query.isBlank()) {
                try {
                    filterExpr = JEXL_ENGINE.createExpression(query);
                } catch (Exception e) {
                    throw new IllegalArgumentException("query JEXL表达式语法错误: " + query);
                }
            }

            JexlExpression globalFilter = null;
            if (filters != null && !filters.isBlank()) {
                try {
                    globalFilter = JEXL_ENGINE.createExpression(filters);
                } catch (Exception e) {
                    throw new IllegalArgumentException("filters JEXL表达式语法错误: " + filters);
                }
            }

            List<FileQueryTask> allTasks = new ArrayList<>();
            for (Map<String, String> source : sources) {
                String path = source.get("path");
                if (path == null || path.isBlank()) {
                    continue;
                }

                File file = getSafeFile(path);
                if (file.isDirectory()) {
                    String pattern = source.getOrDefault("pattern", "*.xlsx");
                    String regex = pattern.replace("*", ".*").replace("?", ".");
                    Pattern compiledPattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

                    File[] files = file.listFiles(f -> compiledPattern.matcher(f.getName()).matches());
                    if (files != null) {
                        String baseAlias = source.getOrDefault("alias", file.getName());
                        for (int i = 0; i < files.length; i++) {
                            FileQueryTask task = new FileQueryTask();
                            task.filePath = files[i].getAbsolutePath();
                            task.sheetName = source.get("sheetName");
                            task.alias = baseAlias + "_" + i;
                            allTasks.add(task);
                        }
                    }
                } else {
                    FileQueryTask task = new FileQueryTask();
                    task.filePath = file.getAbsolutePath();
                    task.sheetName = source.get("sheetName");
                    task.alias = source.getOrDefault("alias", file.getName());
                    allTasks.add(task);
                }
            }

            if (allTasks.isEmpty()) {
                throw new IllegalArgumentException("未找到任何可查询的文件");
            }

            List<Map<String, Object>> allResults = Collections.synchronizedList(new ArrayList<>());
            List<Map<String, Object>> errors = Collections.synchronizedList(new ArrayList<>());

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (FileQueryTask task : allTasks) {
                    JexlExpression finalFilterExpr = filterExpr;
                    Future<?> future = executor.submit(() -> {
                        try {
                            List<Map<String, Object>> results = querySingleFile(task, finalFilterExpr);
                            for (Map<String, Object> row : results) {
                                row.put("_source", task.alias);
                            }
                            allResults.addAll(results);
                        } catch (Exception e) {
                            errors.add(Map.of("file", task.filePath, "error", e.getMessage()));
                        }
                    });
                    futures.add(future);
                }

                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        // 单个任务失败已在上方捕获
                    }
                }
            }

            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("totalFiles", allTasks.size());
            summary.put("successfulFiles", allTasks.size() - errors.size());
            summary.put("failedFiles", errors.size());
            summary.put("totalRows", allResults.size());
            if (!errors.isEmpty()) {
                summary.put("errors", errors);
            }

            List<Map<String, Object>> finalResults;
            if (groupBy != null && !groupBy.isBlank()) {
                finalResults = aggregateGroupBy(allResults, groupBy, aggregations, globalFilter);
                summary.put("groupBy", groupBy);
                if (aggregations != null) {
                    summary.put("aggregations", aggregations);
                }
            } else if (globalFilter != null) {
                finalResults = new ArrayList<>();
                for (Map<String, Object> row : allResults) {
                    MapContext context = new MapContext();
                    for (Map.Entry<String, Object> entry : row.entrySet()) {
                        String key = entry.getKey().replaceAll("\\s+", "_");
                        context.set(key, entry.getValue());
                    }
                    if (evaluateExpression(globalFilter, context)) {
                        finalResults.add(row);
                    }
                }
            } else {
                finalResults = allResults;
            }

            int total = finalResults.size();
            int start = Math.min(offset, total);
            int end = Math.min(offset + limit, total);
            List<Map<String, Object>> pagedResults = finalResults.subList(start, end);

            for (Map<String, Object> row : pagedResults) {
                row.remove("_rowIndex");
            }

            return successResponse(Map.of(
                    "summary", summary,
                    "returnedCount", pagedResults.size(),
                    "totalRows", total,
                    "results", pagedResults,
                    "pagination", Map.of("limit", limit, "offset", offset, "hasMore", end < total)
            ));
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private static class FileQueryTask {
        String filePath;
        String sheetName;
        String alias;
    }

    private List<Map<String, Object>> querySingleFile(FileQueryTask task, JexlExpression filterExpr) throws IOException {
        File file = new File(task.filePath);
        try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(file)) {
            Sheet sheet = getSheet(workbook, task.sheetName, null);
            Iterator<Row> rowIterator = sheet.iterator();

            List<String> headers = new ArrayList<>();
            if (rowIterator.hasNext()) {
                for (Cell cell : rowIterator.next()) {
                    headers.add(getCellValueAsString(cell));
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                MapContext context = createJexlContext(headers, row);

                if (filterExpr == null || evaluateExpression(filterExpr, context)) {
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < headers.size(); i++) {
                        Object value = getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                        rowData.put(headers.get(i), value);
                    }
                    results.add(rowData);
                }
            }
            return results;
        }
    }

    private List<Map<String, Object>> aggregateGroupBy(List<Map<String, Object>> data, String groupBy,
                                                        List<Map<String, String>> aggregations,
                                                        JexlExpression globalFilter) {
        Map<List<String>, Map<String, Object>> grouped = new LinkedHashMap<>();

        for (Map<String, Object> row : data) {
            Object groupKey = row.get(groupBy);
            String keyStr = groupKey != null ? groupKey.toString() : "";

            Map<String, Object> aggRow = grouped.computeIfAbsent(List.of(keyStr), k -> {
                Map<String, Object> newRow = new LinkedHashMap<>();
                newRow.put(groupBy, keyStr);
                if (aggregations != null) {
                    for (Map<String, String> agg : aggregations) {
                        newRow.put(agg.getOrDefault("alias", agg.get("column")), 0);
                    }
                }
                return newRow;
            });

            if (aggregations != null) {
                for (Map<String, String> agg : aggregations) {
                    String col = agg.get("column");
                    String func = agg.getOrDefault("func", "count");
                    String alias = agg.getOrDefault("alias", col);

                    Object val = row.get(col);
                    Object current = aggRow.get(alias);

                    switch (func.toLowerCase()) {
                        case "count" -> {
                            int count = current instanceof Number ? ((Number) current).intValue() : 0;
                            aggRow.put(alias, count + 1);
                        }
                        case "sum" -> {
                            double sum = current instanceof Number ? ((Number) current).doubleValue() : 0;
                            if (val instanceof Number) {
                                aggRow.put(alias, sum + ((Number) val).doubleValue());
                            }
                        }
                    }
                }
            }
        }

        List<Map<String, Object>> results = new ArrayList<>(grouped.values());

        if (globalFilter != null) {
            results = results.stream()
                    .filter(row -> {
                        MapContext context = new MapContext();
                        for (Map.Entry<String, Object> entry : row.entrySet()) {
                            String key = entry.getKey().replaceAll("\\s+", "_");
                            context.set(key, entry.getValue());
                        }
                        return evaluateExpression(globalFilter, context);
                    })
                    .toList();
        }

        return results;
    }

    // ========== 工具 11: 模糊匹配 ==========
    @McpTool(name = "fuzzy_match", description = "模糊匹配工具，解决同名不同字的关联难题。\n" +
            "【功能】：使用Levenshtein距离算法计算字符串相似度，匹配最相似的记录。\n" +
            "【参数】：\n" +
            "  - sourceFile: 源数据文件\n" +
            "  - targetFile: 目标数据文件\n" +
            "  - sourceColumn: 源文件匹配列\n" +
            "  - targetColumn: 目标文件匹配列\n" +
            "  - threshold: 相似度阈值(0-1)，默认0.6\n" +
            "【示例】：匹配客户表和交易表中的客户名（处理张三和张山的情况）\n" +
            "  sourceColumn=\"姓名\", targetColumn=\"客户姓名\", threshold=0.8")
    public Map<String, Object> fuzzyMatch(
            @McpToolParam(name = "sourceFile", description = "源数据文件路径") String sourceFile,
            @McpToolParam(name = "targetFile", description = "目标数据文件路径") String targetFile,
            @McpToolParam(name = "sourceSheet", description = "源文件Sheet名称") String sourceSheet,
            @McpToolParam(name = "targetSheet", description = "目标文件Sheet名称") String targetSheet,
            @McpToolParam(name = "sourceColumn", description = "源文件匹配列") String sourceColumn,
            @McpToolParam(name = "targetColumn", description = "目标文件匹配列") String targetColumn,
            @McpToolParam(name = "threshold", description = "相似度阈值(0-1)") Double threshold,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            File srcFile = getSafeFile(sourceFile);
            File tgtFile = getSafeFile(targetFile);
            double simThreshold = threshold != null ? threshold : 0.6;

            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            List<Map<String, Object>> srcData = loadColumnData(srcFile, sourceSheet, sourceColumn);
            List<Map<String, Object>> tgtData = loadColumnData(tgtFile, targetSheet, targetColumn);

            Map<String, Object> bestMatch;
            Map<String, List<MatchResult>> allMatches = new LinkedHashMap<>();

            for (Map<String, Object> srcRow : srcData) {
                String srcValue = srcRow.get(sourceColumn) != null ? srcRow.get(sourceColumn).toString() : "";
                List<MatchResult> matches = new ArrayList<>();

                for (Map<String, Object> tgtRow : tgtData) {
                    String tgtValue = tgtRow.get(targetColumn) != null ? tgtRow.get(targetColumn).toString() : "";
                    double similarity = calculateSimilarity(srcValue, tgtValue);

                    if (similarity >= simThreshold) {
                        matches.add(new MatchResult(tgtValue, similarity, tgtRow));
                    }
                }

                matches.sort((a, b) -> Double.compare(b.similarity, a.similarity));
                if (!matches.isEmpty()) {
                    allMatches.put(srcValue, matches);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            for (Map.Entry<String, List<MatchResult>> entry : allMatches.entrySet()) {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("source_value", entry.getKey());
                result.put("match_count", entry.getValue().size());
                result.put("best_match", entry.getValue().get(0).targetValue);
                result.put("similarity", entry.getValue().get(0).similarity);
                results.add(result);
            }

            int total = results.size();
            int start = Math.min(offset, total);
            int end = Math.min(offset + limit, total);
            List<Map<String, Object>> pagedResults = results.subList(start, end);

            return successResponse(Map.of(
                    "sourceColumn", sourceColumn,
                    "targetColumn", targetColumn,
                    "threshold", simThreshold,
                    "matchedCount", total,
                    "results", pagedResults,
                    "pagination", Map.of("limit", limit, "offset", offset, "hasMore", end < total)
            ));
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private static class MatchResult {
        String targetValue;
        double similarity;
        Map<String, Object> row;

        MatchResult(String targetValue, double similarity, Map<String, Object> row) {
            this.targetValue = targetValue;
            this.similarity = similarity;
            this.row = row;
        }
    }

    private static final int FUZZY_MATCH_MAX_ROWS = 10000;

    private List<Map<String, Object>> loadColumnData(File file, String sheetName, String column) throws IOException {
        try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(file)) {
            Sheet sheet = getSheet(workbook, sheetName, null);
            Iterator<Row> rowIterator = sheet.iterator();

            List<String> headers = new ArrayList<>();
            if (rowIterator.hasNext()) {
                for (Cell cell : rowIterator.next()) {
                    headers.add(getCellValueAsString(cell));
                }
            }

            int colIndex = headers.indexOf(column);
            if (colIndex < 0) {
                throw new IllegalArgumentException("列不存在: " + column + "，可用列: " + headers);
            }

            List<Map<String, Object>> data = new ArrayList<>();
            int rowCount = 0;
            while (rowIterator.hasNext() && rowCount < FUZZY_MATCH_MAX_ROWS) {
                Row row = rowIterator.next();
                Map<String, Object> rowData = new LinkedHashMap<>();
                rowData.put(column, getCellValue(row.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)));
                data.add(rowData);
                rowCount++;
            }
            return data;
        }
    }

    private double calculateSimilarity(String s1, String s2) {
        if (s1 == null || s2 == null) return 0;
        if (s1.isEmpty() && s2.isEmpty()) return 1;
        if (s1.isEmpty() || s2.isEmpty()) return 0;

        int[][] dp = new int[s1.length() + 1][s2.length() + 1];
        for (int i = 0; i <= s1.length(); i++) dp[i][0] = i;
        for (int j = 0; j <= s2.length(); j++) dp[0][j] = j;

        for (int i = 1; i <= s1.length(); i++) {
            for (int j = 1; j <= s2.length(); j++) {
                if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + Math.min(dp[i - 1][j - 1], Math.min(dp[i - 1][j], dp[i][j - 1]));
                }
            }
        }

        int distance = dp[s1.length()][s2.length()];
        return 1.0 - (double) distance / Math.max(s1.length(), s2.length());
    }

    // ========== 工具 12: 透视转换 ==========
    @McpTool(name = "pivot_table", description = "数据透视转换，类似Excel数据透视表。\n" +
            "【功能】：将数据按行/列维度分组，聚合数值。\n" +
            "【参数】：\n" +
            "  - rows: 行维度列（数组）\n" +
            "  - columns: 列维度列（数组）\n" +
            "  - values: 值列（需要聚合的列）\n" +
            "  - aggregations: 聚合函数(sum/avg/count)\n" +
            "【示例】：按城市和产品统计销售额\n" +
            "  rows=[\"城市\"], columns=[\"产品\"], values=[\"销售额\"], aggregations=[\"sum\"]")
    public Map<String, Object> pivotTable(
            @McpToolParam(name = "filePath", description = "Excel文件路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称") String sheetName,
            @McpToolParam(name = "rows", description = "行维度列数组") List<String> rows,
            @McpToolParam(name = "columns", description = "列维度列数组") List<String> columns,
            @McpToolParam(name = "values", description = "值列数组") List<String> values,
            @McpToolParam(name = "aggregations", description = "聚合函数数组") List<String> aggregations,
            @McpToolParam(name = "filters", description = "过滤条件") String filters,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            File safeFile = getSafeFile(filePath);
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            JexlExpression filterExpr = null;
            if (filters != null && !filters.isBlank()) {
                filterExpr = JEXL_ENGINE.createExpression(filters);
            }

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                for (String col : rows) {
                    if (!headers.contains(col)) throw new IllegalArgumentException("行列不存在: " + col);
                }
                for (String col : columns) {
                    if (!headers.contains(col)) throw new IllegalArgumentException("列不存在: " + col);
                }
                for (String col : values) {
                    if (!headers.contains(col)) throw new IllegalArgumentException("值列不存在: " + col);
                }

                Map<String, PivotCell> pivotData = new LinkedHashMap<>();

                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    MapContext context = createJexlContext(headers, row);
                    if (filterExpr != null && !evaluateExpression(filterExpr, context)) continue;

                    StringBuilder rowKey = new StringBuilder();
                    for (String r : rows) {
                        rowKey.append(context.get(r.replaceAll("\\s+", "_"))).append("|");
                    }

                    StringBuilder colKey = new StringBuilder();
                    for (String c : columns) {
                        colKey.append(context.get(c.replaceAll("\\s+", "_"))).append("|");
                    }

                    String pivotKey = rowKey + "::" + colKey;
                    PivotCell cell = pivotData.computeIfAbsent(pivotKey, k -> {
                        PivotCell newCell = new PivotCell(rows, columns, values);
                        String[] rowKeyParts = rowKey.toString().split("\\|", -1);
                        String[] colKeyParts = colKey.toString().split("\\|", -1);
                        for (int i = 0; i < rowKeyParts.length && i < newCell.rowKeys.length; i++) {
                            newCell.rowKeys[i] = rowKeyParts[i];
                        }
                        for (int i = 0; i < colKeyParts.length && i < newCell.colKeys.length; i++) {
                            newCell.colKeys[i] = colKeyParts[i];
                        }
                        return newCell;
                    });

                    for (int i = 0; i < values.size(); i++) {
                        String valCol = values.get(i);
                        Object val = context.get(valCol.replaceAll("\\s+", "_"));
                        if (val instanceof Number) {
                            cell.values[i] = cell.values[i] + ((Number) val).doubleValue();
                            cell.counts[i]++;
                        }
                    }
                }

                List<Map<String, Object>> results = new ArrayList<>();
                for (PivotCell cell : pivotData.values()) {
                    results.add(cell.toResult(aggregations));
                }

                int total = results.size();
                int start = Math.min(offset, total);
                int end = Math.min(offset + limit, total);
                List<Map<String, Object>> pagedResults = results.subList(start, end);

                return successResponse(Map.of(
                        "rows", rows,
                        "columns", columns,
                        "values", values,
                        "aggregations", aggregations,
                        "totalCells", total,
                        "results", pagedResults,
                        "pagination", Map.of("limit", limit, "offset", offset, "hasMore", end < total)
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private static class PivotCell {
        String[] rowKeys;
        String[] colKeys;
        double[] values;
        int[] counts;
        List<String> rowNames;
        List<String> colNames;

        PivotCell(List<String> rows, List<String> columns, List<String> values) {
            this.rowKeys = new String[rows.size()];
            this.colKeys = new String[columns.size()];
            this.values = new double[values.size()];
            this.counts = new int[values.size()];
            this.rowNames = rows;
            this.colNames = columns;
        }

        Map<String, Object> toResult(List<String> aggregations) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (int i = 0; i < rowKeys.length; i++) {
                result.put(rowNames.get(i), rowKeys[i]);
            }
            for (int i = 0; i < colKeys.length; i++) {
                result.put(colNames.get(i), colKeys[i]);
            }
            for (int i = 0; i < values.length; i++) {
                String agg = aggregations != null && i < aggregations.size() ? aggregations.get(i) : "sum";
                if ("avg".equalsIgnoreCase(agg) && counts[i] > 0) {
                    result.put("value_" + i, values[i] / counts[i]);
                } else {
                    result.put("value_" + i, values[i]);
                }
            }
            return result;
        }
    }

    // ========== 工具 13: 类型推断 ==========
    @McpTool(name = "infer_types", description = "自动推断Excel列的数据类型，减少配置负担。\n" +
            "【功能】：扫描数据自动识别日期、金额、整数、浮点、布尔、字符串等类型。\n" +
            "【返回】：每列的类型推断结果和样本值\n" +
            "【示例】：自动识别\"交易日期\"为DATE，\"金额\"为DECIMAL")
    public Map<String, Object> inferTypes(
            @McpToolParam(name = "filePath", description = "Excel文件路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称") String sheetName,
            @McpToolParam(name = "sampleSize", description = "采样行数，默认100") Integer sampleSize) {
        try {
            File safeFile = getSafeFile(filePath);
            int sample = sampleSize != null ? sampleSize : 100;

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(safeFile)) {
                Sheet sheet = getSheet(workbook, sheetName, null);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                Map<String, TypeInfo> typeMap = new LinkedHashMap<>();
                for (String header : headers) {
                    typeMap.put(header, new TypeInfo());
                }

                int rowCount = 0;
                while (rowIterator.hasNext() && rowCount < sample) {
                    Row row = rowIterator.next();
                    for (int i = 0; i < headers.size(); i++) {
                        Object value = getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                        typeMap.get(headers.get(i)).analyze(value);
                        rowCount++;
                    }
                }

                Map<String, Object> results = new LinkedHashMap<>();
                for (Map.Entry<String, TypeInfo> entry : typeMap.entrySet()) {
                    results.put(entry.getKey(), entry.getValue().getExtendedInfo(rowCount));
                }

                return successResponse(Map.of(
                        "fileName", safeFile.getName(),
                        "sheetName", sheet.getSheetName(),
                        "columns", headers.size(),
                        "sampledRows", rowCount,
                        "columnTypes", results,
                        "note", "增强版：新增数值范围(min/max)、字符串长度、唯一值、日期格式等元信息"
                ));
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private static class TypeInfo {
        int nullCount = 0;
        int dateCount = 0;
        int decimalCount = 0;
        int integerCount = 0;
        int booleanCount = 0;
        int stringCount = 0;
        Object sampleValue = null;

        double minValue = Double.MAX_VALUE;
        double maxValue = -Double.MAX_VALUE;
        int minStrLen = Integer.MAX_VALUE;
        int maxStrLen = 0;
        Set<String> uniqueValues = new LinkedHashSet<>();
        String detectedDateFormat = null;
        private static final int MAX_UNIQUE_VALUES = 20;

        void analyze(Object value) {
            if (value == null || value.toString().isBlank()) {
                nullCount++;
                return;
            }

            if (sampleValue == null) sampleValue = value;

            String str = value.toString();
            int strLen = str.length();
            if (strLen < minStrLen) minStrLen = strLen;
            if (strLen > maxStrLen) maxStrLen = strLen;

            if (uniqueValues.size() < MAX_UNIQUE_VALUES && !uniqueValues.contains(str)) {
                uniqueValues.add(str);
            }

            if (value instanceof Number) {
                double numVal = ((Number) value).doubleValue();
                if (numVal < minValue) minValue = numVal;
                if (numVal > maxValue) maxValue = numVal;

                if (numVal % 1 == 0) {
                    integerCount++;
                } else {
                    decimalCount++;
                }
            } else if ("true".equalsIgnoreCase(str) || "false".equalsIgnoreCase(str)) {
                booleanCount++;
            } else if (detectDateFormat(str)) {
                dateCount++;
            } else {
                stringCount++;
            }
        }

        private boolean detectDateFormat(String str) {
            if (str.matches("\\d{4}-\\d{2}-\\d{2}")) {
                detectedDateFormat = "yyyy-MM-dd";
                return true;
            } else if (str.matches("\\d{4}/\\d{2}/\\d{2}")) {
                detectedDateFormat = "yyyy/MM/dd";
                return true;
            } else if (str.matches("\\d{2}-\\d{2}-\\d{4}")) {
                detectedDateFormat = "dd-MM-yyyy";
                return true;
            } else if (str.matches("\\d{2}/\\d{2}/\\d{4}")) {
                detectedDateFormat = "dd/MM/yyyy";
                return true;
            } else if (str.matches("\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}")) {
                detectedDateFormat = "yyyy-MM-dd HH:mm";
                return true;
            } else if (str.matches("\\d{4}\\年\\d{1,2}\\月\\d{1,2}\\日")) {
                detectedDateFormat = "yyyy年M月d日";
                return true;
            }
            return false;
        }

        String inferType() {
            int max = Math.max(Math.max(dateCount, decimalCount), Math.max(integerCount, Math.max(booleanCount, stringCount)));
            if (max == dateCount && dateCount > 0) return "DATE";
            if (max == decimalCount && decimalCount > 0) return "DECIMAL";
            if (max == integerCount && integerCount > 0) return "INTEGER";
            if (max == booleanCount && booleanCount > 0) return "BOOLEAN";
            return "STRING";
        }

        double getConfidence() {
            int total = dateCount + decimalCount + integerCount + booleanCount + stringCount;
            if (total == 0) return 0;
            int max = Math.max(dateCount, Math.max(decimalCount, Math.max(integerCount, Math.max(booleanCount, stringCount))));
            return (double) max / total;
        }

        Map<String, Object> getExtendedInfo(int totalCount) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("inferredType", inferType());
            info.put("confidence", getConfidence());
            info.put("sampleValue", sampleValue);
            info.put("nullCount", nullCount);
            info.put("totalCount", totalCount);
            info.put("nullRatio", totalCount > 0 ? (double) nullCount / totalCount : 0);

            String type = inferType();
            if ("INTEGER".equals(type) || "DECIMAL".equals(type)) {
                if (minValue != Double.MAX_VALUE) {
                    info.put("min", minValue);
                    info.put("max", maxValue);
                    info.put("range", maxValue - minValue);
                }
            } else if ("STRING".equals(type)) {
                if (minStrLen != Integer.MAX_VALUE) {
                    info.put("minLength", minStrLen);
                    info.put("maxLength", maxStrLen);
                }
                info.put("uniqueCount", uniqueValues.size());
                if (uniqueValues.size() <= MAX_UNIQUE_VALUES) {
                    info.put("uniqueValues", new ArrayList<>(uniqueValues));
                } else {
                    List<String> topValues = new ArrayList<>(uniqueValues);
                    info.put("uniqueValues", topValues.subList(0, MAX_UNIQUE_VALUES));
                    info.put("note", "显示前 " + MAX_UNIQUE_VALUES + " 个唯一值");
                }
            } else if ("DATE".equals(type) && detectedDateFormat != null) {
                info.put("dateFormat", detectedDateFormat);
            }

            return info;
        }
    }

    // ========== 工具 14: 公式计算 ==========
    @McpTool(name = "evaluate_formula", description = "执行Excel原生公式，如=SUM(A1:B1)、=AVERAGE(C1:C10)等。\n" +
            "【功能】：使用POI FormulaEvaluator计算单元格中的公式值。\n" +
            "【参数】：\n" +
            "  - filePath: 文件路径\n" +
            "  - row: 行索引(0-based)\n" +
            "  - col: 列索引(0-based，A=0，B=1...)\n" +
            "【示例】：计算第5行C列单元格的公式值\n" +
            "  row=4, col=2")
    public Map<String, Object> evaluateFormula(
            @McpToolParam(name = "filePath", description = "Excel文件路径") String filePath,
            @McpToolParam(name = "sheetName", description = "Sheet名称") String sheetName,
            @McpToolParam(name = "row", description = "行索引(0-based)") int row,
            @McpToolParam(name = "col", description = "列索引(0-based)") int col) {
        try {
            File safeFile = getSafeFile(filePath);

            int estimatedRows = estimateRowCount(safeFile, sheetName);
            if (estimatedRows > FORMULA_MAX_ROWS) {
                return Map.of("success", false, "error",
                        "文件行数(" + estimatedRows + ")超过公式计算限制(" + FORMULA_MAX_ROWS + ")，请使用 streaming reader 进行大数据处理");
            }

            try (FileInputStream fis = new FileInputStream(safeFile);
                 XSSFWorkbook workbook = new XSSFWorkbook(fis)) {

                FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
                Sheet sheet = getSheet(workbook, sheetName, null);

                Row targetRow = sheet.getRow(row);
                if (targetRow == null) {
                    return Map.of("success", false, "error", "行不存在: " + row);
                }

                Cell cell = targetRow.getCell(col, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);

                CellType cellType = cell.getCellType();
                if (cellType == CellType.FORMULA) {
                    CellValue cellValue = evaluator.evaluate(cell);
                    Object result = getCellValueFromCellValue(cellValue, cell);
                    evaluator.clearAllCachedResultValues();

                    return successResponse(Map.of(
                            "row", row,
                            "column", col,
                            "cellReference", getCellReference(col, row),
                            "formula", cell.getCellFormula(),
                            "value", result,
                            "type", cellValue.getCellType().toString()
                    ));
                } else {
                    return successResponse(Map.of(
                            "row", row,
                            "column", col,
                            "cellReference", getCellReference(col, row),
                            "value", getCellValue(cell),
                            "type", cellType.toString(),
                            "note", "该单元格不是公式类型"
                    ));
                }
            }
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 16: 跨文件 Schema 对比 ==========
    @McpTool(name = "compare_schemas", description = "跨文件 Schema 对比工具，对比多个Excel文件的列结构。\n" +
            "【功能】：\n" +
            "  1. 提取每个文件的列信息（列名、类型、范围等）\n" +
            "  2. 基于列名相似度匹配，识别相同/相似列\n" +
            "  3. 提供列映射建议，用于 join_tables 等操作\n" +
            "【参数】：\n" +
            "  - sources: 文件列表 [{path, sheetName, alias}, ...]\n" +
            "  - threshold: 相似度阈值 (0-1)，默认 0.6\n" +
            "【返回】：每个文件的 schema 摘要 + 列匹配建议\n" +
            "【示例】：对比客户表A和客户表B的列结构\n" +
            "  sources=[\n" +
            "    {path:\"/data/客户A.xlsx\", sheetName:\"Sheet1\", alias:\"A\"},\n" +
            "    {path:\"/data/客户B.xlsx\", sheetName:\"客户表\", alias:\"B\"}\n" +
            "  ], threshold=0.7")
    public Map<String, Object> compareSchemas(
            @McpToolParam(name = "sources", description = "文件列表，每个包含 path, sheetName, alias") List<Map<String, String>> sources,
            @McpToolParam(name = "threshold", description = "相似度阈值 (0-1)，默认 0.6") Double threshold,
            @McpToolParam(name = "sampleSize", description = "采样行数，默认100") Integer sampleSize) {
        try {
            double simThreshold = threshold != null ? threshold : 0.6;
            int sample = sampleSize != null ? sampleSize : 100;

            if (sources == null || sources.size() < 2) {
                return Map.of("success", false, "error", "至少需要提供2个文件进行对比");
            }

            Map<String, Map<String, Map<String, Object>>> fileSchemas = new LinkedHashMap<>();

            for (Map<String, String> source : sources) {
                String path = source.get("path");
                String sheetName = source.get("sheetName");
                String alias = source.get("alias");

                if (path == null || path.isBlank()) {
                    return Map.of("success", false, "error", "path 不能为空");
                }

                if (alias == null || alias.isBlank()) {
                    alias = "file_" + fileSchemas.size();
                }

                File safeFile = getSafeFile(path);
                Map<String, Object> schema = extractSchema(safeFile, sheetName, sample);
                fileSchemas.put(alias, (Map<String, Map<String, Object>>) schema.get("columns"));
            }

            List<String> aliases = new ArrayList<>(fileSchemas.keySet());
            List<Map<String, Object>> matches = findColumnMatches(fileSchemas, aliases, simThreshold);

            return successResponse(Map.of(
                    "fileCount", sources.size(),
                    "files", aliases,
                    "schemas", fileSchemas,
                    "columnMatches", matches,
                    "threshold", simThreshold,
                    "matchCount", matches.size(),
                    "note", "columnMatches 中的映射建议可用于 join_tables 的 joinOn 配置"
            ));
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private Map<String, Object> extractSchema(File file, String sheetName, int sampleSize) throws IOException {
        try (Workbook workbook = StreamingReader.builder().rowCacheSize(STREAMING_ROW_CACHE_SIZE).open(file)) {
            Sheet sheet = getSheet(workbook, sheetName, null);
            Iterator<Row> rowIterator = sheet.iterator();

            List<String> headers = new ArrayList<>();
            if (rowIterator.hasNext()) {
                for (Cell cell : rowIterator.next()) {
                    headers.add(getCellValueAsString(cell));
                }
            }

            Map<String, TypeInfo> typeMap = new LinkedHashMap<>();
            for (String header : headers) {
                typeMap.put(header, new TypeInfo());
            }

            int rowCount = 0;
            while (rowIterator.hasNext() && rowCount < sampleSize) {
                Row row = rowIterator.next();
                for (int i = 0; i < headers.size(); i++) {
                    Object value = getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                    typeMap.get(headers.get(i)).analyze(value);
                    rowCount++;
                }
            }

            Map<String, Map<String, Object>> result = new LinkedHashMap<>();
            for (Map.Entry<String, TypeInfo> entry : typeMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().getExtendedInfo(rowCount));
            }

            return Map.of("columns", result, "rowCount", rowCount);
        }
    }

    private List<Map<String, Object>> findColumnMatches(Map<String, Map<String, Map<String, Object>>> schemas,
                                                        List<String> aliases, double threshold) {
        List<Map<String, Object>> matches = new ArrayList<>();

        if (aliases.size() < 2) return matches;

        Map<String, Map<String, Double>> similarityMatrix = new LinkedHashMap<>();

        for (int i = 0; i < aliases.size(); i++) {
            for (int j = i + 1; j < aliases.size(); j++) {
                String alias1 = aliases.get(i);
                String alias2 = aliases.get(j);
                Map<String, Map<String, Object>> schema1 = schemas.get(alias1);
                Map<String, Map<String, Object>> schema2 = schemas.get(alias2);

                for (String col1 : schema1.keySet()) {
                    for (String col2 : schema2.keySet()) {
                        double sim = compareColumnSimilarity(col1, col2);
                        if (sim >= threshold) {
                            Map<String, Object> match = new LinkedHashMap<>();
                            match.put("column1", alias1 + "." + col1);
                            match.put("column2", alias2 + "." + col2);
                            match.put("similarity", sim);
                            match.put("type1", schema1.get(col1).get("inferredType"));
                            match.put("type2", schema2.get(col2).get("inferredType"));
                            matches.add(match);
                        }
                    }
                }
            }
        }

        matches.sort((a, b) -> Double.compare((Double) b.get("similarity"), (Double) a.get("similarity")));
        return matches;
    }

    private double compareColumnSimilarity(String s1, String s2) {
        if (s1 == null || s2 == null) return 0;
        if (s1.equals(s2)) return 1.0;

        s1 = s1.toLowerCase().replaceAll("[\\s_]", "");
        s2 = s2.toLowerCase().replaceAll("[\\s_]", "");

        if (s1.equals(s2)) return 1.0;
        if (s1.contains(s2) || s2.contains(s1)) return 0.8;

        int len1 = s1.length();
        int len2 = s2.length();
        int[][] dp = new int[len1 + 1][len2 + 1];

        for (int i = 1; i <= len1; i++) {
            dp[i][0] = i;
        }
        for (int j = 1; j <= len2; j++) {
            dp[0][j] = j;
        }

        for (int i = 1; i <= len1; i++) {
            for (int j = 1; j <= len2; j++) {
                if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + Math.min(dp[i - 1][j], Math.min(dp[i][j - 1], dp[i - 1][j - 1]));
                }
            }
        }

        int maxLen = Math.max(len1, len2);
        return maxLen > 0 ? 1.0 - (double) dp[len1][len2] / maxLen : 0;
    }

    private Object getCellValueFromCellValue(CellValue cellValue, Cell cell) {
        return switch (cellValue.getCellType()) {
            case NUMERIC -> cellValue.getNumberValue();
            case STRING -> cellValue.getStringValue();
            case BOOLEAN -> cellValue.getBooleanValue();
            case FORMULA -> cell.getCellFormula();
            default -> cellValue.toString();
        };
    }

    private String getCellReference(int col, int row) {
        StringBuilder sb = new StringBuilder();
        int c = col;
        while (c >= 0) {
            sb.insert(0, (char) ('A' + c % 26));
            c = c / 26 - 1;
        }
        return sb.toString() + (row + 1);
    }

    private int estimateRowCount(File file, String sheetName) {
        try (Workbook wb = StreamingReader.builder()
                .rowCacheSize(FORMULA_ROW_ESTIMATE_SAMPLE)
                .bufferSize(STREAMING_BUFFER_SIZE)
                .open(file)) {
            Sheet sheet = getSheet(wb, sheetName, null);
            int rowCount = 0;
            Iterator<Row> iterator = sheet.iterator();

            while (iterator.hasNext()) {
                iterator.next();
                rowCount++;
                if (rowCount >= FORMULA_ROW_ESTIMATE_SAMPLE * 10) {
                    return FORMULA_MAX_ROWS + 1;
                }
            }

            return rowCount;
        } catch (Exception e) {
            return FORMULA_MAX_ROWS + 1;
        }
    }

    // ========== 安全防御核心逻辑 (升级版：多目录匹配) ==========
    private File getSafeFile(String requestedPath) throws IOException {
        File targetFile = new File(requestedPath);
        String targetCanonicalPath = targetFile.getCanonicalPath();

        boolean isAllowed = false;
        for (String allowedDir : ALLOWED_DIRECTORIES) {
            if (targetCanonicalPath.startsWith(allowedDir)) {
                isAllowed = true;
                break;
            }
        }

        if (!isAllowed) {
            throw new SecurityException("🚨 越权拦截：禁止访问授权工作区之外的路径 -> " + targetCanonicalPath +
                    "\n当前允许的目录有: " + ALLOWED_DIRECTORIES);
        }
        return targetFile;
    }

    // ========== 辅助与解析逻辑 ==========
    private static MapContext createJexlContext(List<String> headers, Row row) {
        MapContext context = new MapContext();
        for (int i = 0; i < headers.size(); i++) {
            Object value = getCellValue(row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
            context.set(headers.get(i).replaceAll("\\s+", "_"), value);
            context.set("col_" + convertIndexToColumnLetter(i), value);
        }
        return context;
    }

    private static final ExecutorService JEXL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    private static final long JEXL_TIMEOUT_MS = 5000;

    private static boolean evaluateExpression(JexlExpression expression, MapContext context) {
        FutureTask<Boolean> task = new FutureTask<>(() -> {
            Object result = expression.evaluate(context);
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            if (result instanceof Number) {
                return ((Number) result).doubleValue() != 0.0;
            }
            if (result instanceof String) {
                return !((String) result).isEmpty();
            }
            return false;
        });

        try {
            JEXL_EXECUTOR.submit(task);
            return task.get(JEXL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            task.cancel(true);
            throw new RuntimeException("表达式执行超时(" + JEXL_TIMEOUT_MS + "ms)，请优化查询条件");
        } catch (JexlException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private static Object getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        return switch (cell.getCellType()) {
            case NUMERIC -> {
                // 核心改动：如果 POI 识别到底层带有日期格式，直接让 DataFormatter 格式化输出
                if (DateUtil.isCellDateFormatted(cell)) {
                    yield DATA_FORMATTER.formatCellValue(cell);
                } else {
                    // 普通数字依然返回 Double，确保 JEXL 数学运算 (如 > 16) 完美生效
                    yield cell.getNumericCellValue();
                }
            }
            case BOOLEAN -> cell.getBooleanCellValue();
            case STRING -> cell.getStringCellValue().trim();
            case FORMULA -> {
                // 公式列也做同等升级
                try {
                    if (DateUtil.isCellDateFormatted(cell)) {
                        yield DATA_FORMATTER.formatCellValue(cell);
                    }
                    yield cell.getNumericCellValue();
                } catch (Exception e) {
                    yield cell.getStringCellValue();
                }
            }
            // 空值、错误值等其他兜底情况，统统交给翻译官处理成字符串
            default -> DATA_FORMATTER.formatCellValue(cell);
        };
    }

    private static String getCellValueAsString(Cell cell) {
        Object value = getCellValue(cell);
        return value != null ? value.toString() : "";
    }

    private static void copyCellValue(Cell srcCell, Cell destCell) {
        if (srcCell == null) {
            return;
        }
        switch (srcCell.getCellType()) {
            case STRING -> destCell.setCellValue(srcCell.getStringCellValue());
            case NUMERIC -> destCell.setCellValue(srcCell.getNumericCellValue());
            case BOOLEAN -> destCell.setCellValue(srcCell.getBooleanCellValue());
            case FORMULA -> destCell.setCellFormula(srcCell.getCellFormula());
            default -> destCell.setCellValue(srcCell.toString());
        }
    }

    private static int convertColumnLetterToIndex(String letter) {
        int index = 0;
        for (int i = 0; i < letter.length(); i++) {
            index = index * 26 + (letter.charAt(i) - 'A' + 1);
        }
        return index - 1;
    }

    private static String convertIndexToColumnLetter(int index) {
        StringBuilder sb = new StringBuilder();
        index++;
        while (index > 0) {
            sb.append((char) ('A' + (index - 1) % 26));
            index = (index - 1) / 26;
        }
        return sb.reverse().toString();
    }

    private static Map<String, Object> successResponse(Map<String, Object> data) {
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("success", true);
        res.put("data", data);
        return res;
    }

    private static Map<String, Object> errorResponse(Exception e) {
        logger.error("[SheetMind Error] {}", e.getMessage(), e);
        return Map.of("success", false, "error", e.getMessage());
    }

    // ========== 程序入口 ==========
    public static void main(String[] args) {
        logger.info("SheetMind Server (Enterprise) Started...");
        // 解析命令行传入的目录路径作为安全白名单
        for (String arg : args) {
            File dir = new File(arg);
            if (dir.exists() && dir.isDirectory()) {
                try {
                    // 统一转换为绝对路径形式存入白名单
                    String canonicalPath = dir.getCanonicalPath();
                    // 确保路径以分隔符结尾，防止 /data/app 误匹配 /data/apple
                    if (!canonicalPath.endsWith(File.separator)) {
                        canonicalPath += File.separator;
                    }
                    ALLOWED_DIRECTORIES.add(canonicalPath);
                } catch (IOException e) {
                    logger.warn("无法解析授权路径: {}", arg);
                }
            }
        }

        // 兜底机制：如果没传任何参数，默认放开整个用户主目录 (Convention over Configuration)
        if (ALLOWED_DIRECTORIES.isEmpty()) {
            try {
                // 获取类似 /Users/ryu/ 的路径作为默认工作区
                String defaultDir = new File(System.getProperty("user.home")).getCanonicalPath() + File.separator;
                ALLOWED_DIRECTORIES.add(defaultDir);
                logger.info("未指定工作区参数，已采用默认配置：允许访问整个用户主目录 -> {}", defaultDir);
            } catch (IOException e) {
                logger.error("默认工作区初始化失败", e);
            }
        } else {
            logger.info("已锁定安全工作区: {}", ALLOWED_DIRECTORIES);
        }
// 1. 直接在代码里实例化 Builder，告别外部配置文件！
        McpServerConfiguration.Builder configBuilder = McpServerConfiguration.builder()
                .name("sheetmind-server")
                .version("1.0.0");

        McpServers.run(SheetMindServer.class, args).startStdioServer(configBuilder);
    }
}