package com.openclaw.sheetmind;

import com.github.pjfanning.xlsx.StreamingReader;
import com.github.thought2code.mcp.annotated.McpServers;
import com.github.thought2code.mcp.annotated.annotation.McpServerApplication;
import com.github.thought2code.mcp.annotated.annotation.McpTool;

import com.github.thought2code.mcp.annotated.annotation.McpToolParam;
import com.github.thought2code.mcp.annotated.configuration.McpServerConfiguration;
import org.apache.commons.jexl3.introspection.JexlSandbox;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * SheetMind MCP Server - Excel processing service for AI agents
 * 
 * Handles large Excel files (millions of rows) with streaming I/O
 * to prevent OOM and provide accurate data filtering for AI.
 */
@McpServerApplication
public class SheetMindServer {

    // ========== 核心安全与性能配置 ==========
    // 强制限制大模型的读写范围，防止读取系统密码或敏感配置
    private static final String WORKSPACE_DIR = System.getProperty("user.home") + File.separator + "SheetMind_Workspace";
    private static final int UNIQUE_VALUE_LIMIT = 10000;
    private static final int DEFAULT_SEARCH_LIMIT = 20;
    private static final int STREAMING_ROW_CACHE_SIZE = 100;
    private static final int STREAMING_BUFFER_SIZE = 4096;

    // 🔒 JEXL 安全沙箱配置：只允许基础字符串和数学操作，彻底阻断 RCE 注入
    private static final JexlEngine JEXL_ENGINE;
    static {
        JexlSandbox sandbox = new JexlSandbox(false);
        sandbox.allow(String.class.getName());
        sandbox.allow(Math.class.getName());
        JEXL_ENGINE = new JexlBuilder().sandbox(sandbox).strict(true).silent(false).cache(512).create();
    }

    // ========== 工具 1: 结构探测 ==========
    @McpTool(name = "inspect_spreadsheet", description = "获取工作表元数据和前 5 行预览，以便编写准确的 JEXL 筛选条件")
    public Map<String, Object> inspectSpreadsheet(@McpToolParam(name = "filePath", description = "Excel文件绝对路径") String  filePath) {
        try {
            File file = getSafeFile(filePath);
            try (Workbook workbook = StreamingReader.builder().rowCacheSize(100).open(file)) {
                Sheet sheet = workbook.getSheetAt(0);
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

                return Map.of(
                        "success", true,
                        "fileName", file.getName(),
                        "sheetName", sheet.getSheetName(),
                        "previewRowCount", rowCount,
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
    @McpTool(name = "smart_search_rows", description = "带有 JEXL 逻辑引擎的流式检索...")
    public Map<String, Object> smartSearchRows(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "query", description = "JEXL查询表达式") String query,
            @McpToolParam(name = "pagination", description = "分页参数") Map<String, Integer> pagination) {
        try {
            File file = getSafeFile(filePath);
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;

            JexlExpression expression = (query != null && !query.isBlank()) ? JEXL_ENGINE.createExpression(query) : null;

            try (Workbook workbook = StreamingReader.builder().rowCacheSize(100).open(file)) {
                Sheet sheet = workbook.getSheetAt(0);
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
    @McpTool(name = "update_cell", description = "精准更新特定单元格...")
    public Map<String, Object> updateCell(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "row", description = "行索引") int row,
            @McpToolParam(name = "col", description = "列索引") int col,
            @McpToolParam(name = "value", description = "新值") String value) {
        try {
            File safeFile = getSafeFile(filePath);
            File backupPath = new File(safeFile.getAbsolutePath() + ".bak");
            File tempPath = new File(safeFile.getAbsolutePath() + ".tmp");

            // 1. 创建备份
            Files.copy(safeFile.toPath(), backupPath.toPath(), StandardCopyOption.REPLACE_EXISTING);

            // 2. 双流搬运模式 (O(1) 内存消耗，彻底摒弃 XSSFWorkbook)
            try (InputStream is = new FileInputStream(safeFile);
                 Workbook readWb = StreamingReader.builder().rowCacheSize(100).open(is);
                 SXSSFWorkbook writeWb = new SXSSFWorkbook(100);
                 FileOutputStream fos = new FileOutputStream(tempPath)) {

                Sheet readSheet = readWb.getSheetAt(0);
                Sheet writeSheet = writeWb.createSheet(readSheet.getSheetName());
                Iterator<Row> rowIterator = readSheet.iterator();
                int currentRowIndex = 0;

                while (rowIterator.hasNext()) {
                    Row readRow = rowIterator.next();
                    int logicalRowNum = readRow.getRowNum();

                    // 补齐被流式读取器跳过的空行
                    while (currentRowIndex < logicalRowNum) {
                        if (currentRowIndex == row) {
                            writeSheet.createRow(currentRowIndex).createCell(col).setCellValue(value);
                        }
                        currentRowIndex++;
                    }

                    Row writeRow = writeSheet.createRow(logicalRowNum);
                    currentRowIndex = logicalRowNum + 1;

                    // 拷贝单元格，并在命中目标坐标时进行值替换
                    int maxCol = Math.max((int) readRow.getLastCellNum(), col + 1);
                    for (int c = 0; c < maxCol; c++) {
                        if (logicalRowNum == row && c == col) {
                            writeRow.createCell(c).setCellValue(value);
                        } else {
                            Cell readCell = readRow.getCell(c);
                            if (readCell != null) {
                                copyCellValue(readCell, writeRow.createCell(c));
                            }
                        }
                    }
                }

                // 如果目标行在文件物理尾部之外，追加新行
                while (currentRowIndex <= row) {
                    if (currentRowIndex == row) {
                        writeSheet.createRow(currentRowIndex).createCell(col).setCellValue(value);
                    }
                    currentRowIndex++;
                }

                writeWb.write(fos);
                writeWb.dispose(); // 清理磁盘碎片
            }

            // 3. 原子替换
            Files.move(tempPath.toPath(), safeFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.deleteIfExists(backupPath.toPath());

            return successResponse(Map.of("message", String.format("成功更新单元格 [%d,%d] 为 '%s'", row, col, value)));

        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    // ========== 工具 4: 数据统计分析 ==========
    @McpTool(name = "summarize_column", description = "计算指定数值列的统计信息...")
    public Map<String, Object> summarizeColumn(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "column", description = "列标识") String column) {
        try {
            File safeFile = getSafeFile(filePath);
            try (Workbook workbook = new XSSFWorkbook(safeFile)) {
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();

                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    for (Cell cell : rowIterator.next()) {
                        headers.add(getCellValueAsString(cell));
                    }
                }

                int colIndex = column.matches("[A-Za-z]+") ? convertColumnLetterToIndex(column.toUpperCase()) : Integer.parseInt(column);
                if (colIndex < 0 || colIndex >= headers.size()) {
                    throw new IllegalArgumentException("Column index out of range.");
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

    // ========== 安全防御核心逻辑 ==========
    private File getSafeFile(String requestedPath) throws IOException {
        File workspace = new File(WORKSPACE_DIR);
        if (!workspace.exists()) {
            workspace.mkdirs();
        }

        File targetFile = new File(requestedPath);
        if (!targetFile.getCanonicalPath().startsWith(workspace.getCanonicalPath())) {
            throw new SecurityException("🚨 越权拦截：禁止访问工作区之外的敏感路径 -> " + targetFile.getCanonicalPath());
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

    private static boolean evaluateExpression(JexlExpression expression, MapContext context) {
        try {
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
        } catch (JexlException e) {
            return false; // 静默处理单行脏数据导致的计算失败
        }
    }

    private static Object getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        return switch (cell.getCellType()) {
            case NUMERIC -> DateUtil.isCellDateFormatted(cell) ? cell.getDateCellValue().toString() : cell.getNumericCellValue();
            case BOOLEAN -> cell.getBooleanCellValue();
            case STRING -> cell.getStringCellValue().trim();
            case FORMULA -> {
                try { yield cell.getNumericCellValue(); }
                catch (Exception e) { yield cell.getStringCellValue(); }
            }
            default -> "";
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
        System.err.println("[SheetMind Error] " + e.getMessage());
        return Map.of("success", false, "error", e.getMessage());
    }

    // ========== 程序入口 ==========
    public static void main(String[] args) {
        System.err.println("🚀 SheetMind Server (Enterprise) Started...");
        System.err.println("🔒 Workspace Locked to: " + WORKSPACE_DIR);
// 1. 直接在代码里实例化 Builder，告别外部配置文件！
        McpServerConfiguration.Builder configBuilder = McpServerConfiguration.builder()
                .name("sheetmind-server")
                .version("1.0.0");

        McpServers.run(SheetMindServer.class, args).startStdioServer(configBuilder);
    }
}