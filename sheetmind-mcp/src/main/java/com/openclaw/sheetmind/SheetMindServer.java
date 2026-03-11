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

        // 注册自定义函数库，命名空间为 "utils"
        Map<String, Object> functions = new HashMap<>();
        functions.put("utils", new SheetMindUtils());

        JEXL_ENGINE = new JexlBuilder()
                .sandbox(sandbox)
                .namespaces(functions) // 注入命名空间
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

    // ========== 工具 0: 列出所有 Sheet ==========
    @McpTool(name = "list_sheets", description = "列出Excel文件中的所有Sheet名称")
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
    @McpTool(name = "inspect_spreadsheet", description = "获取工作表元数据和前 5 行预览，以便编写准确的 JEXL 筛选条件")
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
    @McpTool(name = "smart_search_rows", description = "使用 JEXL 引擎流式检索 Excel 数据行。\n" +
            "【⚠️ 严格语法警告：必须使用 Java 语法，绝不能使用 SQL/Python 语法！】\n" +
            "1. 逻辑操作符：必须使用 && (与) 和 || (或)，严禁使用 and / or。\n" +
            "2. 字符串匹配：必须使用变量方法调用，如 资产类型.contains('黄金')，严禁使用 SQL 的 like 或单独的 contains 关键字。\n" +
            "3. 正则/高级匹配：可调用预置函数 utils:match(资产类型, '.*黄金.*')。\n" +
            "4. 判空：utils:isEmpty(列名)。\n" +
            "【正确示例】：资产类型.contains('黄金') && 交易金额 > 3000")
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

    @McpTool(name = "update_cell", description = "精准更新特定单元格。注意：为保护系统内存，不支持修改大于 30MB 的超大文件。")
    public Map<String, Object> updateCell(
            @McpToolParam(name = "filePath", description = "Excel文件绝对路径") String filePath,
            @McpToolParam(name = "row", description = "行索引") int row,
            @McpToolParam(name = "col", description = "列索引") int col,
            @McpToolParam(name = "value", description = "新值") String value,
            @McpToolParam(name = "sheetName", description = "Sheet名称，不指定则默认第一个") String sheetName) {
        try {
            File safeFile = getSafeFile(filePath);

            // 🚨 真正的防 OOM 策略：体积熔断
            if (safeFile.length() > LARGE_FILE_THRESHOLD) {
                return Map.of(
                        "success", false,
                        "error", String.format("文件过大 (%.1f MB)。为防止服务器内存溢出，拒绝执行 update_cell。请手动修改或使用 Python 脚本处理。",
                                safeFile.length() / (1024.0 * 1024.0))
                );
            }

            File backupPath = new File(safeFile.getAbsolutePath() + ".bak");
            File tempPath = new File(safeFile.getAbsolutePath() + ".tmp");

            Files.copy(safeFile.toPath(), backupPath.toPath(), StandardCopyOption.REPLACE_EXISTING);

            // 直接传入 safeFile，底层已是最优的 OPCPackage 随机访问模式
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
            return errorResponse(e);
        }
    }

    // ========== 工具 4: 数据统计分析 ==========
    @McpTool(name = "summarize_column", description = "计算指定数值列的统计信息...")
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
        System.err.println("[SheetMind Error] " + e.getMessage());
        return Map.of("success", false, "error", e.getMessage());
    }

    // ========== JEXL 自定义业务函数库 ==========
    public static class SheetMindUtils {

        /** 正则模糊匹配: query="utils:match(col_A, '.*黄金.*')" */
        public boolean match(Object value, String regex) {
            if (value == null) return false;
            return value.toString().matches(regex);
        }

        /** 日期对比 (判断单元格日期是否在某个字符串日期之后) */
        public boolean isAfter(Object cellValue, String targetDateStr) {
            if (cellValue == null || cellValue.toString().isBlank()) return false;
            try {
                // 简单的字符串字典序对比 (前提是 DataFormatter 输出的是 yyyy-MM-dd 格式)
                // 复杂业务中这里可以引入 LocalDate 解析
                return cellValue.toString().compareTo(targetDateStr) > 0;
            } catch (Exception e) {
                return false;
            }
        }

        /** 判空检查 */
        public boolean isEmpty(Object value) {
            return value == null || value.toString().trim().isEmpty();
        }

        /** 文本长度判断 */
        public int length(Object value) {
            return value == null ? 0 : value.toString().length();
        }
    }

    // ========== 程序入口 ==========
    public static void main(String[] args) {
        System.err.println("🚀 SheetMind Server (Enterprise) Started...");
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
                    System.err.println("⚠️ 无法解析授权路径: " + arg);
                }
            }
        }

        // 兜底机制：如果没传任何参数，默认放开整个用户主目录 (Convention over Configuration)
        if (ALLOWED_DIRECTORIES.isEmpty()) {
            try {
                // 获取类似 /Users/ryu/ 的路径作为默认工作区
                String defaultDir = new File(System.getProperty("user.home")).getCanonicalPath() + File.separator;
                ALLOWED_DIRECTORIES.add(defaultDir);
                System.err.println("💡 未指定工作区参数，已采用默认配置：允许访问整个用户主目录 -> " + defaultDir);
            } catch (IOException e) {
                System.err.println("⚠️ 默认工作区初始化失败");
            }
        } else {
            System.err.println("🔒 已锁定安全工作区: " + ALLOWED_DIRECTORIES);
        }
// 1. 直接在代码里实例化 Builder，告别外部配置文件！
        McpServerConfiguration.Builder configBuilder = McpServerConfiguration.builder()
                .name("sheetmind-server")
                .version("1.0.0");

        McpServers.run(SheetMindServer.class, args).startStdioServer(configBuilder);
    }
}