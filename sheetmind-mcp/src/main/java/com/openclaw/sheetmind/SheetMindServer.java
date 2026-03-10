package com.openclaw.sheetmind;

import com.github.thought2code.mcp.annotated.annotation.McpTool;
import com.github.thought2code.mcp.annotated.McpServers;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * SheetMind MCP Server - Excel processing service for AI agents
 * 
 * Handles large Excel files (millions of rows) with streaming I/O
 * to prevent OOM and provide accurate data filtering for AI.
 */
public class SheetMindServer {
    
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JexlEngine jexlEngine = new JexlBuilder().create();
    
    // Configuration constants
    private static final long UPDATE_FILE_SIZE_LIMIT = 50L * 1024 * 1024; // 50MB
    private static final int UNIQUE_VALUE_LIMIT = 10000;
    private static final int PREVIEW_ROW_LIMIT = 5;
    private static final int DEFAULT_SEARCH_LIMIT = 20;
    private static final int STREAMING_ROW_CACHE_SIZE = 100;
    private static final int STREAMING_BUFFER_SIZE = 4096;
    
    static {
        // JexlEngine configuration is done through JexlBuilder
    }
    
    /**
     * Tool 1: Inspect spreadsheet to get metadata and preview
     */
    @McpTool(name = "inspect_spreadsheet", description = "Get worksheet metadata and preview data (first 5 rows)")
    public String inspectSpreadsheet(@JsonProperty("filePath") String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return errorResponse("File not found: " + filePath);
            }
            
            try (Workbook workbook = StreamingReader.builder()
                    .rowCacheSize(STREAMING_ROW_CACHE_SIZE)
                    .bufferSize(STREAMING_BUFFER_SIZE)
                    .open(path.toFile())) {
                
                Sheet sheet = workbook.getSheetAt(0);
                int rowCount = 0;
                
                // Estimate row count by iterating (streaming doesn't have getPhysicalNumberOfRows)
                Iterator<Row> rowIterator = sheet.iterator();
                List<Map<String, Object>> previewRows = new ArrayList<>();
                List<String> headers = new ArrayList<>();
                
                // Read first row as headers
                if (rowIterator.hasNext()) {
                    Row headerRow = rowIterator.next();
                    rowCount++;
                    for (Cell cell : headerRow) {
                        headers.add(getCellValueAsString(cell));
                    }
                }
                
                // Read up to 5 data rows for preview
                int previewLimit = PREVIEW_ROW_LIMIT;
                while (rowIterator.hasNext() && previewRows.size() < previewLimit) {
                    Row row = rowIterator.next();
                    rowCount++;
                    
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < headers.size(); i++) {
                        Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        rowData.put(headers.get(i), getCellValue(cell));
                    }
                    previewRows.add(rowData);
                }
                
                // Note: Streaming reader cannot provide exact row count without full scan
                // We stop after preview to maintain performance for large files
                ObjectNode response = mapper.createObjectNode();
                response.put("fileName", path.getFileName().toString());
                response.put("sheetName", sheet.getSheetName());
                response.put("previewRowCount", rowCount); // Only rows actually read
                response.put("columnCount", headers.size());
                response.put("note", "Row count is based on preview only; streaming reader cannot determine total rows without full scan");
                
                ArrayNode headersNode = response.putArray("headers");
                headers.forEach(headersNode::add);
                
                ArrayNode previewNode = response.putArray("preview");
                for (Map<String, Object> row : previewRows) {
                    previewNode.add(mapper.valueToTree(row));
                }
                
                return successResponse(response);
                
            } catch (Exception e) {
                return errorResponse("Error reading file: " + e.getMessage());
            }
            
        } catch (Exception e) {
            return errorResponse("Unexpected error: " + e.getMessage());
        }
    }
    
    /**
     * Tool 2: Smart search rows with JEXL expression filtering
     */
    @McpTool(name = "smart_search_rows", description = "Streaming search with JEXL expression filtering (e.g., 'price > 100 && status == \"Done\"')")
    public String smartSearchRows(
            @JsonProperty("filePath") String filePath,
            @JsonProperty("query") String query,
            @JsonProperty("pagination") Map<String, Integer> pagination) {
        
        try {
            int limit = pagination != null ? pagination.getOrDefault("limit", DEFAULT_SEARCH_LIMIT) : DEFAULT_SEARCH_LIMIT;
            int offset = pagination != null ? pagination.getOrDefault("offset", 0) : 0;
            
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return errorResponse("File not found: " + filePath);
            }
            
            // Parse JEXL expression
            JexlExpression expression;
            try {
                expression = jexlEngine.createExpression(query);
            } catch (Exception e) {
                return errorResponse("Invalid JEXL expression: " + e.getMessage());
            }
            
            try (Workbook workbook = StreamingReader.builder()
                    .rowCacheSize(STREAMING_ROW_CACHE_SIZE)
                    .bufferSize(STREAMING_BUFFER_SIZE)
                    .open(path.toFile())) {
                
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                
                // Read headers
                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    Row headerRow = rowIterator.next();
                    for (Cell cell : headerRow) {
                        headers.add(getCellValueAsString(cell));
                    }
                }
                
                List<Map<String, Object>> results = new ArrayList<>();
                int totalFiltered = 0;
                int totalProcessed = 0;
                
                // Skip offset rows
                int skipCount = 0;
                while (rowIterator.hasNext() && skipCount < offset) {
                    Row row = rowIterator.next();
                    totalProcessed++;
                    
                    MapContext context = createJexlContext(headers, row);
                    if (evaluateExpression(expression, context)) {
                        skipCount++;
                    }
                }
                
                // Collect up to limit rows
                while (rowIterator.hasNext() && results.size() < limit) {
                    Row row = rowIterator.next();
                    totalProcessed++;
                    
                    MapContext context = createJexlContext(headers, row);
                    if (evaluateExpression(expression, context)) {
                        Map<String, Object> rowData = new LinkedHashMap<>();
                        for (int i = 0; i < headers.size(); i++) {
                            Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                            rowData.put(headers.get(i), getCellValue(cell));
                        }
                        results.add(rowData);
                        totalFiltered++;
                    }
                }
                
                // Stop after reaching limit to avoid full scan
                // Check if there are more rows (without processing them) to determine hasMore
                boolean hasMore = rowIterator.hasNext();
                
                ObjectNode response = mapper.createObjectNode();
                response.put("rowsProcessed", totalProcessed);
                response.put("rowsFiltered", totalFiltered);
                response.put("returnedCount", results.size());
                
                ArrayNode resultsNode = response.putArray("results");
                for (Map<String, Object> row : results) {
                    resultsNode.add(mapper.valueToTree(row));
                }
                
                ObjectNode paginationNode = response.putObject("pagination");
                paginationNode.put("limit", limit);
                paginationNode.put("offset", offset);
                paginationNode.put("hasMore", hasMore);
                
                return successResponse(response);
                
            } catch (Exception e) {
                return errorResponse("Error processing search: " + e.getMessage());
            }
            
        } catch (Exception e) {
            return errorResponse("Unexpected error: " + e.getMessage());
        }
    }
    
    /**
     * Tool 3: Update cell with atomic backup
     */
    @McpTool(name = "update_cell", description = "Update single cell with atomic backup (creates .bak file before modification)")
    public String updateCell(
            @JsonProperty("filePath") String filePath,
            @JsonProperty("row") int rowIndex,  // 0-based
            @JsonProperty("col") int colIndex,   // 0-based
            @JsonProperty("value") String newValue) {
        
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return errorResponse("File not found: " + filePath);
            }
            
            // Create backup
            Path backupPath = Paths.get(filePath + ".bak");
            Files.copy(path, backupPath, StandardCopyOption.REPLACE_EXISTING);
            
            try {
                // File size check to prevent OOM (XSSFWorkbook loads entire file into memory)
                long fileSize = Files.size(path);
                if (fileSize > UPDATE_FILE_SIZE_LIMIT) {
                    double fileSizeMB = fileSize / (1024.0 * 1024.0);
                    double limitMB = UPDATE_FILE_SIZE_LIMIT / (1024.0 * 1024.0);
                    return errorResponse(String.format("File size (%.2f MB) exceeds limit of %.0f MB for update_cell. Use inspect_spreadsheet and smart_search_rows for large files.", 
                        fileSizeMB, limitMB));
                }
                
                // Read existing workbook
                Workbook workbook;
                if (filePath.toLowerCase().endsWith(".xlsx")) {
                    workbook = new XSSFWorkbook(Files.newInputStream(path));
                } else {
                    return errorResponse("Only .xlsx files are supported for update");
                }
                
                Sheet sheet = workbook.getSheetAt(0);
                Row row = sheet.getRow(rowIndex);
                if (row == null) {
                    row = sheet.createRow(rowIndex);
                }
                
                Cell cell = row.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                
                // Set cell value as string (safe default, preserves leading zeros and large numbers)
                cell.setCellValue(newValue);
                
                // Write to temp file first
                Path tempPath = Files.createTempFile("sheetmind_update_", ".xlsx");
                try (FileOutputStream fos = new FileOutputStream(tempPath.toFile())) {
                    workbook.write(fos);
                }
                workbook.close();
                
                // Atomically replace original file
                Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
                
                ObjectNode response = mapper.createObjectNode();
                response.put("success", true);
                response.put("message", String.format("Cell [%d,%d] updated to '%s'", rowIndex, colIndex, newValue));
                response.put("backupFile", backupPath.toString());
                
                // Delete backup after successful update
                try {
                    Files.deleteIfExists(backupPath);
                    response.put("backupDeleted", true);
                } catch (IOException e) {
                    response.put("backupDeleted", false);
                    response.put("backupDeleteError", e.getMessage());
                }
                
                return successResponse(response);
                
            } catch (Exception e) {
                // Restore from backup on error
                try {
                    if (Files.exists(backupPath)) {
                        Files.copy(backupPath, path, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (Exception restoreEx) {
                    // Log but don't throw
                }
                return errorResponse("Update failed: " + e.getMessage());
            }
            
        } catch (Exception e) {
            return errorResponse("Unexpected error: " + e.getMessage());
        }
    }
    
    /**
     * Tool 4: Summarize column with statistical aggregations
     */
    @McpTool(name = "summarize_column", description = "Calculate statistics for a numeric column (sum, avg, max, min, unique count)")
    public String summarizeColumn(
            @JsonProperty("filePath") String filePath,
            @JsonProperty("column") String column) {  // column letter (A, B, C) or index (0, 1, 2)
        
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return errorResponse("File not found: " + filePath);
            }
            
            try (Workbook workbook = StreamingReader.builder()
                    .rowCacheSize(STREAMING_ROW_CACHE_SIZE)
                    .bufferSize(STREAMING_BUFFER_SIZE)
                    .open(path.toFile())) {
                
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                
                // Read headers
                List<String> headers = new ArrayList<>();
                if (rowIterator.hasNext()) {
                    Row headerRow = rowIterator.next();
                    for (Cell cell : headerRow) {
                        headers.add(getCellValueAsString(cell));
                    }
                }
                
                // Resolve column index
                int colIndex = -1;
                try {
                    // Try as column letter first
                    if (column.matches("[A-Za-z]+")) {
                        colIndex = convertColumnLetterToIndex(column.toUpperCase());
                    } else {
                        // Try as integer index
                        colIndex = Integer.parseInt(column);
                    }
                } catch (Exception e) {
                    return errorResponse("Invalid column identifier: " + column);
                }
                
                if (colIndex < 0 || colIndex >= headers.size()) {
                    return errorResponse("Column index out of range: " + colIndex);
                }
                
                // Statistics
                double sum = 0.0;
                double min = Double.MAX_VALUE;
                double max = Double.MIN_VALUE;
                int count = 0;
                Set<Double> uniqueValues = new HashSet<>();
                final int UNIQUE_LIMIT = UNIQUE_VALUE_LIMIT;
                boolean uniqueLimitReached = false;
                
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    Cell cell = row.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    
                    if (cell.getCellType() == CellType.NUMERIC) {
                        double value = cell.getNumericCellValue();
                        sum += value;
                        min = Math.min(min, value);
                        max = Math.max(max, value);
                        count++;
                        if (!uniqueLimitReached) {
                            uniqueValues.add(value);
                            if (uniqueValues.size() >= UNIQUE_LIMIT) {
                                uniqueLimitReached = true;
                            }
                        }
                    }
                }
                
                ObjectNode response = mapper.createObjectNode();
                response.put("columnName", headers.get(colIndex));
                response.put("columnIndex", colIndex);
                response.put("totalRows", count);
                
                if (count > 0) {
                    response.put("sum", sum);
                    response.put("average", sum / count);
                    response.put("min", min);
                    response.put("max", max);
                    response.put("uniqueCount", uniqueValues.size());
                    if (uniqueLimitReached) {
                        response.put("uniqueCountNote", "Unique count capped at " + UNIQUE_LIMIT + "; actual unique values may be higher");
                    }
                } else {
                    response.put("sum", 0);
                    response.put("average", 0);
                    response.put("min", 0);
                    response.put("max", 0);
                    response.put("uniqueCount", 0);
                    response.put("note", "No numeric values found in column");
                }
                
                return successResponse(response);
                
            } catch (Exception e) {
                return errorResponse("Error summarizing column: " + e.getMessage());
            }
            
        } catch (Exception e) {
            return errorResponse("Unexpected error: " + e.getMessage());
        }
    }
    
    // ========== Helper Methods ==========
    
    private static MapContext createJexlContext(List<String> headers, Row row) {
        MapContext context = new MapContext();
        for (int i = 0; i < headers.size(); i++) {
            Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
            Object value = getCellValue(cell);
            context.set(headers.get(i), value);
            
            // Also set by column letter
            String colLetter = convertIndexToColumnLetter(i);
            context.set("col_" + colLetter, value);
        }
        return context;
    }
    
    private static boolean evaluateExpression(JexlExpression expression, MapContext context) throws JexlException {
        Object result = expression.evaluate(context);
        if (result instanceof Boolean) {
            return (Boolean) result;
        } else if (result instanceof Number) {
            return ((Number) result).doubleValue() != 0.0;
        } else if (result instanceof String) {
            return !((String) result).isEmpty();
        }
        return false;
    }
    
    private static Object getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        
        switch (cell.getCellType()) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                }
                return cell.getNumericCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case FORMULA:
                try {
                    return cell.getNumericCellValue();
                } catch (Exception e) {
                    try {
                        return cell.getStringCellValue();
                    } catch (Exception e2) {
                        return cell.getCellFormula();
                    }
                }
            case STRING:
                return cell.getStringCellValue().trim();
            case BLANK:
                return "";
            default:
                return "";
        }
    }
    
    private static String getCellValueAsString(Cell cell) {
        Object value = getCellValue(cell);
        return value != null ? value.toString() : "";
    }
    
    private static int convertColumnLetterToIndex(String columnLetter) {
        int index = 0;
        for (int i = 0; i < columnLetter.length(); i++) {
            char c = columnLetter.charAt(i);
            index = index * 26 + (c - 'A' + 1);
        }
        return index - 1; // Zero-based
    }
    
    private static String convertIndexToColumnLetter(int index) {
        StringBuilder sb = new StringBuilder();
        index++; // Convert to 1-based
        while (index > 0) {
            int remainder = (index - 1) % 26;
            sb.append((char) ('A' + remainder));
            index = (index - 1) / 26;
        }
        return sb.reverse().toString();
    }
    
    private static String successResponse(ObjectNode data) throws JsonProcessingException {
        ObjectNode response = mapper.createObjectNode();
        response.put("success", true);
        response.set("data", data);
        return mapper.writeValueAsString(response);
    }
    
    private static String errorResponse(String message) {
        try {
            ObjectNode response = mapper.createObjectNode();
            response.put("success", false);
            response.put("error", message);
            return mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            return "{\"success\":false,\"error\":\"Failed to serialize error response\"}";
        }
    }
    
    /**
     * Main entry point for MCP stdio server
     */
    public static void main(String[] args) {
        try {
            McpServers.run(SheetMindServer.class, args);
        } catch (Exception e) {
            System.err.println("SheetMind server error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}