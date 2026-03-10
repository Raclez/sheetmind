package com.openclaw.sheetmind;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * Generate example Excel file for testing SheetMind
 */
public class ExampleDataGenerator {
    
    public static void main(String[] args) {
        String filePath = "examples/sample_data.xlsx";
        generateSampleData(filePath, 1000);
        System.out.println("Generated sample data at: " + filePath);
    }
    
    public static void generateSampleData(String filePath, int rowCount) {
        // Use SXSSF for memory-efficient writing
        SXSSFWorkbook workbook = new SXSSFWorkbook(100); // keep 100 rows in memory
        workbook.setCompressTempFiles(true);
        
        Sheet sheet = workbook.createSheet("SalesData");
        
        // Create header row
        Row header = sheet.createRow(0);
        String[] headers = {"ID", "Product", "Category", "Region", "Price", "Quantity", "Total", "Status", "Date"};
        for (int i = 0; i < headers.length; i++) {
            Cell cell = header.createCell(i);
            cell.setCellValue(headers[i]);
        }
        
        Random random = new Random(42); // Fixed seed for reproducibility
        String[] products = {"Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones"};
        String[] categories = {"Electronics", "Accessories", "Computers", "Mobile"};
        String[] regions = {"North", "South", "East", "West", "Central"};
        String[] statuses = {"Done", "Pending", "Cancelled", "Processing"};
        
        // Create data rows
        for (int rowNum = 1; rowNum <= rowCount; rowNum++) {
            Row row = sheet.createRow(rowNum);
            
            // ID
            row.createCell(0).setCellValue(rowNum);
            
            // Product
            row.createCell(1).setCellValue(products[random.nextInt(products.length)]);
            
            // Category
            row.createCell(2).setCellValue(categories[random.nextInt(categories.length)]);
            
            // Region
            row.createCell(3).setCellValue(regions[random.nextInt(regions.length)]);
            
            // Price (50-2000)
            double price = 50 + random.nextDouble() * 1950;
            row.createCell(4).setCellValue(Math.round(price * 100.0) / 100.0);
            
            // Quantity (1-100)
            int quantity = 1 + random.nextInt(100);
            row.createCell(5).setCellValue(quantity);
            
            // Total = Price * Quantity
            double total = price * quantity;
            row.createCell(6).setCellValue(Math.round(total * 100.0) / 100.0);
            
            // Status
            row.createCell(7).setCellValue(statuses[random.nextInt(statuses.length)]);
            
            // Date (2023-2024)
            int year = 2023 + random.nextInt(2);
            int month = 1 + random.nextInt(12);
            int day = 1 + random.nextInt(28);
            String date = String.format("%04d-%02d-%02d", year, month, day);
            row.createCell(8).setCellValue(date);
        }
        
        // Auto-size columns - for SXSSF we need to track columns first
        if (sheet instanceof SXSSFSheet) {
            ((SXSSFSheet) sheet).trackAllColumnsForAutoSizing();
        }
        for (int i = 0; i < headers.length; i++) {
            sheet.autoSizeColumn(i);
        }
        
        // Write to file
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            workbook.write(fos);
            workbook.dispose(); // Clean up temporary files
            System.out.println("Generated " + rowCount + " rows in " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}