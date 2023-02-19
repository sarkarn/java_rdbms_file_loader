package com.nns.file.loader.service;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class ExcelFileLoader {

    @Autowired
    JdbcTemplate template;

    public void loadExcel(String tableName, String fileNameColumn, String numericFields) {

        try(Connection conn = template.getDataSource().getConnection();) {

            File folder = new File("c:\file_location");
            File [] listOfFiles = folder.listFiles();
            List<String> numericFieldsConversionList = new ArrayList<>();
            numericFieldsConversionList = Arrays.stream(numericFields.split(",")).collect(toList());
            for(File file : listOfFiles) {
                String fileName = file.getName();
                String sourceSystemColumn  = fileName.substring(0,fileName.indexOf("_"));
                try(FileInputStream fileIs = new FileInputStream(file);
                    Workbook workbook = new XSSFWorkbook(fileIs);) {
                    Sheet sheet = workbook.getSheetAt(0);
                    Iterator<Row> rowIterator = sheet.rowIterator();
                    Row headerRow = rowIterator.next();

                    StringBuilder headerSb = new StringBuilder();
                    Iterator<Cell> headerRowIt = headerRow.cellIterator();
                    while(headerRowIt.hasNext()){
                        headerSb.append(headerRowIt.next().toString());
                        headerSb.append(",");
                    }
                    headerSb = headerSb.deleteCharAt(headerSb.lastIndexOf(","));
                    String headerLine = headerSb.toString();
                    headerLine = headerLine +","+fileNameColumn;

                    List<String> headerColumnList = Arrays.stream(headerLine.split(",")).collect(toList());
                    int lastColumn = headerColumnList.size() - 1;

                    StringBuilder sql = new StringBuilder("insert into "+tableName+"(");
                    sql.append(headerLine+") values(");
                    for(int i=0; i<headerColumnList.size();i++ ){
                        sql.append("?,");
                    }
                    sql.deleteCharAt(sql.lastIndexOf(","));
                    sql.append(")");

                    System.out.println(" SQL String :: "+sql.toString());
                    System.out.println("Last Column :: "+ lastColumn);

                    try(PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                        while(rowIterator.hasNext()) {
                            Row valueRow = rowIterator.next();
                            for(int i=0; i<lastColumn;i++){
                                Cell cell = valueRow.getCell(i);
                                String columnHeader = headerColumnList.get(i).toUpperCase();
                                if(cell !=null){
                                    switch(cell.getCellType()) {
                                        case STRING:
                                            pstmt.setString(i+1,cell.getStringCellValue());
                                            break;
                                        case NUMERIC:
                                            if(DateUtil.isCellDateFormatted(cell)){
                                                Date dateValue = cell.getDateCellValue();
                                                pstmt.setDate(i+1,(java.sql.Date)dateValue);
                                            }else{
                                                if(numericFieldsConversionList.contains(columnHeader)){
                                                    DecimalFormat df = new DecimalFormat("####.000");
                                                    pstmt.setString(i+1,df.format(
                                                            new BigDecimal(cell.getNumericCellValue())));
                                                }else{
                                                    DecimalFormat df = new DecimalFormat("####");
                                                    pstmt.setString(i+1,df.format(
                                                            new BigDecimal(cell.getNumericCellValue())));
                                                }
                                            }
                                            break;
                                        case BOOLEAN:
                                            pstmt.setString(i+1,"");
                                            break;
                                        case FORMULA:
                                            pstmt.setString(i+1,"");
                                            break;
                                        case BLANK:
                                            pstmt.setString(i+1,"");
                                            break;
                                        default:
                                            break;
                                    }
                                }else{
                                    pstmt.setString(i+1,"");
                                }
                            }
                            int indexOfFileSourceColumn = lastColumn + 1;
                            pstmt.setString(indexOfFileSourceColumn,fileName);
                            pstmt.addBatch();
                        }
                        pstmt.executeBatch();
                    }catch(Exception ix){
                        ix.printStackTrace();
                    }

                }
            }

        }catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
