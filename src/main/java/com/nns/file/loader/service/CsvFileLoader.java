package com.nns.file.loader.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class CsvFileLoader {

    @Autowired
    JdbcTemplate template;

    public void loadCsv(String tableName, String fileNameColumn, String numericFields) {

        try(Connection conn = template.getDataSource().getConnection();) {

            File folder = new File("c:\file_location");
            File [] listOfFiles = folder.listFiles();
            List<String> numericFieldsConversionList = new ArrayList<>();
            numericFieldsConversionList = Arrays.stream(numericFields.split(",")).collect(toList());
            for(File file : listOfFiles) {
                String fileName = file.getName();
                String sourceSystemColumn  = fileName.substring(0,fileName.indexOf("_"));
                try(Reader reader = Files.newBufferedReader(file.toPath());
                    CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
                    ) {
                    Iterator<CSVRecord> csvRecordIt = csvParser.iterator();
                    CSVRecord headerRow = csvRecordIt.next();

                    String headerLine = headerRow.stream().collect(Collectors.joining(","));
                    headerLine = headerLine + ","+ fileNameColumn;

                    List<String> headerList = Arrays.stream(headerLine.split(",")).collect(toList());

                    StringBuilder sql = new StringBuilder("insert into "+tableName+"(");
                    sql.append(headerLine+") values(");
                    for(int i=0; i<headerList.size();i++ ){
                        sql.append("?,");
                    }
                    sql.deleteCharAt(sql.lastIndexOf(","));
                    sql.append(")");

                    System.out.println(" SQL String :: "+sql.toString());
                    System.out.println("Last Column :: "+ lastColumn);

                    try(PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                        while(csvRecordIt.hasNext()) {
                             CSVRecord csvRecord = csvRecordIt.next();
                             Iterator<String> wordIt = csvRecord.iterator();
                             int i=0;
                             while(wordIt.hasNext()) {
                                 pstmt.setString(++i,wordIt.next());
                            }
                            pstmt.setString(++i,fileName);
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
