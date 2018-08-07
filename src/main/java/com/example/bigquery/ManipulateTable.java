package com.example.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManipulateTable {

    public static void main(String... args) throws Exception {

        // createTable();
        interTable();

    }

    private static void createTable() throws IOException {
        MyUtils.setProxy();
        BigQuery bigQuery = MyUtils.getBigQueryByServiceAccount();

        TableId tableId = TableId.of("dsCreatedByJava02", "tbCreatedByJava01");
        Field field = Field.of("department", LegacySQLTypeName.STRING);
        Schema schema = Schema.of(field);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigQuery.create(tableInfo);
        System.out.printf("Table created: %s", table.getTableId().getTable());
    }

    private static void interTable() throws Exception {
        MyUtils.setProxy();
        BigQuery bigQuery = MyUtils.getBigQueryByServiceAccount();

        TableId tableId = TableId.of("dsCreatedByJava02", "tbCreatedByJava01");

        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("department", "RISK IT");

        InsertAllResponse response = bigQuery.insertAll(
                InsertAllRequest.newBuilder(tableId).addRow(rowContent).build()
        );

        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {

                System.out.printf("Error %s", entry.getValue().get(0).getMessage());

            }
        }

        System.out.printf("Response: %s", response.toString());
    }

    private static void selectTable() {

    }
}
