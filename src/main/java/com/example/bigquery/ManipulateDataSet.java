package com.example.bigquery;

import com.google.cloud.bigquery.*;

import java.io.IOException;

public class ManipulateDataSet {

    public static void main(String... args) throws Exception {
        createDataSet();
    }

    private static void createDataSet() throws IOException {
        MyUtils.setProxy();
        BigQuery bigQuery = MyUtils.getBigQueryByServiceAccount();

        DatasetInfo datasetInfo = DatasetInfo.newBuilder("dsCreatedByJava02").setLocation("asia-northeast1").build();
        Dataset dataset = bigQuery.create(datasetInfo);
        System.out.printf("Dataset created: %s", dataset.getDatasetId().getDataset());
    }
}
