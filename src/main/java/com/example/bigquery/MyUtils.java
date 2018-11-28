package com.example.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class MyUtils {

    public static void setProxy() {
        // HTTP
        System.setProperty("http.proxyHost", "127.0.0.1");
        System.setProperty("http.proxyPort", "1080");
        // HTTPS
        System.setProperty("https.proxyHost", "127.0.0.1");
        System.setProperty("https.proxyPort", "1080");
    }

    public static BigQuery getBigQueryByServiceAccount() throws IOException {
        GoogleCredentials credentials;
        File credentialsPath = new File("F:\\PROJECT\\gcp\\vjxxnet01-4aa4df1065d9.json");
        try(FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }
        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }
}
