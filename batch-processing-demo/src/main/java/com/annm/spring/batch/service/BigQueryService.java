package com.annm.spring.batch.service;

import com.annm.spring.batch.entity.Customer;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class BigQueryService {
    public static GoogleCredentials getCredentials(){

//        List<Customer> customers = new ArrayList<>();

        GoogleCredentials credentials = null;
        File file = new File("D:\\Workspace\\Spring\\SpringBatch\\2023-hn-NguyenMinhAn-SpringBatchDemo\\batch-processing-demo\\src\\main\\resources\\blissful-hash-405809-0af30ef9ddcc.json"); // bigquery.json
        String absolutePath = file.getAbsolutePath();

        final File credentialsPath = new File(absolutePath);
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);

        }catch(Exception e) {
            e.printStackTrace();
        }


//        BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId("blissful-hash-405809").setCredentials(credentials).build().getService();
//
//
//        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT * FROM `blissful-hash-405809.data_demo.customer` WHERE id >= 990;")
//                .setUseLegacySql(false)
//                .build();
//
//        String jobIdStr = UUID.randomUUID().toString();
//
//        JobId jobId = JobId.of(jobIdStr);
//
//        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
//
//
//
//        try {
//            queryJob = queryJob.waitFor();
//
//            if (queryJob == null){
//                System.out.println("Job no longer exists");
//                return null;
//            }
//
//            if (queryJob.getStatus().getError() != null){
//                System.out.println("Job failed");
//                return null;
//            }
//
//            TableResult result = queryJob.getQueryResults();
//
//            for (FieldValueList row : result.iterateAll()){
//                int id = row.get("id").getNumericValue().intValue();
//                String firstName = row.get("firstName").getStringValue();
//                String lastName = row.get("lastName").getStringValue();
//                String email = row.get("email").getStringValue();
//                String gender = row.get("gender").getStringValue();
//                String contactNo = row.get("contactNo").getStringValue();
//                String country = row.get("country").getStringValue();
//                String dob = row.get("dob").getStringValue();
//                Customer customer = new Customer();
//                customer.setId(id);
//                customer.setContactNo(contactNo);
//                customer.setCountry(country);
//                customer.setDob(dob);
//                customer.setEmail(email);
//                customer.setGender(gender);
//                customer.setFirstName(firstName);
//                customer.setLastName(lastName);
//                customers.add(customer);
//            }
//
//        } catch (Exception e){
//            System.out.println(e.getMessage());
//            return null;
//        }

        return credentials;
    }
}
