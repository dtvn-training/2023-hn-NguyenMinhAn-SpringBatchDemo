package com.annm.spring.batch.service;

import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class BigQueryService {
    public String basicQuery(){

        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(
                "SELECT"
                + "flattened_tags,"
                + "COUNT(*) tag_count"
        + "FROM ("
                + "SELECT"
                + "SPLIT(tags, '|')tags"
                + "FROM"
                + "bigquery-public-data.stackoverflow.posts_questions"
        + "WHERE"
        + "EXTRACT(year"
                + "FROM"
                + "creation_date) >= 2022)"
        + "CROSS JOIN"
        + "UNNEST(tags) flattened_tags"
        + "GROUP BY"
        + "flattened_tags"
        + "ORDER BY"
        + "tag_count DESC"
        + "LIMIT"
        + "10"
        )
                .setUseLegacySql(false)
                .build();

        String jobIdStr = UUID.randomUUID().toString();

        JobId jobId = JobId.of(jobIdStr);

        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        try {
            queryJob = queryJob.waitFor();

            if (queryJob == null){
                System.out.println("Job no longer exists");
                return "Job no longer exists";
            }

            if (queryJob.getStatus().getError() != null){
                System.out.println("Job failed");
                return "Job failed";
            }

            TableResult result = queryJob.getQueryResults();

            for (FieldValueList row : result.iterateAll()){
                String flattened_tags = row.get("flattened_tags").getStringValue();
                int tag_count = row.get("tag_count").getNumericValue().intValue();
                System.out.println("Flattened tags: " + flattened_tags + "tag_count" + tag_count);
            }

        } catch (Exception e){
            System.out.println(e.getMessage());
            return "failed to query gcp";
        }

        return "Basic query done";
    }
}
