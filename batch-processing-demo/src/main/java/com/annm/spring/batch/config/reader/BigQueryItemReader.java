package com.annm.spring.batch.config.reader;

import com.annm.spring.batch.entity.Customer;
import com.annm.spring.batch.service.BigQueryService;
import com.google.cloud.bigquery.*;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Iterator;
import java.util.Objects;

public class BigQueryItemReader<Customer> implements ItemReader<Customer>, InitializingBean {
    private Iterator<FieldValueList> iterator;

    private Converter<FieldValueList, Customer> rowMapper;

    private QueryJobConfiguration queryConfig;

    private String query = "";
    public void setQuery(String query){
        this.query = query;
    }

    public void setJobConfiguration(QueryJobConfiguration queryConfig) {
        this.queryConfig = queryConfig;
    }

    public void setRowMapper(Converter<FieldValueList, Customer> rowMapper) {
        this.rowMapper = rowMapper;
    }

    BigQueryService bigQueryService;
    BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId("blissful-hash-405809").setCredentials(BigQueryService.getCredentials()).build().getService();

    @Override
    public Customer read() throws Exception{
        if (Objects.isNull(iterator)){
            doOpen();
        }
        return iterator.hasNext() ? rowMapper.convert(iterator.next()) : null;
    }

    private void doOpen() throws Exception {
        iterator = bigQuery.query(queryConfig).getValues().iterator();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(this.bigQuery, "BigQuery service must be provided");
        Assert.notNull(this.rowMapper, "Row mapper must be provided");
        Assert.notNull(this.queryConfig, "Job configuration must be provided");
    }
}
