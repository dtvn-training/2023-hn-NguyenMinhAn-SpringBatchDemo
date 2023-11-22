package com.annm.spring.batch.config;

import com.annm.spring.batch.entity.Customer;
import com.annm.spring.batch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private DataSource dataSource;

    @Bean
    public FlatFileItemReader<Customer> reader(){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("/Users/nguyenminhan/Workspace/Spring/Spring Batch/SpringBatch/batch-processing-demo/src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Customer> lineMapper(){
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }

    @Bean
    public RepositoryItemWriter<Customer> writer(){
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step step1(){
        return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job runJob(){
        return jobBuilderFactory.get("import-customer")
                .flow(step1()).end().build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }



    private class customerMapper implements RowMapper<Customer>{
        @Override
        public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
            Customer customer = new Customer();
            customer.setId(rs.getInt("customer_id"));
            customer.setContactNo(rs.getString("contact"));
            customer.setCountry(rs.getString("country"));
            customer.setDob(rs.getString("dob"));
            customer.setEmail(rs.getString("email"));
            customer.setFirstName(rs.getString("first_name"));
            customer.setLastName(rs.getString("last_name"));
            return customer;
        }
    }

    @Bean
    public JdbcCursorItemReader<Customer> jdbcCursorItemReader(){
        JdbcCursorItemReader<Customer> jdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcCursorItemReader.setDataSource(dataSource);
        jdbcCursorItemReader.setSql("SELECT * FROM customer");
        customerMapper customerMapper = new customerMapper();
        jdbcCursorItemReader.setRowMapper(customerMapper);
        return jdbcCursorItemReader;
    }

    @Bean
    public FlatFileItemWriter<Customer> dbToCsvWriter(){
        FlatFileItemWriter<Customer> dbToCsvWriter = new FlatFileItemWriter<>();
        dbToCsvWriter.setResource(new FileSystemResource("/Users/nguyenminhan/Workspace/Spring/Spring Batch/SpringBatch/batch-processing-demo/src/main/resources/OutputCustomers.csv"));
        dbToCsvWriter.setLineAggregator(new DelimitedLineAggregator<Customer>() {{
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<Customer>() {{setNames(new String[]{ "id", "firstName","lastName", "email", "gender", "contactNo", "country", "dob"});}});
                }});
        dbToCsvWriter.setHeaderCallback
                (writer -> writer.write("id, firstName, lastName, email, gender, contactNo, country, dob"));
        return dbToCsvWriter;
    }

    @Bean
    public Step step2(){
        return stepBuilderFactory.get("dbToCsv-step")
                .<Customer, Customer>chunk(10)
                .reader(jdbcCursorItemReader())
                .writer(dbToCsvWriter())
                .build();
    }

    @Bean
    public Job runJob2(){
        return jobBuilderFactory.get("dbToCsv")
                .incrementer(new RunIdIncrementer())
                .start(step2())
                .build();
    }



}
