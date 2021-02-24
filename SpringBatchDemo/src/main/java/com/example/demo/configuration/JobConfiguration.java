package com.example.demo.configuration;

import java.io.File;
import java.io.IOException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.example.demo.model.Customer;

@Configuration
@EnableBatchProcessing

public class JobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private DataSource datasource;

	
	@Bean
	public JdbcCursorItemReader<Customer> cursorItemReader() //not thread safe
	{
		System.out.println("here");
		JdbcCursorItemReader<Customer> reader=new JdbcCursorItemReader<Customer>();
		reader.setSql("select id,firstname,lastname,birthdate from customer");
		reader.setDataSource(this.datasource);
		BeanPropertyRowMapper mapper=new BeanPropertyRowMapper();
		mapper.setMappedClass(Customer.class);
		reader.setRowMapper(mapper);
		return reader;
	}
	@Bean
	public ItemWriter<Customer> customItemWriter()
	{
		return items ->{
			for(Customer item:items)
			{
				System.out.println(item.getId()+" "+item.getFirstname()+" "+item.getLastname()+"  "+item.getBirthdate()+"\n");
			}
		};
	}
	
	@Bean 
	public FlatFileItemWriter LocalItemWriter() throws IOException
	{
		FlatFileItemWriter writer=new FlatFileItemWriter();
		String outputpath=File.createTempFile("customerOutput", ".out").getAbsolutePath();
		writer.setResource(new FileSystemResource(outputpath));
		System.out.println(outputpath);
		DelimitedLineAggregator lineAggregator = new DelimitedLineAggregator();
		lineAggregator.setDelimiter("|");
		BeanWrapperFieldExtractor fieldExtractor = new BeanWrapperFieldExtractor();
		String[] names={"id","firstname","lastname","birthdate"};
		fieldExtractor.setNames(names);
		lineAggregator.setFieldExtractor(fieldExtractor);
		writer.setLineAggregator(lineAggregator);
		return writer;
	}
	@Bean 
	public FlatFileItemWriter flatfileitemWriter() throws IOException 
	{
		FlatFileItemWriter writer=new FlatFileItemWriter();
		writer.setResource(new ClassPathResource("/data/customer.csv"));
		ClassPathResource resource=new ClassPathResource("/data/customer.csv");
		System.out.println(resource.getURI());
		DelimitedLineAggregator lineAggregator = new DelimitedLineAggregator();
		lineAggregator.setDelimiter("|");
		BeanWrapperFieldExtractor fieldExtractor = new BeanWrapperFieldExtractor();
		String[] names={"id","firstname","lastname","birthdate"};
		fieldExtractor.setNames(names);
		lineAggregator.setFieldExtractor(fieldExtractor);
		writer.setLineAggregator(lineAggregator);
		return writer;
	}
	@Bean
	public Step step1() throws IOException 
	{
		return stepBuilderFactory.get("step1")
				.<Customer,Customer>chunk(2)
				.reader(cursorItemReader())
				.writer(flatfileitemWriter())
				.build();
	}
	@Bean
	public Job job() throws IOException
	{
		return jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
			    .flow(step1())
			    .end().build();
	}
}
