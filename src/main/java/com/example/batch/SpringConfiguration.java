package com.example.batch;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Configuration
@PropertySource("classpath:application.properties")
public class SpringConfiguration {
  @Bean
  public Set<String> areaCodes(
      @Value("${geographicUnitsMetadata}") String geographicUnitsMetadata,
      @Value("${areaExcelSheet}") String areaExcelSheet,
      @Value("${areaColumn}") int areaColumn,
      @Value("${areaStartRow}") int areaStartRow
  ) throws IOException {
    return getGeographicalUnitCodesByProperties(geographicUnitsMetadata, areaExcelSheet, areaColumn, areaStartRow);
  }

  @Bean
  Set<String> anzsic06Codes(
      @Value("${geographicUnitsMetadata}") String geographicUnitsMetadata,
      @Value("${anzsic06ExcelSheet}") String anzsic06ExcelSheet,
      @Value("${anzisc06Column}") int anzisc06Column,
      @Value("${anzisc06StartRow}") int anzisc06StartRow
  ) throws IOException {
    return getGeographicalUnitCodesByProperties(
        geographicUnitsMetadata,
        anzsic06ExcelSheet,
        anzisc06Column,
        anzisc06StartRow
    );
  }

  private Set<String> getGeographicalUnitCodesByProperties(
      String geographicUnitsMetadata,
      String excelSheetName,
      int codeColumn,
      int codeStartRow
  ) throws IOException {
    FileInputStream file = new FileInputStream(geographicUnitsMetadata);
    Workbook workbook = new XSSFWorkbook(file);
    Set<String> result = new HashSet<>();
    Sheet sheet = workbook.getSheet(excelSheetName);
    for (int i = codeStartRow; i <= sheet.getLastRowNum(); i++) {
      Cell cell = sheet.getRow(i).getCell(codeColumn);
      result.add(cell.getStringCellValue());
    }
    return result;
  }

  @Bean
  public GeographicUnitValidator validator(
      @Qualifier("anzsic06Codes") Set<String> anzsic06Codes,
      @Qualifier("areaCodes") Set<String> areaCodes) {
    return new GeographicUnitValidator(anzsic06Codes, areaCodes);
  }

  @Bean
  public FlatFileItemReader<GeographicUnit> reader(@Value("${geographicUnitsCsv}") String geographicUnitsCsvPath) {
    return new FlatFileItemReaderBuilder<GeographicUnit>()
        .name("personItemReader")
        .resource(new ClassPathResource(geographicUnitsCsvPath))
        .delimited()
        .names("anzsic06", "area", "year", "geoCount", "ecCount")
        .targetType(GeographicUnit.class)
        .build();
  }

  @Bean
  public ValidatingItemProcessor<GeographicUnit> processor(GeographicUnitValidator validator) {
    ValidatingItemProcessor<GeographicUnit> processor = new ValidatingItemProcessor<>();
    processor.setValidator(validator);
    processor.setFilter(true);
    return processor;
  }

  @Bean
  public JdbcBatchItemWriter<GeographicUnit> writer(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<GeographicUnit>()
        .sql("""
            INSERT INTO geographic_units (anzsic06, Area, year, geo_count, ec_count)
            VALUES (:anzsic06, :area, :year, :geoCount, :ecCount)
            """)
        .dataSource(dataSource)
        .beanMapped()
        .build();
  }

  @Bean
  public Step step1(
      JobRepository jobRepository,
      DataSourceTransactionManager transactionManager,
      FlatFileItemReader<GeographicUnit> reader,
      ValidatingItemProcessor<GeographicUnit> processor,
      JdbcBatchItemWriter<GeographicUnit> writer,
      @Value("${chunkSize}") int chunkSize
  ) {
    return new StepBuilder("step1", jobRepository)
        .<GeographicUnit, GeographicUnit>chunk(chunkSize, transactionManager)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
  }

  @Bean
  public Job importGeographicalUnitJob(
      JobRepository jobRepository,
      Step step1,
      JobCompletionNotificationListener listener) {
    return new JobBuilder("importGeographicalUnitJob", jobRepository)
        .listener(listener)
        .start(step1)
        .build();
  }
}
