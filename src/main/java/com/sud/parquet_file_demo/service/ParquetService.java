package com.sud.parquet_file_demo.service;


import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.opencsv.CSVWriter;
import com.sud.parquet_file_demo.constant.AppConstant;
import com.sud.parquet_file_demo.util.BouncyCastleEncryption;

@Service
public class ParquetService {

	private static final Logger logger = LoggerFactory.getLogger(ParquetService.class);
	
	//this logic is used to read all the files from the given folder
	public String readParquet(String url) {
		List<String> failedFiles = new ArrayList<>();
		try {
			List<String> filesInFolder = Files.walk(Paths.get(url)).filter(Files::isRegularFile).map(Path::toString)
					.collect(Collectors.toList());

			for (String fileURL : filesInFolder) {
				try{
					parquetTocsv(fileURL);
				}catch (Exception e) {
					failedFiles.add("Exception while reading file : "+fileURL);
				}
			}
		} catch (Exception e) {
			
		}
		return "All file are completed !!" + failedFiles;
	}
	
	
	private String parquetTocsv(String url) {
		int totalRecords = 0;
		SparkSession session = null;
		try {
			session = SparkSession
									.builder().appName(AppConstant.SPARK_SESSION_APP_NAME).master(AppConstant.SPARK_SESSION_MASTER)
									.config(AppConstant.SPARK_SESSION_CONFIG, AppConstant.SPARK_SESSION_COMPRESSION)
									.getOrCreate();
			logger.info("Spark Session created ....");
			
			session.conf().set(AppConstant.SPARK_MAPREDUCE, AppConstant.FALSE);
			session.conf().set(AppConstant.SPARK_METADATA, AppConstant.FALSE);
			
			logger.info("Spark configuration done ....");
			
			String fileName = url;
			String outPutFileName = fileName.split(".snappy")[0];
			
			Dataset<Row> dataSet = session.read().parquet(fileName);
			
			if (!dataSet.isEmpty()) {
				logger.info("Parquet file is not empty");
				List<Row> listOfRows = dataSet.collectAsList();
				totalRecords = listOfRows.size();
				
				//base64 encoded key used here
				javax.crypto.Mac hmac = BouncyCastleEncryption.getMac("ZjA4NmM1MmEtODc2My00NGZiLWFjYTctMjNiNWIzMmUxNWRm");
				
				logger.info("Total number of records in parquet file : {} ", totalRecords);
				List<String[]> list = new ArrayList<>();
				String[] header = listOfRows.get(0).schema().fieldNames();
				list.add(header);
				for (Row row : listOfRows) {
					try {
						String[] rows = new String[row.length()];
						int i = 0;
						for (String filedName : header) {
							if (AppConstant.COLUMNS_FOR_ENCODE.contains(filedName)) {
								rows[i] = BouncyCastleEncryption.getEncryption(row.getAs(filedName), hmac);
							} else {
								rows[i] = row.getAs(filedName)+"";
							}
							i++;
						}
						list.add(rows);
					} catch (Exception e) {
						logger.info("Exception while parsing the data message : {}",e.getMessage());
					}
				}
				try (CSVWriter writer = new CSVWriter(new FileWriter(outPutFileName + AppConstant.FILE_FORMAT_CSV))) {
					writer.writeAll(list);
					logger.info("CSV File write is done");
				}
			}
			session.close();
		}catch(Exception e) {
			if(null != session)
				session.close();
			
			logger.info("Exception Message : {} ",e.getMessage());
		}
		return "CSV File write is done";
	}
}
