package com.sud.parquet_file_demo.constant;

import java.util.Arrays;
import java.util.List;

public interface AppConstant {

	// Spark session related constant
	String SPARK_MAPREDUCE = "mapreduce.fileoutputcommitter.marksuccessfuljobs";
	String SPARK_METADATA = "parquet.enable.summary-metadata";
	String SPARK_SESSION_APP_NAME = "READ_PARQUET";
	String SPARK_SESSION_MASTER = "local[*]";
	String SPARK_SESSION_CONFIG = "spark.io.compression.codec";
	String SPARK_SESSION_COMPRESSION = "snappy";
	String FILE_FORMAT_CSV = ".csv";
	String FALSE = "false";

	List<String> COLUMNS_FOR_ENCODE = Arrays.asList("build_id", "country", "architecture",
			"channel");

}
