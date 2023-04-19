package com.sud.parquet_file_demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sud.parquet_file_demo.service.ParquetService;

@RestController()
public class ParquetController {


	@Autowired
	ParquetService encodeService;
	
	String url ="C:\\Personal\\OwnGitProject\\sample.snappy.parquet";
	
	@GetMapping("/read")
	public ResponseEntity<?> encode() {
		return ResponseEntity.ok(encodeService.readParquet(url));
	}
}
