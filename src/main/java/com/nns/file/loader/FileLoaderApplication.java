/**
 * 
 */
package com.nns.file.loader;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * @author eostermueller@gmail.com
 *
 */
@SpringBootApplication
public class FileLoaderApplication extends SpringBootServletInitializer {

	
	public static void main(String[] args) {
		SpringApplication.run(FileLoaderApplication.class, args);

	}

}
