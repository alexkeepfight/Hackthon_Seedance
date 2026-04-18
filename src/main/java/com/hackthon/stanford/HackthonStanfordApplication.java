package com.hackthon.stanford;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class HackthonStanfordApplication {

    public static void main(String[] args) {
        SpringApplication.run(HackthonStanfordApplication.class, args);
    }
}

