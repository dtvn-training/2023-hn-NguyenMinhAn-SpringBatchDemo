package com.annm.spring.batch.controller;


import com.annm.spring.batch.service.BigQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class BigqueryController {

    @Autowired
    BigQueryService bigQueryService;

    @GetMapping("/queryDB")
    public String querydb(){
        bigQueryService.basicQuery();
        return "Done";
    }
}
