package com.example.gmalllogger.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author lihaoran
 */

@Controller
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test")
    /**
     * 不加则返回页面，加上则返回字符串或者使用restController
     */
    @ResponseBody
    public String test(){
        System.out.println("success");
        return "page.html";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String log){
        //将数据写入kafka
//        kafkaTemplate.send("ODS_BASE_LOG",log);
        System.out.println(log);
        return "success";
    }





}
