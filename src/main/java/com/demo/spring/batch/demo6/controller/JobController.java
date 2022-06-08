package com.demo.spring.batch.demo6.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.demo.spring.batch.demo6.runner.JobRunner;

/*
url: http://localhost:8080/run/job
 */

@RestController
@RequestMapping("/run")
public class JobController {

    private JobRunner jobRunner;

    @Autowired
    public JobController(JobRunner jobRunner) {
        this.jobRunner = jobRunner;
    }

    @RequestMapping(value = "/job")
    public String runJob() {
        jobRunner.runBatchJob();
        return String.format("Job Demo6 submitted successfully.");
    }
    
	 @RequestMapping(value = "/runFailJob")
	 public String runFailJob(@RequestParam int id) {
		 System.out.println("Get runFailJob Started");
		 jobRunner.runFailedBatchJob(id);
		 System.out.println("Get runFailJob End");
		 return String.format("Failed Job "+id+" submitted successfully."); 
	 }
	 
}
