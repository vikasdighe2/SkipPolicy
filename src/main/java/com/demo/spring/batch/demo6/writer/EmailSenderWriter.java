package com.demo.spring.batch.demo6.writer;

import org.springframework.batch.item.ItemWriter;

import com.demo.spring.batch.demo6.dto.EmployeeDTO;

import java.util.List;

public class EmailSenderWriter implements ItemWriter<EmployeeDTO> {
    @Override
    public void write(List<? extends EmployeeDTO> list) throws Exception {
    	
        System.out.println("Email send Step 2 successfully to all the employees. "+ list.size());
        //throw new Exception();
    }
}
