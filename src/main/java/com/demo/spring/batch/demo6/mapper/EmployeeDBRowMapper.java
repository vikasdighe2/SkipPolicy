package com.demo.spring.batch.demo6.mapper;


import org.springframework.jdbc.core.RowMapper;

import com.demo.spring.batch.demo6.model.Employee;

import java.sql.ResultSet;
import java.sql.SQLException;


public class EmployeeDBRowMapper implements RowMapper<Employee> {


    @Override
    public Employee mapRow(ResultSet resultSet, int i) throws SQLException {
        Employee employee = new Employee();
        employee.setEmployeeId(resultSet.getString("employee_id"));
        employee.setFirstName(resultSet.getString("first_name"));
        employee.setLastName(resultSet.getString("last_name"));
        employee.setEmail(resultSet.getString("email"));
        employee.setAge(resultSet.getInt("age"));
        return employee;
    }
}
