package com.tutorial.csvsparkkafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Employee implements Serializable {

    private String id;
    private String name;
    private String department;
    private double salary;
    private String joiningDate;
}
