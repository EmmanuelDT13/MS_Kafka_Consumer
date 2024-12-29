package com.kafka.entity;

import java.time.LocalDateTime;

public class Employee {

	private String idEmployee;

	private String nameEmployee;

	private LocalDateTime birthDayEmployee;

	public Employee() {
		super();
	}

	public Employee(String idEmployee, String nameEmployee, LocalDateTime birthDayEmployee) {
		super();
		this.idEmployee = idEmployee;
		this.nameEmployee = nameEmployee;
		this.birthDayEmployee = birthDayEmployee;
	}

	public String getIdEmployee() {
		return idEmployee;
	}

	public void setIdEmployee(String idEmployee) {
		this.idEmployee = idEmployee;
	}

	public String getNameEmployee() {
		return nameEmployee;
	}

	public void setNameEmployee(String nameEmployee) {
		this.nameEmployee = nameEmployee;
	}

	public LocalDateTime getBirthDayEmployee() {
		return birthDayEmployee;
	}

	public void setBirthDayEmployee(LocalDateTime birthDayEmployee) {
		this.birthDayEmployee = birthDayEmployee;
	}

	@Override
	public String toString() {
		return "Employee [idEmployee=" + idEmployee + ", nameEmployee=" + nameEmployee + ", birthDayEmployee="
				+ birthDayEmployee + "]";
	}
	
}
