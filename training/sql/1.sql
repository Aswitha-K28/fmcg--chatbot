 CREATE TABLE employee_table(
id int NOT NULL,
name varchar(45) NOT NULL,
occupation varchar(35) NOT NULL,
age int NOT NULL check(age>180),
PRIMARY KEY (id)
);