create table Pro(ProjectID varchar(20),ProjectName varchar(30),primary key(ProjectID));
create table Dep(departmentCode varchar(10),DepartmentName varchar(30),primary key(departmentCode));
create table Employees(Employee_Number 
int,EmployeeName varchar(30),
Date_Of_Birth date,date_of_joining date,
designation varchar(20),
salary int, 
managerEmployeenumber int, 
departmentCode varchar(10), primary key(Employee_Number), 
foreign key(departmentCode) references Department(departmentCode), 
foreign key(managerEmployeenumber) references
Employees(Employee_Number));
create table Employee_Projects(Employee_Number int, ProjectID varchar(10),StartDate DATE,
EndDate DATE, primary key(ProjectID,Employee_Number), foreign key(Employee_Number) references Employee(Employee_Number),
foreign key(ProjectID) references Project(ProjectID));
show tables;
Describe Employees;
Describe Dep;
INSERT INTO Dep VALUES ('3','Ai');
INSERT INTO Dep VALUES ('4','DS');
UPDATE Dep SET DepartmentName = 'Cyber' WHERE departmentCode = '3';
INSERT INTO Employees VALUES (1239, 'Dinesh', '2003-05-12', '2024-07-17', 'ASE', 20000, 1239, '3');
INSERT INTO Employees VALUES (1238, 'Karthik', '2003-07-12', '2024-07-17', 'ASE', 20000, 1238, '3');
INSERT INTO Employees VALUES (1236, 'Karthika', '2003-09-12', '2024-07-17', 'ASE', 20000, 1238, '3');
INSERT INTO Employees VALUES (1232, 'Ramesh', '2003-07-12', '2024-07-17', 'ASE', 20000, 1236, '3');
INSERT INTO Employees VALUES (1231, 'Ram', '2003-07-12', '2024-07-17', 'ASE', 20000, 1238, '3');
INSERT INTO Employees VALUES (1230, 'Sita', '2003-07-12', '2024-07-17', 'ASE', 20000, 1232, '3');
INSERT INTO Employees VALUES (1235, 'Gita', '2003-07-12', '2024-07-17', 'ASE', 20000, 1238, '3');
INSERT INTO Employees VALUES (1435, 'Preeti', '2003-07-12', '2024-07-17', 'ASE', 209000, 1238, '3');
INSERT INTO Employees VALUES (1535, 'Hari', '2003-05-28', '2024-07-17', 'AE', 160000, 1435, '3');
INSERT INTO Employees VALUES (1635, 'Praneeth', '2003-07-22', '2024-07-17', 'SF',120000, 1238, '3');
INSERT INTO Employees VALUES (1735, 'Veda', '2003-12-12', '2024-07-17', 'SD', 20500, 1735, '3');
INSERT INTO Employees VALUES (1835, 'Pranavi', '2003-09-12', '2024-07-17', 'DS', 49000, 1238, '3');
INSERT INTO Employees VALUES (1825, 'Ashreya', '1983-09-12', '2024-07-17', 'SM', 49000, 1238, '3');
INSERT INTO Employees VALUES (1845, 'Lakshya', '1997-09-12', '2024-07-17', 'SSE', 49000, 1238, '3');
INSERT INTO Employees VALUES (1855, 'Suresh', '2000-09-12', '2024-07-17', 'SM', 49000, 1238, '3');
INSERT INTO Employees VALUES (1865, 'Surya', '1966-09-12', '2000-07-17', 'SSE', 49000, 1238, '3');
INSERT INTO Employees VALUES (1875, 'Shiva', '1965-09-12', '2000-07-17', 'SM', 49000, 1238, '3');
INSERT INTO Employees VALUES (1885, 'Srikanth', '1960-09-12', '2000-07-17', 'SE', 49000, 1238, '3');
INSERT INTO Employees VALUES (1895, 'Santosh', '1975-09-12', '2000-07-17', 'SSE', 49000, 1238, '3');
SELECT EmployeeName FROM Employees WHERE EmployeeName LIKE 'A%';
SELECT EmployeeName FROM Employees WHERE EmployeeName LIKE '%A';
SELECT EmployeeName FROM Employees WHERE EmployeeName LIKE '__A%';
SELECT EmployeeName FROM Employees WHERE EmployeeName LIKE '___';




SELECT EmployeeName, Designation, Salary FROM Employees;
SELECT EmployeeName FROM Employees WHERE Salary > 35000 and Designation in ('SSE') ;
SELECT EmployeeName,Designation,Salary FROM Employees WHERE Designation in('SSE','SM','SE');
SELECT EXTRACT(year FROM date_of_joining ) AS year FROM Employees;
SELECT EmployeeName,date_of_joining FROM Employees WHERE year in ('2024');
SELECT DISTINCT Designation FROM Employees;








