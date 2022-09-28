CREATE TABLE employee (
                          empid   INT64 NOT NULL,
                          empname  STRING(20),
                          designation   STRING(20)
) PRIMARY KEY(empid);

select * from employee;