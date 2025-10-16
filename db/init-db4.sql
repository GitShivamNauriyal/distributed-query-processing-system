-- Node 4: South Zone Employees
-- This database holds employee information for the South region sales offices.

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    hire_date DATE,
    city VARCHAR(50),
    region VARCHAR(20) NOT NULL CHECK (region = 'South')
);

INSERT INTO employees (employee_id, first_name, last_name, hire_date, city, region) VALUES
(6, 'Deepak', 'M', '2023-01-18', 'Bangalore', 'South'),
(7, 'Lakshmi', 'V', '2023-02-22', 'Chennai', 'South'),
(8, 'Karthik', 'R', '2023-03-12', 'Hyderabad', 'South'),
(9, 'Pooja', 'S', '2023-04-08', 'Bangalore', 'South'),
(10, 'Suresh', 'K', '2023-05-25', 'Chennai', 'South');
