-- Node 2: North Zone Employees
-- This database holds employee information for the North region sales offices.

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    hire_date DATE,
    city VARCHAR(50),
    region VARCHAR(20) NOT NULL CHECK (region = 'North')
);

INSERT INTO employees (employee_id, first_name, last_name, hire_date, city, region) VALUES
(1, 'Priya', 'Rana', '2023-01-15', 'Delhi', 'North'),
(2, 'Rohan', 'Thakur', '2023-02-20', 'Gurgaon', 'North'),
(3, 'Neha', 'Bisht', '2023-03-10', 'Noida', 'North'),
(4, 'Amit', 'Chauhan', '2023-04-05', 'Chandigarh', 'North'),
(5, 'Sunita', 'Rawat', '2023-05-21', 'Delhi', 'North');
