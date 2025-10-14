-- Add employee_id to sales table
CREATE TABLE sales (
    id INT PRIMARY KEY,
    product VARCHAR(50),
    region VARCHAR(50) CHECK (region = 'North America'),
    amount NUMERIC(10, 2),
    employee_id INT
);

INSERT INTO sales VALUES
(1, 'Laptop', 'North America', 1200.00, 10),
(2, 'Mouse', 'North America', 25.00, 20);

-- Create a new employees table ONLY on this node
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    department VARCHAR(50)
);

INSERT INTO employees VALUES
(10, 'Alice', 'Sales'),
(30, 'Charlie', 'Engineering');