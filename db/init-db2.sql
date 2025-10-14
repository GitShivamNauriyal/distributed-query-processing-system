-- Add employee_id to sales table
CREATE TABLE sales (
    id INT PRIMARY KEY,
    product VARCHAR(50),
    region VARCHAR(50) CHECK (region = 'Europe'),
    amount NUMERIC(10, 2),
    employee_id INT
);

INSERT INTO sales VALUES
(101, 'Keyboard', 'Europe', 75.00, 20),
(102, 'Monitor', 'Europe', 300.50, 10);