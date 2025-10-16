-- Node 5: Sales Transactions for the first half of the year (H1)
-- This table is partitioned by date.

CREATE TABLE sales (
    sale_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    sale_amount NUMERIC(10, 2),
    sale_date DATE NOT NULL CHECK (sale_date >= '2024-01-01' AND sale_date <= '2024-06-30'),
    customer_id INT,
    employee_id INT
);

INSERT INTO sales (sale_id, product_name, sale_amount, sale_date, customer_id, employee_id) VALUES
(1001, 'Smartphone', 45000.00, '2024-01-20', 101, 1),
(1002, 'Laptop', 85000.00, '2024-02-15', 201, 6),
(1003, 'Headphones', 7500.00, '2024-02-18', 102, 2),
(1004, 'Smartwatch', 22000.00, '2024-03-05', 202, 7),
(1005, 'Camera', 62000.00, '2024-04-11', 103, 3),
(1006, 'Tablet', 35000.00, '2024-05-29', 203, 8),
(1007, 'Printer', 15000.00, '2024-06-10', 104, 4),
(1008, 'External Hard Drive', 6000.00, '2024-06-25', 204, 9);
