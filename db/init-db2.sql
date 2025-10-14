-- This script sets up the table for the European region on the second worker.
CREATE TABLE sales (
    id INT PRIMARY KEY,
    product VARCHAR(50),
    region VARCHAR(50) CHECK (region = 'Europe'),
    amount NUMERIC(10, 2)
);

-- Insert some sample data.
INSERT INTO sales (id, product, region, amount) VALUES
(101, 'Keyboard', 'Europe', 75.00),
(102, 'Monitor', 'Europe', 300.50);