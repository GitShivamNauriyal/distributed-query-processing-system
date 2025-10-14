-- This script sets up the table for the North American region on the first worker.
CREATE TABLE sales (
    id INT PRIMARY KEY,
    product VARCHAR(50),
    region VARCHAR(50) CHECK (region = 'North America'),
    amount NUMERIC(10, 2)
);

-- Insert some sample data.
INSERT INTO sales (id, product, region, amount) VALUES
(1, 'Laptop', 'North America', 1200.00),
(2, 'Mouse', 'North America', 25.00);