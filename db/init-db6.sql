
-- Node 6: Sales Transactions for the second half of the year (H2)
-- This table is partitioned by date.

CREATE TABLE sales (
    sale_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    sale_amount NUMERIC(10, 2),
    sale_date DATE NOT NULL CHECK (sale_date >= '2024-07-01' AND sale_date <= '2024-12-31'),
    customer_id INT,
    employee_id INT
);

INSERT INTO sales (sale_id, product_name, sale_amount, sale_date, customer_id, employee_id) VALUES
(2001, 'Gaming Console', 55000.00, '2024-07-22', 105, 5),
(2002, '4K TV', 120000.00, '2024-08-10', 205, 10),
(2003, 'Bluetooth Speaker', 9000.00, '2024-09-01', 106, 1),
(2004, 'Fitness Tracker', 4500.00, '2024-09-18', 206, 6),
(2005, 'Drone', 78000.00, '2024-10-30', 107, 2),
(2006, 'E-Reader', 12500.00, '2024-11-15', 207, 7),
(2007, 'Projector', 95000.00, '2024-12-05', 108, 3),
(2008, 'Mechanical Keyboard', 11000.00, '2024-12-20', 208, 8);



-- Create an audit log table
CREATE TABLE sales_audit_log (
    log_id SERIAL PRIMARY KEY,
    sale_id INT,
    action_type VARCHAR(50),
    action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details TEXT
);

-- Create the function to handle updates
CREATE OR REPLACE FUNCTION log_sale_update()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO sales_audit_log (sale_id, action_type, details)
    VALUES (
        NEW.sale_id, 
        'UPDATE', 
        'Updated product: ' || NEW.product_name || '. Amount changed from ' || OLD.sale_amount || ' to ' || NEW.sale_amount
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--  Create the trigger for UPDATE events
CREATE TRIGGER after_sale_update
AFTER UPDATE ON sales
FOR EACH ROW