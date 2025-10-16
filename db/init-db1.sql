-- Node 1: North Zone Customers
-- This database holds customer information for cities like Delhi and Chandigarh.

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    city VARCHAR(50),
    region VARCHAR(20) NOT NULL CHECK (region = 'North')
);

INSERT INTO customers (customer_id, first_name, last_name, email, city, region) VALUES
(101, 'Aarav', 'Sharma', 'aarav.sharma@email.com', 'Delhi', 'North'),
(102, 'Vihaan', 'Verma', 'vihaan.verma@email.com', 'Gurgaon', 'North'),
(103, 'Advik', 'Mehra', 'advik.mehra@email.com', 'Noida', 'North'),
(104, 'Kabir', 'Singh', 'kabir.singh@email.com', 'Chandigarh', 'North'),
(105, 'Anika', 'Gupta', 'anika.gupta@email.com', 'Delhi', 'North'),
(106, 'Saanvi', 'Patel', 'saanvi.patel@email.com', 'Jaipur', 'North'),
(107, 'Ishaan', 'Kumar', 'ishaan.kumar@email.com', 'Lucknow', 'North'),
(108, 'Diya', 'Chopra', 'diya.chopra@email.com', 'Delhi', 'North'),
(109, 'Reyansh', 'Malhotra', 'reyansh.malhotra@email.com', 'Gurgaon', 'North'),
(110, 'Myra', 'Jain', 'myra.jain@email.com', 'Chandigarh', 'North');
