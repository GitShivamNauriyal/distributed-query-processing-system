-- Node 3: South Zone Customers
-- This database holds customer information for cities like Bangalore and Chennai.

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    city VARCHAR(50),
    region VARCHAR(20) NOT NULL CHECK (region = 'South')
);

INSERT INTO customers (customer_id, first_name, last_name, email, city, region) VALUES
(201, 'Aadhya', 'Nair', 'aadhya.nair@email.com', 'Bangalore', 'South'),
(202, 'Arjun', 'Menon', 'arjun.menon@email.com', 'Chennai', 'South'),
(203, 'Ishita', 'Pillai', 'ishita.pillai@email.com', 'Hyderabad', 'South'),
(204, 'Vivaan', 'Reddy', 'vivaan.reddy@email.com', 'Bangalore', 'South'),
(205, 'Kiara', 'Iyer', 'kiara.iyer@email.com', 'Kochi', 'South'),
(206, 'Sai', 'Rao', 'sai.rao@email.com', 'Chennai', 'South'),
(207, 'Shanaya', 'Kumar', 'shanaya.kumar@email.com', 'Hyderabad', 'South'),
(208, 'Yuvan', 'Shetty', 'yuvan.shetty@email.com', 'Bangalore', 'South'),
(209, 'Ananya', 'Naidu', 'ananya.naidu@email.com', 'Kochi', 'South'),
(210, 'Zoya', 'Krishnan', 'zoya.krishnan@email.com', 'Chennai', 'South');
