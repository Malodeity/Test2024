

-- Create database
CREATE DATABASE test24;

-- Create customers table for customer information
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table for product information
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spend_categories table for categorization
CREATE TABLE spend_categories (
    spend_category_id SERIAL PRIMARY KEY,
    spend_category_name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create transaction_types table
CREATE TABLE transaction_types (
    transaction_type_id SERIAL PRIMARY KEY,
    transaction_type_name VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create amount_categories table
CREATE TABLE amount_categories (
    amount_category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(20) NOT NULL, 
    min_amount DECIMAL(10,2),
    max_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_amount DECIMAL(10,2) NOT NULL,
    transaction_type_id INTEGER NOT NULL,
    spend_category_id INTEGER NOT NULL,
    amount_category_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (transaction_type_id) REFERENCES transaction_types(transaction_type_id),
    FOREIGN KEY (spend_category_id) REFERENCES spend_categories(spend_category_id),
    FOREIGN KEY (amount_category_id) REFERENCES amount_categories(amount_category_id)
);

-- Create index for common queries
CREATE INDEX idx_transaction_date ON transactions(transaction_date);
CREATE INDEX idx_customer_transactions ON transactions(customer_id, transaction_date);

-- Create view for customer total transactions
CREATE VIEW customer_transaction_totals AS
SELECT 
    customer_id,
    COUNT(*) as total_transactions,
    SUM(transaction_amount) as total_amount
FROM transactions
GROUP BY customer_id;

-- Insert initial amount categories
INSERT INTO amount_categories (category_name, min_amount, max_amount) VALUES
    ('low', 0, 49.99),
    ('medium', 50, 200),
    ('high', 200.01, NULL);



--Scripts to retrieve useful insights

-- 1. Total transactions per product category
SELECT 
    p.product_category,
    COUNT(*) as transaction_count,
    SUM(t.transaction_amount) as total_amount
FROM transactions t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.product_category
ORDER BY total_amount DESC;

-- 2. Top 5 accounts by total transaction value
SELECT 
    c.customer_id,
    COUNT(*) as transaction_count,
    SUM(t.transaction_amount) as total_spend,
    ROUND(AVG(t.transaction_amount), 2) as avg_transaction_amount
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY total_spend DESC
LIMIT 5;

-- 3. Monthly spend trends over the past year
WITH monthly_trends AS (
    SELECT 
        DATE_TRUNC('month', t.transaction_date::date) as month,
        COUNT(*) as transaction_count,
        SUM(t.transaction_amount) as total_spend,
        COUNT(DISTINCT t.customer_id) as unique_customers,
        ROUND(AVG(t.transaction_amount), 2) as avg_transaction_amount
    FROM transactions t
    WHERE t.transaction_date::date >= NOW() - INTERVAL '1 year'
    GROUP BY DATE_TRUNC('month', t.transaction_date::date)
)
SELECT 
    TO_CHAR(month, 'Month YYYY') as month,
    transaction_count,
    total_spend,
    unique_customers,
    avg_transaction_amount,
    ROUND((total_spend - LAG(total_spend) OVER (ORDER BY month)) / LAG(total_spend) OVER (ORDER BY month) * 100, 2) as month_over_month_growth
FROM monthly_trends
ORDER BY month DESC;
