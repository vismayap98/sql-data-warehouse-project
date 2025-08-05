<!-- ============================================================================ -->
## 🛡️ Gold Layer Quality Checks
<!-- ============================================================================ -->

### 📌 Script Purpose
This script performs **data quality validations** on the Gold Layer to ensure integrity, reliability, and readiness for analytics. These checks help confirm that:

- 🔑 Surrogate keys in dimension tables are **unique**
- 🔗 Relationships between fact and dimension tables maintain **referential integrity**
- 🧠 The data model structure aligns with **analytical expectations**

---

### 📋 Usage Notes
- Execute this script **after creating the Gold views**
- Investigate and resolve any issues found (e.g., duplicate keys or broken joins)
- Ensures your star schema is trustworthy for BI tools or dashboards

---

### ✅ Quality Checks Included

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
