<!-- ============================================================================ -->
## ⚙️ ETL: Load Silver Layer (Bronze → Silver)
<!-- ============================================================================ -->

### 📌 Script Purpose
This stored procedure executes the **ETL (Extract, Transform, Load)** pipeline to populate cleaned and standardized data into the `silver` schema from the raw `bronze` layer.

### 🔧 Key Actions
- 🗑 **Truncates** each target table in the `silver` schema
- 🧼 **Cleans** and **transforms** incoming raw data
- 🔄 **Loads** standardized records for analysis-ready use

---

### 📋 Parameters
- **None** — This procedure accepts no input and returns no output.

---

### ✅ Usage Example
```sql
EXEC silver.load_silver;



CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

    PRINT '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
    select
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname, --------trimming removing unwanted spaces----
    TRIM(cst_lastname) AS cst_lastname,


    CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
         else 'n/a'  ------handled missing values instead of null n/a--
    END cst_marital_status,  ----normalize marital status values to readable format---

    CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
         else 'n/a'
    END cst_gndr, ------------normalize gender values to readable format---
    cst_create_date

    FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    where cst_id is not null
    )t where flag_last =1 -------select most recent customer through cust_id-----

    -------------silver.crm_prd_ifo------------------------------------------------
    PRINT '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info(
        prd_id,      
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt)

    select
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,--Extract category ID
    SUBSTRING(prd_key, 7,len(prd_key)) AS prd_key,--Extract product key
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,

    CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line,---Map product line codes to descriptive values

    CAST (prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt----calculate end date as one day before the next start date
    FROM bronze.crm_prd_info

    --------------------silver.crm_sales_details--------------------------------------
    PRINT '>> Truncating Table:silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details';


    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price   
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,

        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,

        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,

        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END AS sls_sales,

        sls_quantity,

        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END AS sls_price

    FROM bronze.crm_sales_details;

    --------------------silver.erp_cust_az12-------------------------------------
    PRINT '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Data Into: silver.erp_cust_az12';


    INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
    SELECT 
     CASE when cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))------handled nas prefix if present removed it------
          ELSE cid
    END AS cid,
    CASE when bdate > getdate() THEN NULL---------------set future dates to null-----------
          ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'------------normalize gender and unkown case----
         WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
         ELSE 'n/a'
    END AS gen

    FROM bronze.erp_cust_az12

    --------------------------silver.erp_loc_a101------------------------------------------
    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101(cid,cntry)
    SELECT 
    REPLACE(cid,'-','') cid,----handled invalid value
    CASE WHEN TRIM(cntry) ='DE' THEN 'Germany'------------data normalization---
         WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
         WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'-----null and unwanted spaces----
         ELSE TRIM(cntry)
    END AS cntry
    FROM bronze.erp_loc_a101;

    -----------------------silver.erp_px_cat_g1v2-------------------------------------------------
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2 ;
    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(id,
    cat,
    subcat,
    maintenance)
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2
END
