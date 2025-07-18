/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schema. It includes checks for:
        - Null or duplicate primary keys.
        - Unwanted spaces in string fields.
        - Data standardization and consistency.
        - Invalid date ranges and orders.
        - Data consistency between related fields.

Usage Notes:
        - Run these checks after data loading Silver Layer.
        - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


PRINT '========================================='
PRINT 'Checking Data Quality for CRM Tables'
PRINT '========================================='
  
PRINT '========================================='
PRINT 'crm_cust_info: Checking Data Quality'
PRINT '========================================='

--before writing any data transformation, 
--we have to check and identify any quality
--issues of the table.

--Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT * FROM [silver].[crm_cust_info]

SELECT 
	[cst_id],
	COUNT(*) 
FROM [silver].[crm_cust_info]
GROUP BY [cst_id]
HAVING COUNT(*) > 1 OR [cst_id] IS NULL;


--Quality Check
--Check unwanted spaces

--TRIM()
--Removes leading and trailing spaces from a string

-- Check if first name has spaces
SELECT [cst_firstname]
FROM [silver].[crm_cust_info]
WHERE [cst_firstname] != TRIM([cst_firstname])


-- Check if last name has spaces
SELECT [cst_lastname]
FROM [silver].[crm_cust_info]
WHERE [cst_lastname] != TRIM([cst_lastname])

-- Check if gendor has spaces
SELECT [cst_gndr]
FROM [silver].[crm_cust_info]
WHERE [cst_gndr] != TRIM([cst_gndr])


--Quality Check 
--Check the consistency of values in low cardinality columns
--Data Standarization and Consistency

--In our data warehouse,
--We aim to store clear and meaningful values
--rather than using abbreviated terms


--In our data warehouse,
--we use the default values 'n/a'
--for missing values!

SELECT DISTINCT [cst_gndr]
FROM [silver].[crm_cust_info]


SELECT DISTINCT [cst_marital_status]
FROM [silver].[crm_cust_info]  


PRINT '========================================='
PRINT 'crm_prd_info: Checking Data Quality'
PRINT '========================================='

--Check For Nulls or Duplicates in primary key
-- Expectation: No Result

SELECT 
	[prd_id],
	COUNT([prd_id])
FROM [silver].[crm_prd_info]
GROUP BY [prd_id]
HAVING COUNT([prd_id]) > 1 OR [prd_id] IS NULL;


-- Check for unwanted spaces
-- Expectation: No Result

SELECT [prd_nm]
FROM [silver].[crm_prd_info]
WHERE [prd_nm] != TRIM([prd_nm])


-- Check for Nulls or Negative Numbers
-- Expectation: No Results

SELECT [prd_cost]
FROM [silver].[crm_prd_info]
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- Data Standardization & Consistency
SELECT DISTINCT [prd_line]
FROM [silver].[crm_prd_info]


-- Check for Invalid Date Orders
-- End date must not be earlier than the start date
SELECT *
FROM [silver].[crm_prd_info]
WHERE prd_end_dt < prd_start_dt;

 
  
PRINT '========================================='
PRINT 'crm_sales_details: Checking Data Quality'
PRINT '========================================='

  
-- Check for Inavlid Dates

--NULLIF() 
--Return NULL if two given values are equal; otherwise, it returns the first expression. 

SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM [silver].[crm_sales_details] 
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101


SELECT 
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM  [silver].[crm_sales_details]
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101


SELECT 
	NULLIF(sls_due_dt, 0) AS sls_ship_dt
FROM  [silver].[crm_sales_details]
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101


-- Check for Invalid Date Orders
SELECT *
FROM [silver].[crm_sales_details]
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


--Business Rules
--Sales = Quantity * Price
--Negative, Zeros, Nulls are not Allowed!

--RULES 
--If sales is negative, zero, or null, drive it using Quantity and price.
--If price is zero or null, calculate it using Sales and Quantity.
--IF Price is negative, convert it to a positive value.

SELECT DISTINCT
	[sls_sales],
	[sls_quantity],
	[sls_price]

FROM [silver].[crm_sales_details]
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

  
PRINT '========================================='
PRINT 'Checking Data Quality for ERP Tables'
PRINT '========================================='


PRINT '========================================='
PRINT 'erp_cust_az12: Checking Data Quality'
PRINT '========================================='

--Build Silver Layer
--Clean & Load
--erp_cust_az12

SELECT 
	cid, 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid 
	END AS cid,
	bdate,
	gen
FROM [silver].[erp_cust_az12]
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid 
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- Identify Out-Of-Range Dates
-- Check for very old
SELECT 
bdate 
FROM [silver].[erp_cust_az12]
WHERE bdate > GETDATE()


-- Data standardization & Consistency
-- Check all possible values of gen 

SELECT DISTINCT 
	gen
FROM [silver].[erp_cust_az12];


SELECT * FROM [silver].[erp_cust_az12];


PRINT '========================================='
PRINT 'erp_loc_a101: Checking Data Quality'
PRINT '========================================='

SELECT DISTINCT  
	cntry
FROM [silver].[erp_loc_a101]
ORDER BY cntry;


SELECT DISTINCT 
	[cntry]
FROM [silver].[erp_loc_a101]


SELECT * FROM [silver].[erp_loc_a101];


PRINT '========================================='
PRINT 'erp_erp_px_cat_g1v2: Checking Data Quality'
PRINT '========================================='


--Check Unwanted Spaces
SELECT 
	*
FROM [bronze].[erp_px_cat_g1v2]
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)


-- Data Standardization & Consistency
SELECT DISTINCT 
	cat
FROM [silver].[erp_px_cat_g1v2]


SELECT DISTINCT 
	subcat 
FROM [silver].[erp_px_cat_g1v2]

SELECT DISTINCT 
	maintenance 
FROM [silver].[erp_px_cat_g1v2]


SELECT * FROM [silver].[erp_px_cat_g1v2];


