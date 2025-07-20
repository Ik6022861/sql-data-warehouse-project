/*
================================================================================
Quality Checks
================================================================================

Script Purpose:
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
================================================================================
*/

-- ===================================================================
-- Checking 'gold.customer_key'
-- ===================================================================

--After Joining table, check if any duplicates were introduced by the join logic

SELECT cst_id, COUNT(*) FROM (
	SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM [silver].[crm_cust_info] AS ci
	LEFT JOIN [silver].[erp_cust_az12] AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN [silver].[erp_loc_a101] AS la
	ON ci.cst_key = la.cid 
)t GROUP BY cst_id
HAVING COUNT(*) > 1


--Nulls often come from joined tables!
--Null will appear if SQL finds no match.


SELECT DISTINCT 
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr     --crm is the Master for gender info.
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM [silver].[crm_cust_info] AS ci
LEFT JOIN [silver].[erp_cust_az12] AS ca
ON ci.cst_key = ca.cid
LEFT JOIN [silver].[erp_loc_a101] AS la
ON ci.cst_key = la.cid 
ORDER BY 1,2


--Quality Check of the Gold Table
SELECT * FROM gold.dim_customers;
SELECT distinct gender FROM gold.dim_customers;



-- ===================================================================
-- Checking 'gold.product_key'
-- ===================================================================

--After Joining table, check if any duplicates were introduced by the join logic
SELECT prd_key, COUNT(*) FROM (
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	--prd_end_dt
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM [silver].[crm_prd_info] AS pn
LEFT JOIN [silver].[erp_px_cat_g1v2] AS pc
ON pn.cat_id = pc.id 
WHERE prd_end_dt IS NULL -- Filter out all historical data 
) AS t GROUP BY prd_key
HAVING COUNT(*) > 1;


-- ===================================================================
-- Checking 'gold.fact_key'
-- ===================================================================

--Quality check of the Gold Table
--Foriegn key Integrity
SELECT 
	* 
FROM [gold].[fact_sales] AS f
LEFT JOIN [gold].[dim_customers] AS C
ON f.customer_key = c.customer_key
LEFT JOIN [gold].[dim_products] AS P
ON f.product_key = P.product_key
--WHERE c.customer_key IS NULL;
WHERE P.product_key IS NULL

