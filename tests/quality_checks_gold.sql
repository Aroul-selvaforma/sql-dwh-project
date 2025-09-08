--CUSTOMER
-- to check if there are some duplicates
SELECT cst_id FROM
(SELECT
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
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid) t
GROUP BY cst_id
HAVING COUNT(*) > 1

-- gender don't match so we do data integration 
SELECT
DISTINCT
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender info
	ELSE COALESCE(ca.gen, 'n/a')
END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid

-- checking the gold table (view)
SELECT DISTINCT gender FROm gold.dim_customers

-- PRODUCT
-- checking if there are duplicate prd_key
SELECT prd_key , COUNT(*) FROM(
SELECT  pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pn.prd_end_dt,
        pn.dwh_create_date,
        pc.cat,
        pc.subcat,
        pc.maintenance
  FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
  WHERE pn.prd_end_dt is NULL -- because we want to select only current products, it's when the end date is null
  )t GROUP BY prd_key
  HAVING COUNT(*) > 1


  --checking Foreing key Integrity in the final model
  SELECT * FROM gold.fact_sales f 
  LEFT JOIN gold.dim_customers c ON f.customer_key = c.Customer_key
  LEFT JOIN gold.dim_products p ON f.product_key = p.product_key