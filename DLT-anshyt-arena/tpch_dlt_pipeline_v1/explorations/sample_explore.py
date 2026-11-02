# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC SELECT
# MAGIC         YEAR(f.order_date)                            AS sales_year,
# MAGIC         SUM(f.net_price)                              AS net_revenue,
# MAGIC         COUNT(DISTINCT f.order_key)                   AS total_orders,
# MAGIC         COUNT(DISTINCT f.customer_key)                AS active_customers,
# MAGIC         SUM(f.net_price) / COUNT(DISTINCT f.order_key) AS avg_order_value
# MAGIC     FROM dlt_retail.gold.fact_sales f
# MAGIC     GROUP BY YEAR(f.order_date)