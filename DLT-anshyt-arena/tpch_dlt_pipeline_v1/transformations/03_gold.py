import dlt
from pyspark.sql.functions import *

# =====================================================
# GOLD: FACT_SALES
# Batch read (dlt.read) -> DLT builds a MATERIALIZED VIEW
# =====================================================

@dlt.table(
    name="gold.fact_sales",
    comment="Grain: one row per order line (order_key, line_number). "
            "Keys are natural keys matching dim_customer / dim_product / dim_supplier.",
    # table_properties={"layer": "gold"}
)
@dlt.expect("valid_order_key",   "order_key IS NOT NULL")
@dlt.expect("valid_line_number", "line_number IS NOT NULL")
@dlt.expect("valid_fk_keys",     "customer_key IS NOT NULL AND product_key IS NOT NULL AND supplier_key IS NOT NULL")
@dlt.expect("non_negative_net",  "net_price >= 0")
def fact_sales():
    li = dlt.read("silver.lineitem")
    o  = dlt.read("silver.orders")

    joined = li.join(o, li.l_orderkey == o.o_orderkey, "inner")

    base = joined.select(
        col("l_orderkey").cast("bigint").alias("order_key"),
        col("l_linenumber").cast("int").alias("line_number"),

        # foreign keys
        col("o_custkey").cast("bigint").alias("customer_key"),
        col("l_partkey").cast("bigint").alias("product_key"),
        col("l_suppkey").cast("bigint").alias("supplier_key"),

        # base measures
        col("l_quantity").cast("decimal(18,2)").alias("quantity"),
        col("l_extendedprice").cast("decimal(18,2)").alias("extended_price"),
        col("l_discount").cast("decimal(18,2)").alias("discount"),
        col("l_tax").cast("decimal(18,2)").alias("tax"),

        # order attributes
        col("o_orderstatus").alias("order_status"),
        col("o_orderpriority").alias("order_priority"),
        col("o_orderdate").alias("order_date"),

        # line attributes
        col("l_linestatus").alias("line_status"),
        col("l_returnflag").alias("return_flag"),
        col("l_shipdate").alias("ship_date"),
        col("l_commitdate").alias("commit_date"),
        col("l_receiptdate").alias("receipt_date"),
        col("l_shipmode").alias("ship_mode"),
        col("l_shipinstruct").alias("ship_instruct"),
    )

    return (
        base
        # derived measures (order matters: each builds on the previous)
        .withColumn("discount_amount",
                    expr("CAST(extended_price * discount AS DECIMAL(37,4))"))
        .withColumn("net_price",
                    expr("CAST(extended_price * (1 - discount) AS DECIMAL(38,4))"))
        .withColumn("tax_amount",
                    expr("CAST(net_price * tax AS DECIMAL(38,6))"))
        .withColumn("net_revenue",
                    expr("CAST(net_price * (1 + tax) AS DECIMAL(38,6))"))

        # window: sum of extended_price over all lines of the same order
        .withColumn("order_total_price",
                    expr("CAST(SUM(extended_price) OVER (PARTITION BY order_key) AS DECIMAL(18,2))"))

        # final column order exactly as per your spec
        .select(
            "order_key", "line_number",
            "customer_key", "product_key", "supplier_key",
            "quantity", "extended_price", "discount", "tax",
            "discount_amount", "net_price", "tax_amount", "net_revenue",
            "order_total_price",
            "order_status", "order_priority", "order_date",
            "line_status", "return_flag",
            "ship_date", "commit_date", "receipt_date",
            "ship_mode", "ship_instruct"
        )
    )
# =====================================================
# GOLD: DIM_CUSTOMER
# Grain: one row per customer (current version only)
# =====================================================

@dlt.table(
    name="gold.dim_customer",
    comment="Customer dimension. One row per customer (current SCD2 version). "
            "Nation and region are denormalised. PK: customer_key.",
    # table_properties={"layer": "gold"}
)
@dlt.expect_or_fail("pk_not_null", "customer_key IS NOT NULL")
@dlt.expect("name_present",        "customer_name IS NOT NULL AND trim(customer_name) <> ''")
@dlt.expect("nation_resolved",     "nation_name IS NOT NULL")
@dlt.expect("region_resolved",     "region_name IS NOT NULL")
@dlt.expect("valid_segment",
            "market_segment IN ('AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY')")
def dim_customer():
    # Current version only -> guarantees 1 row per customer
    c = dlt.read("silver.customer")

    n = dlt.read("silver.nation").select(
        col("n_nationkey"), col("n_name"), col("n_regionkey")
    )
    r = dlt.read("silver.region").select(
        col("r_regionkey"), col("r_name")
    )

    return (
        c.join(n, c.c_nationkey == n.n_nationkey, "left")
         .join(r, n.n_regionkey == r.r_regionkey, "left")
         .select(
             col("c_custkey").cast("bigint").alias("customer_key"),
             col("c_name").alias("customer_name"),
             col("c_mktsegment").alias("market_segment"),
             col("n_name").alias("nation_name"),
             col("r_name").alias("region_name"),
             col("c_acctbal").cast("decimal(18,2)").alias("account_balance"),
             col("c_address").alias("address"),
             col("c_phone").alias("phone"),
             col("c_comment").alias("comment")
         )
    )

# =====================================================
# GOLD: DIM_PRODUCT
# Grain: one row per part. silver_part is SCD Type 1,
# so it already has exactly one row per p_partkey.
# =====================================================

@dlt.table(
    name="gold.dim_product",
    comment="Product dimension. One row per part. "
            "manufacturer and brand are numeric codes extracted from 'Manufacturer#N' / 'Brand#NN'. PK: product_key.",
    # table_properties={"layer": "gold"}
)
@dlt.expect_or_fail("pk_not_null",       "product_key IS NOT NULL")
@dlt.expect("name_present",              "product_name IS NOT NULL AND trim(product_name) <> ''")
@dlt.expect("manufacturer_code_parsed",  "manufacturer IS NOT NULL")
@dlt.expect("brand_code_parsed",         "brand IS NOT NULL")
@dlt.expect("valid_size",                "size > 0")
@dlt.expect("valid_retail_price",        "retail_price > 0")
def dim_product():
    p = dlt.read("silver.part")

    return p.select(
        col("p_partkey").cast("bigint").alias("product_key"),
        col("p_name").alias("product_name"),
        expr("CAST(regexp_extract(p_mfgr,  '(\\\\d+)', 1) AS INT)").alias("manufacturer"),
        expr("CAST(regexp_extract(p_brand, '(\\\\d+)', 1) AS INT)").alias("brand"),
        col("p_type").alias("product_type"),
        col("p_size").cast("int").alias("size"),
        col("p_container").alias("container"),
        col("p_retailprice").cast("decimal(18,2)").alias("retail_price")
    )

# =====================================================
# GOLD: DIM_SUPPLIER
# Grain: one row per supplier. silver_supplier is SCD Type 1,
# so no version filter is needed.
# =====================================================

@dlt.table(
    name="gold.dim_supplier",
    comment="Supplier dimension. One row per supplier. "
            "Nation and region are denormalised. PK: supplier_key.",
    # table_properties={"layer": "gold"}
)
@dlt.expect_or_fail("pk_not_null", "supplier_key IS NOT NULL")
@dlt.expect("name_present",        "supplier_name IS NOT NULL AND trim(supplier_name) <> ''")
@dlt.expect("nation_resolved",     "nation_name IS NOT NULL")
@dlt.expect("region_resolved",     "region_name IS NOT NULL")
def dim_supplier():
    s = dlt.read("silver.supplier")

    n = dlt.read("silver.nation").select(
        col("n_nationkey"), col("n_name"), col("n_regionkey")
    )
    r = dlt.read("silver.region").select(
        col("r_regionkey"), col("r_name")
    )

    return (
        s.join(n, s.s_nationkey == n.n_nationkey, "left")
         .join(r, n.n_regionkey == r.r_regionkey, "left")
         .select(
             col("s_suppkey").cast("bigint").alias("supplier_key"),
             col("s_name").alias("supplier_name"),
             col("n_name").alias("nation_name"),
             col("r_name").alias("region_name"),
             col("s_acctbal").cast("decimal(18,2)").alias("account_balance"),
             col("s_address").alias("address"),
             col("s_phone").alias("phone"),
             col("s_comment").alias("comment")
         )
    )

# =====================================================
# GOLD: DIM_GEOGRAPHY  (reference dimension)
# Grain: one row per nation. Region is denormalised in.
# =====================================================

@dlt.table(
    name="gold.dim_geography",
    comment="Geography reference dimension. One row per nation with its region. PK: nation_key.",
    # table_properties={"layer": "gold"}
)
@dlt.expect_or_fail("pk_not_null", "nation_key IS NOT NULL")
@dlt.expect("nation_name_present", "nation_name IS NOT NULL AND trim(nation_name) <> ''")
@dlt.expect("region_resolved",     "region_name IS NOT NULL")
def dim_geography():
    n = dlt.read("silver.nation")
    r = dlt.read("silver.region")

    return (
        n.join(r, n.n_regionkey == r.r_regionkey, "left")
         .select(
             col("n_nationkey").cast("bigint").alias("nation_key"),
             col("n_name").alias("nation_name"),
             col("r_name").alias("region_name"),
             col("n_comment").alias("nation_comment"),
             col("r_comment").alias("region_comment")
         )
    )




