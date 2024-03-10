# Choosing Appropriate Techniques:
----------------------------------------------------------------------------
#sample records

customer_id,address_id,address1,      address2,  city,state,zipcode
1001     ,NULL,        "123 Main St","Apt. B"     ,"Anytown","CA","90210"  # Current address for customer 1001
1002     ,2,           "456 Elm St",NULL,"Anytown","NY","10001"  # Historical address for customer 1002
1003     ,NULL,        "789 Maple Ave",NULL       ,"Metropolis","TX","78759"  # Current address for customer 1003
1002     ,1,           "10 Pine St",NULL,"Anytown","NY","10001"  # Previous address for customer 1002 (replaced by address_id 2)

#Explanation of sample records:
#This sample data includes updates for three customers.
#Customer 1001 receives a new address (no address_id as it's the current one).
#Customer 1002 has an existing address record (address_id 2) and receives a new address (NULL address_id).
#Customer 1003 receives a new address (no address_id).
#The data also demonstrates a scenario where a customer's address changes (customer 1002, address_id 1 is replaced by address_id 2).

# 1. When to Use merge (SCD Type 1):
#For attributes that don't require full historical tracking, use merge to efficiently update the hub table directly:

# Load update data
customer_update_df = spark.read.csv("customer_updates.csv")
# Merge updates into hub table
customer_update_df.write.format("delta").mode("merge").saveAsTable("customer_hub")

---------------------------------------------------------------------------

#2. When Not to Use merge (SCD Type 2):
#For address changes requiring historical tracking, use a separate SCD table and avoid merge:

# Join update data with hub table
joined_df = customer_update_df.join(spark.table("customer_hub"), on="customer_id", how="left")

# Determine current and historical records
current_customer_df = joined_df.filter(col("address_id").isNull())  # Latest address
scd_records_df = joined_df.filter(col("address_id").isNotNull())  # Historical addresses

# Update hub table with current address (no merge)
current_customer_df.write.option("mergeSchema", "true").format("delta").mode("overwrite").saveAsTable("customer_hub")

# Write historical addresses to SCD table
scd_records_df.select(
    coalesce(col("customer_id"), lit(-1)).alias("customer_id"),  # Handle potential nulls
    col("address_id"),
    col("address1"),
    col("address2"),
    col("city"),
    col("state"),
    col("zipcode"),
    current_date().cast("date").alias("start_date"),  # Real number for start date
    date_add(current_date(), -1).cast("date").alias("end_date")  # Real number for end date
).write.format("delta").saveAsTable("customer_scd")



