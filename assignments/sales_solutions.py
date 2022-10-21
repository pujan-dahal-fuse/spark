from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql.functions import col, sum as sum_, collect_list, collect_set, explode, row_number, rank, substring, max as max_, min as min_
from pyspark.sql import Window as W


# Load data
spark = SparkSession\
            .builder\
            .appName('sales')\
            .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-19-openjdk/lib/postgresql-42.5.0.jar')\
            .getOrCreate()

sales_df = spark.read.format('json').load('data/sales_records.json')
sales_df.printSchema()

# all records are in the form of strings, so we need to do type casting
sales_df = sales_df\
            .withColumn('Total Cost', col('Total Cost').cast(FloatType()))\
            .withColumn('Total Profit', col('Total Profit').cast(FloatType()))\
            .withColumn('Total Revenue', col('Total Revenue').cast(FloatType()))\
            .withColumn('Unit Cost', col('Unit Cost').cast(FloatType()))\
            .withColumn('Unit Price', col('Unit Price').cast(FloatType()))\
            .withColumn('Units Sold', col('Units Sold').cast(IntegerType()))\
            .withColumnRenamed('_corrupt_record', 'Corrupt Record')

sales_df = sales_df.na.drop(subset=['Region'])

sales_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.sales',
        properties={'user': 'postgres', 'password': ''})

# 1. Find the total cost, total revenue, total profit on the basis of each region
totals_df = sales_df\
    .groupBy('Region')\
    .agg(sum_(col('Total Cost')).alias('Total Cost'), sum_(col('Total Revenue')).alias('Total Revenue'), sum_('Total Profit').alias('Total Profit'))

totals_df.show()
totals_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.all_totals',
        properties={'user': 'postgres', 'password': ''})


# 2. Find the Item List on the basis of each country
window_spec = W.partitionBy('Country')
country_item_list_df = sales_df\
                        .withColumn('Item List', collect_set('Item Type').over(window_spec))\
                        .select('Country', 'Item List')\
                        .distinct()

country_item_list_df.show()

totals_df.show()
totals_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.country_item_lists',
        properties={'user': 'postgres', 'password': ''})


# 3. Find the total number of items sold in each country
total_items_sold_df = sales_df\
    .groupBy('Country')\
    .agg(sum_('Units Sold').alias('Num items sold'))

total_items_sold_df.show()
total_items_sold_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.total_items_sold',
        properties={'user': 'postgres', 'password': ''})


# 4. Find the top five famous items list on the basis of each region.(Consider units sold while doing this.)
region_sales_df = sales_df\
                    .groupBy('Region', 'Item Type')\
                    .agg(sum_('Units Sold').alias('Total Units Sold'))\
                    .orderBy('Region')

window_spec = W.partitionBy('Region')\
                .orderBy(col('Total Units Sold').desc())

region_sales_ranked_df = region_sales_df\
                            .withColumn('rn', rank().over(window_spec))\
                            .where('rn <= 5')\
                            .drop('rn')

# final result
top_famous_items_df = region_sales_ranked_df\
    .withColumn('Item List', collect_list('Item Type').over(W.partitionBy('Region')))\
    .select('Region', 'Item List')\
    .distinct()

top_famous_items_df.show()

top_famous_items_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.total_famous_items',
        properties={'user': 'postgres', 'password': ''})



# 5. Find all the regions and their famous sales channels.
famous_sales_channels_df = sales_df\
    .withColumn('Sales Channels', collect_set('Sales Channel').over(W.partitionBy('Region')))\
    .select('Region', 'Sales Channels')\
    .distinct()

famous_sales_channels_df.show()

famous_sales_channels_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.famous_sales_channels',
        properties={'user': 'postgres', 'password': ''})


# 6. Find  the list of countries and items and their respective units.

item_units_df = sales_df\
    .groupBy('Country', 'Item Type')\
    .agg(sum_('Units Sold').alias('Units Sold'))\
    .orderBy('Country')

item_units_df.show()
item_units_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.item_units',
        properties={'user': 'postgres', 'password': ''})



# 7. In 2013, identify the regions which sold maximum and minimum units of item type Meat.
sales_13_df = sales_df\
                .withColumn('year', substring('Order Date', -4, 4))\
                .where(col('year') == '2013')

# sales_13_df.show()


meat_sales_grouped_region_df = sales_13_df\
                                .where(col('Item Type') == 'Meat')\
                                .groupBy('Region', 'Item Type')\
                                .agg(sum_('Units Sold').alias('Units Sold'))

meat_sales_grouped_region_df\
    .orderBy('Units Sold')\
    .show()

meat_sales_grouped_region_df\
    .orderBy('Units Sold')\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.region_sales',
        properties={'user': 'postgres', 'password': ''})  


# 8. List all the items whose unit cost is less than 500
unit_cost_lt_500_df = sales_df\
    .filter(col('Unit Cost') < 500)\
    .select('Item Type', 'Unit Cost')\
    .distinct()

unit_cost_lt_500_df.show()
unit_cost_lt_500_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.unit_cost_lt_500',
        properties={'user': 'postgres', 'password': ''})  



# 9. Find the total cost, revenue and profit of each year.
year_sales_df = sales_df\
                    .withColumn('Year', substring('Order Date', -4, 4))\

totals_per_yr_df = year_sales_df\
    .groupBy('Year')\
    .agg(sum_('Total Cost').alias('Total Cost'), sum_('Total Revenue').alias('Total Revenue'), sum_('Total Profit').alias('Total Profit'))\
    .orderBy('Year')

totals_per_yr_df.show()
totals_per_yr_df\
    .write\
    .mode('overwrite')\
    .jdbc('jdbc:postgresql:spark', 'sales.totals_per_yr',
        properties={'user': 'postgres', 'password': ''})  

