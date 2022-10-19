from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql.functions import col, sum as sum_, collect_list, collect_set, explode, row_number, rank, substring, max as max_, min as min_
from pyspark.sql import Window as W


# Load data
spark = SparkSession.builder.appName('sales').getOrCreate()
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

# 1. Find the total cost, total revenue, total profit on the basis of each region
sales_df\
    .groupBy('Region')\
    .agg(sum_(col('Total Cost')).alias('Total Cost'), sum_(col('Total Revenue')).alias('Total Revenue'), sum_('Total Profit').alias('Total Profit'))\
    .show()


# 2. Find the Item List on the basis of each country
window_spec = W.partitionBy('Country')
country_item_list_df = sales_df\
                        .withColumn('Item List', collect_set('Item Type').over(window_spec))\
                        .select('Country', 'Item List')\
                        .distinct()

country_item_list_df.show()


# 3. Find the total number of items sold in each country
sales_df\
    .groupBy('Country')\
    .agg(sum_('Units Sold').alias('Num items sold'))\
    .show()



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
region_sales_ranked_df\
    .withColumn('Item List', collect_list('Item Type').over(W.partitionBy('Region')))\
    .select('Region', 'Item List')\
    .distinct()\
    .show()


# 5. Find all the regions and their famous sales channels.
sales_df\
    .withColumn('Sales Channels', collect_set('Sales Channel').over(W.partitionBy('Region')))\
    .select('Region', 'Sales Channels')\
    .distinct()\
    .show()


# 6. Find  the list of countries and items and their respective units.

sales_df\
    .groupBy('Country', 'Item Type')\
    .agg(sum_('Units Sold').alias('Units Sold'))\
    .orderBy('Country')\
    .show()



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


# 8. List all the items whose unit cost is less than 500
sales_df\
    .filter(col('Unit Cost') < 500)\
    .select('Item Type', 'Unit Cost')\
    .distinct()\
    .show()


# 9. Find the total cost, revenue and profit of each year.
year_sales_df = sales_df\
                    .withColumn('Year', substring('Order Date', -4, 4))\

year_sales_df\
    .groupBy('Year')\
    .agg(sum_('Total Cost').alias('Total Cost'), sum_('Total Revenue').alias('Total Revenue'), sum_('Total Profit').alias('Total Profit'))\
    .orderBy('Year')\
    .show()
