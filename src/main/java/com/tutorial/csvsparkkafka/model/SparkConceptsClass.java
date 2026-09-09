package com.tutorial.csvsparkkafka.model;

import org.apache.spark.sql.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.types.*;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;


public class SparkConceptsClass {

    public static void main(String[] args) {

        // ============================================================
        // 1. CREATE SPARK SESSION
        // ============================================================

        SparkSession spark = SparkSession.builder()
                .appName("CustomerOrderAnalytics")
                .master("local[*]")       // For local development only
                .getOrCreate();

        /*
         * NOTE:
         * SparkSession is the entry point for DataFrame, Dataset
         * and Spark SQL operations.
         *
         * In production, we normally don't hardcode master("local[*]").
         * The cluster manager provides the resources.
         */


        // ============================================================
        // 2. READ CUSTOMERS AS DATAFRAME
        // ============================================================

        Dataset<Row> customerDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("data/customers.csv");

        /*
         * Example customers.csv:
         *
         * customer_id,name,city
         * 101,Ganesh,Pune
         * 102,Rahul,Mumbai
         * 103,Amit,Delhi
         *
         * DataFrame = Dataset<Row>
         *
         * Spark understands the schema and column names.
         */

        customerDF.printSchema();

        customerDF.show();


        // ============================================================
        // 3. READ ORDERS AS DATAFRAME
        // ============================================================

        Dataset<Row> orderDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("data/orders.csv");

        /*
         * Example:
         *
         * order_id,customer_id,amount,status
         * 1,101,5000,COMPLETED
         * 2,102,3000,COMPLETED
         * 3,101,2000,CANCELLED
         * 4,103,7000,COMPLETED
         */

        orderDF.printSchema();

        orderDF.show();


        // ============================================================
        // 4. DATAFRAME TRANSFORMATION - FILTER
        // ============================================================

        Dataset<Row> completedOrders = orderDF
                .filter(col("status").equalTo("COMPLETED"));

        /*
         * filter() is a TRANSFORMATION.
         *
         * IMPORTANT:
         * Spark does NOT execute this immediately.
         *
         * Spark uses LAZY EVALUATION.
         *
         * The actual execution happens when we call an ACTION
         * such as show(), count(), write(), etc.
         */


        // ============================================================
        // 5. SELECT REQUIRED COLUMNS
        // ============================================================

        Dataset<Row> selectedOrders = completedOrders
                .select(
                        col("order_id"),
                        col("customer_id"),
                        col("amount")
                );

        /*
         * select() is also a transformation.
         *
         * We are reducing the number of columns.
         *
         * This can reduce data movement and memory usage.
         */


        // ============================================================
        // 6. CACHE
        // ============================================================

        selectedOrders.cache();

        /*
         * CACHE NOTE:
         *
         * We cache this DataFrame because suppose multiple downstream
         * operations use selectedOrders.
         *
         * Without cache:
         *
         *     Operation 1
         *          ↓
         *     recompute transformations
         *
         *     Operation 2
         *          ↓
         *     recompute transformations
         *
         * With cache:
         *
         *     First action
         *          ↓
         *     compute + cache
         *          ↓
         *     Second action → reuse cached data
         *
         * cache() uses Spark's default persistence strategy.
         *
         * IMPORTANT:
         * Never cache everything.
         * Cache when:
         *
         * 1. Dataset is reused
         * 2. Computation is expensive
         * 3. Dataset fits reasonably in executor memory
         */


        // ============================================================
        // 7. ACTION TO MATERIALIZE CACHE
        // ============================================================

        selectedOrders.count();

        /*
         * count() is an ACTION.
         *
         * This triggers Spark execution.
         *
         * Until an action occurs, cache() itself does not actually
         * compute the data.
         */


        // ============================================================
        // 8. DATAFRAME JOIN
        // ============================================================

        Dataset<Row> customerOrders = selectedOrders
                .join(
                        customerDF,
                        selectedOrders.col("customer_id")
                                .equalTo(customerDF.col("customer_id")),
                        "inner"
                )
                .select(
                        selectedOrders.col("order_id"),
                        selectedOrders.col("customer_id"),
                        customerDF.col("name"),
                        customerDF.col("city"),
                        selectedOrders.col("amount")
                );

        /*
         * JOIN NOTE:
         *
         * We join orders with customers using customer_id.
         *
         * Conceptually:
         *
         * Orders
         * -----------------
         * customer_id
         * amount
         *
         *          JOIN
         *
         * Customers
         * -----------------
         * customer_id
         * name
         * city
         *
         * Result:
         * ------------------------------
         * customer_id | name | city | amount
         *
         *
         * IMPORTANT INTERVIEW POINT:
         *
         * A normal join can cause SHUFFLE.
         *
         * Data may need to move between partitions so that matching
         * customer_id records are brought together.
         */


        // ============================================================
        // 9. DATASET - TYPED OBJECT
        // ============================================================

        Encoder<Customer> customerEncoder =
                Encoders.bean(Customer.class);

        Dataset<Customer> customerDataset =
                customerDF.as(customerEncoder);

        /*
         * DATAFRAME:
         *
         * Dataset<Row>
         *
         * DATASET:
         *
         * Dataset<Customer>
         *
         * Dataset provides stronger type safety.
         *
         * This is particularly useful in Java/Scala applications
         * where we want to work with domain objects.
         */

        customerDataset.show();


        // ============================================================
        // 10. RDD
        // ============================================================

        JavaSparkContext javaSparkContext =
                JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<Customer> customerRDD =
                customerDataset.javaRDD();

        /*
         * We have now converted:
         *
         * DataFrame
         *      ↓
         * Dataset<Customer>
         *      ↓
         * RDD<Customer>
         *
         * RDD is a lower-level Spark abstraction.
         *
         * In real applications, we generally prefer DataFrame/Dataset
         * APIs for structured data because Spark can optimize them.
         *
         * RDD is useful when we need low-level control or custom
         * processing that doesn't naturally fit DataFrame operations.
         */


        JavaRDD<String> customerNames =
                customerRDD.map(customer -> customer.getName());

        /*
         * map() here is an RDD TRANSFORMATION.
         *
         * It converts:
         *
         * Customer → String
         *
         * Again, this is lazy.
         */


        // ============================================================
        // 11. AGGREGATION
        // ============================================================

        Dataset<Row> customerRevenue = customerOrders
                .groupBy(
                        col("customer_id"),
                        col("name"),
                        col("city")
                )
                .agg(
                        sum("amount").alias("total_revenue"),
                        count("order_id").alias("total_orders"),
                        avg("amount").alias("average_order_value")
                );

        /*
         * AGGREGATION:
         *
         * groupBy() + agg()
         *
         * Example:
         *
         * Customer 101
         *
         * Order 1 = 5000
         * Order 2 = 2000
         *
         * total_revenue = 7000
         *
         *
         * IMPORTANT:
         *
         * groupBy() is normally a WIDE TRANSFORMATION.
         *
         * Why?
         *
         * Data for the same customer may exist in different
         * partitions.
         *
         * Spark needs to move/shuffle data so records with the
         * same grouping key can be processed together.
         *
         * This is called SHUFFLE.
         */


        // ============================================================
        // 12. FILTER AGGREGATED RESULT
        // ============================================================

        Dataset<Row> highValueCustomers =
                customerRevenue
                        .filter(col("total_revenue").gt(5000))
                        .orderBy(col("total_revenue").desc());

        /*
         * More transformations:
         *
         * filter()
         * orderBy()
         *
         * orderBy() can also cause shuffle because global ordering
         * requires data to be redistributed.
         */


        // ============================================================
        // 13. ACTION - SHOW RESULT
        // ============================================================

        highValueCustomers.show(false);

        /*
         * show() = ACTION
         *
         * This triggers the complete execution pipeline.
         *
         * Spark roughly performs:
         *
         *     Read
         *       ↓
         *     Filter
         *       ↓
         *     Select
         *       ↓
         *     Join
         *       ↓
         *     GroupBy
         *       ↓
         *     Aggregation
         *       ↓
         *     Filter
         *       ↓
         *     OrderBy
         *       ↓
         *     Show
         */


        // ============================================================
        // 14. WRITE RESULT
        // ============================================================

        highValueCustomers
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv("output/high-value-customers");

        /*
         * write() is an ACTION.
         *
         * Spark writes the result to the specified destination.
         *
         * In real projects, this could be:
         *
         *     S3
         *     HDFS
         *     Data Warehouse
         *     PostgreSQL
         *     Cassandra
         *     Kafka
         */


        // ============================================================
        // 15. RELEASE CACHE
        // ============================================================

        selectedOrders.unpersist();

        /*
         * Once the cached DataFrame is no longer required,
         * release it from executor memory.
         *
         * This is good memory management practice.
         */


        // ============================================================
        // 16. STOP SPARK
        // ============================================================

        spark.stop();
    }


    // ================================================================
    // DOMAIN OBJECT FOR DATASET
    // ================================================================

    public static class Customer implements Serializable {

        private int customer_id;
        private String name;
        private String city;

        public Customer() {
        }

        public int getCustomer_id() {
            return customer_id;
        }

        public void setCustomer_id(int customer_id) {
            this.customer_id = customer_id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }
}

