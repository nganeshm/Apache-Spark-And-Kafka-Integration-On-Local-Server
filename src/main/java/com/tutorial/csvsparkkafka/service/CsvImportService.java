package com.tutorial.csvsparkkafka.service;

import com.tutorial.csvsparkkafka.kafka.CsvRecordProducer;
import com.tutorial.csvsparkkafka.model.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;

import java.util.List;

/**
 * Reads a CSV file with Apache Spark, applies a couple of illustrative
 * transformations, then hands each resulting row off to Kafka.
 *
 * WHY USE SPARK FOR "JUST" READING A CSV?
 * For a tiny file, plain Java (e.g. OpenCSV / Apache Commons CSV) would be
 * simpler. Spark earns its keep once files get large (millions of rows,
 * multiple GBs) or once you need distributed transformations — filtering,
 * joins, aggregations, schema inference — before the data goes anywhere.
 * This tutorial uses a small sample file so you can see the mechanics, but
 * the same code scales to much bigger inputs without changes.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CsvImportService {

    /**
     * Explicit schema so Spark does not infer joiningDate as a Date.
     * inferSchema + date columns calls SparkDateTimeUtils, which reflects into
     * sun.util.calendar.ZoneInfo and fails on JDK 17+ without
     * --add-opens=java.base/sun.util.calendar=ALL-UNNAMED.
     * joiningDate stays a String, matching Employee.
     */
    private static final StructType EMPLOYEE_CSV_SCHEMA = new StructType()
            .add("id", DataTypes.StringType, true)
            .add("name", DataTypes.StringType, true)
            .add("department", DataTypes.StringType, true)
            .add("salary", DataTypes.DoubleType, true)
            .add("joiningDate", DataTypes.StringType, true);

    private final SparkSession sparkSession;
    private final CsvRecordProducer csvRecordProducer;

    /**
     * Reads the CSV at the given path, validates/cleans it with Spark, and
     * publishes each valid row to Kafka.
     *
     * @param filePath absolute path to a CSV file on disk
     * @return the number of rows successfully published to Kafka
     */
    public long importCsvAndPublish(String filePath) {

        // ---- STEP 1: Read the CSV into a Spark Dataset<Row> ----
        // header=true  -> first line holds column names
        // schema(...)  -> fixed types; do NOT use inferSchema (see EMPLOYEE_CSV_SCHEMA)
        Dataset<Row> rawDataset = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .schema(EMPLOYEE_CSV_SCHEMA)
                .load(filePath);

        log.info("Loaded CSV '{}' with schema:", filePath);
        rawDataset.printSchema(); // prints to stdout — useful while learning/debugging

        // ---- STEP 2: Clean / transform with Spark's DataFrame API ----
        // Example transformations that are trivial in Spark and painful by hand
        // once you're dealing with large files:
        //   - drop rows missing a required field ("name")
        //   - filter out obviously bad data (negative or zero salary)
        //   - keep only the columns we actually care about, in a fixed order
        Dataset<Row> cleaned = rawDataset
                .na().drop(new String[]{"name"})      // remove rows where "name" is null
                .filter(col("salary").gt(0))          // remove rows with salary <= 0
                .select("id", "name", "department", "salary", "joiningDate");

        long totalRows = cleaned.count();
        log.info("{} row(s) remain after cleaning (out of {} originally read)",
                totalRows, rawDataset.count());

        // ---- STEP 3: Bring the (small, already-filtered) result back to the driver ----
        // collectAsList() pulls all rows into the JVM's regular heap memory.
        // This is fine for demo-sized files. For genuinely large datasets you
        // would instead write the cleaned Dataset to a sink in a distributed
        // way (e.g. foreachPartition, or Spark's built-in Kafka sink via
        // `.write().format("kafka")`) rather than collecting everything here.
        List<Row> rows = cleaned.collectAsList();

        long publishedCount = 0;
        for (Row row : rows) {
            Employee employee = mapRowToEmployee(row);
            csvRecordProducer.publish(employee);
            publishedCount++;
        }

        log.info("Published {} employee record(s) to Kafka", publishedCount);
        return publishedCount;
    }

    /**
     * Converts a single Spark Row into our plain Java Employee POJO.
     * Doing this conversion explicitly (rather than passing Spark Rows
     * around your codebase) keeps Spark's types from leaking into layers
     * of the app that shouldn't need to know Spark exists.
     */
    private Employee mapRowToEmployee(Row row) {
        // Use typed getters. Do NOT write String.valueOf(row.getAs(...)):
        // Java resolves String.valueOf(char[]) and infers getAs as char[],
        // then the real String value throws ClassCastException (String -> [C).
        int salaryIdx = row.fieldIndex("salary");
        return new Employee(
                row.getString(row.fieldIndex("id")),
                row.getString(row.fieldIndex("name")),
                row.getString(row.fieldIndex("department")),
                row.isNullAt(salaryIdx) ? 0.0 : row.getDouble(salaryIdx),
                row.getString(row.fieldIndex("joiningDate"))
        );
    }
}
