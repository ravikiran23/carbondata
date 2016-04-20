package org.carbondata.integration.spark.testsuite.createTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * test class for testing the create cube DDL.
  */
class TestCreateTableNewDDL extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
  }

  test("create table Test with new DDL") {
    checkAnswer(
      sql("CREATE TABLE table1 (empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " COMMENT 'This is the page view table'  PARTITIONED BY (empno BIGINT COMMENT 'PART')"
        + " STORED BY 'org.apache.carbondata.format' "
        + "TBLPROPERTIES(\"COLUMN_GROUPS\"=\"(empno,empname),(deptno,deptname)\",\"DICTIONARY_EXCLUDE\"=\"deptname\"," +
        "\"DICTIONARY_INCLUDE\"=\"deptno\",\"PARTITIONCOUNT\"=\"2\"," +
        "\"PARTITIONCLASS\"=\"org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl\")"),
      Seq())

  }
  test("create table Test with new DDL with if not exists tag") {
    checkAnswer(
      sql("CREATE TABLE IF NOT EXISTS table2(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " COMMENT 'This is the page view table'  PARTITIONED BY (empno BIGINT COMMENT 'PART')"
        + " STORED BY 'org.apache.carbondata.format' "
        + "TBLPROPERTIES(\"COLUMN_GROUPS\"=\"(empno,empname),(deptno,deptname)\",\"DICTIONARY_EXCLUDE\"=\"deptname\"," +
        "\"DICTIONARY_INCLUDE\"=\"deptno\",\"PARTITIONCOUNT\"=\"2\"," +
        "\"PARTITIONCLASS\"=\"org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl\")"),
      Seq())
  }

  test("create table Test with new DDL without partittion details") {
    checkAnswer(
      sql("CREATE TABLE IF NOT EXISTS table3(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED BY 'org.apache.carbondata.format' "
        + "TBLPROPERTIES(\"COLUMN_GROUPS\"=\"(empno,empname),(deptno,deptname)\",\"DICTIONARY_EXCLUDE\"=\"deptname\"," +
        "\"DICTIONARY_INCLUDE\"=\"deptno\")"),
      Seq())
  }

  test("create table Test with new DDL without col groups") {
    checkAnswer(
      sql("CREATE TABLE IF NOT EXISTS table4(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED BY 'org.apache.carbondata.format' "
        + "TBLPROPERTIES(\"DICTIONARY_EXCLUDE\"=\"deptname\",\"DICTIONARY_INCLUDE\"=\"deptno\")"),
      Seq())
  }

  test("create table Test with new DDL without table properties") {
    checkAnswer(
      sql("CREATE TABLE IF NOT EXISTS table4(empno Int, empname Array<String>, designation String, doj Timestamp, "
        + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
        + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
        + " STORED BY 'org.apache.carbondata.format' "),
      Seq())
  }

  override def afterAll {
  }

}
