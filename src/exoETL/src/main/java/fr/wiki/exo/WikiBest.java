package fr.wiki.exo;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static fr.wiki.exo.Download.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class WikiBest {

  private static final Logger logger      = LogManager.getLogger(WikiBest.class);
  private static       String MAX_TO_KEEP = "25";

  public static void main(String[] args) {

    String[] dateHour = ParseArgs.parseArgs(args);

    if (dateHour == null) {
      logger.info("DOWNLOAD CURRENT FILE");
      LocalDateTime localDate = LocalDateTime.now();
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd-HH");
      dateHour = dtf.format(localDate).split("-");
    }

    String wikiFilePath = null;
    String blackListFilePath = null;

    if (fileExist(computeWikiFileName(dateHour))) {
      logger.info("FILE ALREADY EXIST");
      System.exit(0);
    }

    try {
      wikiFilePath = downloadPageView(dateHour);
      blackListFilePath = downloadBlackList();
    } catch (IOException e) {
      e.printStackTrace();
      logger.info("DOWNLOAD FAILED");
      System.exit(1);
    }

    SparkSession.builder()
                .appName("fr.wiki.exo")
                .config("spark.driver.maxResultSize", "0")
                .config("spark.master", "local[*]")
                .config("spark.executor.memory", "6g")
                .config("spark.driver.memory", "6g")
                .getOrCreate();

    SparkSession sparkSession = SparkSession.builder().getOrCreate();

    StructType schemaPageCount = new StructType(
      new StructField[]{
        createStructField("domain_code", StringType, true),
        createStructField("page_title", StringType, true),
        createStructField("count_views", IntegerType, true),
        createStructField("total_response_size", IntegerType, true),
        });

    Dataset<Row> pageCount = sparkSession.read().format("csv")
                                         .schema(schemaPageCount)
                                         .option("header", "false")
                                         .option("delimiter", " ")
                                         .option("charset", "UTF-8")
                                         .load(wikiFilePath);

    StructType schemaBlackList = new StructType(
      new StructField[]{
        createStructField("domain_code", StringType, true),
        createStructField("page_title", StringType, true),
        });

    Dataset<Row> blackList = sparkSession.read().format("csv")
                                         .schema(schemaBlackList)
                                         .option("header", "false")
                                         .option("delimiter", " ")
                                         .option("charset", "UTF-8")
                                         .load(blackListFilePath);

    Dataset<Row> filtered = filterPage(pageCount, blackList);

    Dataset<Row> bestByDomains = computeBestByDomain(filtered);
    bestByDomains
      .orderBy(bestByDomains.col("domain_code").asc_nulls_last())
      .coalesce(1)
      .write()
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(wikiFilePath + ".csv");

  }

  /**
   * @param domains
   * @return
   */
  private static Dataset<Row> computeBestByDomain(Dataset<Row> domains) {
    WindowSpec windowSpec = Window
      .partitionBy(domains.col("domain_code"))
      .orderBy(domains.col("count_views").desc_nulls_last());

    return domains.withColumn("rank", functions.row_number().over(windowSpec))
                  .where("rank <= " + MAX_TO_KEEP);

  }

  /**
   * @param pageCount
   * @param blackList
   * @return
   */
  private static Dataset<Row> filterPage(Dataset<Row> pageCount, Dataset<Row> blackList) {

    Column joinCondition = pageCount.col("domain_code")
                                    .equalTo(blackList.col("domain_code"))
                                    .and(pageCount.col("page_title")
                                                  .equalTo(blackList.col("page_title")));

    pageCount = pageCount.repartition(pageCount.col("domain_code"));
    blackList = blackList.repartition(blackList.col("domain_code"));

    return pageCount.join(blackList, joinCondition, "leftanti");

  }
}