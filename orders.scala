import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object orders {
  val spark = SparkSession.builder().master("local[*]").appName("orders").getOrCreate()

  def main(args:Array[String]):Unit={
    val df = get_table("orders")

    //analysis 1
    val df_count = df.groupBy("Customer_ID")
      .agg(count("Order_ID").alias("TotalOrders"), avg("Sales").alias("AverageOrder"))

    val Threshold = df_count.select(avg("AverageOrder")).head().getDouble(0)

    val df_class = df_count.withColumn("Category",
      when(col("AverageOrder") >= 2 * Threshold, "Gold")
        .when(col("AverageOrder") >= Threshold, "Silver")
        .otherwise("Bronze"))

    df_class.show()
    //    val output_path = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\output"
    //
    //    df_class.write.option("header","true").csv(output_path)

    val df_weekly = df
      .withColumn("DayOfWeek", dayofweek(col("Order_Date")))
      .groupBy("DayOfWeek")
      .agg(sum("Sales").alias("TotalSales"))
      .orderBy(desc("TotalSales"))
    df_weekly.show()

    //analysis 2
    val dfWithDays = df
      .withColumn("DaysBetween", datediff(col("Ship_Date"), col("Order_Date")))

    val filteredData = dfWithDays
      .filter(col("Ship_Mode").isin("Second Class", "First Class", "Standard Class"))

    val avgDaysByCategoryAndShipMode = filteredData
      .groupBy("Category", "Ship_Mode")
      .agg(avg("DaysBetween").alias("AvgDays"))
      .withColumn("AvgDays", floor(col("AvgDays")))
      .orderBy(col("Ship_Mode"))

    avgDaysByCategoryAndShipMode.show()

    val dfWithMonth = df.withColumn("Month", month(col("Order_Date")))

    val seasonCategories = dfWithMonth.withColumn("Season",
      when(col("Month").between(3, 5), lit("Spring"))
        .when(col("Month").between(6, 8), lit("Summer"))
        .when(col("Month").between(9, 11), lit("Autumn"))
        .otherwise(lit("Winter"))
    )

    val totalordercategoryseason = seasonCategories.groupBy("Season", "Category")
      .agg(count("Order_ID").alias("TotalOrders"))
      .orderBy(col("Season"), col("TotalOrders").desc)


    val mostOrderedCategoryBySeason = totalordercategoryseason.groupBy("Season").agg(
      first("Category").alias("MostOrderedCategory"),
      first("TotalOrders").alias("MaxOrders")
    )

    totalordercategoryseason.show()
    mostOrderedCategoryBySeason.show()


    val totalSalesByCategoryAndSubcategory = df
      .groupBy("Category", "Sub_Category")
      .agg(sum("Sales").alias("TotalSales"))
      .orderBy("Category", "Sub_Category")

    totalSalesByCategoryAndSubcategory.show()

    val totalRevenueByCategory = totalSalesByCategoryAndSubcategory
      .groupBy("Category")
      .agg(sum("TotalSales").alias("TotalRevenue"))
      .orderBy("Category")

    totalRevenueByCategory.show()

    val revenuePercentageByCategoryAndSubcategory = totalSalesByCategoryAndSubcategory
      .join(totalRevenueByCategory, "Category")
      .withColumn("RevenuePercentage", col("TotalSales") / col("TotalRevenue"))
      .orderBy("Category", "Sub_Category")

    revenuePercentageByCategoryAndSubcategory.show()

    //analysis 3

    val revenueByRegionAndCategory = df
      .groupBy("Region", "Category")
      .agg(sum("Sales").alias("TotalRevenue"))
    revenueByRegionAndCategory.show()


    val dfWithYearMonth = df.withColumn("Year", year(col("Order_Date")))
      .withColumn("Month", month(col("Order_Date")))

    val df_YearData = dfWithYearMonth.filter("Year = 2017")

    val yearlyMonthlySales = df_YearData.groupBy("Year", "Month")
      .agg(sum("Sales").alias("Total Sales")).orderBy(desc("Total Sales"))

    //analysis 4

    yearlyMonthlySales.show()
    val df1 = df.withColumn("Order_Date", to_date(col("Order_Date"))).withColumn("Ship_Date", to_date(col("Ship_Date")))

    val geographicalDF = df1.select("Region", "State", "Sales", "Order_Date")

    val geographicalDFWithYear = geographicalDF.withColumn("OrderYear", year(to_date(col("Order_Date"))))

    val regionStateYearSalesDF = geographicalDFWithYear.groupBy("Region", "State", "OrderYear").agg(sum("Sales").alias("TotalSales"))

    val mostProfitableWindowSpec = Window.partitionBy("Region").orderBy(desc("TotalSales"))
    val leastProfitableWindowSpec = Window.partitionBy("Region").orderBy("TotalSales")

    val rankedRegionStateYearSalesDF = regionStateYearSalesDF.withColumn("rank_most_profitable", dense_rank().over(mostProfitableWindowSpec)).withColumn("rank_least_profitable", dense_rank().over(leastProfitableWindowSpec))

    val mostProfitableStateYearPerRegionDF = rankedRegionStateYearSalesDF.filter("rank_most_profitable = 1").drop("rank_most_profitable").drop("rank_least_profitable")

    val leastProfitableStateYearPerRegionDF = rankedRegionStateYearSalesDF.filter("rank_least_profitable = 1").drop("rank_least_profitable").drop("rank_most_profitable")

    mostProfitableStateYearPerRegionDF.show()
    leastProfitableStateYearPerRegionDF.show()

    val data = df.withColumn("Order_Date", to_date(col("Order_Date"))).withColumn("Ship_Date", to_date(col("Ship_Date")))

    val dataWithDelay = data.withColumn("ShipmentDelay", datediff(col("Ship_Date"), col("Order_Date")))

    val delayByCategoryCity = dataWithDelay.groupBy("Category", "City").agg(sum("ShipmentDelay").alias("TotalDelay"))

    val windowSpecMost = Window.partitionBy("Category").orderBy(desc("TotalDelay"))
    val citiesWithMostDelay = delayByCategoryCity.withColumn("rank", dense_rank().over(windowSpecMost)).filter("rank = 1").drop("rank")

    val windowSpecLeast = Window.partitionBy("Category").orderBy(asc("TotalDelay"))
    val citiesWithLeastDelay = delayByCategoryCity.withColumn("rank", dense_rank().over(windowSpecLeast)).filter("rank = 1").drop("rank")

    citiesWithMostDelay.show()
    citiesWithLeastDelay.show()


  }

  def get_table(table: String): DataFrame = {
    val df= spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/super_store")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable", table)
      .option("user", "root")
      .option("password", "Mohit@6669")
      .load()

    return df
}

}

