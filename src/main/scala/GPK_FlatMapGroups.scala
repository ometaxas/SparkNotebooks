import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object GPK_FlatMapGroups {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("SPARK_HOME", "/home/ometaxas/Projects/spark-3.0.0-bin-hadoop3.2")
      .config("spark.master", "local[*]")
      //.config("spark.local.dir", "/media/ometaxas/nvme/spark")
      .config("spark.local.dir", "/media/datadisk/Datasets/Spark")
      .config("spark.driver.memory", "110g")
      .appName("TestGroupByKey")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val MAG_NLP = "/media/ometaxas/nvme/datasets/MAGsample/nlp"
    val MAG_HOME = "/media/ometaxas/nvme/datasets/MAGsample/mag"
    val MAG_ADV = "/media/ometaxas/nvme/datasets/MAGsample/advanced"
    val outPath = "/media/ometaxas/nvme/datasets/MAGsample/out"

    val papersTsvFilename = "Papers.txt"
    val paperSchema = new StructType().
      add("paperId", LongType, false).
      add("magRank", IntegerType, true).
      add("doi", StringType, true).
      add("docTypetmp", StringType, true).
      add("normalizedTitle", StringType, true).
      add("title", StringType, false).
      add("bookTitle", StringType, true).
      add("pubYear", IntegerType, true).
      add("pubDate", StringType, true).
      add("onlineDate", StringType, true).
      add("publisherName", StringType, true).
      add("journalId", StringType, true).
      add("conferenceSeriesId", LongType, true).
      add("conferenceInstancesId", LongType, true).
      add("volume", StringType, true).
      add("issue", StringType, true).
      add("firstPage", StringType, true).
      add("lastPage", StringType, true).
      add("referenceCount", LongType, true).
      add("citationCount", LongType, true).
      add("estimatedCitation", LongType, true).
      add("originalVenue", StringType, true).
      add("familyId", StringType, true).
      add("createdDate", DateType, true)


    val papersdf = spark.read.options(Map("sep" -> "\t", "header" -> "false")).
      schema(paperSchema).
      csv(s"file://$MAG_HOME/$papersTsvFilename")
      .select($"paperId", $"normalizedTitle", $"publisherName", $"journalId", $"conferenceSeriesId", $"pubYear", $"pubDate", $"magRank", $"docTypetmp", $"doi")


    val paperFieldsOfStudyTsvFilename = "PaperFieldsOfStudy.txt"

    val paperIdFieldsOfStudyschema = new StructType().
      add("paperId", LongType, false).
      add("fieldsOfStudyId", LongType, false).
      add("score", DoubleType, true)

    val paperFieldsOfStudydf = spark.read.options(Map("sep" -> "\t", "header" -> "false")).
      schema(paperIdFieldsOfStudyschema).
      csv(s"file://$MAG_ADV/$paperFieldsOfStudyTsvFilename")
    //paperFieldsOfStudydf.printSchema
    //paperFieldsOfStudydf.show(5)

    val fieldsOfStudyTsvFilename = "FieldsOfStudy.txt"

    val fieldsOfStudyschema = new StructType().
      add("fieldsOfStudyId", LongType, false).
      add("magRank", IntegerType, true).
      add("normalizedName", StringType, true).
      add("name", StringType, true).
      add("mainType", StringType, true).
      add("level", IntegerType, true).
      add("paperCount", LongType, true).
      add("paperFamilyCount", LongType, true).
      add("citationCount", LongType, true).
      add("createdDate", DateType, true)

    val fieldsOfStudydf = spark.read.options(Map("sep" -> "\t", "header" -> "false")).
      schema(fieldsOfStudyschema).
      csv(s"file://$MAG_ADV/$fieldsOfStudyTsvFilename")
      .select($"fieldsOfStudyId", $"normalizedName", $"level", $"paperCount")
    val journalsTsvFilename = "Journals.txt"

    val journalschema = new StructType().
      add("journalId", LongType, false).
      add("magRank", IntegerType, true).
      add("normalizedName", StringType, true).
      add("journalName", StringType, true).
      add("issn", StringType, true).
      add("publisher", StringType, true).
      add("webpage", StringType, true).
      add("paperCount", LongType, true).
      add("paperFamilyCount", LongType, true).
      add("citationCount", LongType, true).
      add("createdDate", DateType, true)

    val journaldf = spark.read.options(Map("sep" -> "\t", "header" -> "false")).
      schema(journalschema).
      csv(s"file://$MAG_HOME/$journalsTsvFilename")


    // Window based aggregation

    val journal_partition_sorted_by_rank = Window.partitionBy("journalId").orderBy($"magRank")
    val journal_partition_sorted_by_date = Window.partitionBy("journalId").orderBy($"pubYear".desc)
    val journal_id_partition = Window.partitionBy("journalId")

    val top_k_papers = papersdf
      .filter(papersdf("journalId").isNotNull)
      .withColumn("rank_within_journal", row_number().over(journal_partition_sorted_by_rank))
      .withColumn("recency_within_journal", row_number().over(journal_partition_sorted_by_date))
      .withColumn("paperCnt_in_journal", count(col("paperId")).over(journal_id_partition))
      .withColumn("threshold_in_journal", lit(0.2) * count(col("paperId")).over(journal_id_partition))
      .filter($"rank_within_journal" <= $"threshold_in_journal" || $"recency_within_journal" <= $"threshold_in_journal")

    println(papersdf.count())
    println(top_k_papers.count())
    //papersdf.groupBy("journalId").count().show(50)
    //top_k_papers.groupBy("journalId").count().show(50)


    //  top_k_papers.show(20)

    val journal_paper_fos_df = top_k_papers
      .join(paperFieldsOfStudydf, top_k_papers("paperId") === paperFieldsOfStudydf("paperId"), "inner")
      .select(papersdf("journalId"), papersdf("paperId"), paperFieldsOfStudydf("fieldsOfStudyId").as("fosId"), lit(1).as("score"),    papersdf("magRank"), papersdf("pubYear"))
    //.filter(papersdf("journalId").isNotNull)


    /*
    val journal_paper_fos_df = papersdf
      .join(paperFieldsOfStudydf, papersdf("paperId") === paperFieldsOfStudydf("paperId"),  "inner")
      .select(papersdf("journalId"), papersdf("paperId"), paperFieldsOfStudydf("fieldsOfStudyId"))
    */

    /*
     paperId: Long,
                          fosId: Long,
                          score: Double,
                          magRank: Int,
                          pubYear: Int,
                          journalId: String
     */

    /*
    val inputSeq = Seq(
       FosMagPaperDS(1, 1, 1, 1, 2000, "J1")
      ,FosMagPaperDS(1, 2, 1, 1, 2000, "J1")
      ,FosMagPaperDS(1, 3, 1, 1, 2000, "J1")
      ,FosMagPaperDS(2, 1, 1, 1, 2000, "J1")
      ,FosMagPaperDS(2, 2, 1, 1, 2000, "J1")
      ,FosMagPaperDS(2, 4, 1, 1, 2000, "J1"),
       FosMagPaperDS(3, 1, 1, 1, 2000, "J1")
      ,FosMagPaperDS(3, 5, 1, 1, 2000, "J1")
      ,FosMagPaperDS(4, 1, 1, 1, 2000, "J2")
      ,FosMagPaperDS(4, 2, 1, 1, 2000, "J2")
      ,FosMagPaperDS(4, 3, 1, 1, 2000, "J2")
      ,FosMagPaperDS(5, 1, 1, 1, 2000, "J2")
      ,FosMagPaperDS(5, 4, 1, 1, 2000, "J2")
      ,FosMagPaperDS(6, 5, 1, 1, 2000, "J2")
    )

    val journal_paper_fos_df = spark.sparkContext.parallelize(inputSeq).toDF()
*/

    println(journal_paper_fos_df.count())

    val journal_field_of_study_partition = Window.partitionBy("journalId", "fosId")


    val journal_fos_weight_df = journal_paper_fos_df
      .withColumn("total_fields_of_study_alias", count(col("fosId")).over(journal_id_partition)).
      withColumn("fields_of_study_per_journal_alias", count(col("paperId")).over(journal_field_of_study_partition)).
      withColumn("field_of_study_normalized_weight", (col("fields_of_study_per_journal_alias") / col("total_fields_of_study_alias")))

    journal_fos_weight_df.show(20)


    val journal_agg_df = journal_fos_weight_df.
      groupBy("journalId", "fosId").
      agg(
        first(col("field_of_study_normalized_weight")).alias("field_of_study_normalized_weight")
      ).orderBy($"journalId", $"field_of_study_normalized_weight" desc)

    journal_agg_df.show(20)



    journal_agg_df.coalesce(1).write.mode("overwrite").options(Map("sep"->",", "header"-> "true")).csv(s"$outPath/journal_agg_df.csv")


    //Flat map


    val journal_paper_fos_ds = journal_paper_fos_df.as[FosMagPaperDS]

    //  journal_paper_fos_ds.show(20)


    def fosStatistics(entityId: String, allFosList: List[HasFosScore]): List[AnnotationStatisticStep0] = {
      val allFosSumScore = allFosList.foldLeft(0d)(_ + _.score)
      val fosIdMap: Map[Long, List[HasFosScore]] = allFosList.groupBy(_.fosId)
      val statisticList: List[AnnotationStatisticStep0] = fosIdMap.map { case (fosId: Long, fosList: List[HasFosScore]) =>
        val oneFosCount = fosList.size
        val oneFosSumScore = fosList.foldLeft(0d)(_ + _.score)
        val normalizedScore = oneFosSumScore / allFosSumScore
        AnnotationStatisticStep0(entityId, fosId, oneFosCount, oneFosSumScore, normalizedScore)
      }.toList
      statisticList
    }


    val journalFosStep0DS: Dataset[AnnotationStatisticStep0] =
      journal_paper_fos_ds
        .where(col("journalId").isNotNull)
        .groupByKey(_.journalId)
        .flatMapGroups { (journalIdOpt: String, it: Iterator[FosMagPaperDS]) =>
          val journalId = journalIdOpt
          val allFosList = it.toList
          fosStatistics(journalId, allFosList)
        }.orderBy($"journalId", $"normalizedScore" desc)

     journalFosStep0DS.show(20)

    journalFosStep0DS.coalesce(1).write.mode("overwrite").options(Map("sep"->",", "header"-> "true")).csv(s"$outPath/journalFosStep0DS.csv")

  }

}
