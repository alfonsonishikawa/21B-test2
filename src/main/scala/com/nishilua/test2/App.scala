package com.nishilua.test2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scalaz._
import Scalaz._
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {

  val conf = new SparkConf().setMaster("local[3]")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  val RELATED_COUNT = 10

  case class View(id: Int, user_id:String, tag_id:String, product_name:String)
  case class TagName(tag_id: String, product_name:String)
  case class TagSingleRecom(tag_id: TagId, recommended_tag_id: TagId)

  def main(args : Array[String]) {

    val viewsDS = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/alfonso/git/21B-test2/src/main/resources/21B_tag_views_dataset.csv")
      .as[View]
      .cache()

    // ---------------------------------------------------------------------------------------------
    // RDD

    // (tag_id, product_name) to be used as lookup name table at the end
    val tagIdProductName = viewsDS.select($"tag_id", $"product_name").as[TagName].distinct().cache()

    val viewsRDD = viewsDS.rdd

    // Delete columns from the data to get only (user_id, tag_id)
    val userSingleView: RDD[(UserId, TagId)] = viewsRDD.map( view => (view.user_id, view.tag_id))

    // Create (user, views set)
    val userViews = userSingleView.aggregateByKey (Set[TagId]()) (
      { case (buf, tag) => buf + tag },
      { case (buf1, buf2) => buf1 ++ buf2 }
    )

    // Explode the views set. For each views set, generate N (product, Set(related_products))
    val productRelatedProducts = userViews.flatMap {
      case (user_id, views) => {
        for ( view <- views) yield (view, views-view)
      }
    }

    // Aggregate the products:
    // For each product, count the products viewed: (tag_id, map(related_tag_id, count))
    val productAggViews = productRelatedProducts.aggregateByKey (Map[TagId, ViewsCount]()) (
      { case (map, relatedViewsSet) => {
          val newViewsMap = relatedViewsSet.map( v => v->1L ).toMap
          map |+| newViewsMap
        }
      },
      { case (map1, map2) => map1 |+| map2 }
    )

    // Descending ordering.
    // TODO: Can be optimized to O(n) taking the N top elements in one pass
    val productTop10 = productAggViews.mapValues(
      (countMap: Map[TagId, ViewsCount]) => countMap.toSeq.sortBy(-_._2).slice(0, RELATED_COUNT)
    )

    // At this point we have all the raw data: tag_id => top recommended views

    //productTop10.take(10).foreach(println)

    // Now, there is the need to join with the product names.
    // Sadly, we have to explode the grouping, add the name for each recommended product and regroup again the recommendations for each product

    val explodedValues = productTop10.flatMap { case (tag_id, viewsCount) => for (view <- viewsCount) yield (tag_id, view._1) }
    val explodedValuesDS = explodedValues.toDF("tag_id", "recommended_tag_id").as[TagSingleRecom]
    val joinedRecommendedNames = explodedValuesDS.join(tagIdProductName, explodedValuesDS("recommended_tag_id") === tagIdProductName("tag_id"))
      .drop(tagIdProductName("tag_id"))
      .withColumnRenamed("product_name", "recommended_product_name")

      //Add the key product name (not only names for recommendations)
    val explodedResult = joinedRecommendedNames.join(tagIdProductName, explodedValuesDS("tag_id") === tagIdProductName("tag_id"))
      .drop(tagIdProductName("tag_id"))

    // Regroup all
    val tupledExplodedResult = explodedResult.withColumn("recommended", TupleUdfs.toTuple2[String,String].apply(explodedResult("recommended_tag_id"),explodedResult("recommended_product_name")))
        .drop(explodedResult("recommended_tag_id"))
        .drop(explodedResult("recommended_product_name"))
    val recommendationResult = tupledExplodedResult.groupBy($"tag_id", $"product_name").agg(collect_list("recommended") as "recommendations").cache()

    // TODO: Test if the processing of the recommended_tag_ids to get the top 10 would be faster grouping it with its product_name
    //       and sending the name back and forth instead the last 2 joins. There is quite some probabilities that
    //       will be faster

    val searchProduct = recommendationResult.filter("tag_id == 'ff0d3fb21c00bc33f71187a2beec389e9eff5332'").collect()

    searchProduct.foreach(printProduct(_))

  }

  private def printProduct(resultRow: Row) = {
    val productName = resultRow.getAs[String]("product_name")
    val tagId = resultRow.getAs[String]("tag_id")
    println(s"$productName ($tagId)")
    resultRow.getAs[Seq[Row]]("recommendations").foreach( recommendation => {
      val productName = recommendation.getAs[String](1)
      val tagId = recommendation.getAs[String](0)
      println(s"    $productName ($tagId)")
    })
  }


}
