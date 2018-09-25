package com.nishilua.test2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import scalaz._
import Scalaz._

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
    val tagIdProductName = viewsDS.select($"tag_id", $"product_name").distinct()

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

    // Now, there is the need to join with the produt names

    productTop10.toDebugString
    productTop10.take(10).foreach(println)

  }

}
