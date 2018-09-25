# 21B-test2
Second technical tests for 21 Buttons


## Explanatory data diagram

First approach:

![Explanatory data diagram](https://raw.githubusercontent.com/alfonsonishikawa/21B-test2/master/data_diagram.png)

## Points of improvement

As commented in the sourcecode:

- [ ] The sorting and slicing the top 10 results (`productTop10`) can be done in `O(n)` just traversing once the collection and maintaining the set of top 10 elements (and even more optimized optimizing with the number of elements an min value to compare).
- [ ] As shown in the "First approach diagram", the recommended tags are computed without the product_names and joined at the end. Maybe it will be more efficient to make the aggregations with the product names and avoid the last joins.

## Interesting readings found

* https://legacy.gitbook.com/book/umbertogriffo/apache-spark-best-practices-and-tuning
* https://legacy.gitbook.com/book/databricks/databricks-spark-knowledge-base
