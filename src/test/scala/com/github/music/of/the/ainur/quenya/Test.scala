package com.github.music.of.the.ainur.quenya

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class Test extends FunSuite with BeforeAndAfter {
  val spark:SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val data = Seq(
"""{"name": {"nameOne": "Mithrandir","LastName": "Olórin","nickNames": ["Gandalf the Grey","Gandalf the White"]}, "race": "Maiar","age": 3500,"weapon": ["Glamdring", "Narya", "Wizard Staff"]}""",
"""{"name": {"nameOne": "Ilmarë","LastName": null, "nickNames": null}, "race": "Ainur","age": 4500,"weapon": ["Powers of the Ainur"]}""",
"""{"name": {"nameOne": "Morgoth","LastName": null, "nickNames": ["Bauglir","Belegurth","Belegûr","The Great Enemy","The Black Foe"]}, "race": "Ainur","age": 3500,"weapon": ["Powers of the Ainur","Grond","Mace","Sword"]}""",
"""{"name": {"nameOne": "Manwë","LastName": null, "nickNames": ["King of Arda,","Lord of the Breath of Arda","King of the Valar"]}, "race": "Ainur","age": 3500,"weapon": ["Powers of the Ainur"]}""")

  import spark.implicits._

  val df = spark.read.json(spark.sparkContext.parallelize(data).toDS())

  val quenyaDsl = QuenyaDSL

  val dsl = quenyaDsl.compile("""
 |age$age:LongType
 |name.LastName$LastName:StringType
 |name.nameOne$nameOne:StringType
 |name.nickNames[0]$nickNames:StringType
 |race$race:StringType
 |weapon@weapon
 |  weapon$weapon:StringType""".stripMargin)

  val dslDf = quenyaDsl.execute(dsl,df)
  val csvDf = spark.read.option("header","true").csv("src/test/resources/data.csv")
 
  val dslCount = dslDf.count()
  val csvCount = csvDf.count()

  test("number of records should match") {
    assert(dslCount == csvCount)
  }

  val diff=compare(csvDf,dslDf)

  test("Data should match")
  {
    assert(diff==0)
  }
  
  private def compare(df1:DataFrame,df2:DataFrame): Long =
    df1.as("df1").join(df2.as("df2"),joinExpression(df1),"leftanti").count()

  private def joinExpression(df1:DataFrame): Column =

    df1.schema.fields

      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}") )

      .reduce((col1,col2) => col1.and(col2))

  after {
    spark.stop()
  }

}
