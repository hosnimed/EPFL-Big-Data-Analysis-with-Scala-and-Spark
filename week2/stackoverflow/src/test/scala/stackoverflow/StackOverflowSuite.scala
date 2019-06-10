package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll with Matchers{
  //<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
  val input =
    """
      |1,6,,,140,CSS
      |2,7,7,6,140,CSS
      |1,42,,,155,PHP
      |2,43,43,42,155,PHP
      |1,72,,,16,Ruby
      |2,73,73,72,16,Ruby
      |1,126,,,33,Java
      |2,127,127,126,33,Java
      |1,174,,,38,C#
      |2,175,175,176,38,C#
    """.stripMargin.trim
  val inputTruncated =
    """
      |1,6,,,140,CSS
      |2,7,7,6,140,CSS
      |2,8,7,6,200,CSS
      |1,42,,,155,PHP
      |2,43,43,42,155,PHP
      |2,44,43,42,300,PHP
    """.stripMargin.trim

  val posting: Array[Posting] =
    Array(
      Posting(1,6,None,None,140,Some("CSS")),
      Posting(2,7,Some(7),Some(6),140,Some("CSS")),
      Posting(2,8,Some(7),Some(6),200,Some("CSS")),
      Posting(1,42,None,None,155,Some("PHP")),
      Posting(2,43,Some(43),Some(42),155,Some("PHP")),
      Posting(2,44,Some(43),Some(42),300,Some("PHP"))
     )
  val postingGrouped: Array[(QID, Iterable[(Question, Answer)])] =
    Array (
    (6, Iterable(
      (Posting(1,6,None,None,140,Some("CSS")), Posting(2,7,Some(7),Some(6),140,Some("CSS"))),
      (Posting(1,6,None,None,140,Some("CSS")), Posting(2,8,Some(7),Some(6),200,Some("CSS")))
    )),
    (42,Iterable(
      (Posting(1,42,None,None,155,Some("PHP")), Posting(2,43,Some(43),Some(42),155,Some("PHP"))),
      (Posting(1,42,None,None,155,Some("PHP")), Posting(2,44,Some(43),Some(42),300,Some("PHP")))
    ))
    )
  val postingScored : Array[(Question, HighScore)] =
    Array(
      (Posting(1,6,None,None,140,Some("CSS")), 200),
      (Posting(1,42,None,None,155,Some("PHP")), 300)
    )

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }
  def convertToVectors(s: String): (LangIndex, HighScore) = {
    val arrInt = s.split(",").map(_.toInt)
    (arrInt(0), arrInt(1))
  }

  test("scored, should calculate scored RDD from input csv") {
    val lines: RDD[String] = StackOverflow.sc.parallelize(inputTruncated.split("\\s+"))
    val raw: RDD[Posting] = testObject.rawPostings(lines).cache()
//     println("RawPosting RDD : " + raw.collect().mkString(" \n"))
//    assertResult(posting.toIterable)(raw.collect().toIterable)
    posting should contain theSameElementsAs raw.collect()

    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = testObject.groupedPostings(raw)
//      println("Grouped RDD : " + grouped.collect().mkString("|"))
    val actual = grouped.collect()
      .map { pair => (pair._1, pair._2.toList)}
//    println("Grouped RDD Actual: "+actual.mkString("|"))
//    println("Grouped RDD Expected: "+postingGrouped.mkString("|"))
//    assertResult(postingGrouped.toIterable)(actual.toIterable)
    postingGrouped should contain theSameElementsAs actual

    val scored: RDD[(Question, HighScore)] = testObject.scoredPostings(grouped)
//     println("Scored RDD : " + scored.collect().mkString("|"))
//    assertResult(postingScored.sortBy(_._1.id))(scored.collect().sortBy(_._1.id))
    postingScored should contain theSameElementsAs scored.collect()
  }

  test("vectors, should calculate vectors") {
    val scores: Array[(Question, HighScore)] =
      Array(
        (Posting(1, 6, None, None, 140, Some("CSS")), 67),
        (Posting(1, 42, None, None, 155, Some("PHP")), 89),
        (Posting(1, 72, None, None, 16, Some("Ruby")), 3),
        (Posting(1, 126, None, None, 33, Some("Java")), 30),
        (Posting(1, 174, None, None, 38, Some("C#")), 20))

    val scored = StackOverflow.sc.parallelize(scores)
    val vectors: RDD[(LangIndex, HighScore)] = testObject.vectorPostings(scored)

    val expected =
      """
        |350000,67
        |100000,89
        |300000,3
        |50000,30
        |200000,20
      """.stripMargin.trim
    val expectedRdd: Array[(LangIndex, HighScore)] = expected.split("\\s+").map(s => convertToVectors(s))

    assertResult(expectedRdd)(vectors.collect())
  }
}
