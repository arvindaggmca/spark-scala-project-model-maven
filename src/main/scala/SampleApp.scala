import org.apache.spark.sql.SparkSession

object SampleApp {

  def main(args:Array[String]): Unit ={
    import spark.implicits._

    val spark = SparkSession.builder()
      .master("spark://spark:7077")
      .appName("spark-sample-app")
      .getOrCreate();
    
    println("SparkContext:")
    println("APP Name :"+spark.sparkContext.appName);
    println("Deploy Mode :"+spark.sparkContext.deployMode);
    println("Master :"+spark.sparkContext.master);
    val rdd=spark.read.textFile("hello.txt")
    println("word count of hello.txt")

    val rdd1 =rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).collect()
    rdd1.foreach(x=> {
      println(x._1)
      println(x._2)
    }
    )

  }
}
