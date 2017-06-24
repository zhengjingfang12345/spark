
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object TestDEMO {
  def main(args: Array[String]) {
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow,My First Spark Programe") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local") //此时，程序在本地运行，不需要安装Spark集群   
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    val c = sc.parallelize(1 to 15, 3)
    println(c.reduce((x, y) => x + y));

    var dataRow1 = new DataRow("id_card_123", "18681440729", "1@163.com", "abc-123-efc", "zhengjingfang12345")
    var dataRow2 = new DataRow("", "18681440729", "1@163.com", "abc-123-efc", "")

    var dataRow3 = new DataRow("id_card_124", "18681440729", "1@163.com", "abc-123-efc", "zhengjingfang12345")

    var dataRow4 = new DataRow("", "18681440729", "", "abc-123-efc", "zhengjingfang12345")
    var dataRow5 = new DataRow("", "18681440729", "1@163.com", "abc-123-efc", "zhengjingfang12345")

    var dataRow6 = new DataRow("", "18681440730", "1@164.com", "abc-123-efc", "zhengjingfang12345")
    var dataRow7 = new DataRow("id_card_888", "18681440733", "1@164.com", "abc-123-efc", "zhengjingran12345")
    var dataRow8 = new DataRow("", "18681440799", "1@164.com", "abc-123-fff", "zhengjingran12345")
    var dataRowList = List(dataRow1, dataRow2, dataRow3, dataRow4, dataRow5, dataRow6, dataRow7, dataRow8);

    Thread.sleep(1000 * 50);
    sc.stop()
    /* val logFile = "test.txt" // Should be some file on your system  
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))*/
  }

}