
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object ReduceByKey {
  def main(args: Array[String]) {

    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow,My First Spark Programe") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local") //此时，程序在本地运行，不需要安装Spark集群   
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    val a = sc.parallelize(List((1, "phone:18681440729"), (2, "email:163@163.com"), (3, "email:163@163.com"), (3, "imei:abc-123-ecf")))
    var collect = a.reduceByKey((x, y) => x + "_" + y).collect
    println(collect)
    Thread.sleep(1000 * 50);
    sc.stop() //记得关闭创建的SparkContext对象
  }
}