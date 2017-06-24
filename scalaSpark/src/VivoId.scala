
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object VivoId {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Wow,My First Spark Programe")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    ///////////////////////////////////////
    //读取源文件
    var orgData = sc.textFile("c://vivoData.txt")
    //按空格进行拆分，一行记录格式如1,name,phone,emial,imei
    var recorders = orgData.map(_.split(","))
    var matchData = recorders.flatMap(recorder => {
      //第一个元素为记录id
      var recordId: String = recorder(0)
      //用于保存所有匹配规则的匹配结果数据
      var result = new ArrayBuffer[(String, String)]()

      //2个规则
      var rules = List(Array(2, 5), Array(2, 3, 4))
      //处理每个匹配规则
      rules.foreach(rule => {
        var ruleAttr: String = dealOneRule(rule, recorder)
        if (!ruleAttr.isEmpty()) {
          result += ((ruleAttr, recordId))
        }
      })
      if (result.isEmpty) {
        result += ((recordId, recordId))
      }
      result
    })
    //////////////////////////////////////////////////////////
    matchData.collect().foreach(data => { println("matchData:" + data) })
    ///////////////////////////////////////////////////////
    var sameObj = matchData.groupByKey().map(x => x._2.toSet)
    ////////////////////////////////////////////////////
    println("println sameObje")
    sameObj.collect().foreach(obj => { println(obj) })
    ////////////////////////////////////////////////////
    //记录当前对象的数量
    var num2 = sameObj.count();
    var num1 = num2 + 1;
    println(s"start    .. num1=$num1 ,num2=$num2")

    var loopTime: Int = 0;
    //迭代条件，当对象数没有更新时终止
    while (num1 > num2) {
      loopTime += 1;
      //记录广播，告之每个记录所属的对象
      val recBoreacast = sameObj.flatMap(obj => obj.map(o => (o, obj)))
      println("println recBoreacast")
      recBoreacast.collect().foreach(obj => { println(obj) })
      //聚集，收集每个记录极其所属的对象，并合并
      val groups = recBoreacast.groupByKey().flatMap(x => {
        val obs = x._2
        var res = List[(Set[String], Int)]()
        if (obs.size == 1) {
          //如果所属的对象为1，说明没有传递，原对象保留，增加状态值为0
          res ::= (obs.toList(0), 0)

        } else {
          //否则，将所有对象内的记录合并、去重后得到新的对象，并将原对象删除，状态值设为1
          val newObjs = obs.reduce(_ ++ _)
          obs.foreach(x => res ::= (x, 1))
          res ::= (newObjs, 2)
        }
        println("key:" + x._1 + "----value:" + res)
        res
      })
      println("print group")
      groups.collect().foreach(group => println("groupItem:" + group))
      /**
       * 合并每个对象的所有状态并处理
       */
      sameObj = groups.groupByKey().map(x => (x._1, newState(x._2)))
        .filter(_._2 != 0).map(_._1)

      sameObj.collect().foreach(x => println("obj=" + x))
      num1 = num2
      num2 = sameObj.count()
      println("loopTime:" + loopTime + "---num1:" + num1 + "---num2:" + num2)
    }
    sameObj.collect().foreach(x => println("finaly obj=" + x))

  }
  /**
   * 合并对象的各个状态
   */
  def newState(stat: Iterable[Int]): Int = {
    if (stat.exists(_ == 2)) {
      return 1
    } else if (stat.exists(_ == 1)) {
      return 0
    }

    1
  }
  /**
   * 判断记录record是否匹配规则rule,成功返回匹配值，否则返回空值
   */
  def dealOneRule(rule: Array[Int], recorder: Array[String]): String = {
    var attrs = recorder.toArray;
    var ruleAttr = ""
    rule.foreach(r => {
      if (r >= attrs.length || attrs(r).isEmpty()) { return "" }
      ruleAttr += attrs(r) + ","
    })
    if (ruleAttr.length() > 0) {
      ruleAttr = ruleAttr.substring(0, ruleAttr.length() - 1);
    }
    ruleAttr
  }

}