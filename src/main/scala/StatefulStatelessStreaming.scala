import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulStatelessStreaming {
  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9998

    val conf = new SparkConf()
      .setAppName("Stateful vs Stateless Bank Transactions")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Required for updateStateByKey to retain state across batches
    ssc.checkpoint("output/checkpoint")

    // Input format: ACCOUNT_ID,TRANSACTION_ID
    val lines = ssc.socketTextStream(host, port)

    // Stateless: parse valid transaction lines
    val transactions = lines
      .map(_.trim)
      .filter(line => line.nonEmpty && line.contains(","))
      .map { line =>
        val parts = line.split(",", -1).map(_.trim)
        (parts(0), parts(1))
      }
      .filter { case (account, transactionId) =>
        account.nonEmpty && transactionId.nonEmpty
      }

    // Stateless: count transactions per account in the current batch
    val currentBatchCounts = transactions
      .map { case (account, _) => (account, 1) }
      .reduceByKey(_ + _)

    currentBatchCounts.foreachRDD { rdd =>
      println("\n=== STATELESS: Current Batch Counts ===")
      if (rdd.isEmpty()) {
        println("No valid transactions in this batch.")
      } else {
        rdd.collect().sortBy(_._1).foreach {
          case (account, count) =>
            println(s"Account=$account, currentBatchCount=$count")
        }
      }
    }

    // Stateful: accumulate counts across all batches
    val accountCounts = transactions
      .map { case (account, _) => (account, 1) }
      .updateStateByKey[Int] { (newCounts: Seq[Int], previousState: Option[Int]) =>
        Some(previousState.getOrElse(0) + newCounts.sum)
      }

    accountCounts.foreachRDD { rdd =>
      println("\n=== STATEFUL: Running Account Counts ===")
      if (rdd.isEmpty()) {
        println("No account state available yet.")
      } else {
        rdd.collect().sortBy(_._1).foreach {
          case (account, total) =>
            println(s"Account=$account, runningTotal=$total")
        }
      }
    }

    println(s"Listening on $host:$port")
    println("Batch interval: 5 seconds")
    println("Input format: ACCOUNT_ID,TRANSACTION_ID")

    ssc.start()
    ssc.awaitTermination()
  }
}
