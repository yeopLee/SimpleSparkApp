import org.apache.spark._

object SimpleApp {
    // driver
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("SimpleApp")
        val sc = new SparkContext(conf)

        val input = sc.textFile("input.txt")

        // tasks ..
        val tokenized = input.filter(line => line.size > 0)
            .map(line => line.split(" "))
        val counts = tokenized.map(words => (words(0),1)).reduceByKey((a, b) => a + b)

        // action
        counts.collect()

    }
}
