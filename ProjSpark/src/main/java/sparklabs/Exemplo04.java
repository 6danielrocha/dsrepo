package sparklabs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exemplo04 {
	// referencia
	// https://www.devmedia.com.br/introducao-ao-apache-spark/34178
	
	static String FILE = "/home/daniel/eclipse-workspace/files/api-olhovivo/";
	static String ARQUIVO_ROTAS = "routes.txt" ;
	static String ARQUIVO_TEMP = "temp.txt";
	
	public static void main(String[] args) {

		// SparkConf object that contains information about your application
		// appName parameter is a name for your application to show on the cluster UI. 
		// master is a Spark, Mesos or YARN cluster URL, or a special �local� string to run in local mode
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
        
        // JavaSparkContext object, which tells Spark how to access a cluster
        JavaSparkContext ctx = new JavaSparkContext(conf);

		// External Datasets
        // Spark can create distributed datasets from any storage source supported by Hadoop, 
		// including your local file system, HDFS, Cassandra, HBase, Amazon S3, etc. 
		// Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
        JavaRDD<String> linhasSabado = ctx.textFile(FILE + ARQUIVO_ROTAS);
        JavaRDD<String> linhasDomingo = ctx.textFile(FILE + ARQUIVO_ROTAS);

        /*
         * RDDs support two types of operations: transformations, which create a new dataset from an existing one, 
         * and actions, which return a value to the driver program after running a computation on the dataset. 
         * For example, map is a transformation that passes each dataset 
         * element through a function and returns a new RDD representing the results
         * 
         * All transformations in Spark are lazy, in that they do not compute their results right away. 
         * Instead, they just remember the transformations applied to some base dataset (e.g. a file). 
         * The transformations are only computed when an action requires a result to be returned to the driver program.
         * 
         * filter(func)	Return a new dataset formed by selecting those elements of the source on which func returns true
         */
        JavaRDD<String> linhasFiltradasSabado = linhasSabado.filter(s -> s.contains("Jova Rural"));
        JavaRDD<String> linhasFiltradasDomingo = linhasDomingo.filter(s -> s.contains("Pq. Edu Chaves"));
         
        // Union(otherDataset)	Return a new dataset that contains the union of the elements in the source dataset and the argument.
        JavaRDD<String> linhasUniao = linhasFiltradasSabado.union(linhasFiltradasDomingo);
         
        /*
         * Another common idiom is attempting to print out the elements of an RDD using rdd.foreach(println) or rdd.map(println). 
         * On a single machine, this will generate the expected output and print all the RDD�s elements. 
         * However, in cluster mode, the output to stdout being called by the executors is now writing to the executor�s stdout instead, 
         * not the one on the driver, so stdout on the driver won�t show these! 
         * To print all elements on the driver, one can use the collect() method to first bring the RDD to the driver node 
         * thus: rdd.collect().foreach(println). This can cause the driver to run out of memory, 
         * though, because collect() fetches the entire RDD to a single machine; 
         * if you only need to print a few elements of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).
         * by the way... collect is an action
         */
        List<String> resultados = linhasUniao.collect();
        for (String linha : resultados) {
              System.out.println(linha);
        }
        
        /*
         * saveAsTextFile(path)	Write the elements of the dataset as a text file (or set of text files) 
         * in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. 
         * Spark will call toString on each element to convert it to a line of text in the file.
         * 
         * Task: what is _SUCCESS part-00000 and part-00001
         */
        linhasUniao.saveAsTextFile(FILE + ARQUIVO_TEMP);
        
        ctx.close();
    }
	
}
