package sparklabs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exemplo02 {
	
	// referencia
	// https://www.devmedia.com.br/introducao-ao-apache-spark/34178
	
	static String FILE = "/home/daniel/eclipse-workspace/files/api-olhovivo/";
	static String ARQUIVO_ROTAS = "routes.txt" ;
	
	public static void main(String[] args) {
		
		// SparkConf object that contains information about your application
		// appName parameter is a name for your application to show on the cluster UI. 
		// master is a Spark, Mesos or YARN cluster URL, or a special �local� string to run in local mode
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");

        // JavaSparkContext object, which tells Spark how to access a cluster
        JavaSparkContext ctx = new JavaSparkContext(conf);
         
        // carrega os dados dos �nibus de sp
		// External Datasets
        // Spark can create distributed datasets from any storage source supported by Hadoop, 
		// including your local file system, HDFS, Cassandra, HBase, Amazon S3, etc. 
		// Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
        JavaRDD<String> linhas = ctx.textFile(FILE + ARQUIVO_ROTAS);
        
        // Transformations
        // filter(func)	Return a new dataset formed by selecting those elements of the source on which func returns true.
        //s -> s.contains("Jova Rural")  Lambda Expression ... https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html
        JavaRDD<String> linhasFiltradas = linhas.filter(s -> s.contains("Jova Rural"));
         
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
        List<String> resultados = linhasFiltradas.collect();
        for (String linha : resultados) {
              System.out.println(linha);
        }
        
        // escreve o n�mero de �nibus que existem no arquivo
        System.out.print("NUMERO DE LINHAS: ");
        
        // RDDs support two types of operations: transformations, which create a new dataset from an existing one, 
        // and actions, which return a value to the driver program after running a computation on the dataset
        // here we have an action...
        System.out.println(linhasFiltradas.count());
         
        ctx.close();
	}
}
