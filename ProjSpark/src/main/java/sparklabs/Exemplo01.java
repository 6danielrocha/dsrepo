package sparklabs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exemplo01 {
	
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
         
		// External Datasets
        // Spark can create distributed datasets from any storage source supported by Hadoop, 
		// including your local file system, HDFS, Cassandra, HBase, Amazon S3, etc. 
		// Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
        JavaRDD<String> linhas = ctx.textFile(FILE + ARQUIVO_ROTAS);
        
        // RDDs support two types of operations: transformations, which create a new dataset from an existing one, 
        // and actions, which return a value to the driver program after running a computation on the dataset
        // here we have an action...
        long numeroLinhas = linhas.count();
         
        // escreve o numero de onibus que existem no arquivo
        System.out.print("NUMERO DE LINHAS: ");
        System.out.println(numeroLinhas);
         
        ctx.close();
	}
}
