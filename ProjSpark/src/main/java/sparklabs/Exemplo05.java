package sparklabs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * O Apache Spark eh uma ferramenta Big Data que tem o objetivo de processar grandes conjuntos de dados de forma paralela e distribuida. 
 * Ela estende o modelo de programacao MapReduce popularizado pelo Apache Hadoop, 
 * facilitando bastante o desenvolvimento de aplicacoes de processamento de grandes volumes de dados.
 * 
 * A arquitetura de uma aplicacao Spark eh constituida por tres partes principais:
 * 1. O Driver Program, que eh a aplicacao principal que gerencia a criacao e eh quem executarah o processamento definido pelo programados;
 * 2. O Cluster Manager eh um componente opcional que so eh necessahrio se o Spark for executado de forma distribuida. 
 * Ele eh responsahvel por administrar as mahquinas que serao utilizadas como workers;
 * 3. Os Workers, que sao as mahquinas que realmente executarao as tarefas que sao enviadas pelo Driver Program. 
 * Se o Spark for executado de forma local, a mahquina desempenharah tanto o papel de Driver Program como de Worker.
 */
public class Exemplo05 {
	// referencia
	// https://www.devmedia.com.br/introducao-ao-apache-spark/34178
	
	static String FILE = "/home/daniel/eclipse-workspace/files/api-olhovivo/";
	static String ARQUIVO_ROTAS = "trips.txt";
	
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
		JavaRDD<String> onibus = ctx.textFile(FILE + ARQUIVO_ROTAS);

		/*
		 * Por ultimo, a operacao mais conhecida desse tipo de ferramenta que eh o map reduce, como mostra a Listagem 7.
		 * Inicialmente os dados do arquivo sao carregados em um RDD e em seguida, 
		 * utilizando o mehtodo mapToPair, as Strings do arquivo sao mapeados para o nome da linha do onibus. 
		 * Por isso o s.split(" ")[2], que divide a String em tokens separados por um espaco em branco. 
		 * O terceiro dado dos tokens eh o nome da linha do onibus e o numero 1, que indica que eh um registro de uma linha. 
		 * Depois eh executado o mehtodo reduceByKey, que agrupa todos os resultados que tem a mesma chave, isso eh, 
		 * a mesma linha de onibus, e soma os valores das linhas (que sao todos 1). 
		 * Por fim, todos os registros iguais serao agrupados e o somatorio de quantos onibus da mesma linha existem eh feito.
		 * 
		 * No meu caso, no arquivo routes.txt, o separador eh a virgula, e comecando a contar as colunas do zero
		 * temos Coluna 3 - nome da linha
		 */
		
		// faz o map com as linhas de onibus
		// JavaPairRDD<String, Integer> agrupaOnibus = onibus.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[2], 1));
		
		/*
		 * While most Spark operations work on RDDs containing any type of objects, 
		 * a few special operations are only available on RDDs of key-value pairs. 
		 * The most common ones are distributed �shuffle� operations, such as grouping or aggregating the elements by a key.
		 * In Java, key-value pairs are represented using the scala.Tuple2 class from the Scala standard library. 
		 * You can simply call new Tuple2(a, b) to create a tuple, and access its fields later with tuple._1() and tuple._2().
		 * RDDs of key-value pairs are represented by the JavaPairRDD class. 
		 * You can construct JavaPairRDDs from JavaRDDs using special versions of the map operations, 
		 * like mapToPair and flatMapToPair. The JavaPairRDD will have both standard RDD functions and special key-value ones.
		 * For example, the following code uses the reduceByKey operation on key-value pairs 
		 * to count how many times each line of text occurs in a file:
		 */
		JavaPairRDD<String, Integer> agrupaOnibus = onibus.mapToPair(s -> new Tuple2<String, Integer>(s.split(",")[3], 1));
		JavaPairRDD<String, Integer> numeroOnibus = agrupaOnibus.reduceByKey((x, y) -> x + y);
		
        /*
         * Another common idiom is attempting to print out the elements of an RDD using rdd.foreach(println) or rdd.map(println). 
         * On a single machine, this will generate the expected output and print all the RDD�s elements. 
         * However, in cluster mode, the output to stdout being called by the executors is now writing to the executor's stdout instead, 
         * not the one on the driver, so stdout on the driver won't show these! 
         * To print all elements on the driver, one can use the collect() method to first bring the RDD to the driver node 
         * thus: rdd.collect().foreach(println). This can cause the driver to run out of memory, 
         * though, because collect() fetches the entire RDD to a single machine; 
         * if you only need to print a few elements of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).
         * by the way... collect is an action
         */
		
		List<Tuple2<String, Integer>> lista = numeroOnibus.collect();
	
		//You can simply call new Tuple2(a, b) to create a tuple, and access its fields later with tuple._1() and tuple._2().
        for (Tuple2<String, Integer> onibusNumero : lista) {
        	System.out.println("Tupla: " + onibusNumero._1() + " " + onibusNumero._2());
        }
        
        ctx.close();

    }
}
