package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;

public class ColFilJob2 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Vérification des arguments de la ligne de commande
        if (args.length != 2) {
            System.err.println("Usage: ColFilJob2 <input path> <output path>");
            System.err.println("Arguments reçus : " + args.length);
            for (int i = 0; i < args.length; i++) {
                System.err.println("Arg[" + i + "] : " + args[i]);
            }
            return -1; // Retourne un code d'erreur si les arguments sont incorrects
        }

        String inputPath = args[0]; // Chemin d'entrée des données
        String outputPath = args[1]; // Chemin de sortie des résultats

        System.out.println("Using input path: " + inputPath);
        System.out.println("Using output path: " + outputPath);

        // Configuration du job MapReduce
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Job2: Common Friends Counter");
        job.setJarByClass(getClass()); // Définit la classe principale du job

        // Définir les formats d'entrée et de sortie
        job.setInputFormatClass(TextInputFormat.class); // Format d'entrée : texte

        // Définir les classes de mapping et de réduction
        job.setMapperClass(CommonRelationshipMapper.class); // Classe Mapper
        job.setReducerClass(CommonRelationshipReducer.class); // Classe Reducer

        // Définir les types de sortie du Mapper
        job.setMapOutputKeyClass(UserPair.class); // Clé de sortie du Mapper : UserPair
        job.setMapOutputValueClass(IntWritable.class); // Valeur de sortie du Mapper : IntWritable

        // Définir les types de sortie du Reducer
        job.setOutputKeyClass(Text.class); // Clé de sortie du Reducer : Text
        job.setOutputValueClass(IntWritable.class); // Valeur de sortie du Reducer : IntWritable

        // Définir le nombre de reducers
        job.setNumReduceTasks(2); // Utiliser 2 reducers pour paralléliser le traitement

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(inputPath)); // Chemin d'entrée
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); // Chemin de sortie

        // Lancer le job et attendre sa complétion
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1; // Retourne 0 si le job réussit, 1 sinon
    }

    /**
     * Point d'entrée du programme.
     * @param args Arguments de la ligne de commande : <input path> <output path>
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Démarrage de ColFilJob2");
        int exitCode = ToolRunner.run(new Configuration(), new ColFilJob2(), args); // Exécute le job
        System.exit(exitCode); // Termine le programme avec le code de sortie approprié
    }
}