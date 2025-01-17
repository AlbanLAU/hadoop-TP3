package org.epf.hadoop.colfil3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ColFilJob3 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // Vérification des arguments de la ligne de commande
        if (args.length != 2) {
            System.err.println("Usage: ColFilJob3 <input path> <output path>");
            return -1; // Retourne un code d'erreur si les arguments sont incorrects
        }

        String inputPath = args[0]; // Chemin d'entrée des données
        String outputPath = args[1]; // Chemin de sortie des résultats

        // Configuration du job MapReduce
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Job3: Friend Recommendations");
        job.setJarByClass(getClass()); // Définit la classe principale du job

        // Définir le format d'entrée
        job.setInputFormatClass(TextInputFormat.class); // Format d'entrée : texte

        // Définir les classes de mapping et de réduction
        job.setMapperClass(RecommendationMapper.class); // Classe Mapper
        job.setReducerClass(RecommendationReducer.class); // Classe Reducer

        // Définir les types de sortie du Mapper
        job.setMapOutputKeyClass(Text.class); // Clé de sortie du Mapper : Text
        job.setMapOutputValueClass(UserRecommendation.class); // Valeur de sortie du Mapper : UserRecommendation

        // Définir les types de sortie du Reducer
        job.setOutputKeyClass(Text.class); // Clé de sortie du Reducer : Text
        job.setOutputValueClass(Text.class); // Valeur de sortie du Reducer : Text

        // Un seul reducer comme demandé dans l'énoncé
        job.setNumReduceTasks(1);

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
        int exitCode = ToolRunner.run(new Configuration(), new ColFilJob3(), args); // Exécute le job
        System.exit(exitCode); // Termine le programme avec le code de sortie approprié
    }
}