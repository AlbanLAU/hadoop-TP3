package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonRelationshipMapper
        extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    // Constantes pour les valeurs émises
    private final IntWritable ONE = new IntWritable(1); // Marqueur pour un ami commun
    private final IntWritable MINUS_ONE = new IntWritable(-1); // Marqueur pour une relation existante

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Diviser la ligne d'entrée en deux parties : utilisateur et liste de relations
        String[] parts = value.toString().split("\\s+");
        if (parts.length != 2) {
            return; // Ignorer les lignes mal formatées
        }

        String user = parts[0]; // L'utilisateur courant
        List<String> relations = Arrays.asList(parts[1].split(",")); // Liste de ses relations

        // Étape 1 : Émettre des marqueurs négatifs pour les relations existantes
        // Cela permet d'éviter de compter les relations directes comme des amis communs
        for (String relation : relations) {
            // Émettre une paire (utilisateur, relation) avec la valeur -1
            context.write(new UserPair(user, relation), MINUS_ONE);
        }

        // Étape 2 : Émettre des marqueurs positifs pour les paires de relations
        // Cela permet de compter les amis communs entre chaque paire de relations
        for (int i = 0; i < relations.size(); i++) {
            String rel1 = relations.get(i); // Première relation
            for (int j = i + 1; j < relations.size(); j++) {
                String rel2 = relations.get(j); // Deuxième relation
                // Émettre une paire (rel1, rel2) avec la valeur 1
                context.write(new UserPair(rel1, rel2), ONE);
            }
        }
    }
}