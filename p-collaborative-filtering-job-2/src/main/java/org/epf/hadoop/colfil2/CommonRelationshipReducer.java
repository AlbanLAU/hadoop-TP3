package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CommonRelationshipReducer
        extends Reducer<UserPair, IntWritable, Text, IntWritable> {

    private Text outputKey = new Text(); // Clé de sortie : paire d'utilisateurs
    private IntWritable result = new IntWritable(); // Valeur de sortie : nombre d'amis communs

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0; // Compteur pour le nombre d'amis communs
        boolean areDirectFriends = false; // Indicateur pour vérifier si les utilisateurs sont déjà amis

        // Parcourir toutes les valeurs associées à la paire d'utilisateurs
        for (IntWritable val : values) {
            if (val.get() == -1) {
                // Si une valeur -1 est trouvée, cela signifie que les utilisateurs sont déjà amis
                areDirectFriends = true;
                break; // Pas besoin de continuer, on ignore cette paire
            }
            sum += val.get(); // Ajouter la valeur au compteur d'amis communs
        }

        // Émettre la paire d'utilisateurs et le nombre d'amis communs seulement si :
        // 1. Ils ne sont pas déjà amis directs (areDirectFriends == false)
        // 2. Ils ont au moins un ami commun (sum > 0)
        if (!areDirectFriends && sum > 0) {
            outputKey.set(key.toString()); // Convertir la paire d'utilisateurs en chaîne de caractères
            result.set(sum); // Définir le nombre d'amis communs
            context.write(outputKey, result); // Émettre le résultat
        }
    }
}