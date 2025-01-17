package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPair implements WritableComparable<UserPair> {
    private Text user1;
    private Text user2;

    public UserPair() {
        this.user1 = new Text("");
        this.user2 = new Text("");
    }

    public UserPair(String user1, String user2) {
        if(user1.compareTo(user2) <= 0) {
            this.user1 = new Text(user1);
            this.user2 = new Text(user2);
        } else {
            this.user1 = new Text(user2);
            this.user2 = new Text(user1);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        user1.write(out);
        user2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user1.readFields(in);
        user2.readFields(in);
    }

    @Override
    public int compareTo(UserPair o) {
        int cmp = user1.compareTo(o.user1);
        if (cmp != 0) {
            return cmp;
        }
        return user2.compareTo(o.user2);
    }

    @Override
    public String toString() {
        return user1 + "," + user2;
    }

    public String getFirstUser() {
        return user1.toString(); // Retourne une représentation sous forme de chaîne de caractères de l'utilisateur 1.
    }

    public String getSecondUser() {
        return user2.toString(); // Retourne une représentation sous forme de chaîne de caractères de l'utilisateur 2.
    }

    @Override
    public boolean equals(Object o) {
        // Vérifie si l'objet courant (this) est égal à l'objet passé en paramètre (o).
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false; // Si l'objet est null ou n'est pas de la même classe, ils ne sont pas égaux.

        // Cast l'objet `o` en `UserPair` pour pouvoir comparer ses attributs.
        UserPair userPair = (UserPair) o;

        // Retourne `true` si les deux utilisateurs sont égaux, sinon `false`.
        return user1.equals(userPair.user1) && user2.equals(userPair.user2);
    }

    @Override
    public int hashCode() {
        // Calcule et retourne un code de hachage pour l'objet `UserPair`.

        int result = user1.hashCode(); // Calcule le code de hachage de l'utilisateur 1.
        result = 31 * result + user2.hashCode(); // Combine le code de hachage de l'utilisateur 2 avec celui de l'utilisateur 1.
        return result;
    }
}