package testat02.MinHashingAndLSH;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Int;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class HashFunction implements java.io.Serializable {

    private int a;
    private int b;
    private int primeNumber = 131071;
    private int minHashSignatureSize = LSH.numberHashFunctions;
    private int randomLowerBound = 1;
    private int randomUpperBound = primeNumber - 1;

    HashFunction() {
        a = randomNumber();
        b = randomNumber();
    }

    private int randomNumber() {
        return (int) ((Math.random() * (randomUpperBound - randomLowerBound)) + randomLowerBound);
    }

    int hash(int x) {
        return Math.floorMod(Math.floorMod(((a * x) + b), primeNumber), minHashSignatureSize);
    }

}
