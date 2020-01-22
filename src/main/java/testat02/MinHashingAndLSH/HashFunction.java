package testat02.MinHashingAndLSH;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class HashFunction implements java.io.Serializable {

    private int a;
    private int b;

    private int primeNumber = 131071;
    private int minHashSignatureSize = 1000;

    private int randomLowerBound = 1;
    private int randomUpperBound = 1000;

    HashFunction() {
        a = randomNumber();
        b = randomNumber();
    }

    private int randomNumber() {
        return (int) ((Math.random() * (randomUpperBound - randomLowerBound)) + randomLowerBound);
    }

    int hash(int x) {
        return ((((a * x) + b) % primeNumber) % minHashSignatureSize);
    }

}
