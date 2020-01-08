## Testat 3

### 7. Empfehlungsalgorithmus für soziale Netzwerke (10 Punkte)

Schreiben Sie ein Programm, welches einen einfachen Empfehlungsalgorithmus für soziale Netzwerke ("People You Might Know") implementiert. Dabei soll folgende Hypothese umgesetzt werden: Zwei Personen kennen sich mit hoher Wahrscheinlichkeit, wenn sie viele gemeinsame Freunde haben.

Verwenden Sie für diese Aufgabe die Datei `soc-LiveJournal1Adj.txt` aus den Datenordner. Die Zeilen der Datei haben das Format `<Benutzer> TAB <Freunde>`. Dabei ist `<Benutzer>` eine ID, die einem eindeutigen Benutzer entspricht, und `<Freunde>` ist eine kommagetrennte Liste der IDs, welche den Freunden des Benutzers entsprechen. Es ist zu beachten, dass die Freundschaften gegenseitig sind: Wenn A mit B befreundet ist, ist auch B mit A befreundet. Die bereitgestellten Daten stimmen mit dieser Regel überein.

Implementieren Sie den folgenden **Algorithmus**: Für jeden Benutzer U sollen 10 andere Benutzer empfohlen werden, welche die meisten gemeinsamen Freunde mit U haben, aber noch nicht mit U befreundet sind.

Die **Ausgabe** erfolgt in eine Textdatei und sollte eine Zeile pro Benutzer im folgenden Format enthalten: `<Benutzer> TAB <Empfehlungen>`. Dabei ist `<Benutzer>` die eindeutige ID eines Benutzers und `<Empfehlungen>` eine durch Kommas getrennte Liste von 10 IDs anderer Benutzer, sortiert nach abnehmender Anzahl gemeinsamer Freunde. Wenn ein Benutzer weniger als 10 Empfehlungen hat, geben Sie alle IDs in absteigender Reihenfolge der Anzahl der gemeinsamen Freunde aus. Wenn ein Benutzer keine Freunde hat, können Sie eine leere Liste mit Empfehlungen bereitstellen. Wenn empfohlene Benutzer mit der gleichen Anzahl gemeinsamer Freunde vorhanden sind, geben Sie diese IDs in numerisch aufsteigender Reihenfolge aus. 

Die Textdatei sollte z.B. die folgenden drei Zeilen enthalten:

```
0	38737,18591,27383,34211,337,352,1532,12143,12561,17880
1	35621,44891,14150,15356,35630,13801,13889,14078,25228,13805
2	41087,1,5,95,112,1085,1404,2411,3233,4875
```

### 8. PageRank (10 Punkte)

In dieser Aufgabe sollen Sie den, in der Vorlesung besprochenen, PageRank-Algorithmus in Spark implementieren. Die stochastische Adjazenzmatrix M kann sehr groß sein und sollte in Ihrer Lösung als **Blockmatrix** (unterteilt in Quadrate einer festen Länge) vorliegen. Auch der PageRank-Vektor sollte demnach in Blöcke unterteilt und verteilt vorliegen. Verwenden Sie dazu den entsprechenden Datentyp von Spark: <https://spark.apache.org/docs/latest/mllib-data-types.html#blockmatrix>.

Berechnen Sie den PageRank für die Knoten eines kleinen zufällig generierten gerichteten Graphen (Datensatz `graph.txt`). Dieser besteht aus 1000 Knoten (nummeriert mit 1, 2, ..., 1000) und 7521 Kanten. Die erste Spalte in `graph.txt` bezieht sich auf den Startknoten und die zweite Spalte auf den Zielknoten. Es können mehrere gerichtete Kanten zwischen einem Startknoten und einem Zielknoten vorhanden sein. Ihre Lösung sollte Sie als nur eine Kante behandeln.

Führen Sie den Algorithmus mit $`\beta = 0.8`$ und $`\varepsilon = 1.0`$ aus. Listen Sie die zehn Knoten mit den höchsten PageRank-Werten auf! Wenn Sie den PageRank-Vektor, wie auf den Folien der Vorlesung angegeben, mit Einsen initialisieren, sollten Sie die folgenden Werte erhalten (Knoten-ID und PageRank):

```
(537, 2.01)
(502, 1.91)
(965, 1.88)
(187, 1.81)
(4, 1.80)
(986, 1.77)
(16, 1.77)
(387, 1.74)
(215, 1.71)
(736, 1.71)
```

### 9. Streams  (10 Punkte)

Stellen Sie sich nun vor, dass Sie einen Datenstrom aus einzelnen Wörtern beobachten und, alle 10 Sekunden, die 10 häufigsten Wörter ausgeben sollen. Implementieren Sie dazu 3 verschiedene Varianten:

1. Verwendung der absoluten Häufigkeiten über alle Wörter der letzten 10 Sekunden
2. Verwendung der absoluten Häufigkeiten über alle Wörter der letzten 60 Sekunden (aber trotzdem die Berechnung aller 10 Sekunden) 
3. Verwendung eines "Exponentially Decaying Window" (EDW) mit $`c = 0.1`$ und $`s = 1`$ 

Für die Implementierung der dritten Variante können Sie z.B. folgendermaßen vorgehen (siehe auch Vorlesung): 

- Jedes Wort $`x`$ besitzt seinen eigenen Datenstrom $`a_1(x), a_2(x), a_3(x), ..`$, dessen Werte jeweils die Häufigkeiten des Vorkommens des Wortes $`x`$ innerhalb der letzten 10 Sekunden angeben (anstatt 0/1, wie in der Vorlesung eingeführt).
- Sei $`S(x)`$ der aktuelle Wert des EDW für $`x`$ und $`a_t(x)`$ das letzte Element des Datenstroms von $`x`$. Dann wird $`S(x)`$  auf $`a_t(x) + (1 - c) S(x)`$ gesetzt.
- Falls anschließend $`S(x)`$ kleiner als der Schwellenwert $`s`$, dann soll der Zähler $`S(x)`$ entfernt werden. 

Verwenden Sie für diese Aufgaben den Datensatz `word_stream.txt` und die Streaming-Bibliothek von Spark: <https://spark.apache.org/docs/latest/streaming-programming-guide.html>. Lesen Sie zunächst die Dokumentation!  

Wie in dem ersten Beispiel der Dokumentation gezeigt, soll sich Ihr Spark-Programm über TCP zu `localhost:9999` verbinden. Damit die Wörter aus der Datei `word_stream.txt` dort ankommen, können Sie folgende Java-Applikation verwenden:

```java
package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class StreamingSimulator {
	
	public static void main(String[] args) throws NumberFormatException {
		
		try {
			ServerSocket server = new ServerSocket(9999);
			System.out.println("Server is waiting for spark to connect...");

			Socket client = server.accept();
			System.out.println("Accepted spark as client!");
			
			PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream()), true);
			
			File file = new File(args[0]); 
			BufferedReader in = new BufferedReader(new FileReader(file)); 
			
			String st;
			while ((st = in.readLine()) != null) {
				out.println(st);
				Thread.sleep(1);
			}

			in.close();
			out.close(); 
			client.close();
			server.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
  }
}
```

Dem StreamingSimulator wurde eine Verzögerung eingebaut (`Thread.sleep(1)`). Er schreibt somit, je nach System, z.B. etwa 800 Wörter pro Sekunde. Wenn Sie die Datei `word_stream.txt` (bestehehd aus 5.752.645 Wörtern) über `args[0]` einbinden, läuft dieser Simulator ungefähr 1-2 Stunden

**Hinweise**:

- Für die 2. Variante bietet sich die Funktion `reduceByKeyAndWindow()` an
- Für die 3. Variante bietet sich die Verwendung der Zustände an, z.B.: <https://github.com/apache/spark/blob/v2.4.3/examples/src/main/java/org/apache/spark/examples/streaming/JavaStatefulNetworkWordCount.java> 
