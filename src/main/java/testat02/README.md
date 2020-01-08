## Testat 2

### 5. Assoziationsregeln (15 Punkte)

Angenommen Sie sind ein Online-Händler und möchten Ihren Kunden Produkte empfehlen. Dazu verwenden Sie die Daten `browsing.txt`, welche Sie bei den früheren Besuchen auf Ihrer Website gesammelt haben. Jede Zeile repräsentiert eine Browsersitzung eines Kunden. Die darin enthaltenen Zeichenfolgen aus jeweils 8 Zeichen stehen für die IDs der Produkte, welche während dieser Sitzung angezeigt wurden. Da Sie keine Bewertungen der Nutzer für Produkte besitzen, müssen Sie versuchen, die Empfehlungen über Assoziationsregeln zu generieren.

Implementieren Sie für das Auffinden relevanter Assoziationsregeln den **A-priori-Algorithmus**! Setzen Sie dazu folgende 3 Schritte um: 

1. Identifizieren Sie die Elementpaare (X, Y) so, dass der Support von {X, Y} mindestens 1% beträgt. Berechnen Sie für jedes Paar die Confidence der entsprechenden Assoziationsregeln: X ⇒ Y, Y ⇒ X. 
2. Identifizieren Sie die Elementtripel (X, Y, Z) so, dass der Support von {X, Y, Z} mindestens 1% beträgt. Berechnen Sie für jedes Tripel die Confidence der entsprechenden Assoziationsregeln: (X, Y) ⇒ Z, (X, Z) ⇒ Y, (Y, Z) ⇒ X.
3. Wählen Sie die Regeln mit einer Confidence von mind. 0.8 aus und sortieren Sie die Regeln in absteigender Reihenfolge der Confidence.  

In Spark (MLlib) wurde bereits ein effizienterer Algorithmus zum Finden von Assoziationsregeln implementiert: <https://spark.apache.org/docs/latest/mllib-frequent-pattern-mining.html>. Lösen Sie diese Aufgabe also **zusätzlich** über MLlib und vergleichen Sie die Ergebnisse der beiden Algorithmen!

**Hinweise:**

- Verwenden Sie den Operator `cartesian()` sparsam.
- Es ist hilfreich Klassen wie `Itemset` oder `Rule` als Wrapper für mehrwertige Daten zu implementieren. Damit Sie diese auch als Schlüssel in PairRDDs oder in `HashSets` effizient einsetzen können, sollten Sie die Methoden `equals()` und `hashCode()` überschreiben.

### 6. MinHashing & LSH (15 Punkte)

In dieser Aufgabe sollen Sie aus einer großen Menge von Textdokumenten ähnliche Einträge finden. Der Datensatz `imdb.txt` enthält 10.000 Rezensionen zu verschiedenen Filmen. Jede Zeile besteht aus zwei Attributen: einer ID und dem Text.

Implementieren Sie den, in der Vorlesung besprochenen, dreistufigen Algorithmus zum Finden ähnlicher Elemente: Shingling -> MinHashing -> LSH:

1. Die Shingle-Größe können Sie z.B. auf k = 3 setzen.  
2. Zum Vergleich der Texte genügt eine MinHash-Signatur der Größe 1000. Für deren Generierung eignen sich Zufallshashfunktionen der Form h(x) = ((a x + b) mod p) mod N (Siehe Vorlesung). Als Primzahl p können Sie z.B. die Zahl 131.071 nehmen. 
3. Bei dem LSH-Schritt müssen Sie mit verschiedenen Werten für die Anzahl der Hashfunktionen experimentieren. Es sollten sowohl die Rate der False Negatives möglichst gering gehalten als auch nicht zu viele Kandidatenpaare gefunden werden.
4. Berechnen Sie für die Kandidatenpaare sowohl die Jaccard- als auch die MinHash-Ähnlichkeit. Anschließend können Sie auf die Paare von Texten filtern, welche eine Jaccard-Ähnlichkeit von mind. 0.8 aufzeigen.

**Hinweise:** 

- Zum Testen Ihrer Implementieren sollten Sie den Datensatz zunächst auf etwa 1.000 Zeilen reduzieren. 
- Es ist ratsam, auf Shingles zu filtern, die in mind. 2 Dokumenten vorkommen. Bei dem Datensatz `imdb.txt` kommen Sie dann (mit k = 3) auf über 29.000 verschiedene Shingles.
- Hashing für LSH: MurmurHash (<https://github.com/google/guava>)
- Die Rate der False Negatives können Sie überprüfen, indem Sie schrittweise die Anzahl der Hashfunktionen erhöhen. Über den 4. Schritt (Berechnung der tatsächlichen Ähnlichkeiten) können Sie dann beurteilen, wie stark die Rate der False Negatives gesunken ist. Sie sollten *nicht* die tatsächlichen Ähnlichkeiten zwischen allen Dokumentpaaren berechnen.
