package testat02.APriori;

public class Rule implements java.io.Serializable {

    private ItemSet body;
    private ItemSet head;
    private Double confidence = 0.0;

    Rule(ItemSet ruleBody, ItemSet ruleHead) {
        body = ruleBody;
        head = ruleHead;
    }

    ItemSet getBody() {
        return body;
    }

    ItemSet getHead() {
        return head;
    }

    Double getConfidence() {
        return confidence;
    }

    Rule setConfidence(Double confidence) {
        this.confidence = confidence;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getBody().toString());
        stringBuilder.append(" -> ");
        stringBuilder.append(getHead().toString());
        stringBuilder.append(" (Confidence: ");
        stringBuilder.append(getConfidence());
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!o.getClass().equals(this.getClass())) {
            return false;
        }
        return body.equals(((Rule) o).getBody()) && head.equals(((Rule) o).getHead());
    }

    @Override
    public final int hashCode() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(body.hashCode());
        stringBuilder.append(head.hashCode());
        return stringBuilder.toString().hashCode();
    }

}
