package testat02.APriori;

public class Rule {

    private ItemSet body;
    private ItemSet head;
    private Double confidence = 0.0;

    public Rule(ItemSet ruleBody, ItemSet ruleHead) {
        body = ruleBody;
        head = ruleHead;
    }

    public ItemSet getBody() {
        return body;
    }

    public ItemSet getHead() {
        return head;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
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

    /*
    @Override
    public boolean equals(Object o) {
        if (!o.getClass().equals(this.getClass())) {
            return false;
        }
        Rule other = (Rule) o;
        return body.equals(((Rule) o).getBody()) && head.equals(((Rule) o).getHead());
    }

    @Override
    public final int hashCode() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(body.hashCode());
        stringBuilder.append(head.hashCode());
        return stringBuilder.toString().hashCode();
    }
    */

}
