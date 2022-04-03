package io.kgraph.kgiraffe.schema;

public class SchemaContext {
    private final String name;
    private final Mode mode;
    private boolean root;
    private int nameIndex = 0;

    public SchemaContext(String name,
                         Mode mode) {
        this.name = name;
        this.mode = mode;
        this.root = true;
    }

    public String name() {
        return name;
    }

    public Mode mode() {
        return mode;
    }

    public boolean isWhere() {
        return mode == Mode.QUERY_WHERE;
    }

    public boolean isOrderBy() {
        return mode == Mode.QUERY_ORDER_BY;
    }

    public boolean isRoot() {
        return root;
    }

    public void setRoot(boolean root) {
        this.root = root;
    }

    public String qualify(String name1, String name2) {
        return qualify(name1 != null ? name1 + "_" + name2 : name2);
    }

    public String qualify(String name) {
        String suffix;
        switch (mode) {
            case QUERY_WHERE:
                suffix = "_criteria";
                break;
            case QUERY_ORDER_BY:
                suffix = "_order";
                break;
            case MUTATION:
                suffix = "_insert";
                break;
            case SUBSCRIPTION:
                suffix = "_sub";
                break;
            case OUTPUT:
            default:
                suffix = "";
                break;
        }
        return this.name + "_" + name.replace('.', '_') + suffix;
    }

    public int incrementNameIndex() {
        return ++nameIndex;
    }

    public enum Mode {
        QUERY_WHERE,
        QUERY_ORDER_BY,
        MUTATION,
        SUBSCRIPTION,
        OUTPUT
    }
}
