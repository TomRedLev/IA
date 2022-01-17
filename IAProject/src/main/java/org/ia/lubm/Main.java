package org.ia.lubm;

import org.apache.jena.rdf.model.ModelFactory;

public class Main {

    public static void main(String[] args) {
        var model = ModelFactory.createDefaultModel();
        model.read("file:lubm1.ttl", "TTL");
        System.out.println(model.size());
    }

}
