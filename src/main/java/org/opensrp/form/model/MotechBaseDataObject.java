package org.opensrp.form.model;

import org.codehaus.jackson.annotate.JsonProperty;
import org.ektorp.support.CouchDbDocument;

public abstract class MotechBaseDataObject extends CouchDbDocument {
    @JsonProperty
    protected String type;
    private static final long serialVersionUID = 1L;

    protected MotechBaseDataObject() {
        this.type = this.getClass().getSimpleName();
    }

    protected MotechBaseDataObject(String type) {
        this.type = type;
    }
}
