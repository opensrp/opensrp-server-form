package org.opensrp.form.dao;

import org.ektorp.BulkDeleteDocument;
import org.ektorp.CouchDbConnector;
import org.ektorp.ViewQuery;
import org.ektorp.support.CouchDbRepositorySupport;
import org.ektorp.support.GenerateView;
import org.opensrp.form.model.MotechBaseDataObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class MotechBaseRepository<T extends MotechBaseDataObject> extends CouchDbRepositorySupport<T> {
    private Class<T> type;

    protected MotechBaseRepository(Class<T> type, CouchDbConnector db) {
        super(type, db);
        this.type = type;
        this.initStandardDesignDocument();
    }

    protected void addOrReplace(T entity, String businessFieldName, String businessId) {
        List<T> entities = this.entities(businessFieldName, businessId);
        if (entities.size() == 0) {
            this.add(entity);
        } else {
            if (entities.size() != 1) {
                throw new BusinessIdNotUniqueException(businessFieldName, businessId);
            }

            T entityInDb = entities.get(0);
            entity.setId(entityInDb.getId());
            entity.setRevision(entityInDb.getRevision());
            this.update(entity);
        }

    }

    private List<T> entities(String businessFieldName, String businessId) {
        String viewName = String.format("by_%s", businessFieldName);
        ViewQuery q = this.createQuery(viewName).key(businessId).includeDocs(true);
        return this.db.queryView(q, this.type);
    }

    public void removeAll(String fieldName, String value) {
        List<T> entities = this.entities(fieldName, value);
        this.removeAll(entities);
    }

    private void removeAll(List<T> entities) {
        List<BulkDeleteDocument> bulkDeleteQueue = new ArrayList(entities.size());
        Iterator i$ = entities.iterator();

        while(i$.hasNext()) {
            T entity = (T) i$.next();
            bulkDeleteQueue.add(BulkDeleteDocument.of(entity));
        }

        this.db.executeBulk(bulkDeleteQueue);
    }

    public void removeAll() {
        this.removeAll(this.getAll());
    }

    public void safeRemove(T entity) {
        if (this.contains(entity.getId())) {
            this.remove(entity);
        }

    }

    @GenerateView
    public List<T> getAll() {
        return super.getAll();
    }

    protected List<T> getAll(int limit) {
        ViewQuery q = this.createQuery("all").limit(limit).includeDocs(true);
        return this.db.queryView(q, this.type);
    }

    protected T singleResult(List<T> resultSet) {
        return resultSet != null && !resultSet.isEmpty() ? resultSet.get(0) : null;
    }

    public List<T> queryViewWithKeyList(String viewName, List<String> keys) {
        return this.db.queryView(this.createQuery(viewName).includeDocs(true).keys(keys), this.type);
    }
}