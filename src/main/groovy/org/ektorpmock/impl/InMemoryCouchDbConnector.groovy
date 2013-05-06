package org.ektorpmock.impl

import org.ektorp.CouchDbConnector
import org.ektorp.PurgeResult
import org.ektorp.Options
import org.ektorp.Revision
import org.ektorp.AttachmentInputStream
import org.ektorp.ViewQuery
import org.ektorp.Page
import org.ektorp.PageRequest
import org.ektorp.ViewResult
import org.ektorp.StreamingViewResult
import org.ektorp.http.HttpClient
import org.ektorp.DbInfo
import org.ektorp.DesignDocInfo
import org.ektorp.ReplicationStatus
import org.ektorp.DocumentOperationResult
import org.ektorp.changes.DocumentChange
import org.ektorp.changes.ChangesCommand
import org.ektorp.StreamingChangesResult
import org.ektorp.changes.ChangesFeed
import org.ektorp.UpdateHandlerRequest
import org.ektorp.UpdateConflictException
import org.ektorp.util.Assert
import org.ektorp.util.Documents
import org.codehaus.jackson.map.ObjectMapper
import org.ektorp.DocumentNotFoundException
import groovy.json.JsonSlurper
import org.ektorp.support.Revisions
import org.apache.commons.io.IOUtils
import org.ektorp.Attachment
import org.ektorp.support.DesignDocument
import javax.script.Bindings
import javax.script.CompiledScript
import javax.script.Compilable
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager
import org.ektorp.ViewEvaluator
import org.ektorp.impl.JsonSerializer
import org.ektorp.impl.StdObjectMapperFactory
import org.ektorp.impl.ObjectMapperFactory
import org.ektorp.impl.StreamingJsonSerializer


class InMemoryCouchDbConnector implements CouchDbConnector {

    private final ObjectMapper objectMapper
    private JsonSerializer jsonSerializer
    private ViewEvaluator viewEvaluator


    private Map<String, String> data
    private Map<String, Revisions> revisions
    private Map<String, Map<String, String>> revisionMap


    InMemoryCouchDbConnector() {
        this(new StdObjectMapperFactory())
    }

    InMemoryCouchDbConnector(ViewEvaluator ve) {
        this(new StdObjectMapperFactory())
        this.viewEvaluator = ve
    }

    InMemoryCouchDbConnector(ObjectMapperFactory omf) {
        Assert.notNull(omf, "ObjectMapperFactory may not be null");
        this.objectMapper = omf.createObjectMapper(this);
        this.jsonSerializer = new StreamingJsonSerializer(objectMapper);
        data = new LinkedHashMap<String, String>()
        revisions = new LinkedHashMap<String, Revisions>()
        revisionMap = new LinkedHashMap<String, Map<String,String>>()
    }
    /**
     *
     * @param id
     * @param the
     *            object to store in the database
     * @throws org.ektorp.UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    void create(String id, Object o) {
        if (contains(id)) {
            if (id == Documents.getId(o)) {
                update(o)
                return
            } else {
                throw new UpdateConflictException()
            }
        }
        String revision = incrementRevision()
        Documents.setRevision(o, revision);
        String json = jsonSerializer.toJson(o)
        data.put(id, json)
        revisions.put(id, new Revisions(1, [id]))
        def revisionMapEntry = new LinkedHashMap<String, String>()
        revisionMapEntry.put(revision, json)
        revisionMap.put(id, revisionMapEntry)
    }

    /**
     * Creates the Object as a document in the database. If the id is not set it will be generated by the database.
     *
     * The Object's revision field will be updated through the setRevision(String s) method.
     *
     * @param o
     * @throws UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    void create(Object o) {
        Assert.notNull(o, "Document may not be null");
        Assert.isTrue(Documents.isNew(o), "Object must be new");

        String id = Documents.getId(o);
        if (id != null) {
            create(id, o)
        } else {
            id = UUID.randomUUID().toString()
            Documents.setId(o, id);
            create(id, o)
        }
    }

    /**
     * Updates the document.
     *
     * The Object's revision field will be updated through the setRevision(String s) method.
     *
     * @param o
     * @throws UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    void update(Object o) {
        def id = Documents.getId(o)
        if (!id) {
            throw new IllegalArgumentException("id can not be empty")
        }
        def revision = Documents.getRevision(o)
        if (!revision) {
            create(o)
            return
        }
        if (documentOutOfDate(o)) {
            throw new UpdateConflictException(id, Documents.getRevision(o))
        }
        def newRevision = incrementRevision(revision)
        Documents.setRevision(o, newRevision)
        def json = jsonSerializer.toJson(o)
        data.put(id, json)
        def oldRevisions = revisions.get(id)
        revisions.put(id, new Revisions(revisionToInt(newRevision) as long, [id] + oldRevisions.ids))
        revisionMap.get(id).put(newRevision, json)
    }

    /**
     * Deletes the Object in the database.
     *
     * @param o
     * @return the revision of the deleted document
     * @throws UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    String delete(Object o) {
        Assert.notNull(o, "Document cannot be null");
        return delete(Documents.getId(o), Documents.getRevision(o))
    }

    /**
     * Deletes the document in the database.
     *
     * @param id
     * @param revision
     * @return the revision of the deleted document.
     * @throws UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    String delete(String id, String revision) {
        Assert.hasText(id, "document id cannot be empty");
        def o = data.remove(id)
        def map = objectMapper.readValue(o, HashMap)
        def currentRevision = Documents.getRevision(map)
        if ((revisionToInt(revision)) != revisionToInt(currentRevision)) {
            throw new UpdateConflictException(id, revision)
        }
        return incrementRevision(currentRevision)
    }

    @Override
    String copy(String sourceDocId, String targetDocId) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    String copy(String sourceDocId, String targetDocId, String targetRevision) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    PurgeResult purge(Map<String, List<String>> revisionsToPurge) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     *
     * @param <T>
     * @param c
     *            the target class to map to.
     * @param id
     *            the id of the document in the database.
     * @return the document mapped as the specified class.
     * @throws org.ektorp.DocumentNotFoundException
     *             if the document was not found.
     */
    @Override
    def <T> T get(Class<T> c, String id) {
        return get(c, id, new Options())
    }

    /**
     *
     * @param c
     *            the target class to map to.
     * @param id
     *            the id of the document in the database.
     * @param options
     * @return the document mapped as the specified class.
     * @throws DocumentNotFoundException
     *             if the document was not found.
     */
    @Override
    def <T> T get(Class<T> c, String id, Options options) {
        def json = getJsonString(id, options)
        return objectMapper.readValue(json, c)
    }

    /**
     * Same as get(Class<T> c, String id) with the difference that null is return if the document was not found.
     *
     * @param c
     * @param id
     * @return null if the document was not found.
     */
    @Override
    def <T> T find(Class<T> c, String id) {
        return find(c, id, new Options())
    }

    /**
     * Same as get(Class<T> c, String id, Options options) with the difference that null is return if the document was
     * not found.
     *
     * @param c
     * @param id
     * @param options
     * @return null if the document was not found.
     */
    @Override
    def <T> T find(Class<T> c, String id, Options options) {
        try {
            get(c, id)
        } catch (DocumentNotFoundException dnfe) {
            return null
        }
    }

    @Override
    @Deprecated
    def <T> T get(Class<T> c, String id, String rev) {
        return get(c, id, new Options().revision(rev))
    }

    @Override
    @Deprecated
    def <T> T getWithConflicts(Class<T> c, String id) {
        return get(c, id, new Options().includeConflicts())
    }

    /**
     * Check if the database contains a document.
     *
     * @param id
     * @return true if a document with the id exists in the database
     */
    @Override
    boolean contains(String id) {
        return data.containsKey(id)
    }

    /**
     * Please note that the stream has to be closed after usage, otherwise http connection leaks will occur and the
     * system will eventually hang due to connection starvation.
     *
     * @param id
     * @return the document as raw json in an InputStream, don't forget to close the stream when finished.
     * @throws DocumentNotFoundException
     *             if the document was not found.
     */
    @Override
    InputStream getAsStream(String id) {
        return getAsStream(id, new Options())
    }

    @Override
    @Deprecated
    InputStream getAsStream(String id, String rev) {
        return getAsStream(id, new Options().revision(rev))
    }

    @Override
    InputStream getAsStream(String id, Options options) {
        String json = getJsonString(id, options)
        return IOUtils.toInputStream(json)
    }

    @Override
    List<Revision> getRevisions(String id) {
        return revisionMap.get(id).keySet().sort {
            revisionToInt(it)
        }.collect {
            new Revision(it, "ok")
        }
    }

    @Override
    AttachmentInputStream getAttachment(String id, String attachmentId) {
        return getAttachment(id, attachmentId, null)
    }

    @Override
    AttachmentInputStream getAttachment(String id, String attachmentId, String revision) {
        String json = getJsonString(id, new Options())
        Map map = new JsonSlurper().parseText(json)
        Map<String, Attachment> attachmentsMap = map._attachments
        Map attachmentMap = attachmentsMap.get(attachmentId)
        String attachmentString = jsonSerializer.toJson(attachmentMap)
        Attachment attachment = objectMapper.readValue(attachmentString, Attachment)
        return new AttachmentInputStream(attachmentId, IOUtils.toInputStream(attachment.dataBase64), attachment.contentType)
    }

    /**
     * Creates both the document and the attachment
     *
     * @param docId
     * @param a
     *            - the data to be saved as an attachment
     * @return revision of the created attachment document
     * @throws UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    String createAttachment(String docId, AttachmentInputStream data) {
        createAttachment(docId, null, data)
    }

    /**
     * Adds an attachment to the specified document id.
     *
     * @param docId
     * @param revision
     * @param a
     *            - the data to be saved as an attachment
     * @return the new revision of the document
     * @throws UpdateConflictException
     *             if there was an update conflict.
     */
    @Override
    String createAttachment(String docId, String revision, AttachmentInputStream stream) {
        String json
        try {
            json = getJsonString(docId, new Options())
        } catch (DocumentNotFoundException dnfe) {
            json = jsonSerializer.toJson(["_id": docId])
        }

        Map map = new JsonSlurper().parseText(json)

        if (!revision && map._rev) {
            throw new UpdateConflictException()
        }
        if (!map._attachments) {
            map._attachments = new HashMap<String, Attachment>()
        }
        if (map._attachments.get(stream.id)) {
            throw new UpdateConflictException()
        }
        map._attachments.put(stream.id, new Attachment(stream.id, IOUtils.toString(stream), stream.contentType))
        map._rev = incrementRevision(map._rev)
        json = jsonSerializer.toJson(map)
        data.putAt(docId, json)

        return map._rev
    }

    @Override
    String deleteAttachment(String docId, String revision, String attachmentId) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<String> getAllDocIds() {
        return data.keySet()
    }

    @Override
    def <T> List<T> queryView(ViewQuery query, Class<T> type) {
        ViewResult viewResult = queryView(query)
        viewResult.rows.findAll { row ->
            row.doc
        }.collect { row ->
            if (row.doc) {
                objectMapper.readValue(row.doc, type)
            }
        }
    }

    @Override
    def <T> Page<T> queryForPage(ViewQuery query, PageRequest pr, Class<T> type) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    ViewResult queryView(ViewQuery query) {
        def jsonData =  evaluateView(query)
        def node = objectMapper.readTree(jsonData)
        return new ViewResult(node, false)
    }

    @Override
    StreamingViewResult queryForStreamingView(ViewQuery query) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    InputStream queryForStream(ViewQuery query) {
        def jsonData =  evaluateView(query)
        return IOUtils.toInputStream(jsonData)
    }

    @Override
    void createDatabaseIfNotExists() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    String getDatabaseName() {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    String path() {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    HttpClient getConnection() {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    DbInfo getDbInfo() {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    DesignDocInfo getDesignDocInfo(String designDocId) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void compact() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void compactViews(String designDocumentId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void cleanupViews() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    int getRevisionLimit() {
        return 0  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void setRevisionLimit(int limit) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    ReplicationStatus replicateFrom(String source) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    ReplicationStatus replicateFrom(String source, Collection<String> docIds) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    ReplicationStatus replicateTo(String target) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    ReplicationStatus replicateTo(String target, Collection<String> docIds) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void addToBulkBuffer(Object o) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<DocumentOperationResult> flushBulkBuffer() {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void clearBulkBuffer() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<DocumentOperationResult> executeBulk(InputStream inputStream) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<DocumentOperationResult> executeAllOrNothing(InputStream inputStream) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<DocumentOperationResult> executeBulk(Collection<?> objects) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<DocumentOperationResult> executeAllOrNothing(Collection<?> objects) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    List<DocumentChange> changes(ChangesCommand cmd) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    StreamingChangesResult changesAsStream(ChangesCommand cmd) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    ChangesFeed changesFeed(ChangesCommand cmd) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    String callUpdateHandler(String designDocID, String function, String docId) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    String callUpdateHandler(String designDocID, String function, String docId, Map<String, String> params) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    def <T> T callUpdateHandler(UpdateHandlerRequest req, Class<T> c) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    String callUpdateHandler(UpdateHandlerRequest req) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void ensureFullCommit() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void updateMultipart(String id, InputStream stream, String boundary, long length, Options options) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    void update(String id, InputStream document, long length, Options options) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * There must be a more efficient way to do this
     * @param view
     */
    private String evaluateView(ViewQuery query) {
        DesignDocument designDocument = get(DesignDocument, query.designDocId)
        DesignDocument.View view = designDocument.views.get(query.viewName)
        if (!view) throw new DocumentNotFoundException("$query.designDocId/$query.viewName")

        def viewList = viewEvaluator.evaluateView(view, query, data.values().asList())

        def offset = Math.min(Math.max(query.skip, 0), viewList.size())
        def max
        if (query.limit == -1) {
            max = viewList.size()
        } else {
            max = Math.min(viewList.size(), offset + query.limit)
        }
        def resultsMap = [
                rows: viewList ? viewList[offset..<max] : viewList,
                total_rows: viewList.size(),
                offset: offset
        ]

        if (query.includeDocs) {
            resultsMap.rows.each { row ->
                row.doc = data[row.id]
            }
        }

        String jsonData = jsonSerializer.toJson(resultsMap)
        return jsonData
    }

    private String getJsonString(String id, Options options) {
        if (!contains(id)) {
            throw new DocumentNotFoundException(id)
        }
        def json
        if (options.options.rev != null) {
            json = revisionMap.get(id).get(options.options.rev)
            if (!json) {
                throw new DocumentNotFoundException(id)
            }
        } else {
            json = data.get(id)
        }

        if (options.options.revs) {
            Revisions revisions = revisions.get(id)
            Map map = new JsonSlurper().parseText(json)
            map._revisions = revisions
            json = jsonSerializer.toJson(map)
        }
        return json
    }

    private String incrementRevision(String oldRevision = null) {
        int oldRevisionInt = oldRevision? (revisionToInt(oldRevision) + 1) : 1
        return "$oldRevisionInt-${UUID.randomUUID().toString()}"
    }

    private boolean documentOutOfDate(Object o) {
        def id = Documents.getId(o)
        def revision = Documents.getRevision(o)
        int revisionInt = revisionToInt(revision)

        def currentDoc = get(o.class, id)
        def currentRevision = Documents.getRevision(currentDoc)
        int currentRevisionInt = revisionToInt(currentRevision)

        return currentRevisionInt > revisionInt
    }

    private int revisionToInt(String revision) {
        if (!revision) return 0
        return revision.split("-")[0] as int
    }
}
