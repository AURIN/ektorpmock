package org.ektorpmock.impl

import org.junit.BeforeClass
import org.junit.Before
import org.junit.After
import org.ektorp.DbPath
import org.ektorp.impl.StdCouchDbInstance
import org.ektorp.CouchDbInstance
import org.ektorp.http.StdHttpClient
import org.ektorp.http.HttpClient

class StdCouchDbConnectorTests extends CouchDbConnectorTestBase {

    static CouchDbInstance dbInstance
    @BeforeClass
    static void beforeClass() {
        HttpClient httpClient = new StdHttpClient.Builder()
                .url("http://localhost:5984")
                .username("admin")
                .password("password")
                .build();
        dbInstance = new StdCouchDbInstance(httpClient)
        if (dbInstance.checkIfDbExists(new DbPath("ektorp_mock"))) {
            dbInstance.deleteDatabase("ektorp_mock")
        }
        dbInstance.createDatabase("ektorp_mock")

    }

    @Before
    void setUp() {
        db = dbInstance.createConnector("ektorp_mock", true);
    }

    @After
    void tearDown() {
        if (db.contains(staticId)) {
            def revisions = db.getRevisions(staticId)
            def revision = revisions.first()
            db.delete(staticId, revision.rev)
        }
        assert !db.contains(staticId)
    }
}
