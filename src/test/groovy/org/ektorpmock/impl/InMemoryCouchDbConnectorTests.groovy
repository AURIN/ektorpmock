package org.ektorpmock.impl

import org.junit.Before

class InMemoryCouchDbConnectorTests extends CouchDbConnectorTestBase {

    @Before
    void setUp() {
        db = new InMemoryCouchDbConnector(new JsonViewEvaluator())
//        This seems to be required due to the threadlocal nature of the bulk buffer
        db.clearBulkBuffer()
    }
}
