package org.ektorpmock.impl

import org.junit.Before

class InMemoryCouchDbConnectorTests extends CouchDbConnectorTestBase {

    @Before
    void setUp() {
        db = new InMemoryCouchDbConnector(new JsonViewEvaluator())
    }
}
