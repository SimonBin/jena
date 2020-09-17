package org.apache.jena.dboe.storage.advanced.storage;

import org.apache.jena.dboe.storage.advanced.core.DatasetGraphFactoryOrdered;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.junit.Test;

public class TestInsertOrderAwareness {
    @Test
    public void test() {
        // FIXME TODO TO BE DONE
        Dataset ds = DatasetFactory.wrap(DatasetGraphFactoryOrdered.createTestDatasetGraph());
        RDFDataMgr.read(ds, "nato-phonetic-alphabet-example.trig");

        RDFDataMgr.write(System.out, ds, RDFFormat.TRIG_BLOCKS);
    }
}
