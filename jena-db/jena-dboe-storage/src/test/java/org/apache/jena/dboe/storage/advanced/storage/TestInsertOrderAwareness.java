package org.apache.jena.dboe.storage.advanced.storage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.junit.Test;

public class TestInsertOrderAwareness {
    @Test
    public void test() {
        Dataset ds = DatasetFactory.wrap(TupleDataset.createTestDatasetGraph());
        RDFDataMgr.read(ds, "nato-phonetic-alphabet-example.trig");

        RDFDataMgr.write(System.out, ds, RDFFormat.TRIG_BLOCKS);
    }
}
