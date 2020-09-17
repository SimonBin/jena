package org.apache.jena.dboe.storage.advanced.storage;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses( {
    // Most testing happens via the dataset tests run on dataset built out of StorageRDF.
    TestStorageSimple.class
    , TestDatasetGraphStorageTests.class
    , TestDatasetGraphStorageFindTests.class
})

public class TS_TupleAbstraction { }
