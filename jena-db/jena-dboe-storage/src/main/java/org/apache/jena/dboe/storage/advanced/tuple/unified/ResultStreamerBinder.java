package org.apache.jena.dboe.storage.advanced.tuple.unified;

public interface ResultStreamerBinder<D, C, T>
{
    ResultStreamer<D, C, T> bind(Object store);
}
