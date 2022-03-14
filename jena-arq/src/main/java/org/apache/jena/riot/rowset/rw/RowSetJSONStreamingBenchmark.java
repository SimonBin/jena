/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.riot.rowset.rw;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.jena.ext.com.google.common.base.StandardSystemProperty;
import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.apache.jena.ext.com.google.common.collect.Iterators;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetMem;
import org.apache.jena.sparql.util.Context;

public class RowSetJSONStreamingBenchmark {

    public static void download(Path dataFile, Path dataFileTmp, Callable<InputStream> inSupp) throws Exception {
        if (!Files.exists(dataFile)) {
            System.out.println("Attempting to dowload test data");

            if (Files.exists(dataFileTmp)) {
                throw new RuntimeException("Partial data found " + dataFileTmp +
                        " - Either wait for the process writing it or if there is none then delete it manually");
            }

            try (OutputStream out = Files.newOutputStream(dataFileTmp)) {
                try (InputStream in = inSupp.call()) {
                    IOUtils.copy(in, out);
                }
                out.flush();
            }
            try {
                Files.move(dataFileTmp, dataFile, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(dataFileTmp, dataFile);
            }
            System.out.println("Data retrieved");
        }

    }

    public static <T> T benchmark(String label, Callable<T> action) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        T result = action.call();
        float elapsed = sw.elapsed(TimeUnit.MILLISECONDS) * 0.001f;
        System.out.println("Time taken for " + label + ": " + elapsed + "s");
        return result;
    }

    public static void main(String[] args) throws Exception {
        Context cxt = ARQ.getContext().copy();
        cxt.setTrue(ARQ.inputGraphBNodeLabels);

        Path dataFile = Path.of(StandardSystemProperty.JAVA_IO_TMPDIR.value()).resolve("jena-rs.json");
        Path dataFileTmp = dataFile.resolveSibling(dataFile.getFileName() + ".tmp");

        Callable<RowSet> conventionalRowSetFactory = () -> RowSetReaderJSON.factory.create(ResultSetLang.RS_JSON).read(Files.newInputStream(dataFile), cxt);
        Callable<RowSet> streamingRowSetFactory = () -> RowSetReaderJSONStreaming.factory.create(ResultSetLang.RS_JSON).read(Files.newInputStream(dataFile), cxt);


        download(dataFile, dataFileTmp, () ->
            new URL("http://moin.aksw.org/sparql?query=SELECT%20*%20{%20?s%20?p%20?o%20}").openStream());

        benchmark("total", () -> {
	        for (int i = 0; i < 20; ++i) {
	            // benchmarkComparison("iteration" + i, conventionalRowSetFactory, streamingRowSetFactory);
	            // benchmarkConsumption("conventional:iteration" + i, conventionalRowSetFactory);
	            benchmarkConsumption("streaming:iteration" + i, streamingRowSetFactory);
	        }
	        return null;
        });

    }

    public static void benchmarkConsumption(String label, Callable<RowSet> rowSetSupp) throws Exception {
        RowSet actualsInit = benchmark(label + ":setup", rowSetSupp::call);
        int size = benchmark(label + ":consumption", () -> Iterators.size(actualsInit));
        System.out.println("Size: " + size);
        actualsInit.close();
    }

    public static void benchmarkComparison(String label,
    		Callable<RowSet> expectedFactory, Callable<RowSet> actualFactory) throws Exception {

        RowSet expectedsInit = benchmark(label + ":expected:setup", expectedFactory::call);
        RowSet expecteds = benchmark(label + ":expected:consumption", () -> RowSetMem.create(expectedsInit));

        RowSet actualsInit = benchmark(label + ":actual:setup", actualFactory::call);
        RowSet actuals = benchmark(label + ":actual:consumption", () -> RowSetMem.create(actualsInit));


        long seenItems = 0;
        boolean isOk = true;
        while (true) {
            boolean ahn = actuals.hasNext();
            boolean ehn = expecteds.hasNext();

            if (ahn == ehn) {
                if (ahn) {
                    Binding a = actuals.next();
                    Binding e = expecteds.next();

                    if (!Objects.equals(a, e)) {
                        System.out.println(String.format("Difference at %d/%d: %s != %s",
                                actuals.getRowNumber(), expecteds.getRowNumber(), a , e));

                        isOk = false;
                    }
                } else {
                    break;
                }
            } else {
                System.out.println("Result set lengths differ");
                isOk = false;
                break;
            }
            ++seenItems;
        }

        System.out.println("Result sets are " + (isOk ? "" : "NOT ") + "equal - items seen: " + seenItems);

        // boolean isIsomorphic = ResultSetCompare.isomorphic(actuals, expecteds);
        // System.out.println("Isomorphic: " + isIsomorphic);

        actuals.close();
        expecteds.close();
    }
}
