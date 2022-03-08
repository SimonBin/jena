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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;
import org.apache.jena.atlas.data.BagFactory;
import org.apache.jena.atlas.data.DataBag;
import org.apache.jena.atlas.data.ThresholdPolicy;
import org.apache.jena.atlas.data.ThresholdPolicyFactory;
import org.apache.jena.atlas.iterator.IteratorSlotted;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.system.SyntaxLabels;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetBuffered;
import org.apache.jena.sparql.system.SerializationFactoryFinder;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;


/**
 * Streaming RowSet implementation for application/sparql-results+json
 * The {@link #getResultVars()} will return null as long as the header has not
 * been consumed from the underlying stream.
 *
 * Use {@link BufferedRowSet} to modify the behavior such that {@link #getResultVars()}
 * immediately consumes the underlying stream until the header is read,
 * thereby buffering any encountered bindings for replay.
 *
 * Use {@link #createBuffered(InputStream, Context)} to create a buffered row set
 * with appropriate configuration w.r.t. ARQ.inputGraphBNodeLabels and ThresholdPolicyFactory.
 *
 */
public class RowSetJSONStreaming
    extends IteratorSlotted<Binding>
    implements RowSet
{

    public static void main(String[] args) throws MalformedURLException, IOException {
        // TODO Read test data from class path resource
        byte[] data;
        try (InputStream in = new URL("http://moin.aksw.org/sparql?query=SELECT%20*%20{%20?s%20?p%20?o%20}").openStream()) {
            data = IOUtils.toByteArray(in);
        }

        Context cxt = ARQ.getContext().copy();
        cxt.setTrue(ARQ.inputGraphBNodeLabels);

        System.out.println("Data retrieved");
        RowSet actuals = RowSetJSONStreaming.createBuffered(new ByteArrayInputStream(data), cxt);
        RowSet expecteds = RowSetReaderJSON.factory.create(ResultSetLang.RS_JSON).read(new ByteArrayInputStream(data), cxt);

        boolean isOk = true;
        while (actuals.hasNext() && expecteds.hasNext()) {
            Binding a = actuals.next();
            Binding b = expecteds.next();

            if (!Objects.equals(a, b)) {
                System.out.println(String.format("Difference at %d/%d: %s != %s",
                        actuals.getRowNumber(), expecteds.getRowNumber(), a , b));

                isOk = false;
            }
        }

        System.out.println("Success is " + isOk);

        // boolean isIsomorphic = ResultSetCompare.isomorphic(actuals, expecteds);
        // System.out.println("Isomorphic: " + isIsomorphic);

        actuals.close();
        expecteds.close();
    }
    public static RowSetBuffered<RowSetJSONStreaming> createBuffered(InputStream in, Context context) {
        Context cxt = context == null ? ARQ.getContext() : context;

        boolean inputGraphBNodeLabels = cxt.isTrue(ARQ.inputGraphBNodeLabels);
        LabelToNode labelMap = inputGraphBNodeLabels
            ? SyntaxLabels.createLabelToNodeAsGiven()
            : SyntaxLabels.createLabelToNode();

        Supplier<DataBag<Binding>> bufferFactory = () -> {
            ThresholdPolicy<Binding> policy = ThresholdPolicyFactory.policyFromContext(cxt);
            DataBag<Binding> r = BagFactory.newDefaultBag(policy, SerializationFactoryFinder.bindingSerializationFactory());
            return r;
        };

        return createBuffered(in, labelMap, bufferFactory);
    }

    public static RowSetBuffered<RowSetJSONStreaming> createBuffered(InputStream in, LabelToNode labelMap, Supplier<DataBag<Binding>> bufferFactory) {
        return new RowSetBuffered<>(createUnbuffered(in, labelMap), bufferFactory);
    }

    public static RowSetJSONStreaming createUnbuffered(InputStream in, LabelToNode labelMap) {
        Gson gson = new Gson();
        JsonReader reader = gson.newJsonReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        RowSetJSONStreaming result = new RowSetJSONStreaming(gson, reader, LabelToNode.createUseLabelAsGiven());
        return result;
    }

    /** Parsing state; i.e. where we are in the json document */
    public enum State {
        INIT,
        ROOT,
        RESULTS,
        BINDINGS,
        DONE
    }


    protected Gson gson;
    protected JsonReader reader;

    protected List<Var> resultVars = null;
    protected long rowNumber;

    protected Boolean askResult = null;


    protected LabelToNode labelMap;

    // Hold the context for reference?
    // protected Context context;

    protected Function<JsonObject, Node> onUnknownRdfTermType = null;

    protected State state;


    public RowSetJSONStreaming(Gson gson, JsonReader reader, LabelToNode labelMap) {
        this(gson, reader, labelMap, null, 0);
    }

    public RowSetJSONStreaming(Gson gson, JsonReader reader, LabelToNode labelMap, List<Var> resultVars, long rowNumber) {
        super();
        this.gson = gson;
        this.reader = reader;
        this.labelMap = labelMap;

        this.resultVars = resultVars;
        this.rowNumber = rowNumber;

        this.state = State.INIT;
    }

    @Override
    public List<Var> getResultVars() {
        return resultVars;
    }

    @Override
    protected Binding moveToNext() {
        try {
            return computeNextActual();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void onUnexpectedJsonElement() throws IOException {
        reader.skipValue();
    }


    protected Binding computeNextActual() throws IOException {
        Binding result;
        outer: while (true) {
            switch (state) {
            case INIT:
                reader.beginObject();
                state = State.ROOT;
                continue outer;

            case ROOT:
                while (reader.hasNext()) {
                    String topLevelName = reader.nextName();
                    switch (topLevelName) {
                    case JSONResultsKW.kHead:
                        resultVars = parseHead();
                        break;
                    case JSONResultsKW.kResults:
                        reader.beginObject();
                        state = State.RESULTS;
                        continue outer;
                    case JSONResultsKW.kBoolean:
                        askResult = reader.nextBoolean();
                        continue outer;
                    default:
                        onUnexpectedJsonElement();
                        break;
                    }
                }
                reader.endObject();
                state = State.DONE;
                continue outer;

            case RESULTS:
                while (reader.hasNext()) {
                    String elt = reader.nextName();
                    switch (elt) {
                    case JSONResultsKW.kBindings:
                        reader.beginArray();
                        state = State.BINDINGS;
                        continue outer;
                    default:
                        onUnexpectedJsonElement();
                        break;
                    }
                }
                reader.endObject();
                state = State.ROOT;
                break;

            case BINDINGS:
                while (reader.hasNext()) {
                    result = parseBinding(gson, reader, labelMap, onUnknownRdfTermType);
                    ++rowNumber;
                    break outer;
                }
                reader.endArray();
                state = State.RESULTS;
                break;

            case DONE:
                result = null; // endOfData();
                break outer;
            }
        }

        return result;
    }

    protected List<Var> parseHead() throws IOException {
        List<Var> result = null;

        reader.beginObject();
        String n = reader.nextName();
        switch (n) {
        case JSONResultsKW.kVars:
            List<String> varNames = gson.fromJson(reader, new TypeToken<List<String>>() {}.getType());
            result = Var.varList(varNames);
            break;
        default:
            onUnexpectedJsonElement();
            break;
        }
        reader.endObject();

        return result;
    }

    public Boolean getAskResult() {
        return askResult;
    }

    @Override
    public long getRowNumber() {
        return rowNumber;
    }

    @Override
    public void closeIterator() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new JenaException(e);
        }
    }

    @Override
    protected boolean hasMore() {
        return true;
    }


    public static Node parseOneTerm(JsonObject json, LabelToNode labelMap, Function<JsonObject, Node> onUnknownRdfTermType) {
        Node result;

        String type = json.get(JSONResultsKW.kType).getAsString();
        JsonElement valueJson = json.get(JSONResultsKW.kValue);
        String valueStr;
        switch (type) {
        case JSONResultsKW.kUri:
            valueStr = valueJson.getAsString();
            result = NodeFactory.createURI(valueStr);
            break;
        case JSONResultsKW.kTypedLiteral: /* Legacy */
        case JSONResultsKW.kLiteral:
            valueStr = valueJson.getAsString();
            JsonElement langJson = json.get(JSONResultsKW.kXmlLang);
            JsonElement dtJson = json.get(JSONResultsKW.kDatatype);
            result = NodeFactoryExtra.createLiteralNode(
                    valueStr,
                    langJson == null ? null : langJson.getAsString(),
                    dtJson == null ? null : dtJson.getAsString());
            break;
        case JSONResultsKW.kBnode:
            valueStr = valueJson.getAsString();
            result = labelMap.get(null, valueStr);
            break;
        case JSONResultsKW.kTriple:
            JsonObject tripleJson = valueJson.getAsJsonObject();
            Node s = parseOneTerm(tripleJson.get(JSONResultsKW.kSubject).getAsJsonObject(), labelMap, onUnknownRdfTermType);
            Node p = parseOneTerm(tripleJson.get(JSONResultsKW.kPredicate).getAsJsonObject(), labelMap, onUnknownRdfTermType);
            Node o = parseOneTerm(tripleJson.get(JSONResultsKW.kObject).getAsJsonObject(), labelMap, onUnknownRdfTermType);
            result = NodeFactory.createTripleNode(new Triple(s, p, o));
            break;
        default:
            if (onUnknownRdfTermType != null) {
                result = onUnknownRdfTermType.apply(json);
                Objects.requireNonNull(result, "Custom handler returned null for unknown rdf term type '" + type + "'");
            } else {
                throw new IllegalStateException("Unknown rdf term type: " + type);
            }
            break;
        }

        return result;
    }

    public static Binding parseBinding(
            Gson gson, JsonReader reader, LabelToNode labelMap,
            Function<JsonObject, Node> onUnknownRdfTermType) throws IOException {
        JsonObject obj = gson.fromJson(reader, JsonObject.class);

        BindingBuilder bb = BindingFactory.builder();

        for (Entry<String, JsonElement> e : obj.entrySet()) {
            Var v = Var.alloc(e.getKey());
            JsonObject nodeObj = e.getValue().getAsJsonObject();

            Node node = parseOneTerm(nodeObj, labelMap, onUnknownRdfTermType);
            bb.add(v, node);
        }

        return bb.build();
    }

}
