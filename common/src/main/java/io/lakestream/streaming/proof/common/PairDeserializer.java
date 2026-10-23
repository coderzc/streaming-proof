/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** Reads the single-entry object written by Jackson's standard Map.Entry serializer for Pair. */
final class PairDeserializer extends StdDeserializer<Pair<?, ?>> implements ContextualDeserializer {
    private final JavaType entryType;

    PairDeserializer() {
        this(null);
    }

    private PairDeserializer(JavaType entryType) {
        super(Pair.class);
        this.entryType = entryType;
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext context, BeanProperty property) {
        JavaType type = context.getContextualType();
        JavaType objectType = context.constructType(Object.class);
        return new PairDeserializer(context.getTypeFactory().constructMapType(LinkedHashMap.class,
                type == null ? objectType : type.containedTypeOrUnknown(0),
                type == null ? objectType : type.containedTypeOrUnknown(1)));
    }

    @Override
    public Pair<?, ?> deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        Map<?, ?> entry = context.readValue(parser, entryType);
        if (entry.size() != 1) {
            return (Pair<?, ?>) context.handleUnexpectedToken(Pair.class, parser);
        }
        var value = entry.entrySet().iterator().next();
        return Pair.of(value.getKey(), value.getValue());
    }
}
