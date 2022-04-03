/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kgraph.kgiraffe.server.notifier;

import io.kgraph.kgiraffe.notifier.Notifier;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.rxjava3.FlowableHelper;
import org.ojai.Document;

public class VertxNotifier implements Notifier {
    private final EventBus eventBus;
    private final DeliveryOptions options;

    public VertxNotifier(EventBus eventBus) {
        this.eventBus = eventBus.registerCodec(new KryoCodec<Document>());
        this.options = new DeliveryOptions().setCodecName("kryo");
    }

    public void publish(String address, Document doc) {
        eventBus.publish(address, doc, options);
    }

    public Flowable<Document> consumer(String address) {
        return FlowableHelper.toFlowable(eventBus.consumer(address))
            .map(m -> (Document) m.body());
    }
}
