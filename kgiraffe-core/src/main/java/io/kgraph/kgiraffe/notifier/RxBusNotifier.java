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

package io.kgraph.kgiraffe.notifier;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import io.vavr.Tuple2;
import org.ojai.Document;

public class RxBusNotifier implements Notifier {
    private final Subject<Object> bus;

    public RxBusNotifier() {
        bus = PublishSubject.create().toSerialized();
    }

    public void publish(String address, Document doc) {
        bus.onNext(new Tuple2<>(address, doc));
    }

    @SuppressWarnings("unchecked")
    public Flowable<Document> consumer(String address) {
        return bus.toFlowable(BackpressureStrategy.BUFFER)
            .filter(o -> ((Tuple2<String, Document>) o)._1.equals(address))
            .map(o -> ((Tuple2<String, Document>) o)._2);
    }
}