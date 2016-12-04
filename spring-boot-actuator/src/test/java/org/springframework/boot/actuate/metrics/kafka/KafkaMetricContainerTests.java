/*
 * Copyright 2012-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.boot.actuate.metrics.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;

/**
 * Tests for {@link KafkaMetricContainer}.
 *
 * @author Igor Stepanov
 */
public class KafkaMetricContainerTests {

	private Random random;
	private String metricPrefix;
	private String metricGroup;
	private String metricName;

	public KafkaMetricContainerTests() {
		this.random = new Random();
		this.metricPrefix = "first_dummy_value";
		this.metricGroup = "second_dummy_value";
		this.metricName = "third_dummy_value";
	}

	/**
	 * Getter returns the same instance as was set to constructor.
	 */
	@Test
	public void getValue_withKafkaMetricContainer_same() {
		Metric metric = randomMetric();
		KafkaMetricContainer metricContainer = new KafkaMetricContainer(metric, "test");
		assertThat(metricContainer.getValue()).isSameAs(metric);
	}

	/**
	 * Name is calculated once (upon constructing the object) and then the same instance is returned each time.
	 */
	@Test
	public void getMetricName_withKafkaMetricContainer_same() {
		KafkaMetricContainer metricContainer = randomKafkaMetricContainer();
		String name = metricContainer.getMetricName();
		assertThat(metricContainer.getMetricName()).isSameAs(name);
	}

	/**
	 * Name is calculated once (upon constructing the object) and then the same instance is returned each time.
	 */
	@Test
	public void metricName_withConstantValues() {
		assertThat(randomKafkaMetricContainer().getMetricName())
				.isEqualTo(this.metricPrefix + '.' + this.metricGroup + '.' + this.metricName);
	}

	/**
	 * Tags' values are used in metric's name, ordered by the appropriate keys.
	 */
	@Test
	public void metricName_withConstantValuesAndTags() {
		final String tag1 = "some_dummy_key";
		final String tag2 = "another_dummy_key";
		final String value1 = "some_dummy_val";
		final String value2 = "another_dummy_val";
		final Map<String, String> tags = new HashMap<String, String>() {{
			put(tag1, value1);
			put(tag2, value2);
		}};
		assertThat(randomKafkaMetricContainer(new MetricName(this.metricName, this.metricGroup, "Test metric", tags))
				.getMetricName())
				.isEqualTo(this.metricPrefix + '.' + value2 + '.' + value1 + '.' + this.metricGroup + '.' + this.metricName);
	}

	/**
	 * Name's calculation fails for null value of {@link Metric}.
	 * Checks that contract was not changed in Kafka - Metric cannot be initialized with null.
	 */
	@Test
	public void metricName_withNull_NullPointerException() {
		assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
			@Override
			public void call() throws Throwable {
				randomKafkaMetricContainer().metricName(null);
			}
		}).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
			@Override
			public void call() throws Throwable {
				new MetricName(null, "dummy2", "dummy3", Collections.emptyMap());
			}
		}).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
			@Override
			public void call() throws Throwable {
				new MetricName("dummy1", null, "dummy3", Collections.emptyMap());
			}
		}).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
			@Override
			public void call() throws Throwable {
				new MetricName("dummy1", "dummy2", null, Collections.emptyMap());
			}
		}).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
			@Override
			public void call() throws Throwable {
				new MetricName("dummy1", "dummy2", "dummy3", (Map<String, String>) null);
			}
		}).isInstanceOf(NullPointerException.class);
	}

	protected MetricName constMetricName() {
		return new MetricName(this.metricName, this.metricGroup, "Random metric", Collections.emptyMap());
	}

	protected Metric randomMetric() {
		return randomMetric(constMetricName());
	}

	protected Metric randomMetric(MetricName name) {
		Metric metric = mock(Metric.class);
		given(metric.value()).willReturn(this.random.nextDouble());
		given(metric.metricName()).willReturn(name);
		return metric;
	}

	protected KafkaMetricContainer randomKafkaMetricContainer(MetricName name) {
		return new KafkaMetricContainer(randomMetric(name), this.metricPrefix);
	}

	protected KafkaMetricContainer randomKafkaMetricContainer() {
		return randomKafkaMetricContainer(constMetricName());
	}
}
