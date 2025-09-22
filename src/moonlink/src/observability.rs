use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};
use std::sync::Arc;

pub struct SnapshotCreationStats {
    latency_hist: Histogram<f64>,
}

impl SnapshotCreationStats {
    pub fn new() -> Arc<Self> {
        let meter = global::meter("snapshot_creation");
        Arc::new(SnapshotCreationStats {
            latency_hist: meter
                .f64_histogram("snapshot_creation_latency")
                .with_description("snapshot create latency histogram (second)")
                .with_boundaries(vec![0.0, 0.2, 0.5, 0.8, 1.0, 2.0, 5.0])
                .build(),
        })
    }

    pub fn update(&self, t: f64) {
        self.latency_hist
            .record(t, &[KeyValue::new("moonlink.mooncake_table_id", "id")]);
    }
}
