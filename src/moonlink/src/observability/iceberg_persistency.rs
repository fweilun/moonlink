use crate::observability::latency_exporter::BaseLatencyExporter;
use crate::observability::latency_guard::LatencyGuard;
use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};

#[derive(Debug, Clone, Copy)]
pub(crate) enum IcebergPersistentStage {
    DataFiles,
    FileIndices,
    DeletionVectors,
}

#[derive(Debug)]
pub(crate) struct IcebergPersistencyStats {
    pub(crate) mooncake_table_id: String,
    latency: Histogram<u64>,
}

impl IcebergPersistencyStats {
    pub(crate) fn new(mooncake_table_id: String, stats_type: IcebergPersistentStage) -> Self {
        let meter = global::meter("iceberg_persistency");
        let latency = match stats_type {
            IcebergPersistentStage::DataFiles => meter
                .u64_histogram("load_data_files_latency")
                .with_description("Latency (ms) for loading data files")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            IcebergPersistentStage::FileIndices => meter
                .u64_histogram("load_file_indices_latency")
                .with_description("Latency (ms) for loading file indices")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            IcebergPersistentStage::DeletionVectors => meter
                .u64_histogram("load_deletion_vectors_latency")
                .with_description("Latency (ms) for loading deletion vectors")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        };

        Self {
            mooncake_table_id,
            latency,
        }
    }
}

impl BaseLatencyExporter for IcebergPersistencyStats {
    fn start<'a>(&'a self) -> LatencyGuard<'a> {
        LatencyGuard::new(self.mooncake_table_id.clone(), self)
    }

    fn record(&self, latency: std::time::Duration, mooncake_table_id: String) {
        self.latency.record(
            latency.as_millis() as u64,
            &[KeyValue::new(
                "moonlink.mooncake_table_id",
                mooncake_table_id,
            )],
        );
    }
}
