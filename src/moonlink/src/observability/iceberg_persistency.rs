use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};

#[derive(Debug)]
pub(crate) struct IcebergPersistencyStats {
    data_file_latency: Histogram<u64>,
    file_indices_latency: Histogram<u64>,
    deletion_vectors_latency: Histogram<u64>,
}

impl IcebergPersistencyStats {
    pub(crate) fn new() -> Self {
        let meter = global::meter("iceberg_persistency_latency");
        IcebergPersistencyStats {
            data_file_latency: meter
                .u64_histogram("load_data_files_latency")
                .with_description("Latency (ms) for loading data files")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            file_indices_latency: meter
                .u64_histogram("load_file_indices_latency")
                .with_description("Latency (ms) for loading file indices")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            deletion_vectors_latency: meter
                .u64_histogram("load_deletion_vectors_latency")
                .with_description("Latency (ms) for loading deletion vectors")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        }
    }

    pub(crate) fn update_data_file_latency(
        &self,
        should_record: bool,
        data_file_latency: u64,
        mooncake_table_id: String,
    ) {
        if !should_record {
            return;
        }
        let attrs = [KeyValue::new(
            "moonlink.mooncake_table_id",
            mooncake_table_id,
        )];
        self.data_file_latency.record(data_file_latency, &attrs);
    }

    pub(crate) fn update_file_indices_latency(
        &self,
        should_record: bool,
        file_indices_latency: u64,
        mooncake_table_id: String,
    ) {
        if !should_record {
            return;
        }
        let attrs = [KeyValue::new(
            "moonlink.mooncake_table_id",
            mooncake_table_id,
        )];
        self.file_indices_latency
            .record(file_indices_latency, &attrs);
    }

    pub(crate) fn update_deletion_vectors_latency(
        &self,
        should_record: bool,
        deletion_vectors_latency: u64,
        mooncake_table_id: String,
    ) {
        if !should_record {
            return;
        }
        let attrs = [KeyValue::new(
            "moonlink.mooncake_table_id",
            mooncake_table_id,
        )];
        self.deletion_vectors_latency
            .record(deletion_vectors_latency, &attrs);
    }
}
