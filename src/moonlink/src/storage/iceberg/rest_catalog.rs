use super::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::moonlink_catalog::{PuffinBlobType, PuffinWrite, SchemaUpdate};
// use aws_sdk_s3::{Client, types::ByteStream};
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;

use futures::future::join_all;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
/// This module contains the file-based catalog implementation, which relies on version hint file to decide current version snapshot.
/// Despite a few limitation (i.e. atomic rename for local filesystem), it's not a problem for moonlink, which guarantees at most one writer at the same time (for nows).
/// It leverages `opendal` and iceberg `FileIO` as an abstraction layer to operate on all possible storage backends.
///
/// Iceberg table format from object storage's perspective:
/// - namespace_indicator.txt
///   - An empty file, indicates it's a valid namespace
/// - data
///   - parquet files
/// - metadata
///   - version hint file
///     + version-hint.text
///     + contains the latest version number for metadata
///   - metadata file
///     + v0.metadata.json ... vn.metadata.json
///     + records table schema
///   - snapshot file / manifest list
///     + snap-<snapshot-id>-<attempt-id>-<commit-uuid>.avro
///     + points to manifest files and record actions
///   - manifest files
///     + <commit-uuid>-m<manifest-counter>.avro
///     + which points to data files and stats
///
/// TODO(hjiang):
/// 1. Before release we should support not only S3, but also R2, GCS, etc; necessary change should be minimal, only need to setup configuration like secret id and secret key.
/// 2. Add integration test to actual object storage before pg_mooncake release.
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;
use tokio::sync::Mutex;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{
    Schema as IcebergSchema, TableMetadata, TableMetadataBuildResult, TableMetadataBuilder,
};
use iceberg::table::Table;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};
use iceberg::{Error as IcebergError, TableRequirement};

/// Object storage usually doesn't have "folder" concept, when creating a new namespace, we create an indicator file under certain folder.
pub(super) const NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";
/// Metadata directory, which stores all metadata files, including manifest files, metadata files, version hint files, etc.
pub(super) const METADATA_DIRECTORY: &str = "metadata";
/// Version hint file which indicates the latest version for the table, the file exists for all valid iceberg tables.
pub(super) const VERSION_HINT_FILENAME: &str = "version-hint.text";

#[derive(Debug)]
pub struct RestCatalog {
    // client: 
    /// Filesystem operator.
    filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    /// Similar to opendal operator, which also provides an abstraction above different storage backends.
    file_io: FileIO,
    /// Table location.
    warehouse_location: String,
    /// Used to overwrite iceberg metadata at table creation.
    iceberg_schema: Option<IcebergSchema>,
    /// Used for atomic write to commit transaction.
    etag: Mutex<RefCell<Option<String>>>,
    /// Used to record puffin blob metadata in one transaction, and cleaned up after transaction commits.
    ///
    /// Maps from "puffin filepath" to "puffin blob metadata".
    deletion_vector_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    file_index_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    /// A vector of "puffin filepath"s.
    puffin_blobs_to_remove: HashSet<String>,
    /// A set of data files to remove, along with their corresponding deletion vectors and file indices.
    data_files_to_remove: HashSet<String>,
}

impl RestCatalog {
        fn get_iceberg_table_metadata(
        &self,
        table_metadata: TableMetadataBuildResult,
    ) -> IcebergResult<TableMetadata> {
        let metadata = table_metadata.metadata;
        let mut metadata_builder = metadata.into_builder(/*current_file_location=*/ None);
        metadata_builder =
            metadata_builder.add_current_schema(self.iceberg_schema.as_ref().unwrap().clone())?;
        let new_table_metadata = metadata_builder.build()?;
        Ok(new_table_metadata.metadata)
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        todo!("list namespaces is not supported");
    }
    async fn create_namespace(
        &self,
        _namespace_ident: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        todo!("create namespace is not supported");
    }

    async fn get_namespace(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        todo!("get namespace is not supported");
    }

    async fn namespace_exists(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        todo!("namespace exists is not supported");
    }

    async fn drop_namespace(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        todo!("drop namespace is not supported");
    }

    async fn list_tables(
        &self,
        _namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        todo!("list tables is not supported");
    }

    async fn update_namespace(
        &self,
        _namespace_ident: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!("Update namespace is not supported");
    }
    
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let directory = namespace_ident.to_url_string();
        let table_ident = TableIdent::new(namespace_ident.clone(), creation.name.clone());

        // Create version hint file.
        let version_hint_filepath = format!(
            "{}/{}/{}/version-hint.text",
            directory, creation.name, METADATA_DIRECTORY,
        );
        self.filesystem_accessor
            .write_object(
                &version_hint_filepath,
                /*content=*/ "0".as_bytes().to_vec(),
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table {} creation under namespace {:?} ", creation.name, namespace_ident),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        // Create metadata file.
        let metadata_filepath = format!(
            "{}/{}/metadata/v0.metadata.json",
            directory,
            creation.name.clone()
        );

        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        let metadata = self.get_iceberg_table_metadata(table_metadata)?;
        let metadata_json = serde_json::to_vec(&metadata)?;
        self.filesystem_accessor
            .write_object(&metadata_filepath, /*content=*/ metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file {metadata_filepath} at table creation"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        let table = Table::builder()
            .metadata(metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    async fn load_table(&self, _table_ident: &TableIdent) -> IcebergResult<Table> {
        todo!("load table is not supported");
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        todo!("drop table is not supported");
    }

    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        todo!("table exist is not supported");
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!("rename table is not supported");
    }

    async fn update_table(&self, mut _commit: TableCommit) -> IcebergResult<Table> {
        todo!("update table is not supported");
    }

    async fn register_table(
        &self,
        __table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<Table> {
        todo!("register existing table is not supported")
    }
}
