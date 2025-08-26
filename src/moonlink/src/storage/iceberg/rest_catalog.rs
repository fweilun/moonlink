use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::CatalogBuilder;
use iceberg::Result as IcebergResult;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog as IcebergRestCatalog, RestCatalogBuilder as IcebergRestCatalogBuilder,
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct RestCatalog {
    catalog: IcebergRestCatalog,
}

impl RestCatalog {
    #[allow(dead_code)]
    pub async fn new(
        builder: IcebergRestCatalogBuilder,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> Self {
        Self {
            catalog: builder.load(name, props).await.unwrap(),
        }
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
        self.catalog.create_table(namespace_ident, creation).await
    }

    async fn load_table(&self, table_ident: &TableIdent) -> IcebergResult<Table> {
        self.catalog.load_table(table_ident).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.catalog.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.catalog.table_exists(table).await
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
