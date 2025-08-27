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
        namespace_ident: &iceberg::NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        self.catalog
            .create_namespace(namespace_ident, properties)
            .await
    }

    async fn get_namespace(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        todo!("get namespace is not supported");
    }

    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        self.catalog.namespace_exists(namespace_ident).await
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

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use reqwest::Client;

    use super::*;
    const REST_CATALOG_PROP_URI: &str = "uri";

    #[cfg(feature = "storage-gcs")]
    async fn ensure_namespace(catalog: &IcebergRestCatalog, ns: &str) {
        let ns_ident = NamespaceIdent::from_vec(vec![ns.to_string()]).unwrap();
        let _ = catalog.create_namespace(&ns_ident, HashMap::new()).await;
    }

    #[cfg(feature = "storage-gcs")]
    #[tokio::test]
    async fn test_create_table() {
        let builder = IcebergRestCatalogBuilder::default().with_client(Client::new());
        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                )]),
            )
            .await;

        let binding = catalog.unwrap();
        ensure_namespace(&binding, "ns1").await;

        let builder = IcebergRestCatalogBuilder::default().with_client(Client::new());
        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                )]),
            )
            .await;
        assert!(catalog.is_ok());
        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .build()
                    .unwrap(),
            )
            .build();

        let namespace = NamespaceIdent::new("ns1".to_string());
        let result = catalog
            .unwrap()
            .create_table(&namespace, table_creation)
            .await;
        match result {
            Ok(_) => {}
            Err(e) if e.to_string().contains("The table already exists") => {}
            Err(e) => {
                panic!("create_table failure: {e:?}")
            }
        }
    }

    #[cfg(feature = "storage-gcs")]
    #[tokio::test]
    async fn test_drop_table() {
        let builder = IcebergRestCatalogBuilder::default().with_client(Client::new());
        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                )]),
            )
            .await;
        assert!(catalog.is_ok());

        let result = catalog
            .unwrap()
            .drop_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await;
        match result {
            Ok(_) => {}
            Err(e)
                if e.to_string()
                    .contains("Tried to drop a table that does not exist") => {}
            Err(e) => {
                panic!("drop_table failure: {e:?}")
            }
        }
    }

    #[cfg(feature = "storage-gcs")]
    #[tokio::test]
    async fn test_load_table() {
        let builder = IcebergRestCatalogBuilder::default().with_client(Client::new());
        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                )]),
            )
            .await;

        assert!(catalog.is_ok());
        let result = catalog
            .unwrap()
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await;
        match result {
            Ok(_) => {}
            Err(e) if e.to_string().contains("does not exist") => {}
            Err(e) => {
                panic!("load table error: {e:?}");
            }
        }
    }

    #[cfg(feature = "storage-gcs")]
    #[tokio::test]
    async fn test_table_exists() {
        let builder = IcebergRestCatalogBuilder::default().with_client(Client::new());
        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                )]),
            )
            .await;
        assert!(catalog.is_ok());
        let result = catalog
            .unwrap()
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await;

        match result {
            Ok(_) => {}
            Err(e) if e.to_string().contains("does not exist") => {}
            Err(e) => {
                panic!("table exist error: {e:?}");
            }
        }
    }
}
