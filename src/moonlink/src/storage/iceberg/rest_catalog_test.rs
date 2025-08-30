#[cfg(feature = "rest-catalog")]
#[cfg(test)]
mod tests {
    use crate::storage::iceberg::rest_catalog::RestCatalog;
    use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
    use crate::storage::iceberg::rest_catalog_test_utils::*;
    use iceberg::{Catalog, NamespaceIdent};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_create_table() {
        let namespace = random_string();
        let table = random_string();
        let _guard = RestCatalogTestGuard::new(namespace.clone(), None)
            .await
            .expect("Namespace creation fail");
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config).await;
        let namespace = NamespaceIdent::new(namespace);
        let table_creation = default_table_creation(table);
        catalog
            .create_table(&namespace, table_creation)
            .await
            .expect("Table creation fail");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_table() {
        let namespace = random_string();
        let table = random_string();
        let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
            .await
            .expect("Environment setup fail");
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config).await;
        let table_ident = guard.table.clone().unwrap();
        assert!(catalog
            .table_exists(&table_ident)
            .await
            .expect("Table exist function fail"));
        catalog
            .drop_table(&table_ident)
            .await
            .expect("Table creation fail");
        assert!(!catalog
            .table_exists(&table_ident)
            .await
            .expect("Table exist function fail"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_exists() {
        let namespace = random_string();
        let table = random_string();
        let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
            .await
            .expect("Environment setup fail");
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config).await;
        let table_ident = guard.table.clone().unwrap();
        assert!(catalog
            .table_exists(&table_ident)
            .await
            .expect("Table exist function fail"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_load_table() {
        let namespace = random_string();
        let table = random_string();
        let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
            .await
            .expect("Environment setup fail");
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config).await;
        let table_ident = guard.table.clone().unwrap();
        let result = catalog
            .load_table(&table_ident)
            .await
            .expect("Table exist function fail");
        let result_table_ident = result.identifier().clone();
        assert!(table_ident == result_table_ident);
    }
}
