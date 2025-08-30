#[cfg(test)]
mod tests {
    use crate::storage::iceberg::rest_catalog::RestCatalog;
    use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
    use crate::storage::iceberg::rest_catalog_test_utils::*;
    use iceberg::{Catalog, NamespaceIdent};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_create_table() {
        let namespace = get_random_string();
        let table = get_random_string();
        let _guard = RestCatalogTestGuard::new(namespace.clone(), None)
            .await
            .expect(&format!(
                "Rest catalog test guard creation fail, namespace={}",
                namespace
            ));
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config)
            .await
            .expect("Catalog creation fail");
        let namespace = NamespaceIdent::new(namespace);
        let table_creation = default_table_creation(table);
        catalog
            .create_table(&namespace, table_creation)
            .await
            .expect(&format!(
                "Table creation fail, namespace={} table=",
                namespace, table
            ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_table() {
        let namespace = get_random_string();
        let table = get_random_string();
        let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
            .await
            .expect(&format!(
                "Rest catalog test guard creation fail, namespace={}",
                namespace
            ));
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config)
            .await
            .expect("Catalog creation fail");
        let table_ident = guard.table.clone().unwrap();
        assert!(catalog.table_exists(&table_ident).await.expect(&format!(
            "Table exist function fail, namespace={} table=",
            namespace, table
        )));
        catalog.drop_table(&table_ident).await.expect(&format!(
            "Table creation fail, namespace={} table=",
            namespace, table
        ));
        assert!(!catalog.table_exists(&table_ident).await.expect(&format!(
            "Table exist function fail, namespace={} table=",
            namespace, table
        )));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_exists() {
        let namespace = get_random_string();
        let table = get_random_string();
        let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
            .await
            .expect(&format!(
                "Rest catalog test guard creation fail, namespace={}",
                namespace
            ));
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config)
            .await
            .expect("Catalog creation fail");
        let table_ident = guard.table.clone().unwrap();
        assert!(catalog.table_exists(&table_ident).await.expect(&format!(
            "Table exist function fail, namespace={} table=",
            namespace, table
        )));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_load_table() {
        let namespace = get_random_string();
        let table = get_random_string();
        let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
            .await
            .expect(&format!(
                "Rest catalog test guard creation fail, namespace={}",
                namespace
            ));
        let config = default_rest_catalog_config();
        let catalog = RestCatalog::new(config)
            .await
            .expect("Catalog creation fail");
        let table_ident = guard.table.clone().unwrap();
        let result = catalog.load_table(&table_ident).await.expect(&format!(
            "Load table function fail, namespace={} table=",
            namespace, table
        ));
        let result_table_ident = result.identifier().clone();
        assert!(table_ident == result_table_ident);
    }
}
