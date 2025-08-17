use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSourceError, TableCopyStreamError,
};
use crate::rest_ingest::rest_source::RestSourceError;
use crate::rest_ingest::SrcTableId;
use moonlink::Error as MoonlinkError;
use moonlink_error::{ErrorStatus, ErrorStruct};
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0}")]
    PostgresSourceError(ErrorStruct),

    #[error("{0}")]
    TokioPostgres(ErrorStruct),

    #[error("{0}")]
    CdcStream(ErrorStruct),

    #[error("{0}")]
    TableCopyStream(ErrorStruct),

    #[error("{0}")]
    MoonlinkError(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    // Requested database table not found.
    #[error("Table {0} not found")]
    TableNotFound(String),

    // Invalid source type for operation.
    #[error("Invalid source type: {0}")]
    InvalidSourceType(String),

    // REST API error.
    #[error("REST API error: {0}")]
    RestApi(String),

    // REST source error.
    #[error("REST source error: {source}")]
    RestSource { source: Arc<RestSourceError> },

    /// REST source error: duplicate source table to add.
    #[error("REST source error: duplicate source table to add with table id {0}")]
    RestDuplicateTable(SrcTableId),

    /// REST source error: non-existent source table to remove.
    #[error("REST source error: non-existent source table to remove with table id {0}")]
    RestNonExistentTable(SrcTableId),
}

pub type Result<T> = result::Result<T, Error>;

impl From<MoonlinkError> for Error {
    #[track_caller]
    fn from(source: MoonlinkError) -> Self {
        let status = match &source {
            MoonlinkError::Arrow(es)
            | MoonlinkError::Io(es)
            | MoonlinkError::Parquet(es)
            | MoonlinkError::WatchChannelRecvError(es)
            | MoonlinkError::IcebergError(es)
            | MoonlinkError::OpenDal(es)
            | MoonlinkError::JoinError(es)
            | MoonlinkError::Json(es) => es.status,
        };
        Error::MoonlinkError(ErrorStruct {
            message: format!("Moonlink source error"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<PostgresSourceError> for Error {
    #[track_caller]
    fn from(source: PostgresSourceError) -> Self {
        Error::PostgresSourceError(ErrorStruct {
            message: format!("Postgres source error"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<TokioPostgresError> for Error {
    #[track_caller]
    fn from(source: TokioPostgresError) -> Self {
        Error::TokioPostgres(ErrorStruct {
            message: format!("tokio postgres error"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<CdcStreamError> for Error {
    #[track_caller]
    fn from(source: CdcStreamError) -> Self {
        Error::CdcStream(ErrorStruct {
            message: format!("Postgres cdc stream error"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<TableCopyStreamError> for Error {
    #[track_caller]
    fn from(source: TableCopyStreamError) -> Self {
        Error::TableCopyStream(ErrorStruct {
            message: format!("Table copy stream error"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<RestSourceError> for Error {
    fn from(source: RestSourceError) -> Self {
        Error::RestSource {
            source: Arc::new(source),
        }
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let status = match source.kind() {
            std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NetworkDown
            | std::io::ErrorKind::ResourceBusy
            | std::io::ErrorKind::QuotaExceeded => ErrorStatus::Temporary,

            _ => ErrorStatus::Permanent,
        };

        Error::Io(ErrorStruct {
            message: format!("IO error"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    #[track_caller]
    fn from(source: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::Io(ErrorStruct {
            message: format!("Channel send error"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}
