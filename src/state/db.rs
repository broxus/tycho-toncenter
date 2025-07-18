use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::{Connection, OpenFlags};
use tokio::sync::{mpsc, oneshot};

pub type SqlitePool = bb8::Pool<SqliteConnectionManager>;
pub type SqlitePoolConnection<'a> = bb8::PooledConnection<'a, SqliteConnectionManager>;

pub struct SqliteConnectionManager {
    path: PathBuf,
    flags: OpenFlags,
    init: Option<Box<InitFn>>,
}

impl SqliteConnectionManager {
    pub fn file<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            flags: Default::default(),
            init: None,
        }
    }

    #[allow(unused)]
    pub fn with_flags(mut self, flags: OpenFlags) -> Self {
        self.flags = flags;
        self
    }

    #[allow(unused)]
    pub fn with_init<F>(mut self, init: F) -> Self
    where
        F: Fn(&Connection) -> rusqlite::Result<()> + Send + Sync + 'static,
    {
        self.init = Some(Box::new(init));
        self
    }
}

type InitFn = dyn Fn(&Connection) -> rusqlite::Result<()> + Send + Sync + 'static;

impl bb8::ManageConnection for SqliteConnectionManager {
    type Connection = Connection;
    type Error = rusqlite::Error;

    async fn connect(&self) -> rusqlite::Result<Self::Connection> {
        tracing::trace!(db = %self.path.display(), "opening a new connection");

        // TODO: Consume task budget instead?
        tokio::task::yield_now().await;
        let mut conn = Connection::open_with_flags(&self.path, self.flags)?;
        if let Some(init) = &self.init.as_deref() {
            init(&mut conn)?;
        }

        tracing::trace!(db = %self.path.display(), "opened a new connection");
        Ok(conn)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> rusqlite::Result<()> {
        // TODO: Consume task budget instead?
        tokio::task::yield_now().await;
        conn.execute_batch("")
            .inspect_err(|e| tracing::trace!("db check failed: {e:?}"))
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct SqliteDispatcher {
    tx: QueryTx,
}

impl SqliteDispatcher {
    pub fn spawn(mut connection: Connection) -> Self {
        static ID: AtomicUsize = AtomicUsize::new(0);

        let id = ID.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel::<Query>(1);
        std::thread::spawn(move || {
            tracing::debug!(id, "sqlite dispatcher started");
            scopeguard::defer! {
                tracing::debug!(id, "sqlite dispatcher finished");
            }

            while let Some(f) = rx.blocking_recv() {
                f(&mut connection);
            }
        });

        Self { tx }
    }

    pub async fn dispatch<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Connection) -> R + Send + 'static,
        R: Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let query = Box::new(move |conn: &mut Connection| {
            tx.send(f(conn)).ok();
        });

        if self.tx.send(query).await.is_ok() {
            if let Ok(res) = rx.await {
                return res;
            }
        }

        unreachable!("receiver thread cannot be dropped while `self.tx` is still alive");
    }

    pub fn dispatch_blocking<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Connection) -> R + Send + 'static,
        R: Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let query = Box::new(move |conn: &mut Connection| {
            tx.send(f(conn)).ok();
        });

        if self.tx.blocking_send(query).is_ok() {
            if let Ok(res) = rx.blocking_recv() {
                return res;
            }
        }

        unreachable!("receiver thread cannot be dropped while `self.tx` is still alive");
    }
}

// === Statement Helpers ===

pub trait StatementExt<'conn>: Sized {
    fn unbounded_query<P: rusqlite::Params>(
        self,
        params: P,
    ) -> rusqlite::Result<UnboundedRows<'conn>>;

    #[inline]
    fn unbounded_query_map<P: rusqlite::Params, T, F>(
        self,
        params: P,
        f: F,
    ) -> rusqlite::Result<MappedUnboundedRows<'conn, F>>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.unbounded_query(params).map(|rows| rows.map(f))
    }
}

impl<'conn> StatementExt<'conn> for rusqlite::Statement<'conn> {
    #[inline]
    fn unbounded_query<P: rusqlite::Params>(
        self,
        params: P,
    ) -> rusqlite::Result<UnboundedRows<'conn>> {
        UnboundedRows::query(Box::new(self), params)
    }
}

#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct MappedUnboundedRows<'conn, F> {
    inner: UnboundedRows<'conn>,
    map: F,
}

impl<T, F> Iterator for MappedUnboundedRows<'_, F>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    type Item = rusqlite::Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;

        self.inner
            .rows
            .next()
            .transpose()
            .map(|row_result| row_result.and_then(map))
    }
}

pub struct UnboundedRows<'conn> {
    rows: ManuallyDrop<rusqlite::Rows<'static>>,
    #[expect(unused)]
    stmt: Box<rusqlite::Statement<'conn>>,
}

impl<'conn> UnboundedRows<'conn> {
    pub fn query<P: rusqlite::Params>(
        mut stmt: Box<rusqlite::Statement<'conn>>,
        params: P,
    ) -> rusqlite::Result<Self> {
        let rows = stmt.query(params)?;

        Ok(Self {
            // SAFETY: `rows` will be manually dropped before the `statement`.
            rows: ManuallyDrop::new(unsafe {
                std::mem::transmute::<rusqlite::Rows<'_>, rusqlite::Rows<'static>>(rows)
            }),
            stmt,
        })
    }

    #[inline]
    pub fn map<T, F>(self, map: F) -> MappedUnboundedRows<'conn, F>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        MappedUnboundedRows { inner: self, map }
    }
}

impl Drop for UnboundedRows<'_> {
    fn drop(&mut self) {
        // SAFETY: `rows` and `statement` are dropped only once.
        unsafe { ManuallyDrop::drop(&mut self.rows) }
    }
}

type QueryTx = mpsc::Sender<Query>;
type Query = Box<dyn FnOnce(&mut Connection) + Send>;
