use anyhow;
use bincode::{DefaultOptions, Options};
use byteorder::{BigEndian, WriteBytesExt};
use core::task::{Context, Poll};
use futures::{
    future,
    future::Either,
    pin_mut, ready,
    stream::{self, SelectAll},
    Future, Stream, StreamExt,
};
use log::debug;
pub use manifold::Manifold;
use pin_project::pin_project;
use serde::{de::DeserializeOwned, Serialize};
pub use sled;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

pub mod manifold;

#[derive(Clone, Debug)]
pub struct Shed {
    pub db: sled::Db,
    tree: sled::Tree,
}

#[derive(Clone, Debug)]
pub struct Store(pub sled::Tree);

impl Shed {
    pub fn new(name: &str, config: &Config) -> Self {
        let db = sled::open(config.db_path()).expect("db open/create");
        let tree = db.open_tree(name).expect("open tree of db");
        Shed { db, tree }
    }

    /*
     * Continuously write to the DB using a stream that generates both the key and the value
     * and the actual persisted key is prefix+K
     * This
     */
    #[must_use = "This must be polled in order to pump the data"]
    pub fn pipe_put<S, K1, K2, V>(
        &self,
        prefix: K1,
        stream: S,
    ) -> impl Future<Output = Result<(), anyhow::Error>>
    where
        K1: ToBytes,
        K2: ToBytes,
        V: Encodable,
        S: Stream<Item = Option<(K2, V)>>,
    {
        let tree = self.tree.clone();
        async move {
            pin_mut!(stream);
            while let Some(Some((k, v))) = stream.next().await {
                let mut key = prefix.to_bytes();
                key.push(b'/');
                key.extend_from_slice(&k.to_bytes());
                let vv = v.ser()?;
                tree.insert(&key, vv)?;
            }
            Ok(())
        }
    }

    /*
     *  Subscribe to writes to this shed using a prefix.
     *  This returns a Pipe which implements Stream and some other stuff
     */
    pub fn pipe_subscribe<V: Encodable + std::fmt::Debug>(
        &self,
        prefix: &[u8],
        history: History,
    ) -> Result<Pipe<V>, anyhow::Error> {
        let iter =
            match history {
                History::All => {
                    let start = self.tree.get_gt(sled::IVec::from(prefix))?;
                    let scan = self.tree.scan_prefix(sled::IVec::from(prefix));
                    let last = scan.last();
                    if let Some((begin, _)) = start {
                        if let Some(Ok((end, _))) = last {
                            Some(self.tree.range(
                                sled::IVec::from(begin)..=sled::IVec::from(end),
                            ))
                        } else {
                            Some(self.tree.range(sled::IVec::from(begin)..))
                        }
                    } else {
                        None
                    }
                }
                History::Start(k) => {
                    let key = k.to_bytes();
                    let start = prefix
                        .iter()
                        .map(|a| *a)
                        .chain(key.into_iter())
                        .collect::<Vec<u8>>();
                    let scan = self.tree.scan_prefix(sled::IVec::from(prefix));
                    let last = scan.last();
                    if let Some(Ok((end, _))) = last {
                        Some(self.tree.range(
                            sled::IVec::from(start)..=sled::IVec::from(end),
                        ))
                    } else {
                        Some(self.tree.range(sled::IVec::from(start)..))
                    }
                }
                History::None => None,
            };
        Ok(Pipe::from_source(
            Source(self.tree.watch_prefix(prefix)),
            prefix,
            iter,
        ))
    }

    pub fn pipe_subscribe_chunked<V>(
        &self,
        prefix: &[u8],
        chunk: ChunkType,
    ) -> PipeChunked<V>
    where
        V: Encodable + Send + std::fmt::Debug + 'static,
    {
        PipeChunked::from_source_and_type(
            Source(self.tree.watch_prefix(prefix)),
            prefix,
            chunk,
        )
    }

    /*
     *  A hose is like a pipe, but it's more flexible.
     *  hose subscribes to a stream, applies a function, then writes it to a new key
     */
    pub fn hose<F, V1, V2>(
        &self,
        output_prefix: &[u8],
        pipe: Pipe<V1>,
        fun: F,
    ) -> impl Future<Output = Result<(), anyhow::Error>>
    where
        F: FnMut(Result<Action<V1>, anyhow::Error>) -> Option<(Vec<u8>, V2)>,
        V1: Encodable,
        V2: Encodable,
    {
        let s = pipe.map(fun);
        self.pipe_put(output_prefix.to_vec(), s)
    }

    pub fn hose_chunked<F, V1, V2>(
        &self,
        output_prefix: &[u8],
        pipe: PipeChunked<V1>,
        fun: F,
    ) -> impl Future<Output = Result<(), anyhow::Error>>
    where
        F: FnMut(
            Vec<Result<(Vec<u8>, V1), anyhow::Error>>,
        ) -> Option<(Vec<u8>, V2)>,
        V1: Encodable + Send,
        V2: Encodable,
    {
        let s = pipe.map(fun);
        self.pipe_put(output_prefix.to_vec(), s)
    }

    pub fn manifold_subscribe<M: ManifoldAdapter>(
        &self,
    ) -> Manifold<Pin<Box<dyn Stream<Item = Result<M, anyhow::Error>> + Send>>>
    {
        M::connect(Store(self.tree.clone()))
    }

    pub fn insert<K: ToBytes, V: Encodable>(
        &self,
        key: K,
        val: V,
    ) -> Result<Option<V>, anyhow::Error> {
        let kbuf = key.to_bytes();
        let vbuf = val.ser()?;
        debug!(
            "inserting {} - value : {:?}",
            String::from_utf8(kbuf.clone()).unwrap(),
            &vbuf[0..10]
        );
        let vbufopt =
            self.tree.insert(kbuf, vbuf).map_err(anyhow::Error::new)?;
        vbufopt.map(|newbuf| V::des(newbuf)).transpose()
    }

    pub fn remove<K: ToBytes, V: Encodable>(
        &self,
        key: K,
    ) -> Result<Option<V>, anyhow::Error> {
        let kbuf = key.to_bytes();
        let vbufopt = self.tree.remove(kbuf).map_err(anyhow::Error::new)?;
        vbufopt.map(|vbuf| V::des(vbuf)).transpose()
    }

    pub fn get<K: ToBytes, V: Encodable>(
        &self,
        key: K,
    ) -> Result<Option<V>, anyhow::Error> {
        let kbuf = key.to_bytes();
        let vbufopt = self.tree.get(kbuf).map_err(anyhow::Error::new)?;
        vbufopt.map(|vbuf| V::des(vbuf)).transpose()
    }

    pub fn prefix_key<K: ToBytes + ?Sized>(prefix: &str, key: K) -> Vec<u8> {
        let mut v = Vec::with_capacity(prefix.len() + key.len());
        v.extend_from_slice(&prefix.to_bytes());
        v.extend_from_slice(&key.to_bytes());
        v
    }
}

pub enum ChunkType {
    Time(Duration),
    Count(usize),
}

pub trait ToBytes
where
    Self: Sized,
{
    fn to_bytes(&self) -> Vec<u8>;
    fn len(&self) -> usize;
}

impl<T> ToBytes for &T
where
    T: ToBytes,
{
    fn to_bytes(&self) -> Vec<u8> {
        <T as ToBytes>::to_bytes(self)
    }

    fn len(&self) -> usize {
        <T as ToBytes>::len(self)
    }
}

impl ToBytes for &str {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn len(&self) -> usize {
        str::len(self)
    }
}

impl ToBytes for &[u8] {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_vec()
    }
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_owned()
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> Vec<u8> {
        String::as_bytes(self).to_owned()
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ToBytes for u64 {
    fn to_bytes(&self) -> Vec<u8> {
        let mut wtr = Vec::with_capacity(std::mem::size_of::<u64>());
        wtr.write_u64::<BigEndian>(*self).unwrap();
        wtr
    }
    fn len(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl ToBytes for u32 {
    fn to_bytes(&self) -> Vec<u8> {
        let mut wtr = Vec::with_capacity(std::mem::size_of::<u32>());
        wtr.write_u32::<BigEndian>(*self).unwrap();
        wtr
    }
    fn len(&self) -> usize {
        std::mem::size_of::<u32>()
    }
}
impl ToBytes for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        let mut wtr = Vec::with_capacity(std::mem::size_of::<i64>());
        wtr.write_i64::<BigEndian>(*self).unwrap();
        wtr
    }
    fn len(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

impl ToBytes for i32 {
    fn to_bytes(&self) -> Vec<u8> {
        let mut wtr = Vec::with_capacity(std::mem::size_of::<i32>());
        wtr.write_i32::<BigEndian>(*self).unwrap();
        wtr
    }
    fn len(&self) -> usize {
        std::mem::size_of::<i32>()
    }
}

pub trait Encodable
where
    Self: Sized,
{
    fn ser(&self) -> Result<Vec<u8>, anyhow::Error>;
    fn des<I: AsRef<[u8]>>(item: I) -> Result<Self, anyhow::Error>;
}

impl<T> Encodable for T
where
    T: Serialize + DeserializeOwned,
{
    fn ser(&self) -> Result<Vec<u8>, anyhow::Error> {
        serde_json::to_vec(self).map_err(anyhow::Error::new)
    }

    fn des<I: AsRef<[u8]>>(item: I) -> Result<Self, anyhow::Error> {
        serde_json::from_slice(item.as_ref()).map_err(anyhow::Error::new)
    }
    /*
    fn ser(&self) -> Result<Vec<u8>, anyhow::Error> {
        DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(self)
            .map_err(anyhow::Error::new)
    }

    fn des<I: AsRef<[u8]>>(item: I) -> Result<Self, anyhow::Error> {
        DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .deserialize(item.as_ref())
            .map_err(anyhow::Error::new)
    }
    */
}

pub trait ManifoldAdapter
where
    Self: Sized,
{
    fn connect(
        ds: Store,
    ) -> Manifold<Pin<Box<dyn Stream<Item = Result<Self, anyhow::Error>> + Send>>>;
}

#[derive(Debug)]
pub enum Action<V> {
    Insert((Vec<u8>, V)),
    Remove(Vec<u8>),
}

impl<V> Action<V> {
    pub fn insert(self) -> Option<(Vec<u8>, V)> {
        if let Action::Insert(ent) = self {
            Some(ent)
        } else {
            None
        }
    }
    pub fn remove(self) -> Option<Vec<u8>> {
        if let Action::Remove(ent) = self {
            Some(ent)
        } else {
            None
        }
    }
}

#[pin_project]
pub struct Source(#[pin] pub sled::Subscriber);

impl Future for Source {
    type Output = Option<sled::Event>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().0.poll(cx)
    }
}

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct Pipe<V> {
    #[pin]
    src: Source,
    prefix: Vec<u8>,
    history: Option<sled::Iter>,
    batch: Option<Vec<Result<Action<V>, anyhow::Error>>>,
    _pv: PhantomData<V>,
}

impl<V> Pipe<V>
where
    V: Encodable,
{
    pub fn from_source<P: AsRef<[u8]>>(
        src: Source,
        prefix: P,
        history: Option<sled::Iter>,
    ) -> Pipe<V> {
        let p = prefix.as_ref().to_vec();
        debug!(
            "creating pipe {} - len {}",
            String::from_utf8(p).unwrap(),
            prefix.as_ref().len()
        );
        Pipe {
            src,
            history,
            batch: None,
            prefix: prefix.as_ref().to_vec(),
            _pv: PhantomData,
        }
    }

    fn unpack(
        prefix_len: usize,
        key: &sled::IVec,
        value: &Option<sled::IVec>,
    ) -> Result<Action<V>, anyhow::Error> {
        let k = key[prefix_len..].to_vec();
        if let Some(val) = value {
            debug!(
                "unpacking {} - prefix_len: {} - value: {}",
                String::from_utf8(key.to_vec()).unwrap(),
                prefix_len,
                String::from_utf8(val.to_vec()).unwrap()
            );
            let v = V::des(val)?;
            Ok(Action::Insert((k, v)))
        } else {
            Ok(Action::Remove(k))
        }
    }
}

impl<V> Stream for Pipe<V>
where
    V: Encodable,
{
    type Item = Result<Action<V>, anyhow::Error>;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Action<V>, anyhow::Error>>> {
        let mut this = self.project();
        let prefix_len = this.prefix.len();
        // first fetch from our historical iterator if it exists
        if let Some(mut iter) = this.history.take() {
            if let Some(entry) = iter.next() {
                let res = match entry {
                    Ok((kk, vv)) => {
                        Some(Self::unpack(prefix_len, &kk, &Some(vv)))
                    }
                    Err(e) => Some(Err(anyhow::Error::new(e))),
                };
                *this.history = Some(iter);
                return Poll::Ready(res);
            }
        }
        // no historical iterator, let's try a batch that we got from polling
        if let Some(entry) = this.batch.as_mut().and_then(|b| b.pop()) {
            return Poll::Ready(Some(entry));
        }
        // if we're still here, the iterator and the batch is out, so let's
        // poll for a new batch and return an item from it
        let res = ready!(this.src.as_mut().poll(cx)).and_then(
            |event: sled::Event| {
                let mut b = event
                    .iter()
                    .filter_map(|e| Some(Self::unpack(prefix_len, e.1, e.2)))
                    .collect::<Vec<Result<Action<V>, anyhow::Error>>>();
                let res = b.pop();
                this.batch.replace(b);
                res
            },
        );
        Poll::Ready(res)
    }
}

#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct PipeChunked<V>
where
    V: Encodable + Send,
{
    #[pin]
    inner: Box<
        dyn Stream<Item = Vec<Result<(Vec<u8>, V), anyhow::Error>>> + Unpin,
    >,
}

impl<V> PipeChunked<V>
where
    V: Encodable + Send + std::fmt::Debug + 'static,
{
    pub fn from_source_and_type<P: AsRef<[u8]>>(
        src: Source,
        prefix: P,
        chunk: ChunkType,
    ) -> PipeChunked<V> {
        let pipe = Pipe::from_source(src, prefix, None);
        let inner = match chunk {
            ChunkType::Count(sz) => {
                let pp = pipe.filter_map(|ract| {
                    let res = match ract {
                        Ok(act) => {
                            if let Action::Insert(ent) = act {
                                Some(Ok(ent))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(e)),
                    };
                    future::ready(res)
                });
                let ck = pp.chunks(sz);
                Box::new(ck)
                    as Box<
                        dyn Stream<
                                Item = Vec<Result<(Vec<u8>, V), anyhow::Error>>,
                            > + Unpin,
                    >
            }
            ChunkType::Time(dur) => {
                Box::new(IntervalChunks::from_pipe(pipe, dur))
                    as Box<
                        dyn Stream<
                                Item = Vec<Result<(Vec<u8>, V), anyhow::Error>>,
                            > + Unpin,
                    >
            }
        };
        PipeChunked { inner }
    }
}

impl<V> Stream for PipeChunked<V>
where
    V: Encodable + Send,
{
    type Item = Vec<Result<(Vec<u8>, V), anyhow::Error>>;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.project().inner.as_mut().poll_next(cx)
    }
}

#[pin_project]
pub struct IntervalChunks<V>
where
    V: Encodable + Send + 'static,
{
    #[pin]
    inner: SelectAll<
        Box<
            dyn Stream<Item = Either<Result<(Vec<u8>, V), anyhow::Error>, ()>>
                + Send
                + Unpin,
        >,
    >,
    chunks: Vec<Result<(Vec<u8>, V), anyhow::Error>>,
}

impl<V> IntervalChunks<V>
where
    V: Encodable + Send + 'static,
{
    pub fn from_pipe(pipe: Pipe<V>, dur: Duration) -> IntervalChunks<V> {
        let pp = pipe.filter_map(|ract| {
            let res = match ract {
                Ok(act) => {
                    if let Action::Insert(ent) = act {
                        Some(Either::Left(Ok(ent)))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Either::Left(Err(e))),
            };
            future::ready(res)
        });
        let i = Interval::new(dur).map(|i| Either::Right(i));
        let mut inner = SelectAll::new();
        inner.push(Box::new(pp)
            as Box<
                dyn Stream<
                        Item = Either<Result<(Vec<u8>, V), anyhow::Error>, ()>,
                    > + Send
                    + Unpin,
            >);
        inner.push(Box::new(i)
            as Box<
                dyn Stream<
                        Item = Either<Result<(Vec<u8>, V), anyhow::Error>, ()>,
                    > + Send
                    + Unpin,
            >);
        IntervalChunks { inner, chunks: Vec::new() }
    }
}

impl<V> Stream for IntervalChunks<V>
where
    V: Encodable + Send + 'static,
{
    type Item = Vec<Result<(Vec<u8>, V), anyhow::Error>>;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let res = ready!(this.inner.as_mut().poll_next(cx));
        match res {
            Some(Either::Left(i)) => {
                this.chunks.push(i);
                Poll::Pending
            }
            Some(Either::Right(_)) => {
                let output = this.chunks.drain(..).collect();
                Poll::Ready(Some(output))
            }
            None => Poll::Ready(None),
        }
    }
}

pub struct Hose {}

pub struct Config {
    directory: PathBuf,
    filename: PathBuf,
}

impl Config {
    pub fn new() -> Self {
        let directory: PathBuf = "./".into();
        let filename: PathBuf = "shed.db".into();
        Config { directory, filename }
    }
    pub fn set_dir<P: Into<PathBuf>>(mut self, directory: P) -> Self {
        self.directory = directory.into();
        self
    }
    pub fn set_dbfile<P: Into<PathBuf>>(mut self, filename: P) -> Self {
        self.filename = filename.into();
        self
    }
    pub(crate) fn db_path<'a>(&'a self) -> PathBuf {
        self.directory.join(&self.filename)
    }
}

#[pin_project]
pub struct Interval {
    #[pin]
    delay: futures_timer::Delay,
    dur: Duration,
}

impl Interval {
    pub fn new(dur: Duration) -> Self {
        Interval { delay: futures_timer::Delay::new(dur), dur }
    }
}

impl Stream for Interval {
    type Item = ();
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<()>> {
        let mut this = self.project();
        ready!(this.delay.as_mut().poll(cx));
        this.delay.reset(*this.dur);
        Poll::Ready(Some(()))
    }
}

pub enum History {
    Start(Vec<u8>),
    All,
    None,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
