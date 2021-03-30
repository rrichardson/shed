use std::path::PathBuf;
use futures::{Future, future::{ self, Either}, Stream, StreamExt, ready, pin_mut, stream::{self, SelectAll}};
use core::task::{Context, Poll};
use pin_project::pin_project;
use std::pin::Pin;
use bincode::{ Options, DefaultOptions };
use std::time::Duration;
use std::marker::PhantomData;
use serde::{ Serialize, de::DeserializeOwned };
use anyhow;
pub use sled;

#[derive(Clone, Debug)]
pub struct Shed {
    pub db: sled::Db,
    pub tree: sled::Tree,
}

#[derive(Clone, Debug)]
pub struct Store(pub sled::Tree);


impl Shed {
    pub fn new(name: &str, config: &Config) -> Self {
        let db = sled::open(config.db_path()).expect("db open/create");
        let tree = db.open_tree(name).expect("open tree of db");
        Shed {
            db,
            tree
        }
    }

    /*
     * Continuously write to the DB using a stream that generates both the key and the value
     * and the actual persisted key is prefix+K
     * This
     */
    #[must_use = "This must be polled in order to pump the data"]
    pub fn pipe_put<S, K, V>(&self, prefix: Vec<u8>, stream: S) -> impl Future<Output = Result<(), anyhow::Error>>
        where K: Encodable,
              V: Encodable,
              S: Stream<Item=(K, V)>
    {
        let tree = self.tree.clone();
        async move {
            pin_mut!(stream);
            while let Some((k, v)) = stream.next().await {
                let mut key = prefix.to_vec();
                let mut kk = k.encode()?;
                let vv = v.encode()?;
                key.append(&mut kk);
                println!("Writing {:?} -> {:?}", key, vv);
                tree.insert(&key, vv)?;
            }
            Ok(())
        }
        /*
        stream.for_each(move |(k, v)| {
            let mut key = prefix.to_vec();
            match (k.encode(), v.encode()) {
                (Ok(mut kk), Ok(vv)) => {
                    key.append(&mut kk);
                    tree.insert(&key, vv).unwrap();
                }
                e => panic!("Error occured while processing {:?}", e)
            }
            future::ready(())
        })
        */
    }

    /*
     *  Subscribe to writes to this shed using a prefix.
     *  This returns a Pipe which implements Stream and some other stuff
     */
    pub fn pipe_subscribe<K: Encodable, V: Encodable>(&self, prefix: &[u8]) -> Pipe<K, V> {
        Pipe::from_source(Source(self.tree.watch_prefix(prefix)), prefix)
    }

    pub fn pipe_subscribe_chunked<K, V>(&self, prefix: &[u8], chunk: ChunkType) -> PipeChunked<K, V>
        where K: Encodable + Send + 'static,
              V: Encodable + Send + 'static,
    {
        PipeChunked::from_source_and_type(Source(self.tree.watch_prefix(prefix)), prefix, chunk)
    }

    /*
     *  A hose is like a pipe, but it's more flexible.
     *  hose subscribes to a stream, applies a function, then writes it to a new key
     */
    pub fn hose<F, K, V>(&self, subscribe_key: &[u8], output_prefix: &[u8], fun: F) -> Hose
        where F: Fn(&[u8]) -> Option<(K, V)>,
              V: Encodable,
              K: Encodable,
    {
        Hose{}
    }

    pub fn chunked_hose<F, K, V>(&self, subscribe_key: &[u8], output_prefix: &[u8], fun: F) -> Hose
        where F: Fn(&[u8]) -> Option<(K, V)>,
              V: Encodable,
              K: Encodable,
    {
        Hose{}
    }

    pub fn manifold_stream<M: Manifold>(&self) -> stream::SelectAll<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, M), anyhow::Error>> + Send>>> {
        M::connect(Store(self.tree.clone()))
    }
}

pub enum ChunkType {
    Time(Duration),
    Count(usize),
}

pub trait Encodable
where Self: Sized
{
    fn encode(&self) -> Result<Vec<u8>, anyhow::Error>;
    fn decode<I: AsRef<[u8]>>(item: I) -> Result<Self, anyhow::Error>;
}

impl<T> Encodable for T
where T: Serialize + DeserializeOwned
{
    fn encode(&self) -> Result<Vec<u8>, anyhow::Error> {
        DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(self).map_err(anyhow::Error::new)
    }

    fn decode<I: AsRef<[u8]>>(item: I) -> Result<Self, anyhow::Error> {
        DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .deserialize(item.as_ref()).map_err(anyhow::Error::new)
    }
}

pub trait Manifold
    where Self: Sized
{
    fn connect(ds: Store) -> stream::SelectAll<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, Self), anyhow::Error>> + Send>>>;
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
pub struct Pipe<K,V> {
    #[pin]
    src: Source,
    prefix: Vec<u8>,
    _pv: PhantomData<V>,
    _pk: PhantomData<K>,
}

impl<K, V> Pipe<K, V>
where K: Encodable,
      V: Encodable
{
    pub fn from_source<P: AsRef<[u8]>>(src: Source, prefix: P) -> Pipe<K, V> {
        Pipe {
            src,
            prefix: prefix.as_ref().to_vec(),
            _pv: PhantomData,
            _pk: PhantomData,
        }
    }

    fn unpack(prefix_len: usize, key: sled::IVec, value: sled::IVec) -> Result<(K, V), anyhow::Error> {
        let v = V::decode(value)?;
        let k = K::decode(&key[prefix_len..])?;
        Ok((k, v))
    }
}

impl<K,V> Stream for Pipe<K,V>
    where V: Encodable,
          K: Encodable
{
    type Item = Result<(K,V), anyhow::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(K,V), anyhow::Error>>> {
        let prefix_len = self.prefix.len();
        let res = ready!(self.project().src.as_mut().poll(cx)).and_then(|event: sled::Event|
                match event {
                    sled::Event::Insert{ key, value } => Some(Self::unpack(prefix_len, key, value)),
                    sled::Event::Remove{key: _} => None,
                });
        Poll::Ready(res)
    }
}
#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct PipeChunked<K,V>
where K: Encodable + Send,
      V: Encodable + Send,
{
    #[pin]
    inner: Box<dyn Stream<Item=Vec<Result<(K,V), anyhow::Error>>> + Unpin>,
}

impl<K, V> PipeChunked<K, V>
where K: Encodable + Send + 'static,
      V: Encodable + Send + 'static
{
    pub fn from_source_and_type<P: AsRef<[u8]>>(src: Source, prefix: P, chunk: ChunkType) -> PipeChunked<K, V> {
        let pipe = Pipe::from_source(src, prefix);
        let inner =
            match chunk {
                ChunkType::Count(sz) => {
                    Box::new(pipe.chunks(sz)) as Box<dyn Stream<Item=Vec<Result<(K, V), anyhow::Error>>> + Unpin >
                },
                ChunkType::Time(dur) => {
                    Box::new(IntervalChunks::from_pipe(pipe, dur)) as Box<dyn Stream<Item=Vec<Result<(K, V), anyhow::Error>>> + Unpin>
                }
            };
        PipeChunked {
            inner,
        }
    }
}

impl<K,V> Stream for PipeChunked<K,V>
    where V: Encodable + Send,
          K: Encodable + Send,
{
    type Item = Vec<Result<(K, V), anyhow::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(self.project().inner.as_mut().poll_next(cx));
        Poll::Ready(res)
    }
}

#[pin_project]
pub struct IntervalChunks<K, V>
    where K: Encodable + Send + 'static,
          V: Encodable + Send + 'static,
{
    #[pin]
    inner: SelectAll<Box<dyn Stream<Item = Either<Result<(K, V), anyhow::Error>, ()>> + Send + Unpin>>,
    chunks: Vec<Result<(K, V), anyhow::Error>>,
}

impl<K, V> IntervalChunks<K, V>
    where K: Encodable + Send + 'static,
          V: Encodable + Send + 'static,
{
    pub fn from_pipe(pipe: Pipe<K, V>, dur: Duration) -> IntervalChunks<K, V> {
        let p = pipe.map(|i| Either::Left(i));
        let i = Interval::new(dur).map(|i| Either::Right(i));
        let mut inner = SelectAll::new();
        inner.push(Box::new(p) as Box<dyn Stream<Item = Either<Result<(K, V), anyhow::Error>, ()>> + Send + Unpin>);
        inner.push(Box::new(i) as Box<dyn Stream<Item = Either<Result<(K, V), anyhow::Error>, ()>> + Send + Unpin> );
        IntervalChunks {
            inner,
            chunks: Vec::new(),
        }
    }
}

impl<K, V> Stream for IntervalChunks<K, V>
    where K: Encodable + Send + 'static,
          V: Encodable + Send + 'static,
{
    type Item = Vec<Result<(K, V), anyhow::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let res = ready!(this.inner.as_mut().poll_next(cx));
        match res {
            Some(Either::Left(i)) => {
                this.chunks.push(i);
                Poll::Pending
            },
            Some(Either::Right(_)) => {
                let output = this.chunks.drain(..).collect();
                Poll::Ready(Some(output))
            },
            None => Poll::Ready(None)
        }
    }
}

pub struct  Hose {
}

pub struct Config {
    directory: PathBuf,
    filename: PathBuf,
}

impl Config {
    pub fn new() -> Self {
        let directory: PathBuf = "./".into();
        let filename: PathBuf = "shed.db".into();
        Config {
            directory,
            filename,
        }
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
        Interval {
            delay: futures_timer::Delay::new(dur),
            dur
        }
    }
}

impl Stream for Interval {
    type Item = ();
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        let mut this = self.project();
        ready!(this.delay.as_mut().poll(cx));
        this.delay.reset(*this.dur);
        Poll::Ready(Some(()))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
