use sled::{ Db, Tree, Subscriber, IVec, Event };
use std::path::{ PathBuf, Path };
use futures::{Future, FutureExt, Stream, StreamExt, ready, stream};
use core::task::{Context, Poll};
use pin_project::pin_project;
use std::pin::Pin;
use std::time::Duration;
use std::marker::PhantomData;
use serde::{ Serialize, Deserialize, de::DeserializeOwned };
use anyhow;

#[derive(Clone, Debug)]
pub struct Shed {
    db: Db,
    tree: Tree,
}

#[derive(Clone, Debug)]
pub struct Store(pub Tree);


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
     */
    pub fn pipe_put<S, K, V>(&self, prefix: &[u8], stream: S) -> Result<(), anyhow::Error>
        where K: Encodable,
              V: Encodable,
              S: Stream<Item=(K, V)>
    {
        Ok(())
    }

    /*
     * Continuously write to the DB using a stream, but the key is the current timestamp in
     * nanoseconds (+ the prefix)
     */
    pub fn pipe_put_ts<S, V>(&self, prefix: &[u8], stream: S) -> Result<(), anyhow::Error>
        where V: Encodable,
              S: Stream<Item=V>
    {
        Ok(())
    }

    /*
     *  Subscribe to writes to this shed using a prefix.
     *  This returns a Pipe which implements Stream and some other stuff
     */
    pub fn pipe_get<K: Encodable, V: Encodable>(&self, prefix: &[u8]) -> Pipe<K, V> {
        Pipe::from_source(Source(self.tree.watch_prefix(prefix)), prefix)
    }

    pub fn pipe_starting_at<V: Encodable>(&self, prefix: &[u8]) -> Pipe<u64, V> {
        Pipe::from_source(Source(self.tree.watch_prefix(prefix)), prefix)
    }

    /*
    pub fn get_pipe_chunked<K: Encodable, V: Encodable>(&self, prefix: &[u8], chunk: ChunkType) -> Result<Pipe<K, Vec<V>>, anyhow::Error> {

    }*/

    /*
     *  A hose is like a pipe, but it's more flexible.
     *  hose subscribes to a stream, applies a function, then writes it to a new key
     */
    fn hose<F, K, V>(&self, subscribe_key: &[u8], output_prefix: &[u8], fun: F) -> Hose
        where F: Fn(&[u8]) -> Option<(K, V)>,
              V: Encodable,
              K: Encodable,
    {
        Hose{}
    }

    fn chunked_hose<F, K, V>(&self, subscribe_key: &[u8], output_prefix: &[u8], fun: F) -> Hose
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

/*
struct ManifoldAdapter<M> {
    _p: PhantomData<M>
}

impl<M> ManifoldAdapter<M>
    where: M: Encodable
{
    fn run(buf: IVec) -> M
}

trait ManifoldAdapter {
    type Item: Encodable;
    fn get_sub<'a>(&'a self) -> &'a [u8];
    fn transform(buf: IVec) -> Result<Self::Item, anyhow::Error>;
}

*/
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
        bincode::serialize(self).map_err(anyhow::Error::new)
    }

    fn decode<I: AsRef<[u8]>>(item: I) -> Result<Self, anyhow::Error> {
        bincode::deserialize(item.as_ref()).map_err(anyhow::Error::new)
    }
}

pub trait Manifold
    where Self: Sized
{
    fn connect(ds: Store) -> stream::SelectAll<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, Self), anyhow::Error>> + Send>>>;
}

enum TestManifold {
    Test1(TestStruct1),
    Test2(TestStruct2),
    Test3(TestStruct3),
}
/*
impl Manifold for TestManifold {
    fn connect(ds: Store) -> stream::SelectAll<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, Self), anyhow::Error>> + Send>>> {
        stream::select_all(
            vec![Pipe::from_source(Source(ds.0.watch_prefix(b"foo/")), b"foo/").map(|res| res.map(|(k, v)| (k, TestManifold::Test1(v)))).boxed(),
                 Pipe::from_source(Source(ds.0.watch_prefix(b"bar/")), b"bar/").map(|res| res.map(|(k, v)| (k, TestManifold::Test2(v)))).boxed(),
                 Pipe::from_source(Source(ds.0.watch_prefix(b"baz/")), b"baz/").map(|res| res.map(|(k, v)| (k, TestManifold::Test3(v)))).boxed()])
    }
}
*/
impl Manifold for TestManifold {
    fn connect(ds: Store) -> stream::SelectAll<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, Self), anyhow::Error>> + Send>>> {
       ::futures::stream::select_all(vec![
            Pipe::from_source(Source(ds.0.watch_prefix(b"foo/")), b"foo/")
                .map(|res| res.map(|(k, v)| (k, TestManifold::Test1(v))))
                .boxed(),
            Pipe::from_source(Source(ds.0.watch_prefix(b"foo/")), b"foo/")
                .map(|res| res.map(|(k, v)| (k, TestManifold::Test2(v))))
                .boxed(),
            Pipe::from_source(Source(ds.0.watch_prefix(b"foo/")), b"foo/")
                .map(|res| res.map(|(k, v)| (k, TestManifold::Test3(v))))
                .boxed(),
        ])
    }
}

#[derive(Serialize, Deserialize)]
struct TestStruct1 {
    data: u64,
    not_data: u64
}

#[derive(Serialize, Deserialize)]
struct TestStruct2 {
    data: i64,
    not_data: i64
}

#[derive(Serialize, Deserialize)]
struct TestStruct3 {
    data: i16,
    not_data: i16
}

/*
struct Test1MAdapter {
    sub: Vec<u8>,
}

struct Test2MAdapter {
    sub: Vec<u8>,
}

struct Test3MAdapter {
    sub: Vec<u8>,
}

impl ManifoldAdapter for Test1MAdapter {
    type Item = TestStruct1;

    fn get_sub<'a>(&'a self) -> &'a [u8] {
        return self.sub.as_slice()
    }

    fn transform(buf: IVec) -> Result<Self::Item, anyhow::Error> {
        buf.decode()
    }
}

impl ManifoldAdapter for Test2MAdapter {
    type Item = TestStruct2;
    fn get_sub<'a>(&'a self) -> &'a [u8] {
        return self.sub.as_slice()
    }
    fn transform(buf: IVec) -> Result<Self::Item, anyhow::Error> {
        buf.decode()
    }
}

impl ManifoldAdapter for Test3MAdapter {
    type Item = TestStruct3;
    fn get_sub<'a>(&'a self) -> &'a [u8] {
        return self.sub.as_slice()
    }
    fn transform(buf: IVec) -> Result<Self::Item, anyhow::Error> {
        buf.decode()
    }
}
*/

#[pin_project]
pub struct Source(#[pin] pub Subscriber);

impl Future for Source {
    type Output = Option<Event>;
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

    fn unpack(prefix_len: usize, key: IVec, value: IVec) -> Result<(K, V), anyhow::Error> {
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
        let res = ready!(self.project().src.as_mut().poll(cx)).and_then(|event: Event|
                match event {
                    Event::Insert{ key, value } => Some(Self::unpack(prefix_len, key, value)),
                    Event::Remove{key: _} => None,
                });
        Poll::Ready(res)
    }
}

pub struct  Hose {
}

pub struct Config {
    directory: PathBuf,
    filename: PathBuf,
    filepath: PathBuf,
}

impl Config {
    pub fn new() -> Self {
        let directory: PathBuf = "./".into();
        let filename: PathBuf = "shed.db".into();
        let filepath = directory.join(&filename).canonicalize().expect("canonical path");
        Config {
            directory,
            filename,
            filepath
        }
    }
    pub fn set_dir<P: Into<PathBuf>>(mut self, directory: P) -> Self {
        self.directory = directory.into();
        self.filepath = self.directory.join(&self.filename).canonicalize().expect("canonical path");
        self
    }
    pub fn set_dbfile<P: Into<PathBuf>>(mut self, filename: P) -> Self {
        self.filename = filename.into();
        self.filepath = self.directory.join(&self.filename).canonicalize().expect("canonical path");
        self
    }
    pub(crate) fn db_path<'a>(&'a self) -> &'a PathBuf {
        &self.filepath
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
