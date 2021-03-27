use sled::{ Db, Tree, Subscriber, IVec };
use std::path::{ PathBuf, Path };
use futures::{Future, FutureExt, Stream, StreamExt};
use core::task::{Context, Poll};
use pin_project::pin_project;
use std::pin::Pin;
use std::time::Duration;
use anyhow;

#[derive(Clone, Debug)]
pub struct Shed {
    db: Db,
}

impl Shed {
    pub fn new(name: &str, config: &Config) -> Self {
        let db = sled::open(config.db_path()).expect("db open/create");
        Shed {
            db,
        }
    }

    /*
     * Continuously write to the DB using a stream that generates both the key and the value
     * and the actual persisted key is prefix+K
     */
    pub fn put_pipe<S, K, V>(&self, prefix: &[u8], stream: S) -> Result<(), anyhow::Error>
        where K: Encodable,
              V: Encodable,
              S: Stream<Item=(K, V)>
    {

    }

    /*
     * Continuously write to the DB using a stream, but the key is the current timestamp in
     * nanoseconds (+ the prefix)
     */
    pub fn put_pipe_ts<S, V>(&self, prefix: &[u8], stream: S) -> Result<Pipe, anyhow::Error>
        where K: Encodable,
              V: Encodable,
              S: Stream<Item=V>
    {

    }

    /*
     *  Subscribe to writes to this shed using a prefix.
     *  This returns a Pipe which implements Stream and some other stuff
     */
    pub fn get_pipe<V: Encodable>(&self, prefix: &[u8]) -> Result<Pipe<V>, anyhow::Error> {

    }


    pub fn get_pipe_chunked<V: Encodable>(&self, prefix: &[u8], chunk: ChunkType) -> Result<Pipe<Vec<V>>, anyhow::Error> {
    }

    /*
     *  A hose is like a pipe, but it's more flexible.
     *  hose subscribes to a stream, applies a function, then writes it to a new key
     */
    fn hose<F, K, V>(&self, subscribe_key: &[u8], output_prefix: &[u8], fun: F) -> Hose
        where F: Fn(&[u8]) -> Option<K, V>,
              V: Encodable,
              K: Encodable,
    {

    }

    fn chunked_hose<F, K, V>(&self, subscribe_key: &[u8], output_prefix: &[u8], fun: F) -> Hose
        where F: Fn(&[u8]) -> Option<K, V>,
              V: Encodable,
              K: Encodable,
    {

    }

    pub fn manifold_stream<M: Manifold>(&self) -> impl Stream<Item=M> {
    }
}

pub enum ChunkType {
    Time(Duration),
    Count(usize),
}

pub trait Encodable {
    fn encode(&self) -> Vec<u8>;
    fn decode<I: AsRef<[u8]>>(item: Vec<u8>) -> Result<Self, anyhow::Error>;
}

#[pin_project]
#[must_use = "streams do nothing unless polled"]
pub struct Pipe<V> {
    #[pin]
    sub: Subscriber,
    _p: PhantomData<V>,
}

impl<V> Stream for Pipe<V>
    where V: Encodable
{
    type Item = V;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<V>> {
        let item: IVec<u8> = ready!(self.sub.poll(cx));

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
