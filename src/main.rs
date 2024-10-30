use anyhow::{bail, Context as _, Error};
use async_native_tls::TlsStream;
use async_task::{Runnable, Task};
use flume::{Receiver, Sender};
use futures_lite::future;
use http::Uri;
use smol::Async;
use std::{
    future::Future,
    net::{TcpStream, ToSocketAddrs},
    panic::catch_unwind,
    pin::Pin,
    sync::LazyLock,
    task::Poll,
    thread,
    time::Duration,
};

static HIGH_CHANNEL: LazyLock<(Sender<Runnable>, Receiver<Runnable>)> =
    LazyLock::new(flume::unbounded);

static LOW_CHANNEL: LazyLock<(Sender<Runnable>, Receiver<Runnable>)> =
    LazyLock::new(flume::unbounded);

static HIGH_QUEUE: LazyLock<Sender<Runnable>> = LazyLock::new(|| {
    let high_num = std::env::var("HIGH_NUM").unwrap().parse::<usize>().unwrap();
    for _ in 0..high_num {
        thread::spawn({
            let high_receiver = HIGH_CHANNEL.1.clone();
            let low_receiver = LOW_CHANNEL.1.clone();
            move || loop {
                match high_receiver.try_recv() {
                    Ok(runnable) => {
                        let _ = catch_unwind(|| runnable.run());
                    }
                    Err(_) => match low_receiver.try_recv() {
                        Ok(runnable) => {
                            let _ = catch_unwind(|| runnable.run());
                        }
                        Err(_) => {
                            // park the thread here
                            thread::sleep(Duration::from_millis(100));
                        }
                    },
                }
            }
        });
    }

    HIGH_CHANNEL.0.clone()
});

static LOW_QUEUE: LazyLock<Sender<Runnable>> = LazyLock::new(|| {
    let low_num = std::env::var("LOW_NUM").unwrap().parse::<usize>().unwrap();
    for _ in 0..low_num {
        thread::spawn({
            let high_receiver = HIGH_CHANNEL.1.clone();
            let low_receiever = LOW_CHANNEL.1.clone();
            move || loop {
                match low_receiever.try_recv() {
                    Ok(runnable) => {
                        let _ = catch_unwind(|| runnable.run());
                    }
                    Err(_) => match high_receiver.try_recv() {
                        Ok(runnable) => {
                            let _ = catch_unwind(|| runnable.run());
                        }
                        Err(_) => {
                            // thread park here
                            thread::sleep(Duration::from_millis(100));
                        }
                    },
                }
            }
        });
    }

    LOW_CHANNEL.0.clone()
});

#[derive(Debug, Clone, Copy)]
enum FutureType {
    High,
    Low,
}

fn spawn_task<F, T>(future: F, order: FutureType) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let schedule = match order {
        FutureType::High => |runnable| HIGH_QUEUE.send(runnable).unwrap(),
        FutureType::Low => |runnable| LOW_QUEUE.send(runnable).unwrap(),
    };

    let (runnable, task) = async_task::spawn(future, schedule);
    runnable.schedule();
    // unpark the queue threads here.
    task
}

macro_rules! spawn_task {
    ($future:expr) => {
        spawn_task!($future, FutureType::Low)
    };
    ($future:expr, $order:expr) => {
        spawn_task($future, $order)
    };
}

macro_rules! join {
    ($($future:expr),*) => {
        {
            let mut result = Vec::new();
            $(
                result.push(future::block_on($future));
            )*

            result
        }
    };
}

macro_rules! try_join {
    ($($future:expr), *) => {
        {
            let mut result = Vec::new();
            $(
                let future_result  = catch_unwind(|| fucture::block_on($future));
                result.push(future_result);
            )*

            result
        }
    };
}

struct Runtime {
    high_num: usize,
    low_num: usize,
}

impl Runtime {
    pub fn new() -> Self {
        let parallel_cores = std::thread::available_parallelism().unwrap().get();
        Self {
            high_num: parallel_cores - 2,
            low_num: 1,
        }
    }

    pub fn with_high_num(mut self, high_num: usize) -> Self {
        self.high_num = high_num;
        self
    }

    pub fn with_low_num(mut self, low_num: usize) -> Self {
        self.low_num = low_num;
        self
    }

    pub fn run(&self) {
        std::env::set_var("HIGH_NUM", self.high_num.to_string());
        std::env::set_var("LOW_NUM", self.low_num.to_string());

        // insitialize queues by passing an empty future

        let high = spawn_task!(async {}, FutureType::High);
        let low = spawn_task!(async {});

        join!(high, low);
    }
}

// -------------------- networking code --------------------

struct CustomExecutor;

impl<F: Future + Send + 'static> hyper::rt::Executor<F> for CustomExecutor {
    fn execute(&self, fut: F) {
        spawn_task!(async {
            println!("sending request");
            fut.await;
        })
        .detach();
    }
}

enum CustomStream {
    Plain(Async<TcpStream>),
    Tls(TlsStream<Async<TcpStream>>),
}

#[derive(Clone)]
struct CustomConnector;

impl hyper::service::Service<Uri> for CustomConnector {
    type Response = CustomConnector;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(async move {
            let host = uri.host().context("cannot parse host")?;
            match uri.scheme_str() {
                Some("http") => {
                    let socket_addr = {
                        let host = host.to_string();
                        let port = uri.port_u16().unwrap_or(80);
                        smol::unblock(move || (host.as_str(), port).to_socket_addrs())
                            .await?
                            .next()
                            .context("cannot resolve address")?
                    };

                    let stream = Async::<TcpStream>::connect(socket_addr).await?;
                    Ok(CustomStream::Plain(stream))
                }
                Some("https") => {
                    let socket_addr = {
                        let host = host.to_string();
                        let port = uri.port_u16().unwrap_or(443);
                        smol::unblock(move || (host.as_str(), port).to_socket_addrs())
                            .await?
                            .next()
                            .context("cannot resolve address")?
                    };

                    let stream = Async::<TcpStream>::connect(socket_addr).await?;
                    let stream = async_native_tls::connect(host, stream).await?;
                    Ok(CustomStream::Tls(stream))
                }
                scheme => bail!("unsupported scheme: {:?}", scheme),
            }
        })
    }
}

fn main() {
    Runtime::new().run();
}
