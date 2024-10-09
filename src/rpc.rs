use crate::app::AppManagerRef;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::Config;
use crate::grpc::layer::awaittree::AwaitTreeMiddlewareLayer;
use crate::grpc::layer::metric::MetricsMiddlewareLayer;
use crate::grpc::layer::tracing::TracingMiddleWareLayer;
use crate::grpc::protobuf::uniffle::shuffle_server_server::ShuffleServerServer;
use crate::grpc::service::{DefaultShuffleServer, MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use crate::metric::GRPC_LATENCY_TIME_SEC;
use crate::runtime::manager::RuntimeManager;
use crate::signal::details::graceful_wait_for_signal;
use crate::urpc;
use crate::util::is_port_used;
use anyhow::Result;
use async_trait::async_trait;
use log::{debug, error, info};
use once_cell::sync::Lazy;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub static GRPC_PARALLELISM: Lazy<NonZeroUsize> = Lazy::new(|| {
    let available_cores = std::thread::available_parallelism().unwrap();
    std::env::var("GRPC_PARALLELISM").map_or(available_cores, |v| {
        let parallelism: NonZeroUsize = v.as_str().parse().unwrap();
        parallelism
    })
});

pub static URPC_PARALLELISM: Lazy<NonZeroUsize> = Lazy::new(|| {
    let available_cores = std::thread::available_parallelism().unwrap();
    std::env::var("URPC_PARALLELISM").map_or(available_cores, |v| {
        let parallelism: NonZeroUsize = v.as_str().parse().unwrap();
        parallelism
    })
});

#[async_trait]
trait RpcService {
    async fn start(
        &self,
        config: Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
    ) -> Result<()>;
}

pub struct DefaultRpcService;
impl DefaultRpcService {
    fn start_urpc(
        config: &Config,
        runtime_manager: RuntimeManager,
        tx: Sender<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let urpc_port = config.urpc_port.unwrap();
        info!("Starting urpc server with port:[{}] ......", urpc_port);

        for _ in 0..URPC_PARALLELISM.get() {
            let rx = tx.subscribe();
            async fn shutdown(mut rx: Receiver<()>) -> Result<()> {
                if let Err(err) = rx.recv().await {
                    error!("Errors on stopping the urpc service, err: {:?}.", err);
                } else {
                    debug!("urpc service has been graceful stopped.");
                }
                Ok(())
            }

            let app_manager = app_manager_ref.clone();
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), urpc_port as u16);

            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(urpc_serve(addr, shutdown(rx), app_manager));
            });
        }

        Ok(())
    }

    fn start_grpc(
        config: &Config,
        runtime_manager: RuntimeManager,
        tx: Sender<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let grpc_port = config.grpc_port;

        info!("Starting grpc server with port:[{}] ......", grpc_port);
        let parallelism = GRPC_PARALLELISM.get();
        info!("grpc service with parallelism: [{}]", &parallelism);

        let core_ids = core_affinity::get_core_ids().unwrap();
        for (_, core_id) in core_ids.into_iter().enumerate() {
            let shuffle_server = DefaultShuffleServer::from(app_manager_ref.clone());
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), grpc_port as u16);
            let service = ShuffleServerServer::new(shuffle_server)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
            let service_tx = tx.subscribe();

            // every std::thread to bound the tokio thread to eliminate thread context switch.
            // this has been verified by benchmark of terasort 1TB that the long tail latency
            // will be reduced from 2min -> 4sec
            std::thread::spawn(move || {
                core_affinity::set_for_current(core_id);

                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(grpc_serve(service, addr, service_tx));
            });
        }

        Ok(())
    }

    pub fn start(
        &self,
        config: &Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let (tx, _) = broadcast::channel(1);

        let grpc_port = config.grpc_port;
        if is_port_used(grpc_port as u16) {
            panic!("The grpc port of {:?} has been used.", grpc_port);
        }

        DefaultRpcService::start_grpc(
            config,
            runtime_manager.clone(),
            tx.clone(),
            app_manager_ref.clone(),
        )?;

        let urpc_port = config.urpc_port;
        if urpc_port.is_some() {
            if is_port_used(urpc_port.unwrap() as u16) {
                panic!("The urpc port of {:?} has been used.", urpc_port.unwrap());
            }

            DefaultRpcService::start_urpc(
                config,
                runtime_manager.clone(),
                tx.clone(),
                app_manager_ref.clone(),
            )?;
        }

        graceful_wait_for_signal(tx);

        Ok(())
    }
}

async fn urpc_serve(addr: SocketAddr, shutdown: impl Future, app_manager_ref: AppManagerRef) {
    let sock = socket2::Socket::new(
        match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )
    .unwrap();

    sock.set_reuse_address(true).unwrap();
    sock.set_reuse_port(true).unwrap();
    sock.set_nonblocking(true).unwrap();
    sock.bind(&addr.into()).unwrap();
    sock.listen(8192).unwrap();

    let listener = TcpListener::from_std(sock.into()).unwrap();
    let _ = urpc::server::run(listener, shutdown, app_manager_ref).await;
}

async fn grpc_serve(
    service: ShuffleServerServer<DefaultShuffleServer>,
    addr: SocketAddr,
    mut rx: broadcast::Receiver<()>,
) {
    let sock = socket2::Socket::new(
        match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )
    .unwrap();

    sock.set_reuse_address(true).unwrap();
    sock.set_reuse_port(true).unwrap();
    sock.set_nonblocking(true).unwrap();
    sock.bind(&addr.into()).unwrap();
    sock.listen(8192).unwrap();

    let incoming = TcpListenerStream::new(TcpListener::from_std(sock.into()).unwrap());

    Server::builder()
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
        .initial_stream_window_size(STREAM_WINDOW_SIZE)
        .tcp_nodelay(true)
        .layer(TracingMiddleWareLayer::new())
        .layer(MetricsMiddlewareLayer::new(GRPC_LATENCY_TIME_SEC.clone()))
        .layer(AwaitTreeMiddlewareLayer::new_optional(Some(
            AWAIT_TREE_REGISTRY.clone(),
        )))
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, async {
            if let Err(err) = rx.recv().await {
                error!("Errors on stopping the GRPC service, err: {:?}.", err);
            } else {
                debug!("GRPC service has been graceful stopped.");
            }
        })
        .await
        .unwrap();
}
