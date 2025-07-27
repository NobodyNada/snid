use std::{net::SocketAddr, sync::Arc};

mod grpc;

use futures::future::try_join_all;
use grpc::{
    AddressSpace, DeviceCapability, DevicesRequest, DevicesResponse,
    device_memory_server::{DeviceMemory, DeviceMemoryServer},
    devices_response::Device,
    devices_server::{Devices, DevicesServer},
};
use tokio::sync::mpsc::{self};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, async_trait, transport::Server};
use tracing::trace;

use crate::snes::Snes;

/// Runs the SNI gRPC server.
pub async fn run(
    addr: SocketAddr,
    snes: Arc<Snes>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let service = Arc::new(SniService {
        snes,
        cancel: cancel.clone(),
        uri: "mister:://mister".to_string(),
    });
    tracing::info!("SNI server starting on {addr}");

    Server::builder()
        .layer(tower_http::trace::TraceLayer::new_for_grpc())
        .add_service(DevicesServer::new(service.clone()))
        .add_service(DeviceMemoryServer::new(service))
        .serve_with_shutdown(addr, cancel.cancelled())
        .await?;
    Ok(())
}

#[derive(Debug)]
struct SniService {
    snes: Arc<Snes>,
    cancel: CancellationToken,
    uri: String,
}

#[async_trait]
impl Devices for Arc<SniService> {
    async fn list_devices(
        &self,
        _req: Request<DevicesRequest>,
    ) -> Result<Response<DevicesResponse>, Status> {
        #[allow(deprecated)]
        Ok(Response::new(DevicesResponse {
            devices: vec![Device {
                uri: self.uri.clone(),
                display_name: "MiSTer SNES".to_string(),
                kind: "mister".to_string(),
                capabilities: vec![
                    DeviceCapability::ReadMemory as i32,
                    DeviceCapability::WriteMemory as i32,
                ],
                default_address_space: AddressSpace::FxPakPro as i32,
                system: "snes".to_string(),
            }],
        }))
    }
}

/// Converts an anyhow::Error to a gRPC response.
fn internal_error(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

/// Converts an address into the FxPak address space we use.
fn translate_addr(addr: u32, address_space: grpc::AddressSpace) -> u32 {
    match address_space {
        AddressSpace::FxPakPro | AddressSpace::Raw => addr,
        AddressSpace::SnesABus => match addr {
            0x7E_0000..=0x7F_FFFF => (addr & 0x1_ffff) + 0xF5_0000,
            addr if (addr & 0xffff) < 0x2000 && (addr & 0x7F_0000) < 0x40_0000 => {
                (addr & 0xffff) + 0xF5_0000
            }
            _ => 0xFF_FFFF,
        },
    }
}

impl SniService {
    #[tracing::instrument]
    async fn read(
        &self,
        request: grpc::ReadMemoryRequest,
    ) -> Result<grpc::ReadMemoryResponse, tonic::Status> {
        let addr = translate_addr(request.request_address, request.request_address_space());
        Ok(grpc::ReadMemoryResponse {
            request_address: request.request_address,
            request_address_space: request.request_address_space,
            request_memory_mapping: request.request_memory_mapping,
            device_address: addr,
            device_address_space: AddressSpace::FxPakPro.into(),
            data: self
                .snes
                .read(addr, request.size as usize)
                .await
                .map_err(internal_error)?,
        })
    }

    #[tracing::instrument]
    async fn write(
        &self,
        request: grpc::WriteMemoryRequest,
    ) -> Result<grpc::WriteMemoryResponse, tonic::Status> {
        let addr = translate_addr(request.request_address, request.request_address_space());
        let size = request.data.len() as u32;
        self.snes
            .write(addr, request.data)
            .await
            .map_err(internal_error)?;
        Ok(grpc::WriteMemoryResponse {
            request_address: request.request_address,
            request_address_space: request.request_address_space,
            request_memory_mapping: request.request_memory_mapping,
            device_address: addr,
            device_address_space: AddressSpace::FxPakPro.into(),
            size,
        })
    }

    #[tracing::instrument]
    async fn do_multi_read(
        &self,
        request: grpc::MultiReadMemoryRequest,
    ) -> Result<grpc::MultiReadMemoryResponse, Status> {
        let requests = request
            .requests
            .into_iter()
            .map(|request| self.read(request));
        let responses = try_join_all(requests).await?;
        Ok(grpc::MultiReadMemoryResponse {
            uri: self.uri.clone(),
            responses,
        })
    }

    #[tracing::instrument]
    async fn do_multi_write(
        &self,
        request: grpc::MultiWriteMemoryRequest,
    ) -> Result<grpc::MultiWriteMemoryResponse, Status> {
        let requests = request
            .requests
            .into_iter()
            .map(|request| self.write(request));
        let responses = try_join_all(requests).await?;
        Ok(grpc::MultiWriteMemoryResponse {
            uri: self.uri.clone(),
            responses,
        })
    }
}

async fn handle<Req, Resp, F: Future<Output = Result<Resp, Status>>>(
    req: Option<Req>,
    with: impl FnOnce(Req) -> F,
) -> Result<Option<Resp>, Status> {
    if let Some(req) = req {
        with(req).await.map(Some)
    } else {
        Ok(None)
    }
}

#[async_trait]
impl DeviceMemory for Arc<SniService> {
    async fn mapping_detect(
        &self,
        _request: Request<grpc::DetectMemoryMappingRequest>,
    ) -> Result<Response<grpc::DetectMemoryMappingResponse>, Status> {
        Err(Status::unimplemented("Reading from ROM is unsupported"))
    }
    /// read a single memory segment with a given size from the given device:
    async fn single_read(
        &self,
        request: Request<grpc::SingleReadMemoryRequest>,
    ) -> Result<Response<grpc::SingleReadMemoryResponse>, Status> {
        Ok(Response::new(grpc::SingleReadMemoryResponse {
            uri: self.uri.clone(),
            response: handle(request.into_inner().request, |r| self.read(r)).await?,
        }))
    }
    /// write a single memory segment with given data to the given device:
    async fn single_write(
        &self,
        request: Request<grpc::SingleWriteMemoryRequest>,
    ) -> Result<Response<grpc::SingleWriteMemoryResponse>, Status> {
        Ok(Response::new(grpc::SingleWriteMemoryResponse {
            uri: self.uri.clone(),
            response: handle(request.into_inner().request, |r| self.write(r)).await?,
        }))
    }
    /// read multiple memory segments with given sizes from the given device:
    async fn multi_read(
        &self,
        request: Request<grpc::MultiReadMemoryRequest>,
    ) -> Result<Response<grpc::MultiReadMemoryResponse>, Status> {
        Ok(Response::new(
            self.do_multi_read(request.into_inner()).await?,
        ))
    }

    /// write multiple memory segments with given data to the given device:
    async fn multi_write(
        &self,
        request: Request<grpc::MultiWriteMemoryRequest>,
    ) -> Result<Response<grpc::MultiWriteMemoryResponse>, Status> {
        Ok(Response::new(
            self.do_multi_write(request.into_inner()).await?,
        ))
    }

    /// Server streaming response type for the StreamRead method.
    type StreamReadStream = ReceiverStream<Result<grpc::MultiReadMemoryResponse, Status>>;
    /// stream read multiple memory segments with given sizes from the given device:
    async fn stream_read(
        &self,
        request: Request<Streaming<grpc::MultiReadMemoryRequest>>,
    ) -> Result<Response<Self::StreamReadStream>, Status> {
        let mut request = request.into_inner();
        let (tx, rx) = mpsc::channel(4);

        let s = self.clone();
        tokio::spawn(s.cancel.clone().run_until_cancelled_owned(async move {
            while let Ok(Some(request)) = request.message().await {
                trace!("Recieved request: {request:?}");
                let resp = s.do_multi_read(request).await;
                trace!("Generated response: {resp:?}");

                let Ok(()) = tx.send(resp).await else {
                    break;
                };
            }
        }));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Server streaming response type for the StreamWrite method.
    type StreamWriteStream = ReceiverStream<Result<grpc::MultiWriteMemoryResponse, Status>>;
    /// stream write multiple memory segments with given sizes to the given device:
    async fn stream_write(
        &self,
        request: Request<Streaming<grpc::MultiWriteMemoryRequest>>,
    ) -> Result<Response<Self::StreamWriteStream>, Status> {
        let mut request = request.into_inner();
        let (tx, rx) = mpsc::channel(4);

        let s = self.clone();
        tokio::spawn(s.cancel.clone().run_until_cancelled_owned(async move {
            while let Ok(Some(request)) = request.message().await {
                trace!("Recieved request: {request:?}");
                let resp = s.do_multi_write(request).await;
                trace!("Generated response: {resp:?}");

                let Ok(()) = tx.send(resp).await else {
                    break;
                };
            }
        }));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Server streaming response type for the Watch method.
    type WatchStream = ReceiverStream<Result<grpc::MultiReadMemoryResponse, Status>>;
    /// stream a memory segment every frame
    async fn watch(
        &self,
        request: Request<grpc::MultiReadMemoryRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let request = request.into_inner();

        let s = self.clone();

        let mut stream = s
            .snes
            .stream_reads(
                request
                    .requests
                    .iter()
                    .map(|req| {
                        (
                            translate_addr(req.request_address, req.request_address_space()),
                            req.size as usize,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await;

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(self.cancel.clone().run_until_cancelled_owned(async move {
            loop {
                while let Some(responses) = stream.next().await {
                    let response = responses
                        .map(|responses| grpc::MultiReadMemoryResponse {
                            uri: s.uri.clone(),
                            responses: responses
                                .into_iter()
                                .zip(request.requests.iter())
                                .map(|(data, request)| grpc::ReadMemoryResponse {
                                    request_address: request.request_address,
                                    request_address_space: request.request_address_space,
                                    request_memory_mapping: request.request_memory_mapping,
                                    device_address: translate_addr(
                                        request.request_address,
                                        request.request_address_space(),
                                    ),
                                    device_address_space: grpc::AddressSpace::FxPakPro as i32,
                                    data,
                                })
                                .collect(),
                        })
                        .map_err(internal_error);
                    let Ok(()) = tx.send(response).await else {
                        return;
                    };
                }
            }
        }));

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
