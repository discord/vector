use futures_util::{
    stream::{self, BoxStream},
    StreamExt,
};
use tower::Service;
use vector_lib::event::Event;
use vector_lib::sink::StreamSink;
use vector_lib::stream::{BatcherSettings, DriverResponse};

use super::request_builder::BigqueryRequestBuilder;
use super::service::BigqueryRequest;
use crate::sinks::prelude::SinkRequestBuildError;
use crate::sinks::util::builder::SinkBuilderExt;

pub struct BigquerySink<S> {
    pub service: S,
    pub batcher_settings: BatcherSettings,
    pub request_builder: BigqueryRequestBuilder,
}

impl<S> BigquerySink<S>
where
    S: Service<BigqueryRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<BigquerySink<S>>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .batched(self.batcher_settings.as_byte_size_config())
            .incremental_request_builder(self.request_builder)
            .flat_map(stream::iter)
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .protocol("gRPC")
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for BigquerySink<S>
where
    S: Service<BigqueryRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
