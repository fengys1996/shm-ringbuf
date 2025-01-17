use std::error::Error;
use std::fmt::Debug;
use std::future::Future;

use crate::error::DataProcessResult;

use super::session_manager::SessionRef;

pub trait DataProcess: Send + Sync {
    type Message: Debug;

    type Error: Error;

    fn decode(&self, data: &[u8]) -> Result<Self::Message, Self::Error>;

    fn process(
        &self,
        message: Self::Message,
        result_sender: ResultSender,
    ) -> impl Future<Output = ()>;
}

pub struct ResultSender {
    pub(crate) request_id: u32,
    pub(crate) session: SessionRef,
}

impl ResultSender {
    pub async fn push_ok(&self) {
        self.session.push_ok(self.request_id).await
    }

    pub async fn push_result(
        &self,
        result: impl Into<DataProcessResult> + Debug + Send + 'static,
    ) {
        self.session.push_result(self.request_id, result).await
    }
}
