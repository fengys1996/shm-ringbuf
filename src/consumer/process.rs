use std::fmt::Debug;
use std::result::Result as StdResult;

use crate::error::DataProcessResult;

pub trait DataProcess: Send + Sync {
    type Error: Into<DataProcessResult> + Debug + Send + 'static;

    fn process(&self, data: &[u8]) -> StdResult<(), Self::Error>;
}
