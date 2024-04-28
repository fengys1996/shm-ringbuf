use std::result::Result as StdResult;

use crate::error::Result;

pub trait Decode: Send + Sync {
    type Item;

    type Error;

    fn decode(
        &self,
        data: &[u8],
        ctx: Context,
    ) -> StdResult<Self::Item, Self::Error>;
}

pub struct Context {}

pub struct ToStringDecoder;

impl Decode for ToStringDecoder {
    type Item = String;

    type Error = crate::error::Error;

    fn decode(&self, data: &[u8], _ctx: Context) -> Result<Self::Item> {
        Ok(String::from_utf8_lossy(data).to_string())
    }
}
