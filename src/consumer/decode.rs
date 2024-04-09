use crate::error::Result;

pub trait Decode: Send + Sync {
    type Item;

    fn decode(&self, data: &[u8], ctx: Context) -> Result<Self::Item>;
}

pub struct Context {}

pub struct ToStringDecoder;

impl Decode for ToStringDecoder {
    type Item = String;

    fn decode(&self, data: &[u8], _ctx: Context) -> Result<Self::Item> {
        Ok(String::from_utf8_lossy(data).to_string())
    }
}
