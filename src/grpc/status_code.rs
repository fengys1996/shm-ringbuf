#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StatusCode {
    Success = 0,
    MissingFD = 1,
}

impl TryFrom<u32> for StatusCode {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, String> {
        match value {
            0 => Ok(StatusCode::Success),
            1 => Ok(StatusCode::MissingFD),
            _ => Err(format!("Invalid status code: {}", value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::grpc::status_code::StatusCode;

    #[test]
    fn test_try_from() {
        assert_eq!(StatusCode::try_from(0).unwrap(), StatusCode::Success);
        assert_eq!(StatusCode::try_from(1).unwrap(), StatusCode::MissingFD);
        assert!(StatusCode::try_from(2).is_err());
    }
}
