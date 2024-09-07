#[macro_export]
macro_rules! convert_num {
    ($num: expr, $ty: ty) => {
        TryInto::<$ty>::try_into($num).context($crate::error::TryFromIntSnafu {
            from: std::any::type_name_of_val(&$num),
            to: stringify!($ty),
        })
    };
}

#[cfg(test)]
mod tests {
    use snafu::ResultExt;

    use crate::error;

    #[test]
    fn test_convert_num() {
        let result = convert_num!(u32::MAX - 1, u16);

        let err = result.err().unwrap();

        match err {
            error::Error::TryFromInt { from, to, .. } => {
                assert_eq!(from, "u32");
                assert_eq!(to, "u16");
            }
            _ => unreachable!(),
        }
    }
}
