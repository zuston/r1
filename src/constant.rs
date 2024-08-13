#[allow(non_camel_case_types)]
pub enum StatusCode {
    SUCCESS = 0,
    DOUBLE_REGISTER = 1,
    NO_BUFFER = 2,
    INVALID_STORAGE = 3,
    NO_REGISTER = 4,
    NO_PARTITION = 5,
    INTERNAL_ERROR = 6,
    TIMEOUT = 7,
    NO_BUFFER_FOR_HUGE_PARTITION = 8,
}

impl Into<i32> for StatusCode {
    fn into(self) -> i32 {
        self as i32
    }
}