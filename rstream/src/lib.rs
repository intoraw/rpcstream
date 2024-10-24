mod generated {
    pub mod rpcstream;
}

pub mod pb {
    pub use crate::generated::rpcstream::*;
}

pub mod cli;
pub mod remote_buf;
pub mod svc;
