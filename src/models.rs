pub mod google {
    pub mod pubsub {
        pub mod v1 {
            tonic::include_proto!("google.pubsub.v1");
        }
    }
}

pub use google::pubsub::v1::*;