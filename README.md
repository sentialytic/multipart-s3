# multipart-s3

The `multipart-s3` crate provides a simple interface to upload, get, and delete objects from Amazon S3 using the AWS SDK for Rust. It supports both single-part and multipart uploads, allowing you to efficiently upload large files.

## Features

- Upload objects to S3 using single-part or multipart uploads
- Get objects from S3
- Delete single or multiple objects from S3
- Seamless integration with the `axum` web framework

## Installation

Add the following to your `Cargo.toml` file:

```toml
[dependencies]
multipart-s3 = "0.1.0"
```

Usage
Here's a high-level example of how to use the multipart-s3 crate:

```rust
use axum::{
    extract::Multipart,
    routing::post,
    Router,
};
use multipart_s3::{aws_config, upload_object, Client};

async fn upload_handler(mut multipart: Multipart) {
        let bucket = "my-bucket".to_string();
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);

        let part_size = 1024 * 1024 * 5; // 5MB
        let stream_from = 1024 * 1024 * 10; // 10MB

    while let Some(field) = multipart.next_field().await.unwrap() {
        let key = field.name().unwrap().to_string();
        let content_type = field.content_type().unwrap().to_string();

        let result = upload_object(
            &client,
            field,
            bucket.clone(),
            key,
            content_type,
            part_size,
            stream_from,
        )
        .await;

        match result {
            Ok(output) => println!("Upload successful: {:?}", output),
            Err(err) => eprintln!("Upload failed: {}", err),
        }
    }
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/upload", post(upload_handler));
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
```

Environment Variables
To use the multipart-s3 crate, you need to set the following environment variables:

- AWS_ACCESS_KEY_ID: Your AWS access key ID.
- AWS_SECRET_ACCESS_KEY: Your AWS secret access key.
- AWS_REGION: The AWS region where your S3 bucket is located.

Make sure to set these environment variables before running your application.

License
This crate is licensed under the MIT License.
