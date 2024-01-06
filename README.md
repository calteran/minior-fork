# minior

Ergonomic client for Minio, built on top of the `aws_sdk_s3` crate.

Table of Contents
-----------------
- [Requirements](#requirements)
- [Installation](#installation)
- [Documentation](#documentation)
- [Usage](#usage)
    - [Overview](#overview)
    - [Basic example](#basic-example)
- [Bug Reports](#bug-reports)
- [Feature Requests](#feature-requests)
- [Contributing](#contributing)
- [Project Status](#project-status)
- [License](#license)

## Requirements
- Minio deployment:
    - [Official installation guide](https://min.io/download?utm_term=&utm_campaign=Leads-Performance+Max-1-042023&utm_source=adwords&utm_medium=ppc&hsa_acc=8976569894&hsa_cam=20015732098&hsa_grp=&hsa_ad=&hsa_src=x&hsa_tgt=&hsa_kw=&hsa_mt=&hsa_net=adwords&hsa_ver=3&gad_source=1&gclid=Cj0KCQiAkeSsBhDUARIsAK3tiecF1RUejrAWP89hF1q-FM8_LYfmgKqKImAknRLFuXZqQ9OuD8KGv_YaAr5ZEALw_wcB#/kubernetes)
- Rust (version 1.6+):
    - [Official installation guide](https://www.rust-lang.org/tools/install)

## Installation
`cargo add minior`

## Documentation

This README provides a general overview, but does not go over all methods available. [Full crate documentation can be found here at docs.rs](https://docs.rs/mongor/latest/minior/)

## Usage

### Overview

The crate exposes a struct `Minio` that can be used to interface with all `core` modules, however `core` is public so feel free to interact with those methods directly.

### Basic Example

```
use minior::Minio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Construct a client
    let minio = Minio::new("http://127.0.0.1:9000").await;

    // Create a bucket
    minio.create_bucket("bucket_name").await?;

    // Upload a object
    let file = tokio::fs::File::open("some file path").await?;

    minio.upload_object(
        "bucket_name",
        "object_name",
        file,
        None,
        None,
    ).await?;

    // Get a Presigned URL for a get that expires in 1_337 seconds
    let presigned_request = minio.get_object_presigned(
        "bucket_name",
        "object_name",
        1_337,
    ).await?;

    // Delete a object
    minio.delete_object(
        "bucket_name",
        "object_name",
    ).await?;

    // Delete a bucket
    minio.delete_bucket(
        "bucket_name",
        true,
    ).await?;

    Ok(())
}
```

## Bug Reports

Please report bugs by creating an `issue`, or if there is a sufficient fix you are aware of, feel free to open a PR, but please follow the `Contributing` guidelines below.

To report a bug, it must be directly related to this crate, and you must provide as much information as possible, such as:

- Code examples

- Error messages

- Steps to reproduce 

- System information (If applicable)

## Feature requests

If you feel there is something missing, or some variation of the current crate that would require additional dependencies other than `aws-sdk-s3`, `aws-config` or `tokio`; please create an `issue` with the request and discuss why you feel it should be part of this crate and not a third party crate.

## Contributing

I welcome anyone to contribute to the crate. But I do have some general requirements:

- Any additional or modified methods require unit testing with 100% test coverage, that should be placed in the `tests` module.

- Any change that adds in additional dependencies should be created as a separate feature.

- All current unit tests must pass, I.E. run `cargo test` and all should pass.

- Add your name and or handle to `CONTRIBUTORS.md` if not already present, as well as to the `Authors` section on the header comment for the file.

- If adding in a new dependency, please update `License::Third Party` in this README to correspond with their licensing.

If your change meets these guidelines, feel free to open a PR.

## Project Status

I plan to maintain this crate for the forseeable future.

## License

MIT

See `LICENSE.md` for more information

### Third Party

This crate is built on-top of:

- The [`aws-config` and `aws-sdk-s3`](https://github.com/mongodb/mongo-rust-driver/tree/main) crates, which is licensed under Apache License 2.0, [view it here](https://github.com/smithy-lang/smithy-rs/blob/main/LICENSE).

- The [`tokio`](https://github.com/tokio-rs/tokio) crate, which is licensed under MIT, [view it here](https://github.com/tokio-rs/tokio/blob/master/LICENSE).

- The [`uuid`](https://github.com/uuid-rs/uuid) crate (Used in internal testing), which is licensed under MIT, [view it here](https://github.com/uuid-rs/uuid/blob/main/LICENSE-MIT) or Apache 2.0, [view it here](https://github.com/uuid-rs/uuid/blob/main/LICENSE-APACHE).

- The [`reqwest`](https://github.com/seanmonstar/reqwest) crate (Used in internal testing), which is licensed under MIT, [view it here](https://github.com/seanmonstar/reqwest/blob/master/LICENSE-MIT) or Apache 2.0, [view it here](https://github.com/seanmonstar/reqwest/blob/master/LICENSE-APACHE).

