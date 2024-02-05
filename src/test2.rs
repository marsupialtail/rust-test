use std::sync::Arc;
use std::env;
use anyhow::Result;
use opendal::raw::oio::ReadExt;
use opendal::services::S3;
use opendal::services::Fs;
use opendal::Metadata;
use opendal::{Operator, Reader};

#[tokio::main]
async fn main() -> Result<()> {

    // Create s3 backend builder.
    let mut builder = S3::default();
    builder.bucket("maas-data");
    // Set the region. This is required for some services, if you don't care about it, for example Minio service, just set it to "auto", it will be ignored.
    builder.region("us-west-2");
    builder.enable_virtual_host_style();
    builder.endpoint("https://tos-s3-cn-beijing.volces.com");

    let op = Operator::new(builder)?.finish();

    // let mut builder = Fs::default();
    // let current_path = env::current_dir()?;
    // builder.root(current_path.to_str().expect("no path"));
    // let op = Operator::new(builder)?.finish();


    // println!("{:?}", op.clone().list("").await?);
    let mut reader = op.clone().reader("data/Cargo.toml").await?;
    let len = op.stat("data/Cargo.toml").await?.content_length();
    println!("{:?}", len);
    reader.seek(std::io::SeekFrom::Start(10)).await?;
    // now read 10 bytes
    let mut buf = vec![0; 10];
    reader.read(&mut buf).await?;
    println!("{:?}", buf);
    reader.read(&mut buf).await?;
    println!("{:?}", buf);
    
    
    Ok(())
}
