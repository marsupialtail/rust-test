use opendal::services::Fs;
use opendal::{services::S3, Operator};
use opendal::Reader;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Read};
use std::convert::TryFrom;
use std::fs::File;

use arrow_array::{Array, StringArray};
use parquet::basic::{Encoding, GzipLevel, Type, ZstdLevel};
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::compression::{create_codec, Codec, CodecOptionsBuilder};

use parquet::errors::{ParquetError, Result};
use parquet::file::{
    reader::*,
    statistics,
};
use parquet::format::{PageHeader, PageLocation, PageType};
use bytes::Bytes;
use thrift::protocol::TCompactInputProtocol;
use parquet::thrift::TSerializable;
use parquet::arrow::array_reader::make_byte_array_reader;
use parquet::util::{DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator};

use arrow::error::ArrowError;


pub(crate) fn decode_page(
    page_header: PageHeader,
    buffer: Bytes,
    physical_type: Type,
    decompressor: Option<&mut Box<dyn Codec>>,
) -> Result<Page> {
   
    let mut offset: usize = 0;
    let mut can_decompress = true;

    if let Some(ref header_v2) = page_header.data_page_header_v2 {
        offset = (header_v2.definition_levels_byte_length + header_v2.repetition_levels_byte_length)
            as usize;
        // When is_compressed flag is missing the page is considered compressed
        can_decompress = header_v2.is_compressed.unwrap_or(true);
    }

    // TODO: page header could be huge because of statistics. We should set a
    // maximum page header size and abort if that is exceeded.
    let buffer = match decompressor {
        Some(decompressor) if can_decompress => {
            let uncompressed_size = page_header.uncompressed_page_size as usize;
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            let compressed = &buffer.as_ref()[offset..];
            decompressed.extend_from_slice(&buffer.as_ref()[..offset]);
            decompressor.decompress(
                compressed,
                &mut decompressed,
                Some(uncompressed_size - offset),
            )?;

            if decompressed.len() != uncompressed_size {
                return Err(ParquetError::General("messed decompression".to_string()));
            }

            Bytes::from(decompressed)
        }
        _ => buffer,
    };

    let result = match page_header.type_ {
        PageType::DICTIONARY_PAGE => {
            let dict_header = page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                ParquetError::General("Missing dictionary page header".to_string())
            })?;
            let is_sorted = dict_header.is_sorted.unwrap_or(false);
            Page::DictionaryPage {
                buf: buffer,
                num_values: dict_header.num_values as u32,
                encoding: Encoding::try_from(dict_header.encoding)?,
                is_sorted,
            }
        }
        PageType::DATA_PAGE => {
            let header = page_header
                .data_page_header
                .ok_or_else(|| ParquetError::General("Missing V1 data page header".to_string()))?;
            Page::DataPage {
                buf: buffer,
                num_values: header.num_values as u32,
                encoding: Encoding::try_from(header.encoding)?,
                def_level_encoding: Encoding::try_from(header.definition_level_encoding)?,
                rep_level_encoding: Encoding::try_from(header.repetition_level_encoding)?,
                statistics: statistics::from_thrift(physical_type, header.statistics)?,
            }
        }
        PageType::DATA_PAGE_V2 => {
            let header = page_header
                .data_page_header_v2
                .ok_or_else(|| ParquetError::General("Missing V2 data page header".to_string()))?;
            let is_compressed = header.is_compressed.unwrap_or(true);
            Page::DataPageV2 {
                buf: buffer,
                num_values: header.num_values as u32,
                encoding: Encoding::try_from(header.encoding)?,
                num_nulls: header.num_nulls as u32,
                num_rows: header.num_rows as u32,
                def_levels_byte_len: header.definition_levels_byte_length as u32,
                rep_levels_byte_len: header.repetition_levels_byte_length as u32,
                is_compressed,
                statistics: statistics::from_thrift(physical_type, header.statistics)?,
            }
        }
        _ => {
            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
            unimplemented!("Page type {:?} is not supported", page_header.type_)
        }
    };

    Ok(result)
}

fn read_page_header<C: ChunkReader>(reader: &C, offset: u64) -> Result<(usize, PageHeader)> {
    struct TrackedRead<R>(R, usize);

    impl<R: Read> Read for TrackedRead<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let v = self.0.read(buf)?;
            self.1 += v;
            Ok(v)
        }
    }

    let input = reader.get_read(offset)?;
    let mut tracked = TrackedRead(input, 0);
    let mut prot = TCompactInputProtocol::new(&mut tracked);
    let header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok((tracked.1, header))
}

fn read_page_from_column_chunk_dict(
    file_path: &str,
    row_group: usize,
    column_index: usize,
    dict_page_offset: u64,
    dict_page_size: usize,
    page_offset: u64,
    page_size: usize,
) -> io::Result<()> {
    let file1 = File::open(file_path)?;
    let reader = SerializedFileReader::new(file1)?;
    let metadata = reader.metadata();
    let column_descriptor = metadata.row_group(row_group).schema_descr().column(column_index);
    let compression_scheme = metadata.row_group(row_group).column(column_index).compression();

    // Open the Parquet file
    let mut file = File::open(file_path)?;

    let codec_options = CodecOptionsBuilder::default()
        .set_backward_compatible_lz4(false)
        .build();

    let mut codec = create_codec(compression_scheme, &codec_options).unwrap().unwrap();
    
    let (dict_header_len, dict_header) = read_page_header(&file ,dict_page_offset)?;
    let start_position = dict_page_offset + dict_header_len as u64;
    let end_position = dict_page_offset + dict_header_len as u64 + dict_page_size as u64;
    let mut buffer =  vec![0; (end_position - start_position) as usize];
    file.seek(SeekFrom::Start(start_position))?;
    file.read_exact(&mut buffer)?;

    let dict_page = decode_page(dict_header, buffer.into(), Type::BYTE_ARRAY, Some(&mut codec))?;

    let (header_len, header) = read_page_header(&file ,page_offset)?;
    println!("{}, {:?}", header_len, header);
    let start_position = page_offset + header_len as u64;
    let end_position = page_offset + header_len as u64 + page_size as u64;
    let mut buffer =  vec![0; (end_position - start_position) as usize];
    file.seek(SeekFrom::Start(start_position))?;
    file.read_exact(&mut buffer)?;

    let page = decode_page(header, buffer.into(), Type::BYTE_ARRAY, Some(&mut codec))?;
    let num_values = page.num_values();

    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    pages.push(vec![dict_page, page]);
    let page_iterator = InMemoryPageIterator::new(pages);

    let mut array_reader = make_byte_array_reader(Box::new(page_iterator), column_descriptor.clone(), None).unwrap();
    let array = array_reader.next_batch( num_values as usize)?;

    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::ParseError("Expects string array as first argument".to_string())).unwrap();

    println!("{} {}",array.len(), array.value(300000));
    Ok(())
}


use futures::stream::{self, StreamExt};
use std::time::Duration;
use tokio;
use std::env;
use itertools::izip;
use std::collections::HashMap;


struct MyChunkReader(opendal::Reader); // Wrap opendal::Reader in a new struct

impl ChunkReader for Bytes {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        let start = start as usize;
        Ok(self.slice(start..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let start = start as usize;
        Ok(self.slice(start..start + length))
    }
}

async fn read_to_bytes() -> Bytes {

}


#[tokio::main]
async fn read_pages_from_column_chunk(
    column_index: usize,
    file_paths: Vec<&str>,
    row_groups: Vec<usize>,
    page_offsets: Vec<u64>,
    page_sizes: Vec<usize>,
) -> io::Result<()> {

    let codec_options = CodecOptionsBuilder::default()
        .set_backward_compatible_lz4(false)
        .build();
    let mut builder = Fs::default();
    let current_path = env::current_dir().expect("no path").to_str().expect("to string fail on path");
    builder.root(current_path);
    let mut operator = Operator::new(builder)?.finish();

    let mut readers: HashMap<&str, SerializedFileReader<Reader>> = HashMap::new();
    for file_path in file_paths {
        if ! readers.contains_key(file_path) {
            let file1 = operator.reader(file_path).await?;
            let reader = SerializedFileReader::new(file1)?;
            readers.insert(file_path, reader);
        }
    }

    stream::iter(izip!(file_paths, row_groups, page_offsets, page_sizes))
        .map(
            |(file_path, row_group, page_offset, page_size)| 
            {
                async move {
                    let file1 = operator.reader(file_path).await?;
                    let reader = SerializedFileReader::new(file1)?;
                    let metadata = reader.metadata();
                    let column_descriptor = metadata.row_group(row_group).schema_descr().column(column_index);
                    let compression_scheme = metadata.row_group(row_group).column(column_index).compression();

                 }
            }
        )
        .buffer_unordered(10)
        .try_collect::<Vec<_>>()
        .await()?;

    // Open the Parquet file
    let mut file = File::open(file_path)?;
    
    let (header_len, header) = read_page_header(&file ,page_offset)?;
    // println!("{}, {:?}", header_len, header);
    let start_position = page_offset + header_len as u64;
    let end_position = page_offset + header_len as u64 + page_size as u64;
    let mut buffer =  vec![0; (end_position - start_position) as usize];
    file.seek(SeekFrom::Start(start_position))?;
    file.read_exact(&mut buffer)?;

    

    let mut codec = create_codec(compression_scheme, &codec_options).unwrap().unwrap();
    let page = decode_page(header, buffer.into(), Type::BYTE_ARRAY, Some(&mut codec))?;
    let num_values = page.num_values();

    let mut pages: Vec<Vec<parquet::column::page::Page>> = Vec::new();
    pages.push(vec![page]);
    let page_iterator = InMemoryPageIterator::new(pages);

    let mut array_reader = make_byte_array_reader(Box::new(page_iterator), column_descriptor.clone(), None).unwrap();
    let array = array_reader.next_batch( num_values as usize)?;

    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::ParseError("Expects string array as first argument".to_string())).unwrap();

    println!("{}", array.value(0));
    Ok(())
}

fn main() {
    let file_path = "part.parquet";
    let column_index = 2; // Specify the column index
    let page_offset =387765250 ; // Specify the offset of the page within the column chunk
    let page_size = 138628; // Specify the size of the page to read

    if let Err(e) = read_page_from_column_chunk(file_path, 0, column_index, page_offset, page_size) {
        eprintln!("Error reading page from column chunk: {}", e);
    }

    let file_path = "test.parquet";
    let column_index = 1;
    let dict_page_offset = 1261899;
    let dict_page_size = 19;
    let page_offset = 1261931;
    let page_size = 41683;

    if let Err(e) = read_page_from_column_chunk_dict(file_path, 0, column_index, dict_page_offset, dict_page_size, page_offset, page_size) {
        eprintln!("Error reading page from column chunk: {}", e);
    }
}
