
use crate::scan::reader::DeltaReader;
use crate::{delta_table::DeltaTable, storage::StorageClient};
use std::path::PathBuf;

use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use std::ffi::CStr;
use std::os::raw::c_char;

use core::marker::PhantomData;

#[derive(Default, Debug, Clone)]
struct LocalStorageClient {}

impl StorageClient for LocalStorageClient {
    fn list(&self, prefix: &str) -> Vec<std::path::PathBuf> {
        let path = PathBuf::from(prefix);
        std::fs::read_dir(path)
            .unwrap()
            .map(|r| r.unwrap().path())
            .collect()
    }

    fn read(&self, path: &std::path::Path) -> Vec<u8> {
        std::fs::read(path).unwrap()
    }
}

// implement an iterator that can be used from c
#[repr(C)]
pub struct ArrowArrayIterator {
    _file_batches: Vec<RecordBatch>,
    _cur_data_batch: Option<RecordBatch>,
    _storage_client: LocalStorageClient,
    _reader: DeltaReader,
    _marker: PhantomData<core::marker::PhantomPinned>,
}

#[derive(Debug)]
#[repr(C)]
pub struct ArrowArrayAndSchema {
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
}

#[no_mangle]
pub extern fn next_array(iter: *mut ArrowArrayIterator) -> *const ArrowArrayAndSchema {
    let it: &mut ArrowArrayIterator = unsafe { &mut *iter };
    if it._cur_data_batch.is_none() {
        it._cur_data_batch = it._file_batches.pop();
    }
    match it._cur_data_batch.take() {
        Some(batch) => {
            let mut reader_iter = it._reader.read_batch(batch, &it._storage_client);
            match reader_iter.next().unwrap() {
                Ok(data_batch) => {
                    let struct_array: StructArray = data_batch.into();
                    let array_data = struct_array.into_data();
                    match arrow::ffi::to_ffi(&array_data) {
                        Ok((array, schema)) => {
                            Box::into_raw(Box::new(ArrowArrayAndSchema {
                                array, schema
                            }))
                        }
                        Err(e) => {
                            println!("Error converting to ffi: {}", e);
                            std::ptr::null()
                        }
                    }
                }
                Err(e) => {
                    println!("Error reading batch: {}", e);
                    std::ptr::null()
                }
            }
        }
        None => {
            std::ptr::null()
        }
    }
    
    // match iter._cur_data_batch {
    //     Some(v) => {}
    //     None => {
    //         let next = unsafe { (*iter)._file_batches.pop() };
    //         match next {
    //             Some(batch) => {

    //             }
    //             None => {
    //                 std::ptr::null()
    //             }
    //         }
    //     }
    // }
}

#[no_mangle]
pub extern fn delta_scanner(path: *const c_char) -> *mut ArrowArrayIterator {
    let s = unsafe { CStr::from_ptr(path) };
    let table_path = s.to_str().unwrap();

    println!("Reading table at {}", table_path);
    let storage_client = LocalStorageClient::default();
    let table = DeltaTable::new(table_path);
    let snapshot = table.get_latest_snapshot(&storage_client);
    let scan = snapshot.scan().build();
    let files_batches = scan.files(&storage_client);
    let batches: Vec<RecordBatch> = files_batches.collect();

    let iter = ArrowArrayIterator {
        _file_batches: batches,
        _cur_data_batch: None,
        _storage_client: storage_client,
        _reader: scan.create_reader(),
        _marker: PhantomData,
    };

    Box::into_raw(Box::new(iter))
}

