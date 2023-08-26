
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
#[derive(Debug)]
#[repr(C)]
pub struct ArrowArrayIterator {
    _batches: Vec<RecordBatch>,
    _marker: PhantomData<core::marker::PhantomPinned>,
}

#[no_mangle]
pub extern fn next_array(iter: *mut ArrowArrayIterator) -> *const (FFI_ArrowArray, FFI_ArrowSchema) {
    let next = unsafe { (*iter)._batches.pop() };
    match next {
        Some(batch) => {
            let struct_array: StructArray = batch.into();
            let array_data = struct_array.into_data();
            match arrow::ffi::to_ffi(&array_data) {
                Ok(tuple) => {
                    println!("returning tuple");
                    Box::into_raw(Box::new(tuple))
                }
                Err(e) => {
                    println!("Error converting to ffi: {}", e);
                    std::ptr::null()
                }
            }
        }
        None => {
            std::ptr::null()
        }
    }
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
        _batches: batches,
        _marker: PhantomData,
    };
    println!("created with {} batches", iter._batches.len());

    // let ret_str = CString::new("returned from rust").unwrap();
    // ret_str.into_raw()

    Box::into_raw(Box::new(iter))
}

