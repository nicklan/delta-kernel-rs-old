
use crate::{delta_table::DeltaTable, storage::StorageClient};
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;

use std::ffi::{CStr, CString};
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
pub struct RecordBatchIterator {
    _batches: Vec<RecordBatch>,
    _marker: PhantomData<core::marker::PhantomPinned>,
}

#[no_mangle]
pub extern fn next_batch(iter: *mut RecordBatchIterator) {
    unsafe {
        println!("have {} batches", (*iter)._batches.len());
    }
}

#[no_mangle]
pub extern fn delta_scanner(path: *const c_char) -> *mut RecordBatchIterator {
    let s = unsafe { CStr::from_ptr(path) };
    let table_path = s.to_str().unwrap();

    println!("Reading table at {}", table_path);
    let storage_client = LocalStorageClient::default();
    let table = DeltaTable::new(table_path);
    let snapshot = table.get_latest_snapshot(&storage_client);
    let scan = snapshot.scan().build();
    let files_batches = scan.files(&storage_client);

    let batches: Vec<RecordBatch> = files_batches.collect();

    let iter = RecordBatchIterator {
        _batches: batches,
        _marker: PhantomData,
    };
    println!("created with {} batches", iter._batches.len());

    // let ret_str = CString::new("returned from rust").unwrap();
    // ret_str.into_raw()

    Box::into_raw(Box::new(iter))
}

