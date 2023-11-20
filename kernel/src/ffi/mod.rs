/// Contains code the exposes what an engine needs to call from 'c' to interface with kernel
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

/*
 Note: Right now the iterator simply returns void*s, but we could rather have a generic type on
 EngineIterator and have more type safety. This would be at the cost of having to have a
 `create_[type]_iterator` function for each type we want to support, as 'extern' functions can't be
 generic, so we can't write: `extern fn create_iterator<T> -> EngineIterator<T>`, and have to rather
 do: `extern fn create_int_iterator -> EngineIterator<i32>`
 */


// WARNING: the visitor MUST NOT retain internal references to the c_char names passed to visitor methods
// TODO: other types, nullability
#[repr(C)]
pub struct EngineSchemaVisitor {
    // opaque state pointer
    data: *mut c_void,
    // Creates a new field list, optionally reserving capacity up front
    make_field_list: extern fn(data: *mut c_void, reserve: usize) -> *mut c_void,
    // Frees an existing field list that will not be returned to the engine (e.g. on error)
    free_field_list: extern fn(data: *mut c_void, siblings: *mut c_void) -> (),
    // visitor methods that should instantiate and append the appropriate type to the field list
    visit_struct: extern fn(data: *mut c_void, siblings: *mut c_void, name: *const c_char, children: *mut c_void) -> (),
    visit_string: extern fn(data: *mut c_void, siblings: *mut c_void, name: *const c_char) -> (),
    visit_integer: extern fn(data: *mut c_void, siblings: *mut c_void, name: *const c_char) -> (),
    visit_long: extern fn(data: *mut c_void, siblings: *mut c_void, name: *const c_char) -> (),
}

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions.
pub struct EngineIterator {
    // Opaque data that will be iterated over. This data will be passed to the get_next function
    // each time a next item is requested from the iterator
    data: *mut c_void,
    // A function that should advance the iterator and return the next time from the data
    get_next: extern fn(data: *mut c_void) -> *const c_void,
    // A function that can free any memory associated with the data if needed
    release: extern fn(data: *mut c_void) -> (),
}

/// Create an iterator that can be passed to other kernel functions. The engine MUST NOT free this
/// iterator, but should call `free_iterator` when finished
#[no_mangle]
pub extern "C" fn create_iterator(
    data: *mut c_void,
    get_next: extern fn(data: *mut c_void) -> *const c_void,
    release: extern fn(data: *mut c_void) -> (),
) -> *mut EngineIterator {
    let it = EngineIterator { data, get_next , release};
    Box::into_raw(Box::new(it))
}


/// test function to print for items. this assumes each item is an `int`, and will release the
/// iterator after printing the items
#[no_mangle] extern "C" fn iterate(engine_iter: *mut EngineIterator) {
    let it: &mut EngineIterator  = unsafe { &mut *engine_iter };
    for i in it {
        let ip: *const c_int = i as *const c_int;
        let ii: &i32 = unsafe { &*ip };
        println!("Got an item: {:?}", ii);
    }
    // now take ownership and drop it
    let _: Box<EngineIterator> = unsafe { Box::from_raw(engine_iter) };
}

impl Iterator for EngineIterator {
    // Todo: Figure out item type
    type Item = *const c_void;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let next_item = (self.get_next)(self.data);
            if next_item.is_null() {
                None
            } else {
                Some(&*next_item)
            }
        }
    }
}

impl Drop for EngineIterator {
    fn drop(&mut self) {
        (self.release)(self.data);
    }
}

/// Whatever we decide this should be
pub struct ColumnBatch;

/// A client for talking to the filesystem
pub struct FileSystemClient {
    list_from: extern fn(path: *const c_char) -> *mut EngineIterator,
}

/// construct a FileSystemClient from the specified functions
#[no_mangle]
pub extern "C" fn create_filesystem_client(
    list_from: extern fn(path: *const c_char) -> *mut EngineIterator,
) -> *mut FileSystemClient {
    let client = FileSystemClient { list_from };
    Box::into_raw(Box::new(client))
}

/// A client for reading json
pub struct JsonHandler {
    read_json_files:
         extern fn(files: *const *const c_char, file_count: c_int) -> *const ColumnBatch, // schema?
}

/// construct a JsonHandler from the specified functions
#[no_mangle]
pub extern "C" fn create_json_handler(
    read_json_files: extern fn(
        files: *const *const c_char,
        file_count: c_int,
    ) -> *const ColumnBatch,
) -> *mut JsonHandler {
    let handler = JsonHandler { read_json_files };
    Box::into_raw(Box::new(handler))
}

/// Top level client that gets passed into most functions
pub struct EngineClient {
    get_file_system_client: extern fn() -> *const FileSystemClient,
}

/// construct a EngineClient from the specified functions
#[no_mangle]
pub extern "C" fn create_engine_client(
    get_file_system_client: extern fn() -> *const FileSystemClient,
) -> *mut EngineClient {
    let client = EngineClient {
        get_file_system_client,
    };
    Box::into_raw(Box::new(client))
}


// stuff for the default client
use crate::client::executor::tokio::TokioBackgroundExecutor;
use crate::client::{DefaultTableClient, json::JsonReadContext, parquet::ParquetReadContext};
use crate::snapshot::Snapshot;
use crate::schema::{DataType, PrimitiveType, StructField, StructType};
use crate::Table;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

type DefaultTable = Table<JsonReadContext, ParquetReadContext>;
type DefaultSnapshot = Snapshot<JsonReadContext, ParquetReadContext>;


#[no_mangle]
pub extern "C" fn get_table_with_default_client(path: *const c_char) -> *mut DefaultTable {
    let c_str = unsafe { CStr::from_ptr(path) };
    let path = c_str.to_str().unwrap();
    let path = std::fs::canonicalize(PathBuf::from(path));
    let Ok(path) = path else {
        println!("Couldn't open table: {}", path.err().unwrap());
        return std::ptr::null_mut();
    };
    let Ok(url) = url::Url::from_directory_path(path) else {
        println!("Invalid url");
        return std::ptr::null_mut();
    };
    let table_client = DefaultTableClient::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    let Ok(table_client) = table_client else {
        println!(
            "Failed to construct table client: {}",
            table_client.err().unwrap()
        );
        return std::ptr::null_mut();
    };
    let table_client = Arc::new(table_client);

    let table = Table::new(url, table_client.clone());
    Box::into_raw(Box::new(table))
}


/// Get the latest snapshot from the specified table
#[no_mangle]
pub extern "C" fn snapshot(table: *mut DefaultTable) -> *mut DefaultSnapshot {
    let snapshot = unsafe { table.as_ref().unwrap().snapshot(None).unwrap() };
    Box::into_raw(Box::new(snapshot))
}

/// Get the version of the specified snapshot
#[no_mangle]
pub extern "C" fn version(snapshot: *mut DefaultSnapshot) -> u64 {
    unsafe { snapshot.as_ref().unwrap().version() }
}

#[no_mangle]
pub extern "C" fn visit_schema(snapshot: *mut DefaultSnapshot, engine_visitor: *mut EngineSchemaVisitor) -> *mut c_void {
    // Visit all the fields of a struct and return the list of children
    fn visit_struct_fields(visitor: &EngineSchemaVisitor, s: &StructType) -> *mut c_void {
        let children = (visitor.make_field_list)(visitor.data, s.fields.len());
        for field in s.fields.iter() {
            visit_field(visitor, children, field);
        }
        children
    }

    // Visit a struct field (recursively) and add the result to the list of siblings.
    fn visit_field(visitor: &EngineSchemaVisitor, siblings: *mut c_void, field: &StructField) -> () {
        let name = CString::new(field.name.as_bytes()).unwrap();
        match &field.data_type {
            DataType::Primitive(PrimitiveType::Integer) =>
                (visitor.visit_integer)(visitor.data, siblings, name.as_ptr()),
            DataType::Primitive(PrimitiveType::Long) =>
                (visitor.visit_long)(visitor.data, siblings, name.as_ptr()),
            DataType::Primitive(PrimitiveType::String) =>
                (visitor.visit_string)(visitor.data, siblings, name.as_ptr()),
            DataType::Struct(s) => {
                let children = visit_struct_fields(visitor, &s);
                (visitor.visit_struct)(visitor.data, siblings, name.as_ptr(), children);
            },
            other => println!("Unsupported data type: {}", other),
        }
    }

    let visitor: &mut EngineSchemaVisitor = unsafe { &mut *engine_visitor };
    let schema: StructType = unsafe { snapshot.as_ref().unwrap().schema().unwrap() };
    visit_struct_fields(visitor, &schema)
}

#[repr(C)]
pub struct FileList {
    files: *mut *mut c_char,
    file_count: i32,
}

/// Get a FileList for all the files that need to be read from the table. NB: This _consumes_ the
/// snapshot, it is no longer valid after making this call (TODO: We should probably fix this?)
#[no_mangle]
pub extern "C" fn get_scan_files(snapshot: *mut DefaultSnapshot) -> FileList {
    let snapshot_box: Box<DefaultSnapshot> = unsafe { Box::from_raw(snapshot) };
    let scan_adds = snapshot_box.scan().unwrap().build().files().unwrap();
    let mut file_count = 0;
    let mut files: Vec<*mut i8> = scan_adds.into_iter().map(|add| {
        file_count += 1;
        CString::new(add.unwrap().path).unwrap().into_raw()
    }).collect();
    let ptr = files.as_mut_ptr();
    std::mem::forget(files);
    FileList {
        files: ptr,
        file_count,
    }
}
