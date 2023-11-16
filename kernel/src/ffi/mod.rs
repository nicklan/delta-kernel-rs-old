/// Contains code the exposes what an engine needs to call from 'c' to interface with kernel


use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions.
pub struct EngineIterator {
    data: *const c_void,
    get_next: *const fn(data: *const c_void) -> *const c_void,
}

/// Create an iterator that can be passed to other kernel functions. The engine MUST NOT free this
/// iterator, but should call `free_iterator` when finished
#[no_mangle]
pub extern fn create_iterator(data: *const c_void, get_next: *const fn(data: *const c_void) -> *const c_void) -> *mut EngineIterator {
    let it = EngineIterator {
        data,
        get_next,
    };
    Box::into_raw(Box::new(it))
}

impl Iterator for EngineIterator {
    // Todo: Figure out item type
    type Item = *const c_void;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let get_next: &fn(data: *const c_void) -> *const c_void = &*self.get_next;
            let next_item = get_next(self.data);
            if next_item.is_null() {
                None
            } else {
                Some(&*next_item)
            }
        }
    }
}

/// Whatever we decide this should be
pub struct ColumnBatch;

/// A client for talking to the filesystem
pub struct FileSystemClient {
    list_from: *const fn(path: *const c_char) -> *mut EngineIterator,
}

/// construct a FileSystemClient from the specified functions
#[no_mangle]
pub extern fn create_filesystem_client(list_from: *const fn(path: *const c_char) -> *mut EngineIterator) -> *mut FileSystemClient {
    let client = FileSystemClient {
        list_from,
    };
    Box::into_raw(Box::new(client))
}

/// A client for reading json
pub struct JsonHandler {
    read_json_files: *const fn (files: *const *const c_char, file_count: c_int) ->  *const ColumnBatch, // schema?
}

/// construct a JsonHandler from the specified functions
#[no_mangle]
pub extern fn create_json_handler(read_json_files: *const fn(files: *const *const c_char, file_count: c_int) -> *const ColumnBatch) -> *mut JsonHandler {
    let handler = JsonHandler {
        read_json_files,
    };
    Box::into_raw(Box::new(handler))
}

/// Top level client that gets passed into most functions
pub struct EngineClient {
    get_file_system_client: *const fn() -> *const FileSystemClient,
}

/// construct a EngineClient from the specified functions
#[no_mangle]
pub extern fn create_engine_client(get_file_system_client: *const fn() -> *const FileSystemClient) -> *mut EngineClient {
    let client = EngineClient {
        get_file_system_client,
    };
    Box::into_raw(Box::new(client))
}

