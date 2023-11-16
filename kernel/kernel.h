#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Whatever we decide this should be
 */
typedef struct ColumnBatch ColumnBatch;

/**
 * Top level client that gets passed into most functions
 */
typedef struct EngineClient EngineClient;

/**
 * Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
 * the engine functions.
 */
typedef struct EngineIterator EngineIterator;

/**
 * A client for talking to the filesystem
 */
typedef struct FileSystemClient FileSystemClient;

/**
 * A client for reading json
 */
typedef struct JsonHandler JsonHandler;

/**
 * Create an iterator that can be passed to other kernel functions. The engine MUST NOT free this
 * iterator, but should call `free_iterator` when finished
 */
struct EngineIterator *create_iterator(const void *data,
                                       const void *(**get_next)(const void *data));

/**
 * construct a FileSystemClient from the specified functions
 */
struct FileSystemClient *create_filesystem_client(struct EngineIterator *(**list_from)(const char *path));

/**
 * construct a JsonHandler from the specified functions
 */
struct JsonHandler *create_json_handler(const struct ColumnBatch *(**read_json_files)(const char *const *files,
                                                                                      int file_count));

/**
 * construct a EngineClient from the specified functions
 */
struct EngineClient *create_engine_client(const struct FileSystemClient *(**get_file_system_client)(void));
