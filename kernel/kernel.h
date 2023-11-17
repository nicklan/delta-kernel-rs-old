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
 * In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
 * throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
 * have a defined schema (which may change over time for any given table), specific version, and
 * frozen log segment.
 */
typedef struct Snapshot_JsonReadContext__ParquetReadContext Snapshot_JsonReadContext__ParquetReadContext;

/**
 * In-memory representation of a Delta table, which acts as an immutable root entity for reading
 * the different versions (see [`Snapshot`]) of the table located in storage.
 */
typedef struct Table_JsonReadContext__ParquetReadContext Table_JsonReadContext__ParquetReadContext;

typedef struct Table_JsonReadContext__ParquetReadContext DefaultTable;

typedef struct Snapshot_JsonReadContext__ParquetReadContext DefaultSnapshot;

typedef struct FileList {
  char **files;
  int32_t file_count;
} FileList;

/**
 * Create an iterator that can be passed to other kernel functions. The engine MUST NOT free this
 * iterator, but should call `free_iterator` when finished
 */
struct EngineIterator *create_iterator(void *data,
                                       const void *(*get_next)(void *data),
                                       void (*release)(void *data));

/**
 * test function to print for items. this assumes each item is an `int`, and will release the
 * iterator after printing the items
 */
void iterate(struct EngineIterator *engine_iter);

/**
 * construct a FileSystemClient from the specified functions
 */
struct FileSystemClient *create_filesystem_client(struct EngineIterator *(*list_from)(const char *path));

/**
 * construct a JsonHandler from the specified functions
 */
struct JsonHandler *create_json_handler(const struct ColumnBatch *(*read_json_files)(const char *const *files,
                                                                                     int file_count));

/**
 * construct a EngineClient from the specified functions
 */
struct EngineClient *create_engine_client(const struct FileSystemClient *(*get_file_system_client)(void));

DefaultTable *get_table_with_default_client(const char *path);

/**
 * Get the latest snapshot from the specified table
 */
DefaultSnapshot *snapshot(DefaultTable *table);

/**
 * Get the version of the specified snapshot
 */
uint64_t version(DefaultSnapshot *snapshot);

/**
 * Get a FileList for all the files that need to be read from the table. NB: This _consumes_ the
 * snapshot, it is no longer valid after making this call (TODO: We should probably fix this?)
 */
struct FileList get_scan_files(DefaultSnapshot *snapshot);
