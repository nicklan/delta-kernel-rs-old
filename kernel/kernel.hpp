#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

struct JsonReadContext;

struct KernelExpressionVisitorState;

struct ParquetReadContext;

/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
template<typename JRC = void, typename PRC = void>
struct Snapshot;

/// In-memory representation of a Delta table, which acts as an immutable root entity for reading
/// the different versions (see [`Snapshot`]) of the table located in storage.
template<typename JRC = void, typename PRC = void>
struct Table;

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
struct EngineIterator {
  void *data;
  const void *(*get_next)(void *data);
};

using DefaultTable = Table<JsonReadContext, ParquetReadContext>;

using DefaultSnapshot = Snapshot<JsonReadContext, ParquetReadContext>;

struct EngineSchemaVisitor {
  void *data;
  uintptr_t (*make_field_list)(void *data, uintptr_t reserve);
  void (*visit_struct)(void *data,
                       uintptr_t sibling_list_id,
                       const char *name,
                       uintptr_t child_list_id);
  void (*visit_string)(void *data, uintptr_t sibling_list_id, const char *name);
  void (*visit_integer)(void *data, uintptr_t sibling_list_id, const char *name);
  void (*visit_long)(void *data, uintptr_t sibling_list_id, const char *name);
};

struct FileList {
  char **files;
  int32_t file_count;
};

struct EnginePredicate {
  void *predicate;
  uintptr_t (*visitor)(void *predicate, KernelExpressionVisitorState *state);
};

extern "C" {

/// test function to print for items. this assumes each item is an `int`
void iterate(EngineIterator *it);

DefaultTable *get_table_with_default_client(const char *path);

/// Get the latest snapshot from the specified table
DefaultSnapshot *snapshot(DefaultTable *table);

/// Get the version of the specified snapshot
uint64_t version(DefaultSnapshot *snapshot);

uintptr_t visit_schema(DefaultSnapshot *snapshot, EngineSchemaVisitor *visitor);

uintptr_t visit_expression_and(KernelExpressionVisitorState *state, EngineIterator *children);

uintptr_t visit_expression_lt(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_le(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_gt(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_ge(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_eq(KernelExpressionVisitorState *state, uintptr_t a, uintptr_t b);

uintptr_t visit_expression_column(KernelExpressionVisitorState *state, const char *name);

uintptr_t visit_expression_literal_string(KernelExpressionVisitorState *state, const char *value);

uintptr_t visit_expression_literal_long(KernelExpressionVisitorState *state, int64_t value);

/// Get a FileList for all the files that need to be read from the table. NB: This _consumes_ the
/// snapshot, it is no longer valid after making this call (TODO: We should probably fix this?)
FileList get_scan_files(DefaultSnapshot *snapshot, EnginePredicate *predicate);

} // extern "C"
