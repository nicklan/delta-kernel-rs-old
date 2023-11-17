#include <stdio.h>

#include "kernel.h"

/* #include <glib.h> */
/* #include <arrow-glib/arrow-glib.h> */
/* #include "arrow/c/abi.h" */

typedef struct iter_data {
  int lim;
  int cur;
} iter_data;

const void* next(void* data) {
  iter_data *id = (iter_data*)data;
  if (id->cur >= id->lim) {
    return 0;
  } else {
    id->cur++;
    return &id->cur;
  }
}

void release(void* data) {
  printf("released\n");
}

void test_iter() {
  iter_data it;
  it.lim = 10;
  it.cur = 0;

  EngineIterator *eit = create_iterator(&it, &next, &release);
  iterate(eit);
}

typedef Table_JsonReadContext__ParquetReadContext Table;
typedef Snapshot_JsonReadContext__ParquetReadContext Snapshot;

int main(int argc, char* argv[]) {

  if (argc < 2) {
    printf("Usage: %s [table_path]\n", argv[0]);
    return -1;
  }

  char* table_path = argv[1];
  printf("Opening table at %s\n", table_path);
  Table *table = get_table_with_default_client(table_path);
  Snapshot *ss = snapshot(table);
  uint64_t v = version(ss);
  printf("Got version: %lu\n", v);
  
  return 0;
}
