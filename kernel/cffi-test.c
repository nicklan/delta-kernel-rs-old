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

int main(void) {
  iter_data it;
  it.lim = 10;
  it.cur = 0;

  EngineIterator *eit = create_iterator(&it, &next, &release);
  iterate(eit);
  
  return 0;
}
