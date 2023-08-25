#include <stdio.h>

struct RecordBatchIterator;

extern struct RecordBatchIterator* delta_scanner(char*);
extern next_batch(struct RecordBatchIterator*);

int main(void) {
  struct RecordBatchIterator* ret = delta_scanner("tests/data/table-with-dv-small");
  next_batch(ret);
  return 0;
}
