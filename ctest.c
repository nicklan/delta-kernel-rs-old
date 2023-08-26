#include <stdio.h>

struct ArrowArrayIterator;

extern struct ArrowArrayIterator* delta_scanner(const char*);
extern next_array(struct ArrowArrayIterator*);

int main(void) {
  struct ArrowArrayIterator* ret = delta_scanner("tests/data/table-with-dv-small");
  next_array(ret);
  return 0;
}
