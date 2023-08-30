#include <stdio.h>

#include <glib.h>
#include <arrow-glib/arrow-glib.h>
#include "arrow/c/abi.h"


struct ArrowArrayIterator;

typedef struct ArrowArrayAndSchema {
  struct ArrowArray array;
  struct ArrowSchema schema;
} ArrowArrayAndSchema_t;

extern struct ArrowArrayIterator* delta_scanner(const char*);
extern void* next_array(struct ArrowArrayIterator*);

static void
print_array(GArrowArray *array)
{
  GArrowType value_type;
  gint64 i, n;

  value_type = garrow_array_get_value_type(array);

  g_print("[");
  n = garrow_array_get_length(array);

#define ARRAY_CASE(type, Type, TYPE, format)                            \
  case GARROW_TYPE_ ## TYPE:                                            \
    {                                                                   \
      GArrow ## Type ## Array *real_array;                              \
      real_array = GARROW_ ## TYPE ## _ARRAY(array);                    \
      for (i = 0; i < n; i++) {                                         \
        if (i > 0) {                                                    \
          g_print(", ");                                                \
        }                                                               \
        g_print(format,                                                 \
                garrow_ ## type ## _array_get_value(real_array, i));    \
      }                                                                 \
    }                                                                   \
    break

  switch (value_type) {
    ARRAY_CASE(uint8,  UInt8,  UINT8,  "%hhu");
    ARRAY_CASE(uint16, UInt16, UINT16, "%" G_GUINT16_FORMAT);
    ARRAY_CASE(uint32, UInt32, UINT32, "%" G_GUINT32_FORMAT);
    ARRAY_CASE(uint64, UInt64, UINT64, "%" G_GUINT64_FORMAT);
    ARRAY_CASE( int8,   Int8,   INT8,  "%hhd");
    ARRAY_CASE( int16,  Int16,  INT16, "%" G_GINT16_FORMAT);
    ARRAY_CASE( int32,  Int32,  INT32, "%" G_GINT32_FORMAT);
    ARRAY_CASE( int64,  Int64,  INT64, "%" G_GINT64_FORMAT);
    ARRAY_CASE( float,  Float,  FLOAT, "%g");
    ARRAY_CASE(double, Double, DOUBLE, "%g");
  default:
    break;
  }
#undef ARRAY_CASE

  g_print("]\n");
}

static void
print_record_batch(GArrowRecordBatch *record_batch)
{
  guint nth_column, n_columns;

  n_columns = garrow_record_batch_get_n_columns(record_batch);
  for (nth_column = 0; nth_column < n_columns; nth_column++) {
    GArrowArray *array;

    g_print("column[%u](name: %s): ",
            nth_column,
            garrow_record_batch_get_column_name(record_batch, nth_column));
    array = garrow_record_batch_get_column_data(record_batch, nth_column);
    print_array(array);
    g_object_unref(array);
  }
}

int main(void) {
  struct ArrowArrayIterator* ret = delta_scanner("../../tests/data/table-without-dv-small");
  ArrowArrayAndSchema_t* n = next_array(ret);
  GError *err = NULL;
  while (n) {
    GArrowSchema* schema = garrow_schema_import(&n->schema, &err);
    if (err != NULL) {
      printf("Error converting schema: %s\n", err->message);
      g_error_free(err);
      return -1;
    }
    GArrowRecordBatch * batch = garrow_record_batch_import (&n->array, schema, &err);
    if (err != NULL) {
      printf("Error converting batch: %s\n", err->message);
      g_error_free(err);
      return -2;
    } 

    print_record_batch(batch);
    n = next_array(ret);
  }

  return 0;
}
