#include <string.h>
#include "duckdb/main/capi_internal.hpp"

void close(duckdb_database *db) { duckdb_close(db); }
void disconnect(duckdb_connection *con) { duckdb_disconnect(con); }
void free_blob(duckdb_blob *blob) { duckdb_free(blob->data); free(blob); }
void free_result(duckdb_result *result) { duckdb_destroy_result(result); free(result); }
void free_prepare(duckdb_prepared_statement *prepare) { duckdb_destroy_prepare(prepare); }

duckdb_prepared_statement prepare(duckdb_connection* con, const char* query) {
  duckdb_prepared_statement p; duckdb_prepare(con, query, &p); return p;
}

duckdb_connection connect(duckdb_database* db) {
  duckdb_connection con; if (DuckDBError == duckdb_connect(db, &con)) return 0; return con;
}

duckdb_database open(bool in_memory, const char* path) {
  duckdb_database db; if (DuckDBError == duckdb_open(in_memory ? NULL : path, &db)) return 0; return db;
}

duckdb_result* query(duckdb_connection* con, const char* query) {
  duckdb_result *res = (duckdb_result*)malloc(sizeof(duckdb_result)); duckdb_query(con, query, res); return res;
}

duckdb_result* query_prepared(duckdb_prepared_statement* prepare) {
  duckdb_result *res = (duckdb_result*)malloc(sizeof(duckdb_result)); duckdb_execute_prepared(prepare, res); return res;
}

void* blob_data(duckdb_blob* blob) { return blob->data; }
uint32_t blob_size(duckdb_blob* blob) { return blob->size; }
uint32_t row_count(duckdb_result* result) { return duckdb_row_count(result); }
uint64_t row_count_slow(duckdb_result* result) { return duckdb_row_count(result); }
uint32_t column_count(duckdb_result* result) { return duckdb_column_count(result); }
// uint32_t rows_changed(duckdb_result *result) { return duckdb_rows_changed(result); }
const char* result_error(duckdb_result* result) { return duckdb_result_error(result); }
uint32_t param_count(duckdb_prepared_statement* prepare) { return duckdb_nparams(prepare); }
bool row_count_large(duckdb_result* result) { return 4294967296 < duckdb_row_count(result); }
const char* prepare_error(duckdb_prepared_statement* prepare) { return duckdb_prepare_error(prepare); }
uint32_t column_type(duckdb_result* result, uint32_t offset) { return duckdb_column_type(result, offset); }
const char* column_name(duckdb_result* result, uint32_t offset) { return duckdb_column_name(result, offset); }
uint32_t param_type(duckdb_prepared_statement* prepare, uint32_t offset) { return duckdb_param_type(prepare, 1 + offset); }

bool bind_null(duckdb_prepared_statement* prepare, uint32_t offset) { return DuckDBError == duckdb_bind_null(prepare, 1 + offset); }
bool bind_i8(duckdb_prepared_statement* prepare, uint32_t offset, int8_t value) { return DuckDBError == duckdb_bind_int8(prepare, 1 + offset, value); }
bool bind_f32(duckdb_prepared_statement* prepare, uint32_t offset, float value) { return DuckDBError == duckdb_bind_float(prepare, 1 + offset, value); }
bool bind_u8(duckdb_prepared_statement* prepare, uint32_t offset, uint8_t value) { return DuckDBError == duckdb_bind_uint8(prepare, 1 + offset, value); }
bool bind_i16(duckdb_prepared_statement* prepare, uint32_t offset, int16_t value) { return DuckDBError == duckdb_bind_int16(prepare, 1 + offset, value); }
bool bind_i32(duckdb_prepared_statement* prepare, uint32_t offset, int32_t value) { return DuckDBError == duckdb_bind_int32(prepare, 1 + offset, value); }
bool bind_i64(duckdb_prepared_statement* prepare, uint32_t offset, int64_t value) { return DuckDBError == duckdb_bind_int64(prepare, 1 + offset, value); }
bool bind_f64(duckdb_prepared_statement* prepare, uint32_t offset, double value) { return DuckDBError == duckdb_bind_double(prepare, 1 + offset, value); }
bool bind_u16(duckdb_prepared_statement* prepare, uint32_t offset, uint16_t value) { return DuckDBError == duckdb_bind_uint16(prepare, 1 + offset, value); }
bool bind_u32(duckdb_prepared_statement* prepare, uint32_t offset, uint32_t value) { return DuckDBError == duckdb_bind_uint32(prepare, 1 + offset, value); }
bool bind_u64(duckdb_prepared_statement* prepare, uint32_t offset, uint64_t value) { return DuckDBError == duckdb_bind_uint64(prepare, 1 + offset, value); }
bool bind_boolean(duckdb_prepared_statement* prepare, uint32_t offset, bool value) { return DuckDBError == duckdb_bind_boolean(prepare, 1 + offset, value); }
bool bind_blob(duckdb_prepared_statement* prepare, uint32_t offset, const void* value, uint32_t length) { return DuckDBError == duckdb_bind_blob(prepare, 1 + offset, value, length); }
bool bind_string(duckdb_prepared_statement* prepare, uint32_t offset, const char* value, uint32_t length) { return DuckDBError == duckdb_bind_varchar_length(prepare, 1 + offset, value, length); }
bool bind_timestamp(duckdb_prepared_statement* prepare, uint32_t offset, uint64_t value) { duckdb_timestamp timestamp = { .micros = 1000 * value }; return DuckDBError == duckdb_bind_timestamp(prepare, 1 + offset, timestamp); }
bool bind_interval(duckdb_prepared_statement* prepare, uint32_t offset, uint32_t ms, uint32_t days, uint32_t months) { duckdb_interval interval = { .days = days, .months = months, .micros = ms * 1000 }; return DuckDBError == duckdb_bind_interval(prepare, 1 + offset, interval); }

int8_t value_i8(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int8(result, column, row); }
float value_f32(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_float(result, column, row); }
uint8_t value_u8(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint8(result, column, row); }
double value_f64(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_double(result, column, row); }
int16_t value_i16(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int16(result, column, row); }
int32_t value_i32(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int32(result, column, row); }
int64_t value_i64(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_int64(result, column, row); }
uint16_t value_u16(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint16(result, column, row); }
uint32_t value_u32(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint32(result, column, row); }
uint64_t value_u64(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_uint64(result, column, row); }
bool value_boolean(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_boolean(result, column, row); }
bool value_is_null(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_is_null(result, column, row); }
uint32_t value_date(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_date(result, column, row).days; }
char* value_string(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_varchar_internal(result, column, row); }
uint32_t value_time(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_time(result, column, row).micros / 1000; }
uint32_t value_interval_months(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_interval(result, column, row).months; }
uint64_t value_timestamp_ms(duckdb_result* result, uint32_t row, uint32_t column) { return duckdb_value_timestamp(result, column, row).micros / 1000; }
uint32_t value_interval_days(duckdb_result* result, uint32_t row, uint32_t column) { duckdb_interval interval = duckdb_value_interval(result, column, row); return interval.micros / 1000 + 24 * 60 * 60000 * interval.days; }

int8_t value_i8_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int8(result, column, row); }
float value_f32_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_float(result, column, row); }
uint8_t value_u8_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint8(result, column, row); }
double value_f64_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_double(result, column, row); }
int16_t value_i16_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int16(result, column, row); }
int32_t value_i32_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int32(result, column, row); }
int64_t value_i64_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_int64(result, column, row); }
uint16_t value_u16_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint16(result, column, row); }
uint32_t value_u32_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint32(result, column, row); }
uint64_t value_u64_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_uint64(result, column, row); }
bool value_boolean_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_boolean(result, column, row); }
bool value_is_null_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_is_null(result, column, row); }
uint32_t value_date_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_date(result, column, row).days; }
char* value_string_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_varchar_internal(result, column, row); }
uint32_t value_time_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_time(result, column, row).micros / 1000; }
uint32_t value_interval_months_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_interval(result, column, row).months; }
uint64_t value_timestamp_ms_slow(duckdb_result* result, uint64_t row, uint32_t column) { return duckdb_value_timestamp(result, column, row).micros / 1000; }
uint32_t value_interval_days_slow(duckdb_result* result, uint64_t row, uint32_t column) { duckdb_interval interval = duckdb_value_interval(result, column, row); return interval.micros / 1000 + 24 * 60 * 60000 * interval.days; }

duckdb_blob* value_blob(duckdb_result* result, uint32_t row, uint32_t column) {
  duckdb_blob stack = duckdb_value_blob(result, column, row);
  duckdb_blob *blob = (duckdb_blob*)malloc(sizeof(duckdb_blob));

  memcpy(blob, &stack, sizeof(duckdb_blob));

  return blob;
}

duckdb_blob* value_blob_slow(duckdb_result* result, uint64_t row, uint32_t column) {
  duckdb_blob stack = duckdb_value_blob(result, column, row);
  duckdb_blob *blob = (duckdb_blob*)malloc(sizeof(duckdb_blob));

  memcpy(blob, &stack, sizeof(duckdb_blob));

  return blob;
}