# ddl

Table definitions and migration scripts live here.

- Canonical tables that must always exist are declared in
  `data_platform.ddl.iceberg_tables.DEFAULT_TABLE_SPECS`.
- Formal and Analytical namespaces are always ensured in the Iceberg catalog, but
  most tables there are registered at runtime with `TableSpec` + `register_table`
  when a concrete object type is introduced or published.
- Raw artifacts stay outside the Iceberg catalog and must not be modeled as Iceberg
  tables.
