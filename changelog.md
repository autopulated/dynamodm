### 1.5.1
 * Fix temporary files included in published package.

## 1.5.0
 * Support for projected attributes in indexes, and constructing partial models
   from only projected attributes with the 'onlyProjected' option to queries.
 * Models are constructed directly from indexes that project all attributes,
   without an additional request to the table.
 * The type index is no longer automatically created if there is only a single
   model type.
 * The type field is no longer automatically required by every schema.

### 1.4.2
 * update dependencies

### 1.4.1
 * documentation updates and additional test coverage

## 1.4.0
 * Support queryMany with limit > 100, by internally batching requests.
 * Better errors for invalid abortSignal option values.
 * Fixed error message for invalid index specification.

### 1.3.2
 * Fixes compatibility of Schemas with tables from different dynamodm module instances.

### 1.3.1
 * Fixes removal of unversioned documents via a versioned model.

## 1.3.0
 * Adds support for model versioning, with associated schema fragment to name
   the version field. To disable versioning pass options.versioning: false to
   the Schema constructor.

## 1.2.0
 * Support $between and $begins query operators
 * Improved error messages for unsupported queries

## 1.1.0
 * Support options.retry in Table()
 * Support table name as an argument to Table(), in addition to options.name
 * readme updates

### 1.0.3
 * readme and repository URL updates

### 1.0.2
 * Fix asynchronous promise handling in queryMany.
 * Removed remains of deprecated query APIs from earlier development.

### 1.0.1
 * Updated readme and example code.

# 1.0.0
Initial public release.
