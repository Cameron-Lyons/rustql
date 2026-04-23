# RustQL v1 Type Semantics Contract

This contract defines the v1 behavior for casts, comparisons, ordering, temporal
values, and floating-point edge cases. It applies to the public engine/session
API, planner-backed execution, and typed table writes.

## NULL

- `NULL` casts to `NULL` for every target type.
- Binary comparison operators with a `NULL` operand evaluate to false in filters.
- Use `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, or `IS NOT DISTINCT FROM`
  for null-aware predicates.
- `ORDER BY` treats `NULL` as higher than every non-null value. Ascending order
  places nulls last; descending order places nulls first.

## Casts

- `INTEGER`
  - `INTEGER` values are unchanged.
  - Finite `FLOAT` values truncate toward zero and must be in the `i64` range.
  - `TEXT` must be a trimmed base-10 integer.
  - `BOOLEAN` casts to `1` for true and `0` for false.
- `FLOAT`
  - `FLOAT` values are unchanged if finite.
  - `INTEGER` values cast to finite `f64` values.
  - `TEXT` must parse as a finite float. `NaN`, `inf`, and infinities are
    rejected.
- `TEXT`
  - Any non-null value casts through the same display form returned in result
    rows.
- `BOOLEAN`
  - `BOOLEAN` values are unchanged.
  - `INTEGER` values cast to false for `0` and true for any other value.
  - Trimmed text accepts `true`, `1`, `yes`, `false`, `0`, and `no`
    case-insensitively.
- `DATE`, `TIME`, and `DATETIME`
  - Text input must use canonical fixed-width formats:
    `YYYY-MM-DD`, `HH:MM:SS`, and `YYYY-MM-DD HH:MM:SS`.
  - Calendar dates are validated, including leap-year rules.
  - `DATE` from `DATETIME` keeps the date part.
  - `TIME` from `DATETIME` keeps the time part.
  - `DATETIME` from `DATE` appends `00:00:00`.

## Comparisons

- `INTEGER` and `FLOAT` are numeric-compatible and compare in the floating-point
  domain.
- Numeric equality uses RustQL's existing epsilon check for finite values, so
  small binary floating-point representation differences compare equal.
- Text compares lexicographically.
- Booleans support equality and inequality only.
- `DATE`, `TIME`, and `DATETIME` compare within their own type. Because values
  are normalized to fixed-width canonical strings, this is chronological order.
- Mixed nonnumeric comparisons are type errors. RustQL does not implicitly parse
  text as numbers, booleans, or temporal values in comparison predicates.

## Sort Order

`ORDER BY` uses a total value order so dynamic expressions are deterministic:

1. numeric values (`INTEGER` and `FLOAT`, ordered by numeric value)
2. `TEXT`
3. `BOOLEAN` (`false` before `true`)
4. `DATE`
5. `TIME`
6. `DATETIME`
7. `NULL`

Ascending sort uses this order directly; descending sort reverses it. Values
that compare equal, such as `1` and `1.0`, have no guaranteed relative order
unless the query supplies an additional sort key.

## Temporal Normalization

Typed temporal writes and casts validate and store only canonical values. Date
values have no timezone. Time values have second precision. Datetime values use
a single space between date and time. Invalid calendar dates, non-canonical
times, and date-only text casts to `DATETIME` are rejected.

## Float Edge Cases

- Public casts, typed writes, and float literals accept only finite floats.
- Text casts of `NaN`, `inf`, `+inf`, `-inf`, and infinity spellings are errors.
- Float-to-integer casts reject non-finite and out-of-range values.
- Division by zero is an error.
- Result rendering uses Rust's `f64` display form, so `42.0` renders as `42`.
