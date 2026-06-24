# Rust API Guidelines for Cline

Purpose: keep Rust public APIs idiomatic, predictable, interoperable, and stable.
Source basis: https://rust-lang.github.io/api-guidelines/

Use this file as a compact rule set for code generation and review. Prefer these rules for public crate APIs, library boundaries, exported traits, exported structs, public modules, and public error types. For private implementation code, follow the same style unless it hurts clarity.

## Core behavior

- Generate simple, boring, idiomatic Rust first.
- Prefer standard library conventions over clever custom patterns.
- Do not invent API patterns when the standard ecosystem already has one.
- Do not optimize API shape for micro-performance unless the user explicitly asks or benchmarks prove it.
- Public API stability matters more than internal convenience.
- When changing public API, preserve backward compatibility unless the task is explicitly a breaking change.

## 1. Naming

MUST:

- Use Rust casing:
  - crates/modules/functions/methods/variables: `snake_case`
  - types/traits/enums/variants: `UpperCamelCase`
  - constants/statics: `SCREAMING_SNAKE_CASE`
  - lifetimes: short lowercase, usually `'a`, `'de`, etc.
- Use conversion method prefixes consistently:
  - `as_*`: cheap borrowed view, no allocation, no ownership transfer
  - `to_*`: creates owned value, may allocate or copy
  - `into_*`: consumes `self`
- Getter methods should be named after the field: `name()`, not `get_name()`.
- Mutable getters should be explicit: `name_mut()`.
- Collection iteration methods:
  - `iter(&self)`
  - `iter_mut(&mut self)`
  - `into_iter(self)`
- Iterator types should match their producing method, e.g. `Iter`, `IterMut`, `IntoIter`.
- Feature names must be meaningful. Avoid `use-foo`, `with-bar`, `default2`, `unstable-stuff`.
- Keep word order consistent: if using `read_file`, also use `write_file`, not `file_write`.

AVOID:

- Java/C# style getters: `get_*` unless the operation is not a plain field-like getter.
- Abbreviations unless common in Rust/domain context.
- Names that encode implementation details that may change.

## 2. Interoperability

MUST:

- Implement common traits where semantically correct:
  - `Debug`
  - `Clone`
  - `Copy` only for small value-like types where copying is unsurprising
  - `PartialEq`, `Eq`
  - `PartialOrd`, `Ord`
  - `Hash`
  - `Default`
  - `Display` for user-facing formatting
- Prefer standard conversion traits:
  - `From<T>` for infallible conversions
  - `TryFrom<T>` for fallible conversions
  - `AsRef<T>` / `AsMut<T>` for cheap reference conversion
- If implementing `Into<T>`, usually implement `From<Self> for T` instead.
- Collections should implement `FromIterator` and `Extend` when useful.
- Public data types should support `serde::Serialize` / `Deserialize` when the crate is data-oriented and serde is acceptable.
- Public types should be `Send` and `Sync` where possible. Do not block auto traits accidentally with unnecessary `Rc`, `RefCell`, raw pointers, or non-thread-safe internals.
- Reader/writer APIs should usually accept generic values:
  - `fn read_from<R: std::io::Read>(reader: R)`
  - `fn write_to<W: std::io::Write>(writer: W)`

SHOULD:

- Prefer ecosystem-standard traits over custom traits.
- Keep trait implementations unsurprising and mathematically consistent.

## 3. Error handling

MUST:

- Use `Result<T, E>` for recoverable errors.
- Do not return strings as errors from public APIs.
- Public error types must implement:
  - `Debug`
  - `Display`
  - `std::error::Error` when appropriate
- Error variants must be specific enough for callers to handle.
- Preserve lower-level source errors when useful via `source()`.
- Document all normal error cases in `# Errors`.

SHOULD:

- Use an enum error type for library APIs when callers need to match cases.
- Use `thiserror` for library error definitions if dependencies are acceptable.
- Use `anyhow` only in applications, tests, examples, or internal glue, not as the main public library error type.

AVOID:

- `unwrap()`, `expect()`, and panics in library code except for impossible internal invariant violations.
- Catch-all `Other(String)` variants unless there is a real forward-compatibility reason.

## 4. Documentation

MUST:

- Every public item should have useful rustdoc.
- Crate-level docs must explain what the crate does and show a basic example.
- Important public methods should include examples.
- Examples should compile where possible.
- Examples should use `?` for error propagation, not `unwrap()`.
- Document these sections when applicable:
  - `# Errors`
  - `# Panics`
  - `# Safety`
- Unsafe functions must have a precise `# Safety` section.
- Link related types and methods using rustdoc intra-doc links: [`Type`], [`method`].
- Hide irrelevant implementation details from docs with `#[doc(hidden)]` only when appropriate.

SHOULD:

- Keep examples short and realistic.
- Show the common path first, advanced use later.
- Document feature flags.
- Keep `Cargo.toml` metadata complete: description, license, repository, documentation, keywords, categories.
- Maintain release notes for significant public changes.

## 5. Predictability

MUST:

- If a function has a natural receiver, make it a method.
- Constructors should be inherent associated functions, usually `new`, `with_*`, or `from_*`.
- Do not use out-parameters. Return values directly, usually tuples or structs.
- Operator overloads must behave like the operator normally implies.
- Implement `Deref` / `DerefMut` only for smart-pointer-like types.
- Smart pointer types should avoid inherent methods that could conflict with target methods.
- Put conversions on the most specific involved type.

AVOID:

- Surprising side effects in methods that look like simple accessors.
- Hidden allocation in methods named `as_*`.
- Hidden mutation in methods that do not require `&mut self` unless interior mutability is the core abstraction.

## 6. Flexibility

MUST:

- Let callers control allocation and ownership where reasonable.
- Prefer borrowing parameters when ownership is not needed:
  - `&str` instead of `String`
  - `&[T]` instead of `Vec<T>`
  - `impl AsRef<Path>` for path-like inputs
- Use generics to avoid unnecessary assumptions about concrete types.
- Expose intermediate results when it prevents duplicate expensive work.
- Traits intended for dynamic dispatch should be object-safe.

SHOULD:

- Accept `impl IntoIterator<Item = T>` when the API only needs iteration.
- Return concrete iterator types or `impl Iterator` where it keeps the API clean.
- Avoid forcing callers into a specific collection type.

## 7. Type safety

MUST:

- Use newtypes to distinguish values that share the same primitive representation but have different meaning.
- Avoid boolean parameters in public APIs when the meaning is not obvious.
- Replace ambiguous `bool` or `Option` parameters with named enums or config structs.
- Use builders for complex construction with many optional parameters.
- Use `bitflags`-style flags for combinable flags, not ad-hoc integer masks.

SHOULD:

- Encode invariants in types instead of runtime comments.
- Prefer non-empty/domain-specific validated types when invalid states matter.

AVOID:

- `fn configure(true, false, None)` style APIs.
- Exposing raw IDs as plain `u64`/`String` when mixing them up would be easy.

## 8. Dependability

MUST:

- Validate public function arguments.
- Make invalid states impossible where practical.
- Destructors must not fail.
- Destructors should not block unexpectedly. If cleanup may block or fail, provide an explicit `close`, `flush`, or `shutdown` method returning `Result`.
- Public APIs must clearly define what happens on invalid input.

SHOULD:

- Keep panic behavior explicit and documented.
- Make resource ownership and cleanup obvious.

## 9. Debuggability

MUST:

- All public types should implement `Debug`.
- `Debug` output must contain useful information. It must not be empty.
- Do not leak secrets in `Debug` output.

SHOULD:

- Redact tokens, passwords, private keys, and credentials.
- Include enough state to diagnose common problems.

## 10. Future-proofing

MUST:

- Keep public struct fields private unless direct field access is a deliberate API commitment.
- Use constructors, getters, and builders to preserve freedom to change internals.
- Use `#[non_exhaustive]` on public enums/structs when future variants/fields are likely.
- Seal traits that should not be implemented by downstream users.
- Do not put unnecessary trait bounds on struct definitions. Put bounds on impls/methods instead.
- Hide implementation details behind newtypes or private modules.

SHOULD:

- Avoid exposing unstable dependency types in public API.
- Think before exposing generic parameters publicly; they become part of the API contract.

## 11. Macros

MUST:

- Macro syntax should resemble the code it expands to.
- Item macros should work anywhere items are allowed.
- Item macros should support visibility modifiers such as `pub`.
- Macros should compose with attributes where possible.
- Macro inputs should accept flexible type/path fragments where users expect normal Rust syntax.

SHOULD:

- Prefer normal functions, traits, and derives before writing custom macros.
- Document macro examples clearly.

## 12. Dependencies and crate metadata

MUST:

- Stable public crates should avoid exposing unstable or experimental dependency APIs.
- Public dependencies are part of the API contract; choose them carefully.
- License must be clear and compatible with normal Rust ecosystem use.
- Cargo metadata should be complete enough for crates.io and docs.rs.

SHOULD:

- Keep public dependency surface small.
- Avoid unnecessary feature flag complexity.
- Make default features reasonable and minimal.

## 13. Code generation rules for Cline

When generating or modifying Rust API code:

1. First design the public surface: types, traits, constructors, errors, and ownership.
2. Then implement internals.
3. Add rustdoc for all public items.
4. Add tests or doctests for public behavior.
5. Run or suggest:
   - `cargo fmt`
   - `cargo clippy --all-targets --all-features`
   - `cargo test --all-features`
6. Prefer clear APIs over clever abstractions.
7. Do not introduce performance-specialized containers (`SmallVec`, arenas, compact strings, etc.) unless asked or justified.
8. Do not change public API casually. If a change is breaking, say so explicitly.
9. If unsure, choose the more standard Rust ecosystem convention.

## 14. Public API review checklist

Before finalizing Rust library code, verify:

- Names follow Rust conventions.
- Ownership and borrowing are natural.
- Constructors are clear.
- Errors are typed, documented, and useful.
- Public items have rustdoc.
- Examples avoid `unwrap()` where realistic error propagation is better.
- Public types implement useful common traits.
- `Debug` is implemented and safe.
- No ambiguous bool/config parameters are exposed.
- No unnecessary concrete collection types are required.
- No public fields expose internals accidentally.
- Trait object use is considered where relevant.
- `Deref` is used only for smart-pointer-like behavior.
- Destructors do not fail or unexpectedly block.
- Future extension will not require avoidable breaking changes.
- Public dependencies and feature flags are intentional.

## 15. Priority order

If rules conflict, use this order:

1. Correctness and safety.
2. Clear public API semantics.
3. Compatibility with Rust ecosystem conventions.
4. Backward compatibility.
5. Simplicity.
6. Performance, only when measured or explicitly required.

