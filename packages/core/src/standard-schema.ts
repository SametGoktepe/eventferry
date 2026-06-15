/**
 * The Standard Schema v1 interface, inlined verbatim from the spec
 * (https://github.com/standard-schema/standard-schema, MIT). Inlining keeps
 * `@eventferry/core` dependency-free while accepting any compliant validator
 * (Zod 3.24+, Valibot, ArkType, …) — the spec is explicitly meant to be copied.
 */
export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": StandardSchemaV1.Props<Input, Output>;
}

// eslint-disable-next-line @typescript-eslint/no-namespace
export declare namespace StandardSchemaV1 {
  /** The properties carried under the `~standard` key. */
  export interface Props<Input = unknown, Output = Input> {
    readonly version: 1;
    /** The vendor name of the schema library (e.g. "zod"). */
    readonly vendor: string;
    /** Validate (and optionally transform) an unknown input. May be async. */
    readonly validate: (
      value: unknown,
    ) => Result<Output> | Promise<Result<Output>>;
    /** Inferred input/output types. Phantom — never present at runtime. */
    readonly types?: Types<Input, Output> | undefined;
  }

  export type Result<Output> = SuccessResult<Output> | FailureResult;

  export interface SuccessResult<Output> {
    readonly value: Output;
    readonly issues?: undefined;
  }

  export interface FailureResult {
    readonly issues: ReadonlyArray<Issue>;
  }

  export interface Issue {
    readonly message: string;
    readonly path?: ReadonlyArray<PropertyKey | PathSegment> | undefined;
  }

  export interface PathSegment {
    readonly key: PropertyKey;
  }

  export interface Types<Input = unknown, Output = Input> {
    readonly input: Input;
    readonly output: Output;
  }

  export type InferInput<Schema extends StandardSchemaV1> = NonNullable<
    Schema["~standard"]["types"]
  >["input"];

  export type InferOutput<Schema extends StandardSchemaV1> = NonNullable<
    Schema["~standard"]["types"]
  >["output"];
}
