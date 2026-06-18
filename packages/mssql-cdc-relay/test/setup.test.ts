import { describe, expect, it } from "vitest";
import { createCdcEnablementSql } from "../src/setup.js";

describe("createCdcEnablementSql", () => {
  describe("default arguments", () => {
    const sql = createCdcEnablementSql();

    it("enables CDC at the database level", () => {
      expect(sql).toContain("EXEC sys.sp_cdc_enable_db");
    });

    it("enables CDC on the default dbo.outbox table with default capture instance", () => {
      expect(sql).toContain("EXEC sys.sp_cdc_enable_table");
      expect(sql).toMatch(/@source_schema\s*=\s*N'dbo'/);
      expect(sql).toMatch(/@source_name\s*=\s*N'outbox'/);
      expect(sql).toMatch(/@capture_instance\s*=\s*N'dbo_outbox'/);
    });

    it("references the default-named watermark table", () => {
      expect(sql).toContain("[dbo].[outbox_cdc_watermark]");
    });
  });

  describe("custom arguments", () => {
    const sql = createCdcEnablementSql({
      schema: "app",
      table: "event_outbox",
      captureInstance: "app_event_outbox",
    });

    it("interpolates custom schema in bracketed identifier and N'...' literal", () => {
      expect(sql).toContain("[app].");
      expect(sql).toMatch(/@source_schema\s*=\s*N'app'/);
    });

    it("interpolates custom table in bracketed identifier and N'...' literal", () => {
      expect(sql).toContain(".[event_outbox]");
      expect(sql).toMatch(/@source_name\s*=\s*N'event_outbox'/);
    });

    it("interpolates custom captureInstance into the sp_cdc_enable_table call", () => {
      expect(sql).toMatch(/@capture_instance\s*=\s*N'app_event_outbox'/);
    });

    it("respects explicit watermarkSchema + watermarkTable opts (not inferred from source)", () => {
      const customWatermark = createCdcEnablementSql({
        schema: "app",
        table: "event_outbox",
        captureInstance: "app_event_outbox",
        watermarkSchema: "app",
        watermarkTable: "event_outbox_cdc_watermark",
      });
      expect(customWatermark).toContain("[app].[event_outbox_cdc_watermark]");
    });
  });

  describe("Azure SQL Database guard", () => {
    const sql = createCdcEnablementSql();

    it("checks EngineEdition = 5 (Azure SQL Database)", () => {
      expect(sql).toMatch(/CAST\s*\(\s*SERVERPROPERTY\(\s*'EngineEdition'\s*\)\s*AS\s+int\s*\)\s*=\s*5/);
    });

    it("RAISERRORs and RETURNs on Azure SQL Database with a CDC-related message", () => {
      expect(sql).toMatch(/RAISERROR\s*\([^)]*CDC[^)]*\)/i);
      expect(sql).toMatch(/\bRETURN\b/);
    });
  });

  describe("assertIdent rejection", () => {
    it("rejects schema with quote/semicolon/DROP injection attempt", () => {
      expect(() =>
        createCdcEnablementSql({ schema: "\"); DROP TABLE" }),
      ).toThrow(TypeError);
    });

    it("rejects table with injection attempt", () => {
      expect(() =>
        createCdcEnablementSql({ table: "outbox\"); DROP TABLE--" }),
      ).toThrow(TypeError);
    });

    it("rejects captureInstance with injection attempt", () => {
      expect(() =>
        createCdcEnablementSql({ captureInstance: "x'); DROP TABLE" }),
      ).toThrow(TypeError);
    });

    it("rejects identifier starting with a digit", () => {
      expect(() => createCdcEnablementSql({ schema: "1bad" })).toThrow(
        TypeError,
      );
    });

    it("rejects identifier containing a space", () => {
      expect(() => createCdcEnablementSql({ table: "bad name" })).toThrow(
        TypeError,
      );
    });

    it("rejects empty identifier", () => {
      expect(() => createCdcEnablementSql({ schema: "" })).toThrow(TypeError);
    });
  });

  describe("watermark table DDL", () => {
    const sql = createCdcEnablementSql();

    it("creates the watermark table with the expected column shape", () => {
      expect(sql).toContain("CREATE TABLE [dbo].[outbox_cdc_watermark]");
      // Column can be declared as NVARCHAR(128) NOT NULL PRIMARY KEY (the
      // emitted shape) — match flexibly across word-order variants.
      expect(sql).toMatch(/capture_instance\s+NVARCHAR\(128\)[^,]*PRIMARY KEY/);
      expect(sql).toMatch(/last_processed_lsn\s+BINARY\(10\)\s+NOT NULL/);
    });
  });

  describe("idempotency", () => {
    const sql = createCdcEnablementSql();

    it("guards CDC database enablement (does not re-enable if already on)", () => {
      // is_cdc_enabled flag on sys.databases, checked before sp_cdc_enable_db.
      // The guard can be either `is_cdc_enabled = 1` (inside a NOT EXISTS) or
      // `is_cdc_enabled = 0` (inside a direct EXISTS). Match either shape.
      expect(sql).toMatch(/is_cdc_enabled\s*=\s*[01]/);
    });

    it("guards CDC table enablement (does not re-enable if capture instance already exists)", () => {
      // Either an IF NOT EXISTS against sys.cdc.change_tables / cdc.captured_columns
      // or a NOT IS_CDC_ENABLED-style probe on the source object.
      expect(sql).toMatch(/IF\s+NOT\s+EXISTS\s*\([^)]*cdc\.change_tables/i);
    });

    it("guards watermark table creation with IF OBJECT_ID(...) IS NULL", () => {
      expect(sql).toMatch(
        /IF\s+OBJECT_ID\(\s*N'\[dbo\]\.\[outbox_cdc_watermark\]'[^)]*\)\s+IS\s+NULL/i,
      );
    });

    it("every CREATE/EXEC is gated by an IF guard (no bare DDL)", () => {
      // Sanity heuristic: count CREATE TABLE + EXEC sp_cdc_enable_* statements
      // and ensure each is preceded by an IF guard somewhere in the block.
      const createCount = (sql.match(/\bCREATE\s+TABLE\b/gi) ?? []).length;
      const execCount = (sql.match(/\bEXEC\s+sys\.sp_cdc_enable_/gi) ?? [])
        .length;
      const ifGuardCount = (sql.match(/\bIF\s+(NOT\s+)?(EXISTS|OBJECT_ID|CAST)/gi) ?? [])
        .length;
      // At least one guard per guarded statement (db enable + table enable + watermark).
      expect(ifGuardCount).toBeGreaterThanOrEqual(createCount + execCount);
    });
  });
});
